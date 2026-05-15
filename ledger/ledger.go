package ledger

import (
	"errors"
	"sync"

	"github.com/aliraad79/Gun/models"
)

// Errors returned by the ledger. Stable strings; callers can match on
// them via errors.Is.
var (
	ErrInsufficientFunds = errors.New("ledger: insufficient available balance")
	ErrHoldNotFound      = errors.New("ledger: hold not found")
	ErrUnknownAccount    = errors.New("ledger: unknown account")
	ErrNegativeAmount    = errors.New("ledger: amount must be positive")
	ErrAlreadySettled    = errors.New("ledger: hold already consumed")
)

// numShards is a power of two so the (user_id & mask) shard lookup is
// a single AND. 256 shards is enough to keep contention low up to
// millions of users on a 16-core machine; the wasted bytes are
// negligible (256 small struct pointers).
const numShards = 256
const shardMask = numShards - 1

// shard holds one slice of users plus its mutex.
type shard struct {
	mu       sync.RWMutex
	accounts map[int64]map[string]*Account // user -> asset -> account
}

// holdRecord tracks one reserved chunk of funds. Settled / released
// records stay in the map briefly so duplicate settlement attempts
// (idempotency) return AlreadySettled rather than HoldNotFound, then
// get GC'd by a periodic sweep (out of scope for this revision —
// for now the map grows; in production we GC).
type holdRecord struct {
	UserID   int64
	Asset    string
	Amount   models.Qty
	State    holdState
}

type holdState uint8

const (
	holdActive holdState = iota
	holdReleased
	holdSettled
)

// Ledger is the public face of the in-memory hot ledger. Safe for
// concurrent use across goroutines.
type Ledger struct {
	shards [numShards]*shard

	holdsMu sync.RWMutex
	holds   map[int64]*holdRecord // order_id -> hold record
}

// New returns a freshly initialized ledger.
func New() *Ledger {
	l := &Ledger{
		holds: make(map[int64]*holdRecord, 1<<14),
	}
	for i := range l.shards {
		l.shards[i] = &shard{accounts: make(map[int64]map[string]*Account)}
	}
	return l
}

// shardFor returns the shard owning the given user. Pure function of
// user_id; never blocks.
func (l *Ledger) shardFor(userID int64) *shard {
	return l.shards[userID&shardMask]
}

// Deposit credits an account. Used by the deposit-confirmation pipeline
// and by tests to set up initial state. No hold semantics — straight
// balance increase.
func (l *Ledger) Deposit(userID int64, asset string, amount models.Qty) error {
	if !amount.IsPositive() {
		return ErrNegativeAmount
	}
	s := l.shardFor(userID)
	s.mu.Lock()
	defer s.mu.Unlock()

	acc := l.getOrCreate(s, userID, asset)
	acc.Balance = acc.Balance.Add(amount)
	acc.Version++
	return nil
}

// Withdraw debits an account. Fails if Available < amount. Used by
// the withdrawal pipeline; the hold for an open order goes through
// Hold instead.
func (l *Ledger) Withdraw(userID int64, asset string, amount models.Qty) error {
	if !amount.IsPositive() {
		return ErrNegativeAmount
	}
	s := l.shardFor(userID)
	s.mu.Lock()
	defer s.mu.Unlock()

	acc, ok := l.lookup(s, userID, asset)
	if !ok {
		return ErrUnknownAccount
	}
	if acc.Available().Lt(amount) {
		return ErrInsufficientFunds
	}
	acc.Balance = acc.Balance.Sub(amount)
	acc.Version++
	return nil
}

// Hold reserves `amount` of `asset` for `userID`, keyed by `orderID`.
// Returns ErrInsufficientFunds when Available < amount. The hold is
// guaranteed to be released exactly once — by Release (cancel path) or
// Settle (match path).
//
// Idempotency: a second Hold call with the same orderID is a no-op
// returning nil. (Producers retry on timeout; we must not double-hold.)
func (l *Ledger) Hold(orderID int64, userID int64, asset string, amount models.Qty) error {
	if !amount.IsPositive() {
		return ErrNegativeAmount
	}

	l.holdsMu.RLock()
	if existing, ok := l.holds[orderID]; ok && existing.State == holdActive {
		l.holdsMu.RUnlock()
		// idempotent: same hold already exists
		if existing.UserID == userID && existing.Asset == asset && existing.Amount.Eq(amount) {
			return nil
		}
		return errors.New("ledger: hold orderID collision with different params")
	}
	l.holdsMu.RUnlock()

	s := l.shardFor(userID)
	s.mu.Lock()
	acc, ok := l.lookup(s, userID, asset)
	if !ok || acc.Available().Lt(amount) {
		s.mu.Unlock()
		return ErrInsufficientFunds
	}
	acc.Held = acc.Held.Add(amount)
	acc.Version++
	s.mu.Unlock()

	l.holdsMu.Lock()
	l.holds[orderID] = &holdRecord{
		UserID: userID, Asset: asset, Amount: amount, State: holdActive,
	}
	l.holdsMu.Unlock()
	return nil
}

// Release returns a hold's funds to Available. Used on cancel and on
// engine-rejection paths.
//
// Idempotency: releasing an already-released or already-settled hold
// returns nil (no-op). Releasing an unknown orderID returns
// ErrHoldNotFound.
func (l *Ledger) Release(orderID int64) error {
	l.holdsMu.Lock()
	h, ok := l.holds[orderID]
	if !ok {
		l.holdsMu.Unlock()
		return ErrHoldNotFound
	}
	if h.State != holdActive {
		l.holdsMu.Unlock()
		return nil // idempotent no-op
	}
	h.State = holdReleased
	l.holdsMu.Unlock()

	s := l.shardFor(h.UserID)
	s.mu.Lock()
	acc, ok := l.lookup(s, h.UserID, h.Asset)
	if ok {
		acc.Held = acc.Held.Sub(h.Amount)
		acc.Version++
	}
	s.mu.Unlock()
	return nil
}

// SettleFill consumes `fillAmount` of `orderID`'s hold and moves
// `fillAmount` of asset to the counterparty's balance — but does NOT
// release the remainder of the hold. The caller (clearing.Settle)
// calls this once per side of a Match.
//
// A SettleFill against an unknown order returns ErrHoldNotFound (the
// risk service should have created the hold before the order reached
// the engine).
func (l *Ledger) SettleFill(orderID int64, takerUserID int64, takerAsset string, fillAmount models.Qty) error {
	if !fillAmount.IsPositive() {
		return ErrNegativeAmount
	}

	l.holdsMu.RLock()
	h, ok := l.holds[orderID]
	l.holdsMu.RUnlock()
	if !ok {
		return ErrHoldNotFound
	}
	if h.State != holdActive {
		// Already released by a cancel, or already fully consumed by an
		// earlier settle. Either way the next caller must not double-spend.
		return ErrAlreadySettled
	}
	if fillAmount.Gt(h.Amount) {
		return errors.New("ledger: fill exceeds remaining hold")
	}

	// Two-account update touching holder's shard and taker's shard.
	// Lock order = ascending user_id to avoid deadlocks when concurrent
	// settlements touch the same pair in opposite directions. When
	// both users live on the same shard we only lock once.
	holderShard := l.shardFor(h.UserID)
	takerShard := l.shardFor(takerUserID)

	firstShard, secondShard := holderShard, takerShard
	if h.UserID > takerUserID {
		firstShard, secondShard = takerShard, holderShard
	}
	firstShard.mu.Lock()
	if firstShard != secondShard {
		secondShard.mu.Lock()
	}

	holderAcc, _ := l.lookup(holderShard, h.UserID, h.Asset)
	if holderAcc == nil {
		if firstShard != secondShard {
			secondShard.mu.Unlock()
		}
		firstShard.mu.Unlock()
		return ErrUnknownAccount
	}
	takerAcc := l.getOrCreate(takerShard, takerUserID, takerAsset)

	// Holder: balance and held both decrease by fillAmount.
	holderAcc.Balance = holderAcc.Balance.Sub(fillAmount)
	holderAcc.Held = holderAcc.Held.Sub(fillAmount)
	holderAcc.Version++

	// Taker: balance increases by fillAmount of takerAsset. Note the
	// asset can differ from h.Asset — caller is moving the holder's
	// asset onto the taker, who may be receiving a different asset on
	// the matching leg (base ⇄ quote).
	takerAcc.Balance = takerAcc.Balance.Add(fillAmount)
	takerAcc.Version++

	if firstShard != secondShard {
		secondShard.mu.Unlock()
	}
	firstShard.mu.Unlock()

	// Decrement the hold's remaining amount. If it goes to zero, mark
	// the hold settled so Release on it is a no-op.
	l.holdsMu.Lock()
	h.Amount = h.Amount.Sub(fillAmount)
	if h.Amount.IsZero() {
		h.State = holdSettled
	}
	l.holdsMu.Unlock()
	return nil
}

// Balance and Held queries (read-only).
func (l *Ledger) Balance(userID int64, asset string) models.Qty {
	s := l.shardFor(userID)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if acc, ok := l.lookup(s, userID, asset); ok {
		return acc.Balance
	}
	return models.ZeroQty
}

func (l *Ledger) HeldOf(userID int64, asset string) models.Qty {
	s := l.shardFor(userID)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if acc, ok := l.lookup(s, userID, asset); ok {
		return acc.Held
	}
	return models.ZeroQty
}

// HoldOutstanding returns the remaining (unsettled) amount on a hold,
// or zero if the hold is unknown / released / settled.
func (l *Ledger) HoldOutstanding(orderID int64) models.Qty {
	l.holdsMu.RLock()
	defer l.holdsMu.RUnlock()
	if h, ok := l.holds[orderID]; ok && h.State == holdActive {
		return h.Amount
	}
	return models.ZeroQty
}

// HoldOwner returns the user and asset that originally created the
// hold for orderID. Used by the clearing service to look up the
// counterparty side of each leg of a match. Returns the owner even
// when the hold has been fully consumed (state != active), because
// clearing may legitimately settle multiple matches against the same
// orderID before the hold is exhausted; SettleFill itself enforces
// the not-yet-settled invariant.
func (l *Ledger) HoldOwner(orderID int64) (userID int64, asset string, ok bool) {
	l.holdsMu.RLock()
	defer l.holdsMu.RUnlock()
	h, ok := l.holds[orderID]
	if !ok {
		return 0, "", false
	}
	return h.UserID, h.Asset, true
}

// ---- internal helpers (caller holds the relevant shard lock) ----

func (l *Ledger) lookup(s *shard, userID int64, asset string) (*Account, bool) {
	byAsset, ok := s.accounts[userID]
	if !ok {
		return nil, false
	}
	acc, ok := byAsset[asset]
	return acc, ok
}

func (l *Ledger) getOrCreate(s *shard, userID int64, asset string) *Account {
	byAsset, ok := s.accounts[userID]
	if !ok {
		byAsset = make(map[string]*Account, 4)
		s.accounts[userID] = byAsset
	}
	acc, ok := byAsset[asset]
	if !ok {
		acc = &Account{}
		byAsset[asset] = acc
	}
	return acc
}

