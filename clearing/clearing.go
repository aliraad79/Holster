// Package clearing is Holster's post-trade settler. It consumes match
// events from Gun and turns each one into a two-leg ledger settlement:
// the buyer's quote-asset hold moves to the seller, and the seller's
// base-asset hold moves to the buyer.
//
// One match → two SettleFill calls on the ledger → one trade record in
// the WAL. Idempotency is keyed on Match.Seq: a Settle call for a seq
// the clearer has already processed is a no-op.
package clearing

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"

	"github.com/aliraad79/Gun/models"
	"github.com/aliraad79/Holster/ledger"
	"github.com/aliraad79/Holster/wal"
)

// Errors returned by Settle.
var (
	ErrUnknownHold     = errors.New("clearing: hold for one side of the match not found")
	ErrMalformedSymbol = errors.New("clearing: symbol must be in BASE_QUOTE form")
)

// HoldLookup is how the clearer asks the risk service "who owns this
// orderID, and in what asset is the hold?". In production it's a
// method on Risk; we use a small interface so clearing tests don't
// depend on the whole Risk type.
type HoldLookup interface {
	HoldOwner(orderID int64) (userID int64, asset string, ok bool)
}

// Clearing settles match events against the ledger.
type Clearing struct {
	ledger *ledger.Ledger
	wal    *wal.WAL
	holds  HoldLookup

	mu         sync.Mutex
	lastSeq    map[string]uint64    // per-symbol high-water mark
	processed  map[string]struct{}  // idempotency key = symbol + "/" + seq
}

// New returns a Clearing service. ledger, wal, and holds are all
// required.
func New(l *ledger.Ledger, w *wal.WAL, holds HoldLookup) *Clearing {
	if l == nil || w == nil || holds == nil {
		panic("clearing: nil dependency")
	}
	return &Clearing{
		ledger:    l,
		wal:       w,
		holds:     holds,
		lastSeq:   make(map[string]uint64),
		processed: make(map[string]struct{}, 1<<14),
	}
}

// tradeRecord is the WAL payload written for each settled match.
type tradeRecord struct {
	Seq          uint64     `json:"seq"`
	Symbol       string     `json:"symbol"`
	BuyOrderID   int64      `json:"buy_order_id"`
	SellOrderID  int64      `json:"sell_order_id"`
	BuyerUserID  int64      `json:"buyer_user_id"`
	SellerUserID int64      `json:"seller_user_id"`
	Price        models.Px  `json:"price"`
	Volume       models.Qty `json:"volume"`
	QuoteAmount  models.Qty `json:"quote_amount"`
}

// Settle processes one Match. Symbol is BASE_QUOTE (e.g. "BTC_USDT");
// the clearer needs it to know which asset moves in each direction.
//
// Idempotent on Match.Seq: a re-delivered match is a no-op (used to
// be a producer-retry scenario; under at-least-once delivery this is
// the norm, not the exception).
func (c *Clearing) Settle(symbol string, m models.Match) error {
	base, quote, err := splitSymbol(symbol)
	if err != nil {
		return err
	}

	idempKey := symbol + "/" + strconv.FormatUint(m.Seq, 10)
	c.mu.Lock()
	if _, seen := c.processed[idempKey]; seen {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	// Look up the two holds. Both sides of a real match must have
	// registered holds via Risk.Submit before reaching the engine.
	buyerUser, buyerHoldAsset, ok := c.holds.HoldOwner(m.BuyId)
	if !ok || buyerHoldAsset != quote {
		return fmt.Errorf("%w: buyer order %d", ErrUnknownHold, m.BuyId)
	}
	sellerUser, sellerHoldAsset, ok := c.holds.HoldOwner(m.SellId)
	if !ok || sellerHoldAsset != base {
		return fmt.Errorf("%w: seller order %d", ErrUnknownHold, m.SellId)
	}

	quoteAmount, err := notionalQty(m.Price, m.Volume)
	if err != nil {
		return fmt.Errorf("clearing: %w", err)
	}

	// WAL the trade first (write-ahead). If we crash between WAL and
	// the ledger ops, replay re-runs both SettleFill calls. They are
	// idempotent on (orderID, seq) so the redo is harmless.
	rec := tradeRecord{
		Seq: m.Seq, Symbol: symbol,
		BuyOrderID: m.BuyId, SellOrderID: m.SellId,
		BuyerUserID: buyerUser, SellerUserID: sellerUser,
		Price: m.Price, Volume: m.Volume, QuoteAmount: quoteAmount,
	}
	payload, err := json.Marshal(&rec)
	if err != nil {
		return fmt.Errorf("clearing: marshal trade: %w", err)
	}
	if err := c.wal.Append(payload); err != nil {
		return fmt.Errorf("clearing: wal: %w", err)
	}

	// Leg 1: buyer's quote moves to seller.
	if err := c.ledger.SettleFill(m.BuyId, sellerUser, quote, quoteAmount); err != nil {
		return fmt.Errorf("clearing: settle buy-leg: %w", err)
	}
	// Leg 2: seller's base moves to buyer.
	if err := c.ledger.SettleFill(m.SellId, buyerUser, base, m.Volume); err != nil {
		return fmt.Errorf("clearing: settle sell-leg: %w", err)
	}

	c.mu.Lock()
	c.processed[idempKey] = struct{}{}
	if m.Seq > c.lastSeq[symbol] {
		c.lastSeq[symbol] = m.Seq
	}
	c.mu.Unlock()
	return nil
}

// LastSeq returns the highest match seq this clearer has processed for
// the given symbol. Used by health checks and the persister-watermark.
// Returns 0 if the symbol has no settled matches yet.
func (c *Clearing) LastSeq(symbol string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastSeq[symbol]
}

// ---- helpers ----

func splitSymbol(s string) (base, quote string, err error) {
	i := strings.IndexByte(s, '_')
	if i <= 0 || i == len(s)-1 || strings.IndexByte(s[i+1:], '_') >= 0 {
		return "", "", fmt.Errorf("%w: %q", ErrMalformedSymbol, s)
	}
	return s[:i], s[i+1:], nil
}

// notionalQty: price * volume in 8-decimal Qty units, big.Int for
// overflow safety. Identical implementation to risk.notionalQty so the
// buyer's hold and the settled amount match exactly.
func notionalQty(price models.Px, volume models.Qty) (models.Qty, error) {
	if price.IsZero() || volume.IsZero() {
		return 0, nil
	}
	bp := big.NewInt(int64(price))
	bv := big.NewInt(int64(volume))
	prod := new(big.Int).Mul(bp, bv).Quo(new(big.Int).Mul(bp, bv), big.NewInt(1_0000_0000))
	if !prod.IsInt64() {
		return 0, errors.New("clearing: notional overflow")
	}
	v := prod.Int64()
	if v < 0 {
		return 0, errors.New("clearing: notional overflow")
	}
	return models.Qty(v), nil
}
