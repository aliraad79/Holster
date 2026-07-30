// Package funding is the durable front door for balance changes.
//
// ledger.Deposit and ledger.Withdraw are in-memory primitives: they
// mutate an account and record nothing. Anything that calls them
// directly is unrecoverable — after a restart the balance is simply
// gone, and no amount of WAL replay brings it back, because the deposit
// was never in a WAL to begin with.
//
// Funding wraps those primitives with a write-ahead record so that
// replaying the log reconstructs balances as well as holds and
// settlements. Production callers should deposit and withdraw through
// here; the raw ledger methods are for tests and for the recovery
// replayer itself.
package funding

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aliraad79/Gun/models"
	"github.com/aliraad79/Holster/ledger"
	"github.com/aliraad79/Holster/wal"
)

// Record kinds written by this package. They share the WAL with risk's
// hold/release records and clearing's trade records; see the recovery
// package for the replay side.
const (
	KindDeposit  = "deposit"
	KindWithdraw = "withdraw"
)

// walRecord is the on-disk shape. Field names line up with risk's
// record so a single decoder can read the whole stream.
type walRecord struct {
	Kind   string     `json:"kind"`
	UserID int64      `json:"user_id"`
	Asset  string     `json:"asset"`
	Amount models.Qty `json:"amount"`
}

// Funding applies durable balance changes.
type Funding struct {
	ledger *ledger.Ledger
	wal    *wal.WAL
}

// New returns a Funding service. Both dependencies are required —
// there is no implicit "skip durability" mode, since that is exactly
// the failure this package exists to prevent.
//
// Pass the SAME *wal.WAL used by risk and clearing. Recovery replays a
// single ordered stream; split across files, deposits, holds and trades
// have no relative order and replay cannot reconstruct state.
func New(l *ledger.Ledger, w *wal.WAL) *Funding {
	if l == nil {
		panic("funding: nil ledger")
	}
	if w == nil {
		panic("funding: nil wal")
	}
	return &Funding{ledger: l, wal: w}
}

// Deposit durably credits an account. The record hits the WAL before the
// ledger, so a crash in between replays as a completed deposit — the
// safe direction for a credit the user has already been told landed.
func (f *Funding) Deposit(userID int64, asset string, amount models.Qty) error {
	if err := f.append(KindDeposit, userID, asset, amount); err != nil {
		return err
	}
	return f.ledger.Deposit(userID, asset, amount)
}

// Withdraw durably debits an account.
//
// Note the ordering hazard here, which is the opposite of Deposit's: the
// record is written before the ledger check, so a crash between the two
// replays as a withdrawal that never actually happened. Replay re-runs
// ledger.Withdraw, which re-applies the same Available check, so an
// unaffordable withdrawal fails identically on replay and the ledger
// stays consistent. What it cannot do is distinguish "crashed before
// applying" from "applied successfully" — both replay the same way, and
// for a debit guarded by its own balance check that is the conservative
// outcome.
func (f *Funding) Withdraw(userID int64, asset string, amount models.Qty) error {
	if err := f.append(KindWithdraw, userID, asset, amount); err != nil {
		return err
	}
	return f.ledger.Withdraw(userID, asset, amount)
}

func (f *Funding) append(kind string, userID int64, asset string, amount models.Qty) error {
	if !amount.IsPositive() {
		return ledger.ErrNegativeAmount
	}
	if asset == "" {
		return errors.New("funding: asset must not be empty")
	}
	payload, err := json.Marshal(&walRecord{
		Kind: kind, UserID: userID, Asset: asset, Amount: amount,
	})
	if err != nil {
		return fmt.Errorf("funding: marshal %s record: %w", kind, err)
	}
	if err := f.wal.Append(payload); err != nil {
		return fmt.Errorf("funding: wal append: %w", err)
	}
	return nil
}
