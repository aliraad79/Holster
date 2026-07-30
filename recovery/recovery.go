// Package recovery rebuilds an in-memory ledger from the write-ahead
// log after a restart.
//
// Without this, the WAL is write-only: Holster appended records nobody
// ever read back, so every balance and hold vanished on restart while
// the docs called the WAL the source of truth in flight.
//
// # What replay assumes
//
// Recovery is a full rebuild over one ordered stream into an EMPTY
// ledger. Both halves of that matter:
//
//   - One stream. funding, risk and clearing must share a single
//     *wal.WAL. Split across files there is no relative order between a
//     deposit, the hold that spends it, and the trade that consumes the
//     hold, and no ordering of the files can recover it. Replaying all
//     holds before all trades, for instance, turns
//     hold -> settle -> release into hold -> release -> settle, and the
//     settle is then rejected against a released hold and silently lost.
//
//   - Empty ledger. ledger.SettleFill is not idempotent: it takes no
//     sequence number and decrements the hold on every call. Replaying a
//     trade record against a ledger that already reflects it spends the
//     hold twice. Rebuild therefore requires a fresh ledger and refuses
//     to run against a populated one.
//
// # What replay reproduces
//
// Every mutation that went through funding, risk or clearing. Callers
// that reach past those into ledger.Deposit / ledger.Withdraw /
// ledger.Hold directly are writing state that recovery cannot see; that
// is the whole reason the funding package exists.
package recovery

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aliraad79/Gun/models"
	"github.com/aliraad79/Holster/clearing"
	"github.com/aliraad79/Holster/funding"
	"github.com/aliraad79/Holster/ledger"
	"github.com/aliraad79/Holster/risk"
	"github.com/aliraad79/Holster/wal"
)

// ErrLedgerNotEmpty is returned by Rebuild when handed a ledger that
// already holds state. Replaying onto live state double-applies every
// record; see the package doc.
var ErrLedgerNotEmpty = errors.New("recovery: refusing to replay into a non-empty ledger")

// ErrCorruptRecord is returned when a WAL payload cannot be decoded, or
// carries a kind this build does not understand. Both are treated as
// fatal rather than skipped: a record we cannot interpret means the
// rebuilt ledger would be silently wrong.
var ErrCorruptRecord = errors.New("recovery: corrupt WAL record")

// record is the union of every kind written to the stream. Each producer
// writes its own struct; the field names line up so one decoder reads
// them all.
type record struct {
	Kind string `json:"kind"`

	// funding (deposit / withdraw) and risk (hold / release)
	OrderID int64      `json:"order_id"`
	UserID  int64      `json:"user_id"`
	Asset   string     `json:"asset"`
	Amount  models.Qty `json:"amount"`

	// clearing (trade)
	Seq          uint64     `json:"seq"`
	Symbol       string     `json:"symbol"`
	BuyOrderID   int64      `json:"buy_order_id"`
	SellOrderID  int64      `json:"sell_order_id"`
	BuyerUserID  int64      `json:"buyer_user_id"`
	SellerUserID int64      `json:"seller_user_id"`
	Volume       models.Qty `json:"volume"`
	QuoteAmount  models.Qty `json:"quote_amount"`
}

// Stats reports what a rebuild did. Rejected counts records that
// replayed to a business-rule error rather than a corruption — an
// unaffordable hold, say, which failed the same way on the original run
// and whose record is benign by design. A non-zero Rejected is expected
// on any real log; it is reported rather than hidden so an operator can
// see the shape of what came back.
type Stats struct {
	Total     int
	Applied   int
	Rejected  int
	Deposits  int
	Withdraws int
	Holds     int
	Releases  int
	Trades    int
}

// Rebuild replays the WAL at path into l, which must be empty.
//
// A missing WAL file is not an error: a first-ever start has nothing to
// recover and returns zeroed Stats.
func Rebuild(l *ledger.Ledger, path string) (Stats, error) {
	var st Stats

	if l == nil {
		return st, errors.New("recovery: nil ledger")
	}
	if !l.IsEmpty() {
		return st, ErrLedgerNotEmpty
	}

	err := wal.Replay(path, func(payload []byte) error {
		st.Total++

		var rec record
		if err := json.Unmarshal(payload, &rec); err != nil {
			return fmt.Errorf("%w at record %d: %v", ErrCorruptRecord, st.Total, err)
		}

		applyErr, err := apply(l, rec, &st)
		if err != nil {
			return fmt.Errorf("%w at record %d: %v", ErrCorruptRecord, st.Total, err)
		}
		if applyErr != nil {
			st.Rejected++
			return nil
		}
		st.Applied++
		return nil
	})
	if err != nil {
		return st, err
	}
	return st, nil
}

// apply dispatches one record. It returns two errors deliberately:
// applyErr is a business-rule rejection that replay should count and
// continue past, while the second is a structural problem that must
// abort the rebuild.
func apply(l *ledger.Ledger, rec record, st *Stats) (applyErr error, fatal error) {
	switch rec.Kind {
	case funding.KindDeposit:
		st.Deposits++
		return l.Deposit(rec.UserID, rec.Asset, rec.Amount), nil

	case funding.KindWithdraw:
		st.Withdraws++
		return l.Withdraw(rec.UserID, rec.Asset, rec.Amount), nil

	case risk.KindHold:
		st.Holds++
		return l.Hold(rec.OrderID, rec.UserID, rec.Asset, rec.Amount), nil

	case risk.KindRelease:
		st.Releases++
		return l.Release(rec.OrderID), nil

	case clearing.KindTrade:
		st.Trades++
		return applyTrade(l, rec)

	case "":
		return nil, errors.New("record has no kind field")

	default:
		return nil, fmt.Errorf("unknown record kind %q", rec.Kind)
	}
}

// applyTrade re-runs the two settlement legs for one trade record, using
// the amounts the record carries rather than recomputing them. Recomputing
// would risk drifting from whatever rounding produced the original hold.
func applyTrade(l *ledger.Ledger, rec record) (applyErr error, fatal error) {
	base, quote, err := clearing.SplitSymbol(rec.Symbol)
	if err != nil {
		return nil, err
	}

	// Leg 1: buyer's quote to the seller. Leg 2: seller's base to the
	// buyer. Same order as clearing.Settle so a partially-applied
	// settlement rebuilds the same way it originally landed.
	if err := l.SettleFill(rec.BuyOrderID, rec.SellerUserID, quote, rec.QuoteAmount); err != nil {
		return err, nil
	}
	if err := l.SettleFill(rec.SellOrderID, rec.BuyerUserID, base, rec.Volume); err != nil {
		return err, nil
	}
	return nil, nil
}
