// Package risk is the pre-trade hold service. Before an order reaches
// the matching engine, risk:
//
//  1. Computes how much of which asset the order will need to hold
//     (buy: price * volume of quote; sell: volume of base).
//  2. Appends a HOLD record to the WAL.
//  3. Calls Ledger.Hold to reserve the funds.
//
// If step 3 fails (insufficient funds), the WAL record is benign:
// replay re-runs Ledger.Hold, which returns the same error, and the
// system stays consistent. The producer sees the rejection and the
// order never reaches the matching engine.
package risk

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/aliraad79/Gun/models"
	"github.com/aliraad79/Holster/ledger"
	"github.com/aliraad79/Holster/wal"
)

// Errors returned by Risk.Submit.
var (
	ErrUnknownSymbol    = errors.New("risk: symbol must be in BASE_QUOTE form")
	ErrZeroUserID       = errors.New("risk: order.UserID must be non-zero")
	ErrNotionalOverflow = errors.New("risk: notional exceeds int64 range; reject as too large")
)

// Risk is the pre-trade hold service. Construct one with New, then
// route every incoming order through Submit / Cancel.
type Risk struct {
	ledger *ledger.Ledger
	wal    *wal.WAL
}

// New returns a Risk service backed by the given ledger and WAL. Both
// must be non-nil — Risk has no implicit "skip durability" mode.
func New(l *ledger.Ledger, w *wal.WAL) *Risk {
	if l == nil {
		panic("risk: nil ledger")
	}
	if w == nil {
		panic("risk: nil wal")
	}
	return &Risk{ledger: l, wal: w}
}

// recordKind discriminates HOLD vs RELEASE WAL records.
type recordKind string

const (
	recHold    recordKind = "hold"
	recRelease recordKind = "release"
)

type walRecord struct {
	Kind    recordKind `json:"kind"`
	OrderID int64      `json:"order_id"`
	UserID  int64     `json:"user_id,omitempty"`
	Asset   string     `json:"asset,omitempty"`
	Amount  models.Qty `json:"amount,omitempty"`
}

// Submit checks the producer's order, computes the required hold,
// writes the hold to the WAL, and reserves the funds in the ledger.
// On success the caller may forward the order to the matching engine.
//
// Idempotency: Ledger.Hold is idempotent on order_id, so a retried
// Submit (after a producer-side timeout) does not double-hold.
func (r *Risk) Submit(order models.Order) error {
	if order.UserID == 0 {
		return ErrZeroUserID
	}

	asset, amount, err := holdRequirements(order)
	if err != nil {
		return err
	}

	rec := walRecord{
		Kind: recHold, OrderID: order.ID, UserID: order.UserID,
		Asset: asset, Amount: amount,
	}
	payload, err := json.Marshal(&rec)
	if err != nil {
		return fmt.Errorf("risk: marshal hold record: %w", err)
	}
	if err := r.wal.Append(payload); err != nil {
		return fmt.Errorf("risk: wal append: %w", err)
	}

	return r.ledger.Hold(order.ID, order.UserID, asset, amount)
}

// Cancel releases the hold on a given order. Used both by client-
// initiated cancel and by the matching-engine-rejected path (FOK /
// post-only / invalid).
func (r *Risk) Cancel(orderID int64) error {
	rec := walRecord{Kind: recRelease, OrderID: orderID}
	payload, err := json.Marshal(&rec)
	if err != nil {
		return fmt.Errorf("risk: marshal release record: %w", err)
	}
	if err := r.wal.Append(payload); err != nil {
		return fmt.Errorf("risk: wal append: %w", err)
	}
	return r.ledger.Release(orderID)
}

// holdRequirements computes which asset and how much of it an order
// will tie up while resting on the book.
//
// Spot semantics:
//
//   - BUY  side: hold quote asset = price * volume
//   - SELL side: hold base  asset = volume
//   - MARKET BUY: caller must specify a max-notional separately (not
//     supported by this revision; reject and let the API gateway
//     translate market-buy-by-quote into limit IOC at far price).
func holdRequirements(o models.Order) (asset string, amount models.Qty, err error) {
	base, quote, err := splitSymbol(o.Symbol)
	if err != nil {
		return "", 0, err
	}
	switch o.Side {
	case models.SELL:
		// Sell of N base units: hold N base.
		return base, o.Volume, nil
	case models.BUY:
		// Buy at price P for volume V: hold P*V quote, in 8-decimal Qty.
		notional, err := notionalQty(o.Price, o.Volume)
		if err != nil {
			return "", 0, err
		}
		return quote, notional, nil
	default:
		return "", 0, fmt.Errorf("risk: unsupported side %q", o.Side)
	}
}

// splitSymbol parses BASE_QUOTE like "BTC_USDT" -> ("BTC", "USDT").
// Anything without exactly one underscore is rejected; this is the
// contract producers must obey.
func splitSymbol(symbol string) (base, quote string, err error) {
	i := strings.IndexByte(symbol, '_')
	if i <= 0 || i == len(symbol)-1 {
		return "", "", fmt.Errorf("%w: %q", ErrUnknownSymbol, symbol)
	}
	if strings.IndexByte(symbol[i+1:], '_') >= 0 {
		return "", "", fmt.Errorf("%w: %q", ErrUnknownSymbol, symbol)
	}
	return symbol[:i], symbol[i+1:], nil
}

// notionalQty computes price * volume in 8-decimal Qty units, using
// big.Int internally to avoid the int64 overflow that would otherwise
// happen at realistic values (a $10k BTC at 1 BTC is 1e12 * 1e8 = 1e20,
// which doesn't fit).
//
// The math: both price and volume are 8-decimal scaled int64s, so
// their product carries 16 decimals of scale. We divide by 1e8 to drop
// back to 8 decimals.
func notionalQty(price models.Px, volume models.Qty) (models.Qty, error) {
	if price.IsZero() || volume.IsZero() {
		return 0, nil
	}
	bp := big.NewInt(int64(price))
	bv := big.NewInt(int64(volume))
	prod := new(big.Int).Mul(bp, bv)
	prod.Quo(prod, big.NewInt(1_0000_0000))

	if !prod.IsInt64() {
		return 0, ErrNotionalOverflow
	}
	v := prod.Int64()
	if v < 0 {
		return 0, ErrNotionalOverflow
	}
	return models.Qty(v), nil
}
