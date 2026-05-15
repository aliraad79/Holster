package risk_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/aliraad79/Gun/models"
	"github.com/aliraad79/Holster/ledger"
	"github.com/aliraad79/Holster/risk"
	"github.com/aliraad79/Holster/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func q(v int64) models.Qty { return models.Qty(v * 1_0000_0000) }
func p(v int64) models.Px  { return models.Px(v * 1_0000_0000) }

func newRisk(t *testing.T) (*risk.Risk, *ledger.Ledger) {
	t.Helper()
	l := ledger.New()
	w, err := wal.Open(filepath.Join(t.TempDir(), "risk.wal"), wal.Options{
		MaxBatch: 8, MaxLatency: time.Millisecond, FsyncOnFlush: false,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return risk.New(l, w), l
}

func TestSubmit_BuyHoldsQuote(t *testing.T) {
	r, l := newRisk(t)
	require.NoError(t, l.Deposit(1, "USDT", q(10_000)))

	order := models.Order{
		ID: 100, UserID: 1, Symbol: "BTC_USDT",
		Type: models.LIMIT, Side: models.BUY,
		Price: p(1000), Volume: q(2),
	}
	require.NoError(t, r.Submit(order))

	// expected hold = price * volume = 1000 * 2 = 2000 USDT
	assert.Equal(t, q(2000), l.HeldOf(1, "USDT"))
	assert.Equal(t, q(10_000), l.Balance(1, "USDT"), "balance unchanged")
}

func TestSubmit_SellHoldsBase(t *testing.T) {
	r, l := newRisk(t)
	require.NoError(t, l.Deposit(2, "BTC", q(10)))

	order := models.Order{
		ID: 200, UserID: 2, Symbol: "BTC_USDT",
		Type: models.LIMIT, Side: models.SELL,
		Price: p(1000), Volume: q(3),
	}
	require.NoError(t, r.Submit(order))

	// expected hold = volume = 3 BTC
	assert.Equal(t, q(3), l.HeldOf(2, "BTC"))
}

func TestSubmit_RejectsInsufficientFunds(t *testing.T) {
	r, l := newRisk(t)
	require.NoError(t, l.Deposit(1, "USDT", q(100)))

	// Buy notional = 1000 * 1 = 1000 USDT, but user only has 100.
	err := r.Submit(models.Order{
		ID: 100, UserID: 1, Symbol: "BTC_USDT",
		Type: models.LIMIT, Side: models.BUY,
		Price: p(1000), Volume: q(1),
	})
	assert.ErrorIs(t, err, ledger.ErrInsufficientFunds)
	assert.Equal(t, models.ZeroQty, l.HeldOf(1, "USDT"), "no funds should be held after rejection")
}

func TestSubmit_RejectsZeroUserID(t *testing.T) {
	r, _ := newRisk(t)
	err := r.Submit(models.Order{
		ID: 1, UserID: 0, Symbol: "BTC_USDT", Type: models.LIMIT,
		Side: models.BUY, Price: p(1), Volume: q(1),
	})
	assert.ErrorIs(t, err, risk.ErrZeroUserID)
}

func TestSubmit_RejectsMalformedSymbol(t *testing.T) {
	r, l := newRisk(t)
	require.NoError(t, l.Deposit(1, "USDT", q(100)))

	for _, bad := range []string{"", "BTC", "BTC_", "_USDT", "BTC_USDT_PERP"} {
		err := r.Submit(models.Order{
			ID: 1, UserID: 1, Symbol: bad, Type: models.LIMIT,
			Side: models.BUY, Price: p(1), Volume: q(1),
		})
		assert.ErrorIs(t, err, risk.ErrUnknownSymbol, "symbol=%q", bad)
	}
}

func TestCancel_ReleasesHeld(t *testing.T) {
	r, l := newRisk(t)
	require.NoError(t, l.Deposit(1, "USDT", q(10_000)))
	require.NoError(t, r.Submit(models.Order{
		ID: 100, UserID: 1, Symbol: "BTC_USDT", Type: models.LIMIT,
		Side: models.BUY, Price: p(1000), Volume: q(1),
	}))

	require.NoError(t, r.Cancel(100))
	assert.Equal(t, models.ZeroQty, l.HeldOf(1, "USDT"))
	assert.Equal(t, q(10_000), l.Balance(1, "USDT"))
}

// big.Int math: 1 BTC at 1,000,000 USDT/BTC = 1_000_000 USDT. Stays
// within int64 range with the /1e8 divisor. The actual int64 limit
// kicks in around the equivalent of $92B in a single order, which we
// also verify the overflow guard rejects cleanly.
func TestSubmit_NotionalOverflow(t *testing.T) {
	r, l := newRisk(t)
	// Give the user enough that ledger insufficient-funds doesn't
	// short-circuit. Use a near-overflow scenario at the price layer.
	require.NoError(t, l.Deposit(1, "USDT", models.Qty(9_000_000_000_000_000_000)))

	// 10 billion units at 10 billion price overflows int64 product.
	huge := models.Order{
		ID: 1, UserID: 1, Symbol: "BTC_USDT",
		Type: models.LIMIT, Side: models.BUY,
		Price:  models.Px(10_000_000_000 * 1_0000_0000),
		Volume: models.Qty(10_000_000_000 * 1_0000_0000),
	}
	err := r.Submit(huge)
	assert.ErrorIs(t, err, risk.ErrNotionalOverflow)
}
