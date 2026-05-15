// Package integration exercises Gun + Holster end-to-end. A real
// Market (matching engine) runs in-process; a real Holster instance
// (ledger + WAL + risk + clearing) wires up to its OnMatch callback.
// The test asserts that after a trade settles, both sides' balances,
// holds, and matched assets are exactly what spot semantics demand.
package integration_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	gunjournal "github.com/aliraad79/Gun/journal"
	"github.com/aliraad79/Gun/market"
	"github.com/aliraad79/Gun/models"
	"github.com/aliraad79/Holster/clearing"
	"github.com/aliraad79/Holster/ledger"
	"github.com/aliraad79/Holster/risk"
	"github.com/aliraad79/Holster/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func q(v int64) models.Qty { return models.Qty(v * 1_0000_0000) }
func p(v int64) models.Px  { return models.Px(v * 1_0000_0000) }

// stack wires Gun and Holster together for a test scenario.
type stack struct {
	registry *market.Registry
	ledger   *ledger.Ledger
	risk     *risk.Risk
	clearing *clearing.Clearing
	cancel   context.CancelFunc
	wg       *sync.WaitGroup
}

func newStack(t *testing.T) *stack {
	t.Helper()
	t.Setenv("SUPPORTED_SYMBOLS", "BTC_USDT,ETH_USDT")

	dir := t.TempDir()

	// Holster: ledger + per-service WALs + risk + clearing.
	l := ledger.New()
	riskWAL, err := wal.Open(filepath.Join(dir, "risk.wal"), wal.Options{
		MaxBatch: 64, MaxLatency: time.Millisecond, FsyncOnFlush: false,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = riskWAL.Close() })

	clearingWAL, err := wal.Open(filepath.Join(dir, "clearing.wal"), wal.Options{
		MaxBatch: 64, MaxLatency: time.Millisecond, FsyncOnFlush: false,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = clearingWAL.Close() })

	r := risk.New(l, riskWAL)
	c := clearing.New(l, clearingWAL, l) // ledger satisfies HoldLookup

	// Gun: a Registry whose OnMatch drives the clearing service. The
	// symbol comes through as the first callback argument; we forward
	// each match into c.Settle. Errors are logged but should not block
	// the matching goroutine in production (clearing must keep up; if
	// it doesn't, that's a back-pressure alarm).
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	reg := market.NewRegistry(ctx, &wg, market.Options{
		InboxSize: 64,
		Journal:   gunjournal.Discard{},
		OnMatch: func(symbol string, matches []models.Match) {
			for _, m := range matches {
				if err := c.Settle(symbol, m); err != nil {
					t.Errorf("settle %d: %v", m.Seq, err)
				}
			}
		},
	})

	return &stack{
		registry: reg, ledger: l, risk: r, clearing: c,
		cancel: cancel, wg: &wg,
	}
}

func (s *stack) shutdown() {
	// Let any in-flight matches drain through OnMatch before we cancel.
	time.Sleep(100 * time.Millisecond)
	s.cancel()
	s.wg.Wait()
}

// Drives a single full-fill trade through Gun, observes Holster
// settling it.
func TestEndToEnd_SpotTradeSettles(t *testing.T) {
	s := newStack(t)
	defer s.shutdown()

	// Deposit starting balances.
	require.NoError(t, s.ledger.Deposit(1, "USDT", q(10_000)))
	require.NoError(t, s.ledger.Deposit(2, "BTC", q(10)))

	// Seller places resting limit sell: 5 BTC @ 100 USDT.
	sellOrder := models.Order{
		ID: 200, UserID: 2, Symbol: "BTC_USDT",
		Type: models.LIMIT, Side: models.SELL,
		Price: p(100), Volume: q(5),
	}
	require.NoError(t, s.risk.Submit(sellOrder))
	s.registry.Submit(sellOrder)

	// Buyer places crossing limit buy: 5 BTC @ 100 USDT.
	buyOrder := models.Order{
		ID: 100, UserID: 1, Symbol: "BTC_USDT",
		Type: models.LIMIT, Side: models.BUY,
		Price: p(100), Volume: q(5),
	}
	require.NoError(t, s.risk.Submit(buyOrder))
	s.registry.Submit(buyOrder)

	// Wait for match + settle to drain through the OnMatch callback.
	require.Eventually(t, func() bool {
		return s.ledger.HeldOf(1, "USDT").IsZero() &&
			s.ledger.HeldOf(2, "BTC").IsZero()
	}, time.Second, 5*time.Millisecond, "expected holds released after settle")

	// Buyer: paid 500 USDT, received 5 BTC.
	assert.Equal(t, q(9_500), s.ledger.Balance(1, "USDT"))
	assert.Equal(t, q(5), s.ledger.Balance(1, "BTC"))
	assert.Equal(t, models.ZeroQty, s.ledger.HeldOf(1, "USDT"))

	// Seller: paid 5 BTC, received 500 USDT.
	assert.Equal(t, q(5), s.ledger.Balance(2, "BTC"))
	assert.Equal(t, q(500), s.ledger.Balance(2, "USDT"))
	assert.Equal(t, models.ZeroQty, s.ledger.HeldOf(2, "BTC"))

	// Clearing's high-water mark must be >= 1 (we produced at least
	// one match).
	assert.GreaterOrEqual(t, s.clearing.LastSeq("BTC_USDT"), uint64(1))
}

// Cross-symbol independence: a settled BTC_USDT trade does not leak
// into ETH_USDT's ledger.
func TestEndToEnd_CrossSymbolDoesNotLeak(t *testing.T) {
	s := newStack(t)
	defer s.shutdown()

	require.NoError(t, s.ledger.Deposit(1, "USDT", q(10_000)))
	require.NoError(t, s.ledger.Deposit(2, "BTC", q(10)))
	require.NoError(t, s.ledger.Deposit(3, "USDT", q(10_000)))
	require.NoError(t, s.ledger.Deposit(4, "ETH", q(10)))

	for _, o := range []models.Order{
		{ID: 200, UserID: 2, Symbol: "BTC_USDT", Type: models.LIMIT, Side: models.SELL, Price: p(100), Volume: q(2)},
		{ID: 100, UserID: 1, Symbol: "BTC_USDT", Type: models.LIMIT, Side: models.BUY, Price: p(100), Volume: q(2)},
		{ID: 400, UserID: 4, Symbol: "ETH_USDT", Type: models.LIMIT, Side: models.SELL, Price: p(50), Volume: q(3)},
		{ID: 300, UserID: 3, Symbol: "ETH_USDT", Type: models.LIMIT, Side: models.BUY, Price: p(50), Volume: q(3)},
	} {
		require.NoError(t, s.risk.Submit(o))
		s.registry.Submit(o)
	}

	require.Eventually(t, func() bool {
		ok := s.ledger.HeldOf(1, "USDT").IsZero() &&
			s.ledger.HeldOf(3, "USDT").IsZero()
		if !ok {
			t.Logf("u1 USDT held=%s  u3 USDT held=%s  u2 BTC held=%s  u4 ETH held=%s",
				s.ledger.HeldOf(1, "USDT").String(),
				s.ledger.HeldOf(3, "USDT").String(),
				s.ledger.HeldOf(2, "BTC").String(),
				s.ledger.HeldOf(4, "ETH").String())
		}
		return ok
	}, time.Second, 50*time.Millisecond)

	// BTC_USDT users:
	assert.Equal(t, q(2), s.ledger.Balance(1, "BTC"))
	assert.Equal(t, q(9_800), s.ledger.Balance(1, "USDT"))
	assert.Equal(t, q(8), s.ledger.Balance(2, "BTC"))
	assert.Equal(t, q(200), s.ledger.Balance(2, "USDT"))

	// ETH_USDT users (no ETH leaked into BTC, no BTC leaked into ETH):
	assert.Equal(t, q(3), s.ledger.Balance(3, "ETH"))
	assert.Equal(t, q(9_850), s.ledger.Balance(3, "USDT"))
	assert.Equal(t, q(7), s.ledger.Balance(4, "ETH"))
	assert.Equal(t, q(150), s.ledger.Balance(4, "USDT"))

	// And no cross-contamination — user 1 should never have ETH;
	// user 3 should never have BTC.
	assert.Equal(t, models.ZeroQty, s.ledger.Balance(1, "ETH"))
	assert.Equal(t, models.ZeroQty, s.ledger.Balance(3, "BTC"))
}
