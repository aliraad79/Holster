package clearing_test

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aliraad79/Gun/models"
	"github.com/aliraad79/Holster/clearing"
	"github.com/aliraad79/Holster/ledger"
	"github.com/aliraad79/Holster/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func q(v int64) models.Qty { return models.Qty(v * 1_0000_0000) }
func p(v int64) models.Px  { return models.Px(v * 1_0000_0000) }

// stubHolds is a minimal HoldLookup for tests. Real flows wire
// Clearing to risk.Risk (which we test in the integration test).
type stubHolds struct {
	mu    sync.Mutex
	owner map[int64]struct {
		userID int64
		asset  string
	}
}

func newStub() *stubHolds {
	return &stubHolds{
		owner: make(map[int64]struct {
			userID int64
			asset  string
		}),
	}
}

func (s *stubHolds) register(orderID, userID int64, asset string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.owner[orderID] = struct {
		userID int64
		asset  string
	}{userID, asset}
}

func (s *stubHolds) HoldOwner(orderID int64) (int64, string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.owner[orderID]
	if !ok {
		return 0, "", false
	}
	return v.userID, v.asset, true
}

func newClearing(t *testing.T) (*clearing.Clearing, *ledger.Ledger, *stubHolds) {
	t.Helper()
	l := ledger.New()
	w, err := wal.Open(filepath.Join(t.TempDir(), "clearing.wal"), wal.Options{
		MaxBatch: 16, MaxLatency: time.Millisecond, FsyncOnFlush: false,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	stub := newStub()
	return clearing.New(l, w, stub), l, stub
}

// The canonical spot trade: buyer holds quote, seller holds base, match
// at price P for volume V. Post-settle:
//
//   - buyer:  balance -= V*P quote;  balance += V base.  held -= V*P quote.
//   - seller: balance -= V base;     balance += V*P quote. held -= V base.
func TestSettle_HappyPath(t *testing.T) {
	c, l, stub := newClearing(t)

	// buyer: user 1 holds 1000 USDT for orderID 100 (wants to buy 10 BTC @ 100)
	require.NoError(t, l.Deposit(1, "USDT", q(1000)))
	require.NoError(t, l.Hold(100, 1, "USDT", q(1000)))
	stub.register(100, 1, "USDT")

	// seller: user 2 holds 10 BTC for orderID 200
	require.NoError(t, l.Deposit(2, "BTC", q(10)))
	require.NoError(t, l.Hold(200, 2, "BTC", q(10)))
	stub.register(200, 2, "BTC")

	// Match: buy order 100 fills against sell order 200, 10 BTC @ 100 USDT.
	m := models.Match{Seq: 1, BuyId: 100, SellId: 200, Price: p(100), Volume: q(10)}
	require.NoError(t, c.Settle("BTC_USDT", m))

	// Buyer:
	assert.Equal(t, models.ZeroQty, l.Balance(1, "USDT"), "buyer's USDT balance fully consumed")
	assert.Equal(t, models.ZeroQty, l.HeldOf(1, "USDT"), "buyer's USDT hold released")
	assert.Equal(t, q(10), l.Balance(1, "BTC"), "buyer received 10 BTC")

	// Seller:
	assert.Equal(t, models.ZeroQty, l.Balance(2, "BTC"), "seller's BTC balance fully consumed")
	assert.Equal(t, models.ZeroQty, l.HeldOf(2, "BTC"), "seller's BTC hold released")
	assert.Equal(t, q(1000), l.Balance(2, "USDT"), "seller received 1000 USDT")
}

// Partial fill: only some of each side's hold is consumed; the rest
// stays held for subsequent matches.
func TestSettle_PartialFill(t *testing.T) {
	c, l, stub := newClearing(t)

	require.NoError(t, l.Deposit(1, "USDT", q(1000)))
	require.NoError(t, l.Hold(100, 1, "USDT", q(1000)))
	stub.register(100, 1, "USDT")

	require.NoError(t, l.Deposit(2, "BTC", q(10)))
	require.NoError(t, l.Hold(200, 2, "BTC", q(10)))
	stub.register(200, 2, "BTC")

	// Partial fill: 3 BTC at 100 USDT.
	m := models.Match{Seq: 1, BuyId: 100, SellId: 200, Price: p(100), Volume: q(3)}
	require.NoError(t, c.Settle("BTC_USDT", m))

	assert.Equal(t, q(700), l.Balance(1, "USDT"), "buyer paid 300 of 1000")
	assert.Equal(t, q(700), l.HeldOf(1, "USDT"), "buyer's remaining hold is 700")
	assert.Equal(t, q(3), l.Balance(1, "BTC"))

	assert.Equal(t, q(7), l.Balance(2, "BTC"), "seller still has 7 BTC")
	assert.Equal(t, q(7), l.HeldOf(2, "BTC"), "seller's remaining hold is 7")
	assert.Equal(t, q(300), l.Balance(2, "USDT"))
}

// Idempotency: same Match.Seq settled twice must produce one trade.
func TestSettle_IdempotentOnSeq(t *testing.T) {
	c, l, stub := newClearing(t)
	require.NoError(t, l.Deposit(1, "USDT", q(1000)))
	require.NoError(t, l.Hold(100, 1, "USDT", q(1000)))
	stub.register(100, 1, "USDT")
	require.NoError(t, l.Deposit(2, "BTC", q(10)))
	require.NoError(t, l.Hold(200, 2, "BTC", q(10)))
	stub.register(200, 2, "BTC")

	m := models.Match{Seq: 1, BuyId: 100, SellId: 200, Price: p(100), Volume: q(10)}
	require.NoError(t, c.Settle("BTC_USDT", m))
	require.NoError(t, c.Settle("BTC_USDT", m), "second Settle for the same seq is a no-op")

	// State must reflect exactly one settlement, not two.
	assert.Equal(t, q(10), l.Balance(1, "BTC"))
	assert.Equal(t, q(1000), l.Balance(2, "USDT"))
}

// Missing hold for one side: reject the match. (Production: this means
// the producer skipped the risk step or the WAL is corrupt; either
// way, ledger must not move.)
func TestSettle_RejectsMissingHold(t *testing.T) {
	c, l, stub := newClearing(t)
	// Only register the buyer's hold; seller's is missing.
	require.NoError(t, l.Deposit(1, "USDT", q(1000)))
	require.NoError(t, l.Hold(100, 1, "USDT", q(1000)))
	stub.register(100, 1, "USDT")

	m := models.Match{Seq: 1, BuyId: 100, SellId: 999, Price: p(100), Volume: q(10)}
	err := c.Settle("BTC_USDT", m)
	assert.ErrorIs(t, err, clearing.ErrUnknownHold)

	// Buyer's hold must be untouched.
	assert.Equal(t, q(1000), l.HeldOf(1, "USDT"))
}

// LastSeq is the watermark used by the async DB persister and health
// checks.
func TestSettle_LastSeqAdvances(t *testing.T) {
	c, l, stub := newClearing(t)
	require.NoError(t, l.Deposit(1, "USDT", q(10000)))
	require.NoError(t, l.Hold(100, 1, "USDT", q(10000)))
	stub.register(100, 1, "USDT")
	require.NoError(t, l.Deposit(2, "BTC", q(100)))
	require.NoError(t, l.Hold(200, 2, "BTC", q(100)))
	stub.register(200, 2, "BTC")

	for seq := uint64(1); seq <= 5; seq++ {
		m := models.Match{Seq: seq, BuyId: 100, SellId: 200, Price: p(100), Volume: q(1)}
		require.NoError(t, c.Settle("BTC_USDT", m))
	}
	assert.EqualValues(t, 5, c.LastSeq("BTC_USDT"))
}
