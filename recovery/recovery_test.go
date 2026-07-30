package recovery_test

import (
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/aliraad79/Gun/models"
	"github.com/aliraad79/Holster/clearing"
	"github.com/aliraad79/Holster/funding"
	"github.com/aliraad79/Holster/ledger"
	"github.com/aliraad79/Holster/recovery"
	"github.com/aliraad79/Holster/risk"
	"github.com/aliraad79/Holster/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func p(v int64) models.Px  { return models.Px(v * 1_0000_0000) }
func q(v int64) models.Qty { return models.Qty(v * 1_0000_0000) }

func openWAL(t *testing.T, dir string) *wal.WAL {
	t.Helper()
	w, err := wal.Open(filepath.Join(dir, "holster.wal"), wal.Options{
		MaxBatch: 64, MaxLatency: 200 * time.Microsecond, FsyncOnFlush: false,
	})
	require.NoError(t, err)
	return w
}

// stack is the three durable services sharing one ordered WAL, which is
// what recovery requires.
type stack struct {
	l *ledger.Ledger
	f *funding.Funding
	r *risk.Risk
	c *clearing.Clearing
}

func newStack(t *testing.T, w *wal.WAL) stack {
	t.Helper()
	l := ledger.New()
	return stack{
		l: l,
		f: funding.New(l, w),
		r: risk.New(l, w),
		c: clearing.New(l, w, l),
	}
}

// The headline property: replaying the log into a fresh ledger must
// reproduce the original ledger exactly. Deposits, holds, releases and
// settlements all have to come back, in an order that respects how they
// interleaved.
func TestRebuild_ReproducesLedgerState(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir)
	s := newStack(t, w)

	// Fund two users.
	require.NoError(t, s.f.Deposit(1, "USDT", q(100_000)))
	require.NoError(t, s.f.Deposit(2, "BTC", q(10)))

	// Buyer holds quote, seller holds base.
	buy := models.Order{ID: 10, UserID: 1, Symbol: "BTC_USDT",
		Type: models.LIMIT, Side: models.BUY, Price: p(10_000), Volume: q(2)}
	sell := models.Order{ID: 11, UserID: 2, Symbol: "BTC_USDT",
		Type: models.LIMIT, Side: models.SELL, Price: p(10_000), Volume: q(2)}
	require.NoError(t, s.r.Submit(buy))
	require.NoError(t, s.r.Submit(sell))

	// Settle a partial fill, then another.
	require.NoError(t, s.c.Settle("BTC_USDT", models.Match{
		Seq: 1, BuyId: 10, SellId: 11, Price: p(10_000), Volume: q(1)}))
	require.NoError(t, s.c.Settle("BTC_USDT", models.Match{
		Seq: 2, BuyId: 10, SellId: 11, Price: p(10_000), Volume: q(1)}))

	// A hold that gets cancelled rather than filled — exercises the
	// hold-then-release ordering that a per-file replay would scramble.
	cancelled := models.Order{ID: 12, UserID: 1, Symbol: "BTC_USDT",
		Type: models.LIMIT, Side: models.BUY, Price: p(9_000), Volume: q(1)}
	require.NoError(t, s.r.Submit(cancelled))
	require.NoError(t, s.r.Cancel(12))

	// A withdrawal after trading.
	require.NoError(t, s.f.Withdraw(2, "USDT", q(5_000)))

	require.NoError(t, w.Close())

	// Snapshot the live state.
	want := snapshot(s.l)

	// Rebuild from scratch.
	fresh := ledger.New()
	st, err := recovery.Rebuild(fresh, filepath.Join(dir, "holster.wal"))
	require.NoError(t, err)

	assert.Equal(t, 2, st.Deposits)
	assert.Equal(t, 1, st.Withdraws)
	assert.Equal(t, 3, st.Holds)
	assert.Equal(t, 1, st.Releases)
	assert.Equal(t, 2, st.Trades)
	assert.Zero(t, st.Rejected, "no record in this log should have been rejected")

	assert.Equal(t, want, snapshot(fresh),
		"rebuilt ledger does not match the ledger the WAL was written from")

	// Spelled out so the comparison above cannot pass by both sides being
	// trivially empty, and so the arithmetic is reviewable:
	//   user 1 deposited 100k USDT, held 20k for the buy, spent it on 2 BTC
	//   user 2 deposited 10 BTC, sold 2, received 20k USDT, withdrew 5k
	assert.Equal(t, q(80_000), fresh.Balance(1, "USDT"), "buyer quote balance")
	assert.Equal(t, q(2), fresh.Balance(1, "BTC"), "buyer received base")
	assert.Equal(t, q(8), fresh.Balance(2, "BTC"), "seller base balance")
	assert.Equal(t, q(15_000), fresh.Balance(2, "USDT"), "seller quote after withdrawal")
	assert.Equal(t, models.ZeroQty, fresh.HeldOf(1, "USDT"), "buyer hold fully consumed")
	assert.Equal(t, models.ZeroQty, fresh.HeldOf(2, "BTC"), "seller hold fully consumed")
	assert.False(t, fresh.IsEmpty())
}

type accountState struct {
	Balance models.Qty
	Held    models.Qty
}

func snapshot(l *ledger.Ledger) map[string]accountState {
	out := make(map[string]accountState)
	for _, u := range []int64{1, 2} {
		for _, a := range []string{"USDT", "BTC"} {
			out[keyOf(u, a)] = accountState{
				Balance: l.Balance(u, a),
				Held:    l.HeldOf(u, a),
			}
		}
	}
	// hold remainders matter as much as balances
	for _, id := range []int64{10, 11, 12} {
		out[holdKeyOf(id)] = accountState{Balance: l.HoldOutstanding(id)}
	}
	return out
}

func keyOf(u int64, a string) string { return a + "/" + strconv.FormatInt(u, 10) }
func holdKeyOf(id int64) string      { return "hold/" + strconv.FormatInt(id, 10) }

// A first-ever start has no WAL. That must not be an error.
func TestRebuild_MissingWALIsNotAnError(t *testing.T) {
	fresh := ledger.New()
	st, err := recovery.Rebuild(fresh, filepath.Join(t.TempDir(), "absent.wal"))
	require.NoError(t, err)
	assert.Zero(t, st.Total)
	assert.True(t, fresh.IsEmpty())
}

// Replaying into a populated ledger double-applies everything, and
// SettleFill is not idempotent, so it must be refused rather than
// silently corrupting balances.
func TestRebuild_RefusesNonEmptyLedger(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir)
	s := newStack(t, w)
	require.NoError(t, s.f.Deposit(1, "USDT", q(100)))
	require.NoError(t, w.Close())

	_, err := recovery.Rebuild(s.l, filepath.Join(dir, "holster.wal"))
	assert.ErrorIs(t, err, recovery.ErrLedgerNotEmpty)
}

// A record whose kind this build does not understand means the rebuilt
// ledger would be quietly incomplete. Fail loudly instead.
func TestRebuild_UnknownKindIsFatal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "holster.wal")

	w, err := wal.Open(path, wal.Options{MaxBatch: 1, FsyncOnFlush: false})
	require.NoError(t, err)
	require.NoError(t, w.Append([]byte(`{"kind":"teleport","user_id":1}`)))
	require.NoError(t, w.Close())

	_, err = recovery.Rebuild(ledger.New(), path)
	assert.ErrorIs(t, err, recovery.ErrCorruptRecord)
}

// An unaffordable hold was already rejected on the original run; its WAL
// record is benign by design. Replay must count it and carry on, not
// abort the whole rebuild.
func TestRebuild_CountsBenignRejections(t *testing.T) {
	dir := t.TempDir()
	w := openWAL(t, dir)
	s := newStack(t, w)

	require.NoError(t, s.f.Deposit(1, "USDT", q(100)))
	// needs 10_000 quote against a 100 balance: rejected then, rejected now
	err := s.r.Submit(models.Order{ID: 20, UserID: 1, Symbol: "BTC_USDT",
		Type: models.LIMIT, Side: models.BUY, Price: p(10_000), Volume: q(1)})
	require.ErrorIs(t, err, ledger.ErrInsufficientFunds)
	require.NoError(t, w.Close())

	fresh := ledger.New()
	st, err := recovery.Rebuild(fresh, filepath.Join(dir, "holster.wal"))
	require.NoError(t, err)
	assert.Equal(t, 1, st.Rejected, "the unaffordable hold should count as rejected")
	assert.Equal(t, q(100), fresh.Balance(1, "USDT"))
	assert.Equal(t, models.ZeroQty, fresh.HeldOf(1, "USDT"))
}
