package funding_test

import (
	"path/filepath"
	"testing"

	"github.com/aliraad79/Gun/models"
	"github.com/aliraad79/Holster/funding"
	"github.com/aliraad79/Holster/ledger"
	"github.com/aliraad79/Holster/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func q(v int64) models.Qty { return models.Qty(v * 1_0000_0000) }

func newFunding(t *testing.T) (*funding.Funding, *ledger.Ledger, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "holster.wal")
	w, err := wal.Open(path, wal.Options{MaxBatch: 1, FsyncOnFlush: false})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	l := ledger.New()
	return funding.New(l, w), l, path
}

func TestDeposit_AppliesAndIsDurable(t *testing.T) {
	f, l, path := newFunding(t)

	require.NoError(t, f.Deposit(1, "USDT", q(500)))
	assert.Equal(t, q(500), l.Balance(1, "USDT"))

	assert.Equal(t, 1, countRecords(t, path), "deposit must leave a WAL record")
}

func TestWithdraw_AppliesAndIsDurable(t *testing.T) {
	f, l, path := newFunding(t)

	require.NoError(t, f.Deposit(1, "USDT", q(500)))
	require.NoError(t, f.Withdraw(1, "USDT", q(200)))
	assert.Equal(t, q(300), l.Balance(1, "USDT"))

	assert.Equal(t, 2, countRecords(t, path))
}

// An unaffordable withdrawal is rejected by the ledger. The WAL record is
// still there, which is fine and is the documented behavior: replay
// re-applies the same Available check and rejects it identically.
func TestWithdraw_RejectedLeavesBalanceUnchanged(t *testing.T) {
	f, l, _ := newFunding(t)

	require.NoError(t, f.Deposit(1, "USDT", q(100)))
	err := f.Withdraw(1, "USDT", q(500))
	assert.ErrorIs(t, err, ledger.ErrInsufficientFunds)
	assert.Equal(t, q(100), l.Balance(1, "USDT"), "a rejected withdrawal must not move funds")
}

func TestFunding_RejectsNonPositiveAmounts(t *testing.T) {
	f, _, path := newFunding(t)

	assert.ErrorIs(t, f.Deposit(1, "USDT", models.ZeroQty), ledger.ErrNegativeAmount)
	assert.ErrorIs(t, f.Withdraw(1, "USDT", q(-5)), ledger.ErrNegativeAmount)
	assert.Error(t, f.Deposit(1, "", q(5)), "empty asset must be rejected")

	assert.Zero(t, countRecords(t, path),
		"a rejected request must not reach the WAL")
}

func TestFunding_RequiresDependencies(t *testing.T) {
	assert.Panics(t, func() { funding.New(nil, nil) })
	assert.Panics(t, func() { funding.New(ledger.New(), nil) })
}

func countRecords(t *testing.T, path string) int {
	t.Helper()
	n := 0
	require.NoError(t, wal.Replay(path, func([]byte) error { n++; return nil }))
	return n
}
