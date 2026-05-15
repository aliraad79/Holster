package ledger_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aliraad79/Gun/models"
	"github.com/aliraad79/Holster/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func q(v int64) models.Qty { return models.Qty(v * 1_0000_0000) }

func TestDeposit_IncreasesBalance(t *testing.T) {
	l := ledger.New()

	require.NoError(t, l.Deposit(1, "USDT", q(100)))
	assert.Equal(t, q(100), l.Balance(1, "USDT"))
	assert.Equal(t, models.ZeroQty, l.HeldOf(1, "USDT"))
}

func TestWithdraw_RespectsHeld(t *testing.T) {
	l := ledger.New()
	require.NoError(t, l.Deposit(1, "USDT", q(100)))
	require.NoError(t, l.Hold(42, 1, "USDT", q(60)))

	// Available = 40; withdrawing 50 must fail without changing state.
	err := l.Withdraw(1, "USDT", q(50))
	assert.ErrorIs(t, err, ledger.ErrInsufficientFunds)
	assert.Equal(t, q(100), l.Balance(1, "USDT"))
	assert.Equal(t, q(60), l.HeldOf(1, "USDT"))

	require.NoError(t, l.Withdraw(1, "USDT", q(40)))
	assert.Equal(t, q(60), l.Balance(1, "USDT"))
}

func TestHold_ChecksAvailableNotBalance(t *testing.T) {
	l := ledger.New()
	require.NoError(t, l.Deposit(1, "USDT", q(100)))
	require.NoError(t, l.Hold(1, 1, "USDT", q(80)))

	// Available is now 20; a second hold for 30 must fail.
	err := l.Hold(2, 1, "USDT", q(30))
	assert.ErrorIs(t, err, ledger.ErrInsufficientFunds)

	// Available 20 worth of room is still there.
	require.NoError(t, l.Hold(2, 1, "USDT", q(20)))
	assert.Equal(t, q(100), l.HeldOf(1, "USDT"))
	assert.Equal(t, models.ZeroQty, l.Balance(1, "USDT").Sub(l.HeldOf(1, "USDT")))
}

func TestHold_IdempotentOnDuplicateID(t *testing.T) {
	l := ledger.New()
	require.NoError(t, l.Deposit(1, "USDT", q(100)))

	require.NoError(t, l.Hold(7, 1, "USDT", q(40)))
	// Same params -> no-op, no double-hold.
	require.NoError(t, l.Hold(7, 1, "USDT", q(40)))
	assert.Equal(t, q(40), l.HeldOf(1, "USDT"))
}

func TestHold_RejectsCollisionWithDifferentParams(t *testing.T) {
	l := ledger.New()
	require.NoError(t, l.Deposit(1, "USDT", q(100)))

	require.NoError(t, l.Hold(7, 1, "USDT", q(40)))

	// Same orderID but different amount: must error, not silently succeed.
	err := l.Hold(7, 1, "USDT", q(50))
	assert.Error(t, err)
}

func TestRelease_RestoresAvailable(t *testing.T) {
	l := ledger.New()
	require.NoError(t, l.Deposit(1, "USDT", q(100)))
	require.NoError(t, l.Hold(7, 1, "USDT", q(40)))

	require.NoError(t, l.Release(7))
	assert.Equal(t, models.ZeroQty, l.HeldOf(1, "USDT"))
	assert.Equal(t, q(100), l.Balance(1, "USDT"))
}

func TestRelease_IdempotentOnSecondCall(t *testing.T) {
	l := ledger.New()
	require.NoError(t, l.Deposit(1, "USDT", q(100)))
	require.NoError(t, l.Hold(7, 1, "USDT", q(40)))

	require.NoError(t, l.Release(7))
	require.NoError(t, l.Release(7), "second release should be a no-op")
	assert.Equal(t, models.ZeroQty, l.HeldOf(1, "USDT"))
}

func TestRelease_UnknownHoldErrors(t *testing.T) {
	l := ledger.New()
	err := l.Release(999)
	assert.ErrorIs(t, err, ledger.ErrHoldNotFound)
}

// End-to-end one-fill settlement at the ledger primitive level: holder's
// asset moves to taker. The clearing-level test covers the full
// two-leg spot trade (base ⇄ quote); here we just exercise the
// primitive.
func TestSettleFill_DebitsHolderCreditsTaker(t *testing.T) {
	l := ledger.New()

	// holder (user 1) has 100 USDT, holds 60 of it for some order.
	require.NoError(t, l.Deposit(1, "USDT", q(100)))
	require.NoError(t, l.Hold(42, 1, "USDT", q(60)))

	// Settle 25 USDT against the hold; counterparty (user 2) receives 25.
	require.NoError(t, l.SettleFill(42, 2, "USDT", q(25)))

	assert.Equal(t, q(75), l.Balance(1, "USDT"), "holder loses 25 from balance")
	assert.Equal(t, q(35), l.HeldOf(1, "USDT"), "holder's held drops by 25 (60 -> 35)")
	assert.Equal(t, q(25), l.Balance(2, "USDT"), "counterparty receives 25")
	assert.Equal(t, q(35), l.HoldOutstanding(42), "hold has 35 remaining")
}

func TestSettleFill_FullConsumptionMarksHoldSettled(t *testing.T) {
	l := ledger.New()
	require.NoError(t, l.Deposit(1, "USDT", q(50)))
	require.NoError(t, l.Hold(42, 1, "USDT", q(50)))

	require.NoError(t, l.SettleFill(42, 2, "USDT", q(50)))

	assert.Equal(t, models.ZeroQty, l.HoldOutstanding(42))

	// A second attempt against the same hold must fail rather than
	// silently double-spending (the hold is settled, not active).
	err := l.SettleFill(42, 2, "USDT", q(1))
	assert.ErrorIs(t, err, ledger.ErrAlreadySettled)
}

// ---- concurrency ----

// BenchmarkHold_PureMemory measures the in-memory ledger ceiling
// without any WAL involvement. This is what the per-core rate looks
// like when durability is handled separately (async WAL) or not at
// all (test deployments).
func BenchmarkHold_PureMemory(b *testing.B) {
	l := ledger.New()
	const users = 256
	for u := int64(1); u <= users; u++ {
		if err := l.Deposit(u, "USDT", q(1_000_000_000)); err != nil {
			b.Fatal(err)
		}
	}

	b.SetParallelism(16) // 256 concurrent producers
	b.ResetTimer()
	var counter atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := counter.Add(1)
			user := int64(1 + (id % users))
			if err := l.Hold(id, user, "USDT", models.Qty(1)); err != nil {
				continue // funds exhausted in this synthetic stream is fine
			}
		}
	})
}

// Many goroutines holding + releasing against many users must not race
// and must leave the ledger in a consistent state.
func TestConcurrent_HoldsAndReleasesAreSafe(t *testing.T) {
	l := ledger.New()
	const users = 64
	const opsPerUser = 200

	for u := int64(1); u <= users; u++ {
		require.NoError(t, l.Deposit(u, "USDT", q(1_000_000)))
	}

	var wg sync.WaitGroup
	wg.Add(int(users))
	for u := int64(1); u <= users; u++ {
		go func(user int64) {
			defer wg.Done()
			for i := 0; i < opsPerUser; i++ {
				orderID := int64(user*1_000_000 + int64(i))
				if err := l.Hold(orderID, user, "USDT", q(1)); err != nil {
					if !errors.Is(err, ledger.ErrInsufficientFunds) {
						t.Errorf("hold %d: %v", orderID, err)
					}
					continue
				}
				if i%2 == 0 {
					_ = l.Release(orderID)
				}
			}
		}(u)
	}
	wg.Wait()

	// Per user: ~half the holds released, ~half still active.
	for u := int64(1); u <= users; u++ {
		bal := l.Balance(u, "USDT")
		held := l.HeldOf(u, "USDT")
		assert.Equal(t, q(1_000_000), bal, "balance must not have drifted")
		assert.True(t, held.Lte(q(opsPerUser/2+1)),
			"held for user %d = %s should be roughly opsPerUser/2", u, held.String())
		assert.True(t, held.Gte(models.ZeroQty), "held never negative")
	}
}
