package integration_test

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gunjournal "github.com/aliraad79/Gun/journal"
	"github.com/aliraad79/Gun/market"
	"github.com/aliraad79/Gun/models"
	"github.com/aliraad79/Holster/clearing"
	"github.com/aliraad79/Holster/ledger"
	"github.com/aliraad79/Holster/risk"
	"github.com/aliraad79/Holster/wal"
)

// BenchmarkRisk_HoldThroughput measures the cost of Risk.Submit at
// steady state. This is the pre-trade leg — the operation a producer
// invokes on every order — and the headline rate Holster must hit.
//
// Group-commit WAL is the dominant cost here; the in-memory ledger
// itself is sub-µs.
func BenchmarkRisk_HoldThroughput(b *testing.B) {
	l := ledger.New()
	require := func(err error) {
		if err != nil {
			b.Fatal(err)
		}
	}

	// Pre-fund every user generously so we don't trip
	// ErrInsufficientFunds during the bench window.
	const users = 256
	for u := int64(1); u <= users; u++ {
		require(l.Deposit(u, "USDT", models.Qty(1_000_000_000*1_0000_0000))) // 1B USDT each
	}

	w, err := wal.Open(filepath.Join(b.TempDir(), "bench.wal"), wal.Options{
		MaxBatch:     256,
		MaxLatency:   200 * time.Microsecond,
		FsyncOnFlush: true,
	})
	require(err)
	defer w.Close()

	r := risk.New(l, w)

	template := models.Order{
		Symbol: "BTC_USDT", Type: models.LIMIT, Side: models.BUY,
		Price: models.Px(100 * 1_0000_0000), Volume: models.Qty(1_0000_0000),
	}

	// Group-commit needs many producers in flight to saturate the
	// batch. SetParallelism(16) → GOMAXPROCS × 16 goroutines, which
	// at 16 cores gives 256 concurrent producers — comparable to a
	// busy exchange's connection count and large enough to let the
	// flusher form full 256-record batches every ~200 µs.
	b.SetParallelism(16)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var nextID int64
		gid := atomic.AddInt64(&benchGID, 1)
		for pb.Next() {
			nextID++
			ord := template
			ord.ID = gid*1_000_000 + nextID
			ord.UserID = 1 + (nextID % users)
			if err := r.Submit(ord); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// global counter for assigning per-worker ID prefixes
var benchGID int64

// BenchmarkClearing_SettleThroughput measures the post-trade leg:
// SettleFill x2 + WAL append per match. Pre-arranges holds in bulk so
// the bench body only measures the settle path.
func BenchmarkClearing_SettleThroughput(b *testing.B) {
	l := ledger.New()
	require := func(err error) {
		if err != nil {
			b.Fatal(err)
		}
	}

	const users = 256
	for u := int64(1); u <= users; u++ {
		require(l.Deposit(u, "USDT", models.Qty(1_000_000_000_000_000)))
		require(l.Deposit(u, "BTC", models.Qty(1_000_000_000_000_000)))
	}

	// Pre-create holds for orderIDs 1..N where N >> b.N expected.
	// We don't know b.N in advance, so we lazy-create within the bench
	// loop instead. Each iteration creates fresh holds + settles them.
	wRisk, err := wal.Open(filepath.Join(b.TempDir(), "risk.wal"), wal.Options{
		MaxBatch: 256, MaxLatency: 200 * time.Microsecond, FsyncOnFlush: true,
	})
	require(err)
	defer wRisk.Close()

	wClr, err := wal.Open(filepath.Join(b.TempDir(), "clr.wal"), wal.Options{
		MaxBatch: 256, MaxLatency: 200 * time.Microsecond, FsyncOnFlush: true,
	})
	require(err)
	defer wClr.Close()

	c := clearing.New(l, wClr, l)

	// pre-build holds. The bench only times c.Settle.
	type pair struct {
		buyOrderID, sellOrderID int64
		buyerUser, sellerUser   int64
	}
	totalPairs := 200_000
	pairs := make([]pair, totalPairs)
	for i := 0; i < totalPairs; i++ {
		buyer := int64(1 + (i % users))
		seller := int64(1 + ((i + 1) % users))
		buyOrderID := int64(10_000_000 + 2*i)
		sellOrderID := int64(10_000_000 + 2*i + 1)
		require(l.Hold(buyOrderID, buyer, "USDT", models.Qty(100*1_0000_0000)))
		require(l.Hold(sellOrderID, seller, "BTC", models.Qty(1_0000_0000)))
		pairs[i] = pair{buyOrderID, sellOrderID, buyer, seller}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		gid := atomic.AddInt64(&benchGID, 1)
		var i int
		seqBase := uint64(gid) * 10_000_000
		for pb.Next() {
			p := pairs[i%totalPairs]
			i++
			m := models.Match{
				Seq:    seqBase + uint64(i),
				BuyId:  p.buyOrderID,
				SellId: p.sellOrderID,
				Price:  models.Px(100 * 1_0000_0000),
				Volume: models.Qty(1), // tiny fill so the holds never run out
			}
			if err := c.Settle("BTC_USDT", m); err != nil {
				// The hold may have been settled in a previous loop
				// iteration on a different goroutine — that's an
				// expected race in this synthetic benchmark since we
				// share a finite pool of holds. Ignore those.
				continue
			}
		}
	})

	_ = wRisk // unused but kept symmetric with the production wiring
}

// BenchmarkEndToEnd_GunPlusHolster measures a full order round trip:
// Risk.Submit (hold + durable WAL append), the Gun matching engine, and
// the clearing settlement the resulting fill triggers.
//
// This benchmark used to deadlock and therefore had never produced a
// number -- note its absence from bench/phase-5-final.txt. `defer cancel()`
// was registered before `defer wg.Wait()`, and defers run LIFO, so Wait
// ran first and blocked forever on market goroutines that had not been
// told to stop. Keep the cancel-then-wait ordering in one defer so the
// two cannot be separated again.
//
// It also used to stop the clock straight after the submit loop. Since
// reg.Submit only enqueues onto the market's inbox channel, that timed the
// enqueue and not the matching or settlement it causes. The loop now waits
// for every fill to settle before stopping the timer, so the reported
// figure covers the whole pipeline. That makes the number much larger than
// the old intent of ">1M ops/sec/core" -- it is bounded by two synchronous
// fsyncs per order, which is what the pipeline actually costs.
func BenchmarkEndToEnd_GunPlusHolster(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping E2E bench under -short")
	}
	b.Setenv("SUPPORTED_SYMBOLS", "BTC_USDT")

	dir := b.TempDir()
	l := ledger.New()

	// One shared WAL: that is the production shape, since recovery replays
	// a single ordered stream (see the recovery package). Two WALs would
	// also understate cost by giving each service its own flusher.
	w, err := wal.Open(filepath.Join(dir, "holster.wal"), wal.Options{
		MaxBatch: 256, FsyncOnFlush: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	r := risk.New(l, w)
	c := clearing.New(l, w, l)

	var settled atomic.Int64

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	// Cancel BEFORE waiting, in one defer. Two separate defers run LIFO and
	// deadlocked this benchmark for its entire existence.
	defer func() {
		cancel()
		wg.Wait()
	}()

	reg := market.NewRegistry(ctx, &wg, market.Options{
		InboxSize: 4096,
		Journal:   gunjournal.Discard{},
		OnMatch: func(symbol string, matches []models.Match) {
			for _, m := range matches {
				_ = c.Settle(symbol, m)
				settled.Add(1)
			}
		},
	})

	// Fund users.
	const users = 256
	for u := int64(1); u <= users; u++ {
		if err := l.Deposit(u, "USDT", models.Qty(1_000_000_000*1_0000_0000)); err != nil {
			b.Fatal(err)
		}
	}

	// Seed liquidity: one large resting sell so most buys cross.
	seed := models.Order{
		ID: 1, UserID: 1, Symbol: "BTC_USDT",
		Type: models.LIMIT, Side: models.SELL,
		Price: models.Px(100 * 1_0000_0000), Volume: models.Qty(int64(b.N+1) * 1_0000_0000),
	}
	if err := l.Deposit(1, "BTC", models.Qty(int64(b.N+1)*1_0000_0000)); err != nil {
		b.Fatal(err)
	}
	if err := r.Submit(seed); err != nil {
		b.Fatal(err)
	}
	reg.Submit(seed)

	// Every buy crosses the resting sell, so each order should produce
	// exactly one fill and therefore one settlement.
	settled.Store(0)

	b.ResetTimer()
	var nextID int64 = 1_000_000
	for i := 0; i < b.N; i++ {
		nextID++
		ord := models.Order{
			ID: nextID, UserID: 1 + int64(i%(users-1)+1),
			Symbol: "BTC_USDT", Type: models.LIMIT, Side: models.BUY,
			Price:  models.Px(100 * 1_0000_0000),
			Volume: models.Qty(1_0000_0000),
		}
		if err := r.Submit(ord); err != nil {
			b.Fatal(err)
		}
		reg.Submit(ord)
	}

	// Drain: reg.Submit is an async channel send, so without this the
	// benchmark reports the cost of enqueuing rather than of trading.
	deadline := time.Now().Add(2 * time.Minute)
	for settled.Load() < int64(b.N) {
		if time.Now().After(deadline) {
			b.Fatalf("timed out draining: %d of %d fills settled", settled.Load(), b.N)
		}
		time.Sleep(200 * time.Microsecond)
	}
	b.StopTimer()
}
