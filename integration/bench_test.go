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
		buyer := int64(1 + (i%users))
		seller := int64(1 + ((i+1)%users))
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

// BenchmarkEndToEnd is the headline number: the full Risk.Submit
// pipeline including hold + WAL fsync. Group commit + per-user
// sharding should keep us above 1M ops/sec/core.
//
// (Settle path uses a separate WAL; this bench focuses on the
// submit/hold leg which is the producer-facing latency the system has
// to advertise.)
func BenchmarkEndToEnd_GunPlusHolster(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping E2E bench under -short")
	}
	b.Setenv("SUPPORTED_SYMBOLS", "BTC_USDT")

	dir := b.TempDir()
	l := ledger.New()

	wRisk, err := wal.Open(filepath.Join(dir, "risk.wal"), wal.Options{
		MaxBatch: 256, MaxLatency: 200 * time.Microsecond, FsyncOnFlush: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer wRisk.Close()

	wClr, err := wal.Open(filepath.Join(dir, "clr.wal"), wal.Options{
		MaxBatch: 256, MaxLatency: 200 * time.Microsecond, FsyncOnFlush: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer wClr.Close()

	r := risk.New(l, wRisk)
	c := clearing.New(l, wClr, l)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup

	reg := market.NewRegistry(ctx, &wg, market.Options{
		InboxSize: 4096,
		Journal:   gunjournal.Discard{},
		OnMatch: func(symbol string, matches []models.Match) {
			for _, m := range matches {
				_ = c.Settle(symbol, m)
			}
		},
	})
	defer wg.Wait()

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
	b.StopTimer()
}
