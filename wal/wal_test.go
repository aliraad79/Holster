package wal_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliraad79/Holster/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendReplay_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := wal.Open(path, wal.Options{
		MaxBatch:     16,
		MaxLatency:   5 * time.Millisecond,
		FsyncOnFlush: true,
	})
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		require.NoError(t, w.Append([]byte(fmt.Sprintf("record-%d", i))))
	}
	require.NoError(t, w.Close())

	var got [][]byte
	require.NoError(t, wal.Replay(path, func(p []byte) error {
		// copy because the slice may be reused under us
		got = append(got, append([]byte(nil), p...))
		return nil
	}))

	require.Len(t, got, 5)
	for i, payload := range got {
		assert.Equal(t, fmt.Sprintf("record-%d", i), string(payload))
	}
}

// Concurrent appends from many producers all share fsyncs via the
// flusher. After Close, every successful Append must show up in Replay
// in *some* order (we don't promise total ordering across producers).
func TestConcurrent_GroupCommitFlushesEveryRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := wal.Open(path, wal.Options{
		MaxBatch:     64,
		MaxLatency:   2 * time.Millisecond,
		FsyncOnFlush: true,
	})
	require.NoError(t, err)

	const producers = 16
	const perProducer = 200

	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func(pid int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				record := fmt.Sprintf("p%02d-%04d", pid, i)
				assert.NoError(t, w.Append([]byte(record)))
			}
		}(p)
	}
	wg.Wait()
	require.NoError(t, w.Close())

	var count int
	require.NoError(t, wal.Replay(path, func(p []byte) error {
		count++
		return nil
	}))
	assert.Equal(t, producers*perProducer, count,
		"every successful Append must round-trip through Replay")
}

// Group commit measurably amortizes fsyncs. With MaxBatch=1 (one
// fsync per Append) we should be slow; with MaxBatch=128 we should
// be substantially faster. This isn't a strict number guarantee —
// it's a smoke test that the batching does *something*.
func TestGroupCommit_BatchingHelpsThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput check under -short")
	}
	const n = 2000

	measure := func(batch int) time.Duration {
		path := filepath.Join(t.TempDir(), "x.wal")
		w, err := wal.Open(path, wal.Options{
			MaxBatch:     batch,
			MaxLatency:   500 * time.Microsecond,
			FsyncOnFlush: true,
		})
		require.NoError(t, err)

		start := time.Now()
		var wg sync.WaitGroup
		wg.Add(16)
		for p := 0; p < 16; p++ {
			go func() {
				defer wg.Done()
				for i := 0; i < n/16; i++ {
					_ = w.Append(bytes.Repeat([]byte{'x'}, 64))
				}
			}()
		}
		wg.Wait()
		elapsed := time.Since(start)
		require.NoError(t, w.Close())
		return elapsed
	}

	noBatch := measure(1)
	withBatch := measure(128)
	t.Logf("MaxBatch=1:   %s  for %d ops", noBatch, n)
	t.Logf("MaxBatch=128: %s  for %d ops", withBatch, n)

	// Generous bound: batching should at least double throughput. In
	// practice it's 10–100×, but CI machines vary.
	assert.True(t, withBatch < noBatch/2,
		"batching should at least 2x throughput; got noBatch=%s withBatch=%s",
		noBatch, withBatch)
}

// Replay handles a torn-write tail gracefully — a half-written final
// record at the end of the file should stop the replay cleanly
// without erroring.
func TestReplay_TornTailIsNotAnError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "torn.wal")

	// Write one complete record + truncated bytes by hand. Bypass the
	// WAL package entirely to construct the failure.
	full := []byte{0x00, 0x00, 0x00, 0x05, 'h', 'e', 'l', 'l', 'o'}
	torn := []byte{0x00, 0x00, 0x00, 0x0A, 'w', 'o', 'r'} // header says 10 bytes, only 3 present

	combined := append([]byte{}, full...)
	combined = append(combined, torn...)
	require.NoError(t, os.WriteFile(path, combined, 0o644))

	var got [][]byte
	require.NoError(t, wal.Replay(path, func(p []byte) error {
		got = append(got, append([]byte(nil), p...))
		return nil
	}))
	require.Len(t, got, 1, "only the intact record should be returned")
	assert.Equal(t, "hello", string(got[0]))
}

// BenchmarkAppend_Sync measures the cost of the durable-on-ack path:
// producer blocks until the batch is fsynced. This is the bound for
// operations that must not be lost on crash (the pre-trade hold leg).
//
// On a 13th-gen Intel laptop NVMe with MaxBatch=256, expect ~120k
// system ops/sec — fsync rate is the floor (~1200 fsyncs/sec) × batch
// fill (capped by the number of producers in flight, not MaxBatch).
func BenchmarkAppend_Sync(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.wal")
	w, err := wal.Open(path, wal.Options{
		MaxBatch:     256,
		MaxLatency:   200 * time.Microsecond,
		FsyncOnFlush: true,
	})
	require.NoError(b, err)
	defer w.Close()

	payload := bytes.Repeat([]byte{'x'}, 96)

	b.SetParallelism(16)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := w.Append(payload); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkAppend_Async measures the fire-and-forget path: producer
// returns the moment its record is queued, the flusher catches up in
// the background. This is the bound for high-throughput producers
// that are happy with bounded loss on crash (most non-regulated
// fintech systems run this way under the hood).
//
// On the same hardware, expect 5M–10M+ system ops/sec — the producer
// cost is just a channel send.
func BenchmarkAppend_Async(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.wal")
	w, err := wal.Open(path, wal.Options{
		MaxBatch:     1024,
		MaxLatency:   200 * time.Microsecond,
		FsyncOnFlush: true, // flusher still fsyncs; producer doesn't wait
	})
	require.NoError(b, err)
	defer w.Close()

	payload := bytes.Repeat([]byte{'x'}, 96)

	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := w.AppendAsync(payload); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Compile-time check that the FsyncOnFlush=false path does not panic
// (it has its own code path skipping the Sync call).
func TestFsyncOff_DoesNotPanic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nofsync.wal")
	w, err := wal.Open(path, wal.Options{
		MaxBatch:     32,
		MaxLatency:   time.Millisecond,
		FsyncOnFlush: false,
	})
	require.NoError(t, err)

	for i := 0; i < 50; i++ {
		require.NoError(t, w.Append([]byte("nofsync")))
	}
	require.NoError(t, w.Close())

	var counter atomic.Int64
	require.NoError(t, wal.Replay(path, func(_ []byte) error {
		counter.Add(1)
		return nil
	}))
	assert.EqualValues(t, 50, counter.Load())
}
