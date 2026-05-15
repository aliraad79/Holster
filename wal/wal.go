// Package wal is Holster's write-ahead log. It groups concurrent Append
// calls into batched fsyncs so the per-op cost amortizes: one fsync of
// ~20–50 µs covers the whole batch, which lets the system hit the
// >1M ops/sec/core target Holster is designed for.
//
// File format (binary, big-endian, no header):
//
//	[len:uint32][payload bytes] [len:uint32][payload bytes] ...
//
// Each Append's payload is opaque to the WAL — typically JSON or
// protobuf serialized at a higher layer. The WAL only guarantees that
// once Append returns nil, the bytes are durably on disk.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Options tune the throughput / latency tradeoff. Group-commit is
// active when MaxBatch > 1; setting MaxBatch = 1 reduces to
// fsync-per-append (lower throughput, lower latency floor).
type Options struct {
	// MaxBatch is the largest number of Appends that will share a
	// single fsync. Default 128 if zero. Higher values trade latency
	// (the first append in a batch waits longer) for throughput.
	MaxBatch int

	// MaxLatency is the longest a single Append will wait before the
	// flusher gives up on filling the batch and fsyncs what it has.
	// Default 1ms if zero. Lower values protect tail latency at low
	// load; higher values give better throughput under saturation.
	MaxLatency time.Duration

	// FsyncOnFlush forces fsync after each batch write. Default true.
	// Setting it false buys ~10x throughput at the cost of "the last
	// few seconds of trades evaporate on power loss" — only use this
	// for tests or in environments where OS-level durability
	// (battery-backed cache, replicated storage) makes fsync
	// redundant.
	FsyncOnFlush bool
}

// WAL is a single append-only file with group-commit semantics.
type WAL struct {
	path string
	opts Options

	mu     sync.Mutex // guards close + file rotation only (NOT writes; those go through reqs)
	file   *os.File

	reqs    chan request
	closed  chan struct{}
	flushed chan struct{} // signals flusher goroutine exit

	closeOnce sync.Once
}

type request struct {
	payload []byte
	done    chan error
}

// Open creates or appends to the WAL at the given path. The parent
// directory is created if missing.
func Open(path string, opts Options) (*WAL, error) {
	if path == "" {
		return nil, errors.New("wal: empty path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	if opts.MaxBatch == 0 {
		opts.MaxBatch = 128
	}
	if opts.MaxLatency == 0 {
		opts.MaxLatency = time.Millisecond
	}

	w := &WAL{
		path:    path,
		opts:    opts,
		file:    f,
		reqs:    make(chan request, 4096),
		closed:  make(chan struct{}),
		flushed: make(chan struct{}),
	}
	go w.flusher()
	return w, nil
}

// Append blocks until the payload is durably on disk (or
// FsyncOnFlush=false has decided otherwise). Safe for concurrent use
// across goroutines — that is the entire point of this package; the
// flusher batches calls from many producers into one fsync.
func (w *WAL) Append(payload []byte) error {
	req := request{payload: payload, done: make(chan error, 1)}
	select {
	case w.reqs <- req:
	case <-w.closed:
		return errors.New("wal: closed")
	}
	return <-req.done
}

// Close stops the flusher and closes the underlying file. After Close,
// Append returns an error. Pending requests in the queue are flushed
// before Close returns.
func (w *WAL) Close() error {
	var firstErr error
	w.closeOnce.Do(func() {
		close(w.closed)
		<-w.flushed
		w.mu.Lock()
		defer w.mu.Unlock()
		if err := w.file.Close(); err != nil {
			firstErr = err
		}
	})
	return firstErr
}

// flusher is the only writer to w.file. All Append requests funnel
// through w.reqs and the flusher groups them into batches.
func (w *WAL) flusher() {
	defer close(w.flushed)

	bw := bufio.NewWriterSize(w.file, 256*1024)
	batch := make([]request, 0, w.opts.MaxBatch)

	for {
		// Block for the first request of a new batch (or shutdown).
		var first request
		select {
		case first = <-w.reqs:
		case <-w.closed:
			// Drain anything still queued so producers don't lose
			// records that were already in flight.
			w.drainAndFlush(bw)
			return
		}
		batch = append(batch[:0], first)

		// Collect more requests until either:
		//   (a) the batch hits MaxBatch, or
		//   (b) MaxLatency passes since the first request, or
		//   (c) the WAL is closed.
		deadline := time.NewTimer(w.opts.MaxLatency)
	collect:
		for len(batch) < w.opts.MaxBatch {
			select {
			case r := <-w.reqs:
				batch = append(batch, r)
			case <-deadline.C:
				break collect
			case <-w.closed:
				break collect
			}
		}
		deadline.Stop()

		w.flushBatch(bw, batch)

		// After a Close-induced exit from the inner loop, drain and exit.
		select {
		case <-w.closed:
			w.drainAndFlush(bw)
			return
		default:
		}
	}
}

func (w *WAL) drainAndFlush(bw *bufio.Writer) {
	batch := make([]request, 0, 128)
	for {
		select {
		case r := <-w.reqs:
			batch = append(batch, r)
		default:
			if len(batch) > 0 {
				w.flushBatch(bw, batch)
			}
			return
		}
		if len(batch) >= w.opts.MaxBatch {
			w.flushBatch(bw, batch)
			batch = batch[:0]
		}
	}
}

// flushBatch writes every request in the batch into the buffered
// writer, flushes to the OS, fsyncs, then ack's every requester.
//
// Error handling: any write/flush/sync error fails every requester in
// the batch with the same error. There is no partial-success
// semantics — either the whole batch is durable or none of it is.
func (w *WAL) flushBatch(bw *bufio.Writer, batch []request) {
	if len(batch) == 0 {
		return
	}

	var header [4]byte
	var werr error
	for _, r := range batch {
		binary.BigEndian.PutUint32(header[:], uint32(len(r.payload)))
		if _, err := bw.Write(header[:]); err != nil {
			werr = err
			break
		}
		if _, err := bw.Write(r.payload); err != nil {
			werr = err
			break
		}
	}
	if werr == nil {
		if err := bw.Flush(); err != nil {
			werr = err
		}
	}
	if werr == nil && w.opts.FsyncOnFlush {
		if err := w.file.Sync(); err != nil {
			werr = err
		}
	}
	for _, r := range batch {
		r.done <- werr
	}
}

// Replay reads the WAL from the beginning and calls fn for each
// payload, in order. Stops on the first error returned by fn or on a
// corrupt record (truncated header, payload shorter than header
// indicates).
//
// Missing file is not an error: a fresh process with no WAL yet
// starts from an empty state. Replay is read-only and safe to run
// concurrently with Append (the underlying file is open for append).
func Replay(path string, fn func(payload []byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("wal: replay open %s: %w", path, err)
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 256*1024)
	var header [4]byte
	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				// A torn write left a partial header at the tail. Stop
				// cleanly rather than failing — the records up to here
				// are still valid.
				return nil
			}
			return fmt.Errorf("wal: read header: %w", err)
		}
		size := binary.BigEndian.Uint32(header[:])
		payload := make([]byte, size)
		if _, err := io.ReadFull(r, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// Same torn-write story for the payload tail.
				return nil
			}
			return fmt.Errorf("wal: read payload: %w", err)
		}
		if err := fn(payload); err != nil {
			return err
		}
	}
}

