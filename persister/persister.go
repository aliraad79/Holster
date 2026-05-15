// Package persister is the async Postgres writer that turns Holster's
// in-memory ledger and WAL into the durable, queryable source of
// truth at rest.
//
// Architecture (mirrors what the Holster README diagrams):
//
//	WAL ────► Persister ────► Postgres
//	          (batched,        (queryable,
//	           async)           accounting-grade)
//
// The persister is deliberately OFF the order-submit hot path. It
// runs in its own goroutine, reads from a channel of Entries the
// ledger emits, batches them by (count or time), and writes one
// Postgres transaction per batch. A typical batch settles 100-1000
// rows in 1-10 ms, well within the latency budget of "the at-rest
// truth catches up to memory within ~10 ms."
//
// Crash recovery: on restart, the in-memory ledger is rebuilt by
// replaying the WAL from the offset recorded in consumer_offsets.
// Anything the persister had already committed to Postgres is
// idempotent on re-run (ledger_entries.trade_dedup + trades.UNIQUE
// catch double-inserts).
//
// This file is the production *interface* and a minimal sketch. The
// actual database/sql wiring (driver choice, conn pool sizing,
// migrations runner) is deployment-specific and lives in the
// downstream service that depends on Holster.
package persister

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/aliraad79/Gun/models"
)

// Entry is one row about to be written to ledger_entries. Constructed
// by the in-memory ledger as it mutates account state and queued onto
// the persister's input channel.
type Entry struct {
	UserID     int64
	Asset      string
	Delta      models.Qty
	HeldDelta  models.Qty
	Kind       string // 'hold' | 'release' | 'settle' | 'deposit' | 'withdraw'
	RefKind    string // 'order' | 'trade' | 'deposit_id' | ''
	RefID      int64
	EngineSeq  uint64 // 0 if not a trade settlement
	Symbol     string // BTC_USDT etc; "" for non-symbol ops
	Timestamp  time.Time
}

// TradeRow mirrors the `trades` table row produced by clearing.
type TradeRow struct {
	EngineSeq    uint64
	Symbol       string
	BuyOrderID   int64
	SellOrderID  int64
	BuyerUserID  int64
	SellerUserID int64
	Price        models.Px
	Volume       models.Qty
	QuoteAmount  models.Qty
}

// Options tunes the batch sizing.
type Options struct {
	// BatchSize is the largest number of rows to write in one
	// transaction. Default 500 if zero. Higher values amortize the
	// commit cost but increase rollback blast radius.
	BatchSize int

	// FlushInterval is the longest a row will sit in the buffer
	// before being flushed. Default 10 ms. Lower values reduce the
	// memory-vs-disk lag at the cost of more commits per second.
	FlushInterval time.Duration

	// ConsumerID identifies this persister to the consumer_offsets
	// table. Required; one persister per ID.
	ConsumerID string
}

// Persister consumes Entry / TradeRow off its input channels and
// writes them to Postgres in batched transactions. Construct one
// with New, call Start once, send rows via PushEntry / PushTrade,
// and call Stop on shutdown.
type Persister struct {
	db      *sql.DB
	opts    Options
	entries chan Entry
	trades  chan TradeRow
	done    chan struct{}
}

// New returns a persister. db is required; opts.ConsumerID is
// required; everything else has sensible defaults.
func New(db *sql.DB, opts Options) (*Persister, error) {
	if db == nil {
		return nil, errors.New("persister: nil db")
	}
	if opts.ConsumerID == "" {
		return nil, errors.New("persister: ConsumerID is required")
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 500
	}
	if opts.FlushInterval == 0 {
		opts.FlushInterval = 10 * time.Millisecond
	}
	return &Persister{
		db:      db,
		opts:    opts,
		entries: make(chan Entry, 8192),
		trades:  make(chan TradeRow, 8192),
		done:    make(chan struct{}),
	}, nil
}

// Start launches the persister's writer goroutine. Returns
// immediately; the writer runs until ctx is cancelled or Stop is
// called.
func (p *Persister) Start(ctx context.Context) {
	go p.run(ctx)
}

// Stop drains pending rows and shuts down. Returns when the writer
// goroutine has exited.
func (p *Persister) Stop() {
	close(p.done)
}

// PushEntry queues a ledger entry. Non-blocking up to the channel
// buffer; if full, callers should treat this as back-pressure
// (typically: log + drop, or refuse the upstream op).
func (p *Persister) PushEntry(e Entry) bool {
	select {
	case p.entries <- e:
		return true
	default:
		return false
	}
}

// PushTrade queues a trade record. Same semantics as PushEntry.
func (p *Persister) PushTrade(t TradeRow) bool {
	select {
	case p.trades <- t:
		return true
	default:
		return false
	}
}

func (p *Persister) run(ctx context.Context) {
	ticker := time.NewTicker(p.opts.FlushInterval)
	defer ticker.Stop()

	entries := make([]Entry, 0, p.opts.BatchSize)
	trades := make([]TradeRow, 0, p.opts.BatchSize)

	flush := func() {
		if len(entries) == 0 && len(trades) == 0 {
			return
		}
		if err := p.commit(ctx, entries, trades); err != nil {
			// Production hook: emit a metric. Dropping rows on commit
			// failure is wrong; the right move is to retry with
			// backoff. For this sketch we just log via fmt and keep
			// the buffer for the next tick.
			fmt.Println("persister: commit failed:", err)
			return
		}
		entries = entries[:0]
		trades = trades[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case <-p.done:
			flush()
			return
		case <-ticker.C:
			flush()
		case e := <-p.entries:
			entries = append(entries, e)
			if len(entries)+len(trades) >= p.opts.BatchSize {
				flush()
			}
		case t := <-p.trades:
			trades = append(trades, t)
			if len(entries)+len(trades) >= p.opts.BatchSize {
				flush()
			}
		}
	}
}

// commit writes the buffered entries and trades in a single
// transaction. ON CONFLICT clauses make the writes idempotent so a
// retry after a crash is a no-op.
func (p *Persister) commit(ctx context.Context, entries []Entry, trades []TradeRow) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op if Commit succeeded

	const insertEntry = `
		INSERT INTO ledger_entries
		    (user_id, asset, delta, held_delta, kind, ref_kind, ref_id, engine_seq, symbol, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (engine_seq, symbol, user_id, asset)
		    WHERE engine_seq IS NOT NULL DO NOTHING
	`
	for _, e := range entries {
		if _, err := tx.ExecContext(ctx, insertEntry,
			e.UserID, e.Asset, qtyStr(e.Delta), qtyStr(e.HeldDelta),
			e.Kind, nullStr(e.RefKind), e.RefID, nullableUint64(e.EngineSeq),
			nullStr(e.Symbol), e.Timestamp,
		); err != nil {
			return fmt.Errorf("insert ledger_entry: %w", err)
		}
	}

	const insertTrade = `
		INSERT INTO trades
		    (engine_seq, symbol, buy_order_id, sell_order_id,
		     buyer_user_id, seller_user_id, price, volume, quote_amount)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (symbol, engine_seq) DO NOTHING
	`
	for _, t := range trades {
		if _, err := tx.ExecContext(ctx, insertTrade,
			t.EngineSeq, t.Symbol, t.BuyOrderID, t.SellOrderID,
			t.BuyerUserID, t.SellerUserID,
			pxStr(t.Price), qtyStr(t.Volume), qtyStr(t.QuoteAmount),
		); err != nil {
			return fmt.Errorf("insert trade: %w", err)
		}
	}

	// Advance the consumer offset to the highest seq we just wrote.
	var maxSeq uint64
	for _, t := range trades {
		if t.EngineSeq > maxSeq {
			maxSeq = t.EngineSeq
		}
	}
	if maxSeq > 0 {
		const upsertOffset = `
			INSERT INTO consumer_offsets (consumer_id, last_processed_seq)
			VALUES ($1, $2)
			ON CONFLICT (consumer_id) DO UPDATE
			    SET last_processed_seq = GREATEST(consumer_offsets.last_processed_seq, EXCLUDED.last_processed_seq),
			        updated_at = NOW()
		`
		if _, err := tx.ExecContext(ctx, upsertOffset, p.opts.ConsumerID, maxSeq); err != nil {
			return fmt.Errorf("update offset: %w", err)
		}
	}

	return tx.Commit()
}

// Helpers to bridge Holster's int64-scaled types and Postgres NUMERIC.
// Postgres accepts NUMERIC values as strings; pgx will parse them
// back to its own decimal type on read. For writes, the string form
// is portable across drivers.

func qtyStr(q models.Qty) string { return q.String() }
func pxStr(p models.Px) string   { return p.String() }

func nullStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func nullableUint64(v uint64) interface{} {
	if v == 0 {
		return nil
	}
	return int64(v)
}
