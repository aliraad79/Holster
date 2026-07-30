// Package ledger is Holster's in-memory hot ledger. It owns per-user
// per-asset balances and the holds reserved by open orders, and it is
// the source of truth in flight (the Postgres layer is rebuilt async
// from the WAL and is source of truth at rest).
//
// Concurrency: there are two independent sets of shards, each entry with
// its own sync.RWMutex. Accounts are sharded by user_id, holds by
// order_id. A single user's ops serialize against that user's shard;
// independent users run in parallel. Likewise for holds per order.
//
// Three lock-ordering rules keep this deadlock-free, and all three
// matter:
//
//  1. A hold shard is always acquired BEFORE any account shard, never
//     after. Deposit, Withdraw and the balance queries take only an
//     account shard; Hold, Release and SettleFill take a hold shard and
//     then account shards. Nothing may acquire them in the opposite
//     order.
//
//  2. No two hold shards are ever held at once. Every hold-lifecycle
//     call touches exactly one order_id, so hold shards cannot deadlock
//     against each other. A future operation spanning two holds must
//     order them by shard index, per rule 3.
//
//  3. When a settlement needs two account shards it takes them in
//     ascending shard-index order. Ordering by user id is NOT
//     sufficient: user -> shard is userID & shardMask, which is not
//     monotonic, so two settlements over different user pairs can want
//     the same two shards in opposite order. See lockShardPair.
//
// A hold is a spending limit, so each hold-lifecycle operation runs
// wholly under its hold-shard lock rather than checking the hold and
// then mutating it. An interleaving between those two steps lets
// concurrent callers spend the same headroom twice — 200 concurrent
// one-unit settles once drained a hundred-unit hold completely.
package ledger

import (
	"github.com/aliraad79/Gun/models"
)

// Account is one user's balance in one asset. Balance is the total
// owned; Held is the portion reserved by open orders. Available =
// Balance - Held.
//
// Both fields use Gun's scaled-int64 Qty so the wire format and the
// matching-engine arithmetic share a numeric type. Negative balances
// or negative held quantities are invariant violations checked by the
// ledger on every mutation.
type Account struct {
	Balance models.Qty
	Held    models.Qty

	// Version increments on every mutation. Exposed for optimistic
	// concurrency at the persister layer (the async Postgres writer
	// uses it as the WHERE clause guard).
	Version uint64
}

// Available returns the portion of Balance that is not currently
// reserved by open orders. This is the number that risk-checks compare
// against.
func (a *Account) Available() models.Qty {
	return a.Balance.Sub(a.Held)
}
