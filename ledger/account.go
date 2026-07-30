// Package ledger is Holster's in-memory hot ledger. It owns per-user
// per-asset balances and the holds reserved by open orders, and it is
// the source of truth in flight (the Postgres layer is rebuilt async
// from the WAL and is source of truth at rest).
//
// Concurrency: the ledger is sharded by user_id with one sync.RWMutex
// per shard. A single user's ops serialize against that user's shard;
// independent users run in parallel. Separately, one holdsMu guards the
// order_id -> hold map.
//
// Two lock-ordering rules keep this deadlock-free, and both matter:
//
//  1. holdsMu is always acquired BEFORE any shard lock, never after.
//     Deposit, Withdraw and the balance queries take only a shard lock;
//     Hold, Release and SettleFill take holdsMu and then a shard lock.
//     Nothing may acquire them in the opposite order.
//
//  2. When a settlement needs two shards it takes them in ascending
//     shard-index order. Ordering by user id is NOT sufficient: user ->
//     shard is userID & shardMask, which is not monotonic, so two
//     settlements over different user pairs can want the same two
//     shards in opposite order. See lockShardPair.
//
// A hold is a spending limit, so the hold-lifecycle operations run
// wholly under holdsMu rather than checking the hold and then mutating
// it: an interleaving between those two steps lets concurrent callers
// spend the same headroom twice. This makes holdsMu a global
// serialization point for Hold/Release/SettleFill, which is a known
// throughput ceiling — sharding the holds map by order_id is the way
// out, but correctness comes first.
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
