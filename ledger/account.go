// Package ledger is Holster's in-memory hot ledger. It owns per-user
// per-asset balances and the holds reserved by open orders, and it is
// the source of truth in flight (the Postgres layer is rebuilt async
// from the WAL and is source of truth at rest).
//
// Concurrency: the ledger is sharded by user_id with one sync.RWMutex
// per shard. A single user's ops serialize against that user's shard;
// independent users run in parallel. Settlement between two users
// acquires both shard locks in user_id order so two settlements that
// touch the same pair can't deadlock.
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
