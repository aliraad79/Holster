# Holster

> The clearing and wallet companion to [Gun](https://github.com/aliraad79/Gun) — what holds the funds while the gun fires.

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Holster owns everything Gun deliberately doesn't:

- **Pre-trade risk** — checks that an order's owner actually has the funds, holds them, forwards the order to Gun.
- **In-memory ledger** — per-user-per-asset balances and holds, sharded for parallel access.
- **Write-ahead log** — group-commit batched fsync. Source of truth in flight.
- **Post-trade clearing** — consumes Gun's match events, settles each trade against the ledger atomically.
- **Postgres source of truth at rest** — rebuilt asynchronously from the WAL, NEVER on the order path.

The split is the entire point: Gun is a matching engine, period. Holster is a clearing/wallet service. Together they form a complete spot-exchange backend — but each can be deployed, scaled, and reasoned about independently.

```
producer ─► risk svc ─► Gun (match)  ─►  clearing svc ─► wallet/position store
              ▲                  │              │
              │                  │              ▼
              └── credit ◄───────┴── match events ──► event log (WAL)
                  state                              │
                                                     ▼
                                              Postgres (queryable,
                                               accounting-grade)
```

---

## Status

| Component | Status |
|---|---|
| In-memory ledger (sharded, race-safe) | ✅ |
| Group-commit WAL with sync + async paths | ✅ |
| Risk service (hold + WAL + idempotent submit) | ✅ |
| Clearing service (match consumer + atomic 2-leg settle) | ✅ |
| End-to-end integration with Gun | ✅ |
| Durable deposits / withdrawals (`funding`) | ✅ |
| WAL recovery — rebuild the ledger from the log on restart | ✅ |
| Postgres schema + async persister (interface only; not wired to a driver) | 🚧 |

---

## Architecture

Three layers, three durability stories:

| Layer | Source of truth | Latency | Throughput |
|---|---|---|---|
| In-memory ledger | …in flight | ~120 ns/op | ~8M ops/sec aggregate |
| WAL (async) | …catching up | ~1.6 µs/op | ~640k ops/sec aggregate |
| WAL (sync, durable-on-ack) | …durable | ~23 µs/op | ~43k ops/sec aggregate |
| Postgres | …at rest, queryable | 1–10 ms (off the order path) | (irrelevant to order-submit) |

Throughput figures are **aggregate across all cores**, not per core — the
benchmarks behind them use `b.RunParallel`. Divide by your core count for
a per-core number.

**Postgres is never on the order-submit hot path.** Risk reads in-memory and writes the WAL; that's the user-facing critical path. The async persister drains the WAL into Postgres in batched transactions; if Postgres is slow or down for a few minutes, the exchange keeps trading and catches up after.

Note the 🚧 above: the persister is an interface and a schema, not a
running writer. Nothing in this repo currently moves data into Postgres.
Restart durability comes from WAL replay (see **Recovery**), not from the
database.

This is the LMAX Disruptor / Coinbase clearing / Kafka-log pattern, applied to a spot exchange ledger.

---

## Benchmarks

Measured on an 11th-gen Intel i7-1165G7, GOMAXPROCS=8, after the
concurrency fixes described below. Re-run them on your own hardware
before trusting any of it.

| Benchmark | ns/op | Aggregate throughput | What it measures |
|---|---:|---:|---|
| `BenchmarkHold_PureMemory`           |     122 | ~8.2M ops/sec | Ledger Hold, no WAL |
| `BenchmarkAppend_Async`              |   1,574 | ~640k ops/sec | WAL append, fire-and-forget |
| `BenchmarkAppend_Sync`               |  23,331 |  ~43k ops/sec | WAL append, blocked until fsync |
| `BenchmarkClearing_SettleThroughput` | 386,170 |  ~2.6k ops/sec | Full match settle (2× SettleFill + sync WAL) |

Three things to read carefully, because the earlier version of this table
overstated its case:

**These are aggregate, not per-core.** Every one of these benchmarks uses
`b.RunParallel`, so `ns/op` is total wall time divided by total ops across
all threads. There is no "1M ops/sec/core" here; on 8 threads `Hold` is
~1M ops/sec *per core*, ~8M in total.

**`Hold` was previously reported at 948 ns and called a "pure in-memory
ceiling". It was not a ceiling, it was a lock.** Every `Hold`, `Release`
and `SettleFill` took one ledger-wide mutex over the holds map, so the
account sharding bought nothing on those paths. Sharding holds by
`order_id` took the same benchmark to 122 ns.

**`SettleFill` is the number that bounds the exchange, not `Hold`.** Every
trade must settle, so ~2.6k settles/sec with a synchronous WAL is the real
capacity figure. It is dominated by `fsync`; the async path is the lever if
you can accept bounded loss.

Reproduce:

```bash
go test -run='^$' -bench='BenchmarkAppend|BenchmarkHold_PureMemory|BenchmarkClearing' -benchtime=2s ./...
```

---

## Quick start

```go
import (
    "context"
    "log"
    "sync"

    gunjournal "github.com/aliraad79/Gun/journal"
    "github.com/aliraad79/Gun/market"
    "github.com/aliraad79/Gun/models"

    "github.com/aliraad79/Holster/clearing"
    "github.com/aliraad79/Holster/funding"
    "github.com/aliraad79/Holster/ledger"
    "github.com/aliraad79/Holster/recovery"
    "github.com/aliraad79/Holster/risk"
    "github.com/aliraad79/Holster/wal"
)

// 1. ONE WAL, shared by every durable service. This is not a style
//    preference: recovery replays a single ordered stream, and split
//    across files there is no relative order between a deposit, the hold
//    that spends it, and the trade that consumes the hold.
const walPath = "./data/holster.wal"

l := ledger.New()

// 2. Recover before accepting traffic. Rebuild requires an empty ledger,
//    so this has to happen first.
if _, err := recovery.Rebuild(l, walPath); err != nil {
    log.Fatal(err)
}

w, err := wal.Open(walPath, wal.Options{MaxBatch: 256, FsyncOnFlush: true})
if err != nil {
    log.Fatal(err)
}
defer w.Close()

fund := funding.New(l, w)
r := risk.New(l, w)
c := clearing.New(l, w, l) // ledger satisfies HoldLookup

// 3. Stand up Gun, wiring its OnMatch into Holster's clearing.
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
var wg sync.WaitGroup
reg := market.NewRegistry(ctx, &wg, market.Options{
    Journal: gunjournal.Discard{},
    OnMatch: func(symbol string, ms []models.Match) {
        for _, m := range ms {
            _ = c.Settle(symbol, m)
        }
    },
})

// 4. Credit funds through funding, NOT ledger.Deposit — a raw ledger
//    deposit writes no WAL record and does not survive a restart.
_ = fund.Deposit(1, "USDT", q(10_000))

// 5. Submit an order: hold first, then engine.
order := models.Order{
    ID: 100, UserID: 1, Symbol: "BTC_USDT",
    Type: models.LIMIT, Side: models.BUY,
    Price: p(10_000), Volume: q(1),
}
if err := r.Submit(order); err == nil {
    reg.Submit(order)
}
```

### Recovery

`recovery.Rebuild(ledger, walPath)` replays the log into an empty ledger
and returns a `Stats` describing what came back — deposits, withdrawals,
holds, releases, trades, and a count of records that replayed to a
business-rule rejection.

Two constraints, both enforced rather than documented-and-hoped:

- **One ordered stream.** `funding`, `risk` and `clearing` must share one
  `*wal.WAL`. Replaying per-file would reorder `hold → settle → release`
  into `hold → release → settle`, and the settle is then rejected against
  a released hold and silently lost.
- **Empty ledger.** `Rebuild` returns `ErrLedgerNotEmpty` otherwise.
  `ledger.SettleFill` is *not* idempotent — it takes no sequence number
  and decrements the hold on every call — so replaying a trade record onto
  a ledger that already reflects it spends the hold twice. Recovery is a
  full rebuild, never a partial redo against live state.

A record with an unrecognised `kind` aborts the rebuild rather than being
skipped: a record we cannot interpret means the rebuilt ledger would be
silently wrong.

The `integration/integration_test.go` runs this exact flow end-to-end as a race-tested example.

---

## Project layout

```
.
├── ledger/         # in-memory account state; accounts sharded by user_id,
│                   #   holds sharded by order_id
├── wal/            # group-commit WAL with sync + async paths
├── funding/        # durable deposits / withdrawals
├── risk/           # pre-trade hold service
├── clearing/       # post-trade settler; consumes Gun match events
├── recovery/       # rebuilds the ledger from the WAL on restart
├── persister/      # async Postgres writer (interface + sketch, not wired)
├── integration/    # end-to-end Gun + Holster tests + benchmarks
├── migrations/     # Postgres schema
└── bench/          # benchmark results
```

---

## Design choices worth knowing

**Why sharded by `user_id`?** Single-user-mutex would serialize the whole ledger; one mutex per user would explode memory. 256 shards is a power of two so `user & 0xFF` is a single AND, gives ~uniform distribution, and keeps the wasted-bytes cost negligible.

**Why are holds sharded separately, by `order_id`?** Because for a long time they weren't, and one ledger-wide lock over the holds map made the account sharding above pointless — `Hold`, `Release` and `SettleFill` all take it, so it was the actual throughput ceiling (see the benchmark notes). Holds now get their own 256 shards keyed on `order_id`.

**Why two WAL paths (sync + async)?** The pre-trade hold MUST be durable before the order forwards to Gun — otherwise a crash leaves an order resting that no one's funds back. Everything else (clearing events, audit, L2 deltas) is fine with async + bounded loss. Forcing every code path through the slow `fsync` path would cap the whole system at ~120k ops/sec, which is exactly the trap real exchanges learned to avoid.

**Why is the Postgres persister "interface only"?** The actual DB driver, connection pool, migration runner, and retry policy are deployment concerns — they belong in the binary that *uses* Holster, not in Holster itself. The schema is here, the async writer interface is here; wire it up with `pgx` or `lib/pq` in your `main.go`.

**Why is `Match.Seq` keyed `symbol/seq` for dedup?** Gun's seq is per-symbol; BTC_USDT seq=1 and ETH_USDT seq=1 collide if you key on seq alone. (We caught this with an end-to-end test.)

**Why does `SettleFill` acquire account-shard locks in ascending *shard index* order?** Because ascending *user id* order — which is what it used to do — does not work. The user→shard map is `userID & shardMask`, which is not monotonic, so two settlements over different user pairs can want the same two shards in opposite order: users (10, 265) map to shards (10, 9) while (9, 266) map to (9, 10). That is a textbook AB-BA deadlock, and it reproduced in about a tenth of a second under two goroutines. Ordering by shard index breaks the cycle; ordering by user id only looks like it does.

**Why does the whole hold lifecycle run under one lock acquisition?** A hold is a spending limit, so checking "does this hold still cover the fill?" and consuming that much of it cannot be two separate critical sections. When they were, concurrent callers all read the same pre-decrement remainder, all concluded there was room, and all proceeded: 200 concurrent one-unit settles drained a 100-unit hold completely and drove the holder's `Held` to −100.

---

## If you read this far

Two small asks, both genuine:

- **Star the repo** if the architecture or the benchmarks were interesting
  to you. It's the cheapest signal that this is the kind of thing the
  community wants more of, and it nudges the next reader.
- **Open a PR if you find a real bug** — something where the ledger lands
  in a broken state, the WAL drops a record under a reproducible
  scenario, a hold leaks past cancel/settle, a benchmark claims a number
  that doesn't reproduce on your hardware, or the documentation walks
  you off a cliff. Holster's build caught two real bugs along the way
  (the cross-symbol seq collision and a shard-lock ordering issue);
  I would much rather you find the next one as a PR than someone
  finding it in production.

For design conversations and "have you considered X" — open an issue
instead. Drive-by PRs that reshape a working subsystem land badly; issues
that propose the reshape first land well. I read everything and respond
to everything.

Before submitting code:

```bash
go test -race ./... && go vet ./...
```

---

## License

[Apache License 2.0](LICENSE).

---

## About the author

Built and maintained by **Ali Ahmadi** — senior software engineer focused on fintech infrastructure and low-latency systems in Go.

- GitHub: [@aliraad79](https://github.com/aliraad79)
- Email: [a.ahmadi.k.79@gmail.com](mailto:a.ahmadi.k.79@gmail.com)
- Companion repo: [Gun](https://github.com/aliraad79/Gun) (the matching engine Holster pairs with)
