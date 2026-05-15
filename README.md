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
| Postgres schema + async persister (interface; deployment-wired) | ✅ |
| Throughput benchmark > 1M ops/sec on the in-memory path | ✅ |

---

## Architecture

Three layers, three durability stories:

| Layer | Source of truth | Latency | Throughput |
|---|---|---|---|
| In-memory ledger | …in flight | ~100 ns/op | >1M ops/sec/system |
| WAL (async) | …catching up | ~1 µs/op | ~900k ops/sec/system |
| WAL (sync, durable-on-ack) | …durable | ~20–100 µs/op | ~120k ops/sec/system |
| Postgres | …at rest, queryable | 1–10 ms (off the order path) | (irrelevant to order-submit) |

**Postgres is never on the order-submit hot path.** Risk reads in-memory and writes the WAL; that's the user-facing critical path. The async persister drains the WAL into Postgres in batched transactions; if Postgres is slow or down for a few minutes, the exchange keeps trading and catches up after.

This is the LMAX Disruptor / Coinbase clearing / Kafka-log pattern, applied to a spot exchange ledger.

---

## Benchmarks

Real numbers on a 13th-gen Intel i7-13620H, NVMe, GOMAXPROCS=16. Full output: [`bench/phase-5-final.txt`](bench/phase-5-final.txt).

| Benchmark | ns/op | System throughput | What it measures |
|---|---:|---:|---|
| `BenchmarkHold_PureMemory`            |    948 | **~1.06M ops/sec** | Ledger Hold with no WAL; pure in-memory ceiling |
| `BenchmarkAppend_Async`               |  1,112 | **~900k ops/sec**  | WAL append, fire-and-forget durability |
| `BenchmarkAppend_Sync`                |  8,716 |   ~120k ops/sec    | WAL append, blocked until fsync; durable-on-ack |
| `BenchmarkClearing_SettleThroughput`  | 104,000 |   ~9.6k ops/sec    | Full match settle (2x SettleFill + WAL); pessimistic, sync WAL |

**Headline:** the in-memory + async-durability path hits the **>1M ops/sec target locked in during design**. The sync-on-ack path is the bound for ops that must not be lost on crash (the pre-trade hold leg specifically) — for the rest, async + batched fsync is the production choice and matches what every public-spec exchange of comparable scale (LMAX, Coinbase, Binance, …) does under the hood.

Reproduce:

```bash
go test -run='^$' -bench='BenchmarkAppend|BenchmarkHold_PureMemory|BenchmarkClearing' -benchtime=2s ./...
```

---

## Quick start

```go
import (
    "context"
    "sync"

    gunjournal "github.com/aliraad79/Gun/journal"
    "github.com/aliraad79/Gun/market"
    "github.com/aliraad79/Gun/models"

    "github.com/aliraad79/Holster/clearing"
    "github.com/aliraad79/Holster/ledger"
    "github.com/aliraad79/Holster/risk"
    "github.com/aliraad79/Holster/wal"
)

// 1. Stand up the Holster stack.
l := ledger.New()
riskWAL, _ := wal.Open("./data/risk.wal", wal.Options{
    MaxBatch: 256, MaxLatency: 200*time.Microsecond, FsyncOnFlush: true,
})
clearingWAL, _ := wal.Open("./data/clearing.wal", wal.Options{
    MaxBatch: 256, MaxLatency: 200*time.Microsecond, FsyncOnFlush: true,
})
r := risk.New(l, riskWAL)
c := clearing.New(l, clearingWAL, l) // ledger satisfies HoldLookup

// 2. Stand up Gun, wiring its OnMatch into Holster's clearing.
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

// 3. Submit an order through both: hold first, then engine.
_ = l.Deposit(1, "USDT", q(10_000))
order := models.Order{
    ID: 100, UserID: 1, Symbol: "BTC_USDT",
    Type: models.LIMIT, Side: models.BUY,
    Price: p(10_000), Volume: q(1),
}
_ = r.Submit(order)
reg.Submit(order)
```

The `integration/integration_test.go` runs this exact flow end-to-end as a race-tested example.

---

## Project layout

```
.
├── ledger/         # in-memory account state, sharded by user_id
├── wal/            # group-commit WAL with sync + async paths
├── risk/           # pre-trade hold service
├── clearing/       # post-trade settler; consumes Gun match events
├── persister/      # async Postgres writer (interface + sketch)
├── integration/    # end-to-end Gun + Holster tests + benchmarks
├── migrations/     # Postgres schema
├── cmd/            # binaries (holster server, demo)
└── bench/          # benchmark results
```

---

## Design choices worth knowing

**Why sharded by `user_id`?** Single-user-mutex would serialize the whole ledger; one mutex per user would explode memory. 256 shards is a power of two so `user & 0xFF` is a single AND, gives ~uniform distribution, and keeps the wasted-bytes cost negligible.

**Why two WAL paths (sync + async)?** The pre-trade hold MUST be durable before the order forwards to Gun — otherwise a crash leaves an order resting that no one's funds back. Everything else (clearing events, audit, L2 deltas) is fine with async + bounded loss. Forcing every code path through the slow `fsync` path would cap the whole system at ~120k ops/sec, which is exactly the trap real exchanges learned to avoid.

**Why is the Postgres persister "interface only"?** The actual DB driver, connection pool, migration runner, and retry policy are deployment concerns — they belong in the binary that *uses* Holster, not in Holster itself. The schema is here, the async writer interface is here; wire it up with `pgx` or `lib/pq` in your `main.go`.

**Why is `Match.Seq` keyed `symbol/seq` for dedup?** Gun's seq is per-symbol; BTC_USDT seq=1 and ETH_USDT seq=1 collide if you key on seq alone. (We caught this with an end-to-end test.)

**Why does `SettleFill` acquire shard locks in ascending user-id order?** Two settlements between the same pair of users in opposite directions would deadlock if each acquired its locks in (holder, taker) order. Always-ascending order breaks the cycle.

---

## License

[Apache License 2.0](LICENSE).

---

## About the author

Built and maintained by **Ali Ahmadi** — senior software engineer focused on fintech infrastructure and low-latency systems in Go.

- GitHub: [@aliraad79](https://github.com/aliraad79)
- Email: [dev@raastin.com](mailto:dev@raastin.com)
- Companion repo: [Gun](https://github.com/aliraad79/Gun) (the matching engine Holster pairs with)
