-- Holster's durable ledger schema. Postgres is the source of truth at
-- rest; the in-memory ledger is the source of truth in flight, and the
-- WAL is the durability bridge between them. Postgres is rebuilt async
-- from the WAL and is NEVER on the order-submit hot path.
--
-- Conventions:
--   • NUMERIC(36, 18) covers every realistic crypto amount with room
--     to spare. shopspring/decimal-compatible at the application
--     layer; the matching engine's Px/Qty round-trip through it.
--   • All monetary mutations write one row to `ledger_entries` — this
--     is the audit trail. `accounts` is a materialized aggregate; if
--     it's corrupted you can rebuild from ledger_entries.
--   • `held` MUST be <= `balance`. Risk lives or dies on this check.
--   • `engine_seq` on trades is UNIQUE: re-consuming the same match
--     hits the constraint and fails — that's the idempotency lock.

CREATE TABLE accounts (
    user_id     BIGINT NOT NULL,
    asset       VARCHAR(20) NOT NULL,
    balance     NUMERIC(36, 18) NOT NULL DEFAULT 0,
    held        NUMERIC(36, 18) NOT NULL DEFAULT 0,
    version     BIGINT NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, asset),
    CHECK (balance >= 0),
    CHECK (held >= 0),
    CHECK (held <= balance)
);

-- Append-only audit trail. Every state change in `accounts` writes
-- one row here. Reconciliation: SUM(delta) per (user, asset) ==
-- accounts row balance. SUM(held_delta) == accounts row held.
CREATE TABLE ledger_entries (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT NOT NULL,
    asset       VARCHAR(20) NOT NULL,
    delta       NUMERIC(36, 18) NOT NULL,
    held_delta  NUMERIC(36, 18) NOT NULL DEFAULT 0,
    kind        VARCHAR(20) NOT NULL,
    ref_kind    VARCHAR(20),
    ref_id      BIGINT,
    engine_seq  BIGINT,
    symbol      VARCHAR(20),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX ledger_entries_user_asset_id ON ledger_entries (user_id, asset, id);
-- Trade entries deduplicate on (engine_seq, user_id) so a re-consumed
-- match hits the unique constraint and the persister's INSERT fails
-- harmlessly.
CREATE UNIQUE INDEX ledger_entries_trade_dedup
    ON ledger_entries (engine_seq, symbol, user_id, asset)
    WHERE engine_seq IS NOT NULL;

-- Per-order hold receipts. The cancel path looks up by order_id.
CREATE TABLE holds (
    order_id    BIGINT PRIMARY KEY,
    user_id     BIGINT NOT NULL,
    asset       VARCHAR(20) NOT NULL,
    amount      NUMERIC(36, 18) NOT NULL,
    state       VARCHAR(20) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (state IN ('active', 'released', 'consumed'))
);
CREATE INDEX holds_user_state ON holds (user_id, state) WHERE state = 'active';

-- Settled trades. engine_seq is the idempotency key (per-symbol, so
-- the unique constraint composes both).
CREATE TABLE trades (
    id              BIGSERIAL PRIMARY KEY,
    engine_seq      BIGINT NOT NULL,
    symbol          VARCHAR(20) NOT NULL,
    buy_order_id    BIGINT NOT NULL,
    sell_order_id   BIGINT NOT NULL,
    buyer_user_id   BIGINT NOT NULL,
    seller_user_id  BIGINT NOT NULL,
    price           NUMERIC(36, 18) NOT NULL,
    volume          NUMERIC(36, 18) NOT NULL,
    quote_amount    NUMERIC(36, 18) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, engine_seq)
);

-- Watermark of where each consumer is in the event stream. The async
-- persister updates this in the same transaction as the trade /
-- ledger_entry inserts so the watermark is always consistent with
-- the data.
CREATE TABLE consumer_offsets (
    consumer_id        VARCHAR(50) PRIMARY KEY,
    last_processed_seq BIGINT NOT NULL,
    symbol             VARCHAR(20),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
