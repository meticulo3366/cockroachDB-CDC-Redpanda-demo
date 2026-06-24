-- CockroachDB schema for the Redpanda -> CockroachDB streaming demo.
-- Follows CockroachDB best practices:
--   * Every table has an explicit PRIMARY KEY.
--   * UUID primary key (gen_random_uuid) to spread writes across ranges and
--     avoid the sequential-ID write hotspot you'd get from SERIAL/identity.
--   * TIMESTAMPTZ for time, DECIMAL for money (never FLOAT for currency).
--   * Secondary indexes sized to the demo's read patterns, with STORING to
--     make them covering and avoid extra primary-index lookups.
--   * Idempotent (IF NOT EXISTS) so the bootstrap is safe to re-run.

CREATE DATABASE IF NOT EXISTS cdcdemo;

SET database = cdcdemo;

CREATE TABLE IF NOT EXISTS orders (
    order_id     UUID         NOT NULL DEFAULT gen_random_uuid(),
    customer_id  UUID         NOT NULL,
    status       STRING       NOT NULL DEFAULT 'pending',
    item         STRING       NOT NULL,
    quantity     INT8         NOT NULL,
    unit_price   DECIMAL(12,2) NOT NULL,
    -- amount is derived from quantity * unit_price; computed + stored so it is
    -- consistent and indexable without the writer having to send it.
    amount       DECIMAL(12,2) NOT NULL AS (quantity * unit_price) STORED,
    currency     STRING       NOT NULL DEFAULT 'USD',
    region       STRING       NOT NULL,
    -- event time as produced upstream vs. ingest time recorded by the sink:
    -- lets you measure end-to-end lag through Redpanda.
    created_at   TIMESTAMPTZ  NOT NULL,
    ingested_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT pk_orders PRIMARY KEY (order_id),
    CONSTRAINT chk_quantity_positive CHECK (quantity > 0),
    CONSTRAINT chk_status CHECK (status IN ('pending','paid','shipped','delivered','cancelled'))
);

-- Look up a customer's orders, newest first, without touching the base table.
CREATE INDEX IF NOT EXISTS idx_orders_customer_created
    ON orders (customer_id, created_at DESC)
    STORING (status, amount, region);

-- Operational dashboards: orders by status over time.
CREATE INDEX IF NOT EXISTS idx_orders_status_created
    ON orders (status, created_at DESC)
    STORING (amount, region);
