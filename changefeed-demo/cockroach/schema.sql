-- Schema for the CockroachDB CHANGEFEED -> Redpanda CDC demo.
--
-- `accounts` is the OLTP source table that a workload constantly mutates
-- (INSERT / UPDATE / DELETE). A CockroachDB CHANGEFEED captures every row-level
-- change and streams it to Redpanda. `accounts_audit` is the downstream,
-- append-only history that a Redpanda Connect consumer materializes from the
-- CDC stream — one row per captured change.

CREATE DATABASE IF NOT EXISTS cdcbank;
SET database = cdcbank;

-- Source table. INT8 key over a small, bounded id space (1..50) is intentional:
-- the workload must repeatedly target existing rows to produce UPDATE and DELETE
-- changes. Ids are chosen at random (not monotonically), so there is no write
-- hotspot despite not using a UUID here.
CREATE TABLE IF NOT EXISTS accounts (
    id          INT8          NOT NULL,
    owner       STRING        NOT NULL,
    balance     DECIMAL(14,2) NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT pk_accounts PRIMARY KEY (id)
);

-- Downstream CDC history. UUID PK (best practice) since it is insert-only and
-- high-volume. Stores the full before/after row images and the changefeed's
-- MVCC/HLC timestamp so you can replay or audit every change.
CREATE TABLE IF NOT EXISTS accounts_audit (
    audit_id    UUID         NOT NULL DEFAULT gen_random_uuid(),
    account_id  INT8         NOT NULL,
    op          STRING       NOT NULL,   -- insert | update | delete
    before_doc  JSONB,                   -- row image prior to the change (null on insert)
    after_doc   JSONB,                   -- row image after the change (null on delete)
    updated_hlc STRING,                  -- changefeed "updated" MVCC timestamp
    audited_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT pk_accounts_audit PRIMARY KEY (audit_id),
    CONSTRAINT chk_op CHECK (op IN ('insert','update','delete')),
    -- (account_id, updated_hlc) uniquely identifies one row-version change in the
    -- changefeed. Making it UNIQUE lets the consumer UPSERT idempotently, so
    -- at-least-once redelivery from Redpanda never produces duplicate audit rows.
    CONSTRAINT uq_audit_event UNIQUE (account_id, updated_hlc)
);

-- Read the change history for one account, newest first.
CREATE INDEX IF NOT EXISTS idx_audit_account
    ON accounts_audit (account_id, audited_at DESC)
    STORING (op);
