-- ============================================================================
-- R2 Capital pipeline — Logical Data Model (DDL reference)
-- ============================================================================
--
-- This file documents the contract (column types, keys, audit fields) for
-- the three warehouse schemas created by dbt at run time. It is intended
-- as a reference only and is NOT executed by the pipeline.
--
-- Layer summary:
--   raw     — SCD Type 2 snapshots over the landing CSVs + file audit
--   silver  — validated, type-coerced, upserted current state
--   gold    — three reports required by the assessment (Outputs 1-3)
--
-- See docs/design.md for the design narrative and dataflow diagrams.
-- ============================================================================

-- ============================================================================
-- Schema: raw  (SCD Type 2 — managed by dbt snapshots)
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw.raw_stores_snapshot (
    store_token         VARCHAR     NOT NULL,
    store_group         VARCHAR     NOT NULL,
    store_name          VARCHAR     NOT NULL,
    source_file         VARCHAR     NOT NULL,
    batch_date          VARCHAR     NOT NULL,                 -- YYYYMMDD as recorded in the filename
    ingested_at         TIMESTAMP   NOT NULL,
    -- dbt snapshot-managed columns
    dbt_scd_id          VARCHAR     NOT NULL,
    dbt_updated_at      TIMESTAMP   NOT NULL,
    dbt_valid_from      TIMESTAMP   NOT NULL,
    dbt_valid_to        TIMESTAMP            ,                -- NULL = current version
    PRIMARY KEY (dbt_scd_id)
);
-- Logical natural key: (store_token) — uniqueness enforced via SCD2 check strategy.

CREATE TABLE IF NOT EXISTS raw.raw_sales_snapshot (
    transaction_uid     VARCHAR     NOT NULL,                 -- store_token || '|' || transaction_id
    store_token         VARCHAR     NOT NULL,
    transaction_id      VARCHAR     NOT NULL,
    receipt_token       VARCHAR     NOT NULL,
    transaction_time    VARCHAR     NOT NULL,                 -- raw text — parsed in silver
    amount              VARCHAR     NOT NULL,                 -- raw text incl. currency symbol — parsed in silver
    user_role           VARCHAR     NOT NULL,
    source_file         VARCHAR     NOT NULL,
    batch_date          VARCHAR     NOT NULL,
    ingested_at         TIMESTAMP   NOT NULL,
    dbt_scd_id          VARCHAR     NOT NULL,
    dbt_updated_at      TIMESTAMP   NOT NULL,
    dbt_valid_from      TIMESTAMP   NOT NULL,
    dbt_valid_to        TIMESTAMP            ,
    PRIMARY KEY (dbt_scd_id)
);
-- Logical natural key: (store_token, transaction_id).

CREATE TABLE IF NOT EXISTS raw.raw_file_audit (
    source_file         VARCHAR     NOT NULL PRIMARY KEY,
    file_type           VARCHAR     NOT NULL,                 -- 'stores' | 'sales'
    batch_date          VARCHAR     NOT NULL,
    row_count           BIGINT      NOT NULL,
    processed_at        TIMESTAMP   NOT NULL
);

-- ============================================================================
-- Schema: silver  (validated, deduped, type-coerced)
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.silver_stores (
    store_token             VARCHAR     NOT NULL PRIMARY KEY,
    store_group             VARCHAR(8)  NOT NULL,             -- 8 hex upper
    store_name              VARCHAR(200) NOT NULL,
    batch_date              VARCHAR     NOT NULL,
    source_file             VARCHAR     NOT NULL,
    valid_from              TIMESTAMP   NOT NULL,
    snapshot_updated_at     TIMESTAMP   NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.silver_sales (
    store_token             VARCHAR     NOT NULL,
    transaction_id          VARCHAR     NOT NULL,
    receipt_token           VARCHAR(30) NOT NULL,
    transaction_time        TIMESTAMP   NOT NULL,
    amount                  DECIMAL(11,2) NOT NULL CHECK (amount >= 0),
    user_role               VARCHAR(30) NOT NULL,
    batch_date              VARCHAR     NOT NULL,
    source_file             VARCHAR     NOT NULL,
    ingested_at             TIMESTAMP   NOT NULL,
    snapshot_updated_at     TIMESTAMP   NOT NULL,
    PRIMARY KEY (store_token, transaction_id)
);

CREATE TABLE IF NOT EXISTS silver.silver_sales_rejected (
    store_token             VARCHAR,
    transaction_id          VARCHAR,
    receipt_token           VARCHAR,
    transaction_time_raw    VARCHAR,
    amount_raw              VARCHAR,
    user_role               VARCHAR,
    batch_date              VARCHAR     NOT NULL,
    source_file             VARCHAR     NOT NULL,
    ingested_at             TIMESTAMP   NOT NULL,
    snapshot_updated_at     TIMESTAMP   NOT NULL,
    rejection_reason        VARCHAR     NOT NULL              -- enum, see _silver.yml
);

CREATE TABLE IF NOT EXISTS silver.silver_sales_audit_by_batch (
    batch_date                  VARCHAR     NOT NULL PRIMARY KEY,
    total_raw_transactions      BIGINT      NOT NULL,
    valid_transactions          BIGINT      NOT NULL,
    invalid_transactions        BIGINT      NOT NULL,
    processing_date             TIMESTAMP   NOT NULL
);

-- ============================================================================
-- Schema: gold  (3 required reports)
-- ============================================================================

-- Output 1: Transactions processed by batch date — latest 40, DESC.
CREATE TABLE IF NOT EXISTS gold.rpt_transactions_by_batch (
    snapshot_date           DATE        NOT NULL,
    batch_date              DATE        NOT NULL PRIMARY KEY,
    total_raw_transactions  BIGINT      NOT NULL,
    valid_transactions      BIGINT      NOT NULL,
    invalid_transactions    BIGINT      NOT NULL,
    processing_date         TIMESTAMP   NOT NULL
);

-- Output 2: Sales stats by transaction date — latest 40.
CREATE TABLE IF NOT EXISTS gold.rpt_sales_by_transaction_date (
    snapshot_date               DATE          NOT NULL,
    transaction_date            DATE          NOT NULL PRIMARY KEY,
    stores_with_transactions    BIGINT        NOT NULL,
    total_sales_amount          DECIMAL(18,2) NOT NULL,
    total_sales_average         DECIMAL(18,4) NOT NULL,
    month_accumulated_sales     DECIMAL(18,2) NOT NULL,
    top_store_token             VARCHAR
);

-- Output 3: Top 5 stores per transaction date — last 10 dates (≤ 50 rows).
CREATE TABLE IF NOT EXISTS gold.rpt_top_5_stores_by_date (
    snapshot_date           DATE          NOT NULL,
    transaction_date        DATE          NOT NULL,
    top_rank_id             SMALLINT      NOT NULL CHECK (top_rank_id BETWEEN 1 AND 5),
    store_total_sales       DECIMAL(18,2) NOT NULL,
    store_token             VARCHAR       NOT NULL,
    store_name              VARCHAR(200),
    PRIMARY KEY (transaction_date, top_rank_id)
);
