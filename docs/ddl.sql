CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS snapshots;

CREATE TABLE IF NOT EXISTS raw.landing_file_manifest (
    source_file varchar,
    file_name varchar,
    file_type varchar,
    batch_date varchar,
    object_key varchar,
    content_sha256 varchar,
    row_count bigint,
    first_seen_at timestamp(6),
    last_seen_at timestamp(6)
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS raw.landing_file_rejections (
    rejection_id varchar,
    source_file varchar,
    file_name varchar,
    file_type varchar,
    batch_date varchar,
    expected_sha256 varchar,
    actual_sha256 varchar,
    quarantine_path varchar,
    rejected_at timestamp(6),
    reason varchar
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS raw.landing_stores_received (
    row_uid varchar,
    source_file varchar,
    file_name varchar,
    source_row_number bigint,
    store_group varchar,
    store_token varchar,
    store_name varchar,
    batch_date varchar,
    content_sha256 varchar,
    received_at timestamp(6)
)
WITH (format = 'PARQUET');

CREATE TABLE IF NOT EXISTS raw.landing_sales_received (
    row_uid varchar,
    source_file varchar,
    file_name varchar,
    source_row_number bigint,
    store_token varchar,
    transaction_id varchar,
    receipt_token varchar,
    transaction_time varchar,
    amount varchar,
    user_role varchar,
    batch_date varchar,
    content_sha256 varchar,
    received_at timestamp(6)
)
WITH (format = 'PARQUET');
