-- Format-invalid sales records. Append-only audit table — every detected
-- invalid version is recorded for traceability and to power Output 1's
-- invalid_transactions count.

{{ config(
    materialized='incremental',
    unique_key=['store_token', 'transaction_id', 'snapshot_updated_at'],
    incremental_strategy='append',
    tags=['silver']
) }}

select
    store_token,
    transaction_id,
    receipt_token,
    transaction_time_raw,
    amount_raw,
    user_role,
    batch_date,
    source_file,
    ingested_at,
    snapshot_updated_at,
    rejection_reason
from {{ ref('silver_sales_staged') }}
where rejection_reason is not null
{% if is_incremental() %}
  and snapshot_updated_at > (
    select coalesce(max(snapshot_updated_at), '1900-01-01'::timestamp)
    from {{ this }}
  )
{% endif %}
