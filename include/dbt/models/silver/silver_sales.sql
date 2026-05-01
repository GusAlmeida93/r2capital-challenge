-- Validated, cleaned sales. One row per (store_token, transaction_id) — the
-- composite natural key. Latest received version wins via snapshot SCD2 +
-- delete+insert upsert here.

{{ config(
    materialized='incremental',
    unique_key=['store_token', 'transaction_id'],
    incremental_strategy='delete+insert',
    tags=['silver']
) }}

select
    store_token,
    transaction_id,
    receipt_token,
    transaction_time_parsed as transaction_time,
    amount_parsed as amount,
    user_role,
    batch_date,
    source_file,
    ingested_at,
    snapshot_updated_at
from {{ ref('silver_sales_staged') }}
where rejection_reason is null
{% if is_incremental() %}
  and snapshot_updated_at > (
    select coalesce(max(snapshot_updated_at), '1900-01-01'::timestamp)
    from {{ this }}
  )
{% endif %}
