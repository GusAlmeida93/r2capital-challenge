{{ config(
    materialized='incremental',
    unique_key='transaction_uid',
    incremental_strategy='merge',
    tags=['silver']
) }}

with current_snap as (
    select *
    from {{ ref('raw_sales_snapshot') }}
    where dbt_valid_to is null
),
parsed as (
    select
        transaction_uid,
        row_uid,
        store_token,
        transaction_id,
        receipt_token,
        case
            when {{ matches_regex("coalesce(transaction_time, '')", "^[0-9]{8}T[0-9]{6}[.][0-9]{3}$") }}
                then cast(date_parse(transaction_time, '%Y%m%dT%H%i%s.%f') as timestamp(6))
            else null
        end as transaction_time,
        case
            when {{ matches_regex("coalesce(amount, '')", "^[$]-?[0-9]+([.][0-9]{2})?$") }}
                then cast(replace(replace(amount, '$', ''), ',', '') as decimal(11, 2))
            else null
        end as amount,
        user_role,
        batch_date,
        source_file,
        source_row_number,
        content_sha256,
        ingested_at as received_at,
        dbt_updated_at as snapshot_updated_at
    from current_snap
)
select
    transaction_uid,
    row_uid,
    store_token,
    transaction_id,
    receipt_token,
    transaction_time,
    amount,
    user_role,
    batch_date,
    source_file,
    source_row_number,
    content_sha256,
    received_at,
    snapshot_updated_at
from parsed
where {{ matches_regex("coalesce(store_token, '')", "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$") }}
  and {{ matches_regex("coalesce(transaction_id, '')", "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$") }}
  and {{ matches_regex("coalesce(receipt_token, '')", "^[A-Za-z0-9]{5,30}$") }}
  and transaction_time is not null
  and amount is not null
  and amount >= 0
  and user_role is not null
  and length(user_role) between 1 and 30
