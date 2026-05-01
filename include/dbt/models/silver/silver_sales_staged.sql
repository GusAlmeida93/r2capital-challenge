-- Ephemeral staging: parses raw sales fields and flags row-level validity.
-- Consumed by both silver_sales (valid) and silver_sales_rejected (invalid).

{{ config(materialized='ephemeral', tags=['silver']) }}

with src as (
    select *
    from {{ ref('raw_sales_snapshot') }}
    where dbt_valid_to is null
),
parsed as (
    select
        store_token,
        transaction_id,
        receipt_token,
        transaction_time as transaction_time_raw,
        try_strptime(transaction_time, '%Y%m%dT%H%M%S.%f') as transaction_time_parsed,
        amount as amount_raw,
        try_cast(replace(replace(amount, '$', ''), ',', '') as DECIMAL(11,2)) as amount_parsed,
        user_role,
        batch_date,
        source_file,
        ingested_at,
        dbt_updated_at as snapshot_updated_at
    from src
)
select
    *,
    case
        when not regexp_matches(coalesce(store_token, ''),
                '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
            then 'invalid_store_token'
        when not regexp_matches(coalesce(transaction_id, ''),
                '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
            then 'invalid_transaction_id'
        when not regexp_matches(coalesce(receipt_token, ''), '^[A-Za-z0-9]{5,30}$')
            then 'invalid_receipt_token'
        when transaction_time_parsed is null
            then 'invalid_transaction_time'
        when amount_parsed is null
            then 'invalid_amount_format'
        when amount_parsed < 0
            then 'invalid_amount_negative'
        when user_role is null or length(user_role) > 30
            then 'invalid_user_role'
        else null
    end as rejection_reason
from parsed
