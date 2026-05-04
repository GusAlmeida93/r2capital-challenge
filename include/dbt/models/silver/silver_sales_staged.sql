{{ config(materialized='table', tags=['silver']) }}

with src as (
    select *
    from {{ ref('raw_sales_received') }}
),
parsed as (
    select
        row_uid,
        concat(coalesce(store_token, ''), '|', coalesce(transaction_id, '')) as transaction_uid,
        source_file,
        file_name,
        source_row_number,
        store_token,
        transaction_id,
        receipt_token,
        transaction_time as transaction_time_raw,
        case
            when {{ matches_regex("coalesce(transaction_time, '')", "^[0-9]{8}T[0-9]{6}[.][0-9]{3}$") }}
                then cast(date_parse(transaction_time, '%Y%m%dT%H%i%s.%f') as timestamp(6))
            else null
        end as transaction_time_parsed,
        amount as amount_raw,
        case
            when {{ matches_regex("coalesce(amount, '')", "^[$]-?[0-9]+([.][0-9]{2})?$") }}
                then cast(replace(replace(amount, '$', ''), ',', '') as decimal(11, 2))
            else null
        end as amount_parsed,
        user_role,
        batch_date,
        content_sha256,
        received_at
    from src
)
select
    *,
    case
        when not {{ matches_regex("coalesce(store_token, '')", "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$") }}
            then 'invalid_store_token'
        when not {{ matches_regex("coalesce(transaction_id, '')", "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$") }}
            then 'invalid_transaction_id'
        when not {{ matches_regex("coalesce(receipt_token, '')", "^[A-Za-z0-9]{5,30}$") }}
            then 'invalid_receipt_token'
        when transaction_time_parsed is null
            then 'invalid_transaction_time'
        when amount_parsed is null
            then 'invalid_amount_format'
        when amount_parsed < 0
            then 'invalid_amount_negative'
        when user_role is null or length(user_role) not between 1 and 30
            then 'invalid_user_role'
        else null
    end as rejection_reason
from parsed
