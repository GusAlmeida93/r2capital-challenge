{{ config(materialized='view', tags=['raw']) }}

select
    transaction_uid,
    row_uid,
    source_file,
    file_name,
    source_row_number,
    store_token,
    transaction_id,
    receipt_token,
    transaction_time,
    amount,
    user_role,
    batch_date,
    content_sha256,
    received_at
from (
    select
        concat(coalesce(store_token, ''), '|', coalesce(transaction_id, '')) as transaction_uid,
        *,
        row_number() over (
            partition by concat(coalesce(store_token, ''), '|', coalesce(transaction_id, ''))
            order by received_at desc, source_file desc, source_row_number desc
        ) as row_rank
    from {{ ref('raw_sales_received') }}
    where store_token is not null
      and transaction_id is not null
)
where row_rank = 1
