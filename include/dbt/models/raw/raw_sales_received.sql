{{ config(materialized='view', tags=['raw']) }}

select
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
from {{ source('landing', 'landing_sales_received') }}
