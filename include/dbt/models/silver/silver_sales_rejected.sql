{{ config(
    materialized='incremental',
    unique_key='row_uid',
    incremental_strategy='merge',
    tags=['silver']
) }}

select
    row_uid,
    transaction_uid,
    source_file,
    file_name,
    source_row_number,
    store_token,
    transaction_id,
    receipt_token,
    transaction_time_raw,
    amount_raw,
    user_role,
    batch_date,
    content_sha256,
    received_at,
    rejection_reason
from {{ ref('silver_sales_staged') }}
where rejection_reason is not null
