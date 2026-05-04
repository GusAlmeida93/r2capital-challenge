{{ config(materialized='view', tags=['raw']) }}

select
    row_uid,
    source_file,
    file_name,
    source_row_number,
    store_group,
    store_token,
    store_name,
    batch_date,
    content_sha256,
    received_at
from {{ source('landing', 'landing_stores_received') }}
