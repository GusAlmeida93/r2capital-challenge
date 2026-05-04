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
from (
    select
        *,
        row_number() over (
            partition by store_token
            order by received_at desc, source_file desc, source_row_number desc
        ) as row_rank
    from {{ ref('raw_stores_received') }}
    where store_token is not null
)
where row_rank = 1
