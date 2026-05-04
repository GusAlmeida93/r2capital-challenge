{{ config(materialized='view', tags=['raw']) }}

select
    source_file,
    file_name,
    file_type,
    batch_date,
    object_key,
    content_sha256,
    row_count,
    first_seen_at,
    last_seen_at,
    last_seen_at as processed_at
from (
    select
        *,
        row_number() over (
            partition by source_file
            order by last_seen_at desc
        ) as row_rank
    from {{ source('landing', 'landing_file_manifest') }}
)
where row_rank = 1
