with audited as (
    select
        source_file,
        row_count
    from {{ ref('raw_file_audit') }}
    where file_type = 'sales'
),
received as (
    select
        source_file,
        count(*) as row_count
    from {{ ref('raw_sales_received') }}
    group by source_file
)
select
    coalesce(a.source_file, r.source_file) as source_file,
    coalesce(a.row_count, 0) as audited_row_count,
    coalesce(r.row_count, 0) as received_row_count
from audited a
full outer join received r on r.source_file = a.source_file
where coalesce(a.row_count, 0) <> coalesce(r.row_count, 0)
