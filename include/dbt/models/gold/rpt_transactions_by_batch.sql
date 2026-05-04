{{ config(
    materialized='table',
    tags=['gold']
) }}

with ranked as (
    select
        current_date as snapshot_date,
        cast(date_parse(batch_date, '%Y%m%d') as date) as batch_date,
        total_raw_transactions,
        valid_transactions,
        invalid_transactions,
        processing_date,
        row_number() over (order by batch_date desc) as row_rank
    from {{ ref('silver_sales_audit_by_batch') }}
)
select
    snapshot_date,
    batch_date,
    total_raw_transactions,
    valid_transactions,
    invalid_transactions,
    processing_date
from ranked
where row_rank <= 40
order by batch_date desc
