{{ config(
    materialized='table',
    tags=['silver']
) }}

with file_totals as (
    select
        batch_date,
        sum(row_count) as total_raw_transactions,
        max(processed_at) as processing_date
    from {{ ref('raw_file_audit') }}
    where file_type = 'sales'
    group by batch_date
),
valid_counts as (
    select batch_date, count(*) as valid_transactions
    from {{ ref('silver_sales_staged') }}
    where rejection_reason is null
    group by batch_date
),
invalid_counts as (
    select batch_date, count(*) as invalid_transactions
    from {{ ref('silver_sales_staged') }}
    where rejection_reason is not null
    group by batch_date
)
select
    f.batch_date,
    f.total_raw_transactions,
    coalesce(v.valid_transactions, 0) as valid_transactions,
    coalesce(i.invalid_transactions, 0) as invalid_transactions,
    f.processing_date
from file_totals f
left join valid_counts v on v.batch_date = f.batch_date
left join invalid_counts i on i.batch_date = f.batch_date
