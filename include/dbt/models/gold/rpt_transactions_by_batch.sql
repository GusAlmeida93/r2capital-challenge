-- Output 1: Transactions processed by batch date.
-- Latest 40 batch_dates in descending order.

{{ config(materialized='table', tags=['gold']) }}

select
    current_date as snapshot_date,
    cast(strptime(batch_date, '%Y%m%d') as date) as batch_date,
    total_raw_transactions,
    valid_transactions,
    invalid_transactions,
    processing_date
from {{ ref('silver_sales_audit_by_batch') }}
qualify row_number() over (order by batch_date desc) <= 40
order by batch_date desc
