select
    batch_date,
    total_raw_transactions,
    valid_transactions,
    invalid_transactions
from {{ ref('silver_sales_audit_by_batch') }}
where total_raw_transactions <> valid_transactions + invalid_transactions
