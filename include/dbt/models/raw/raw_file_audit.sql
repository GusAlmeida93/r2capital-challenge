-- One row per CSV file ingested. Tracks row counts per file to power Output 1's
-- "total processed raw transactions" without requiring a per-row received table.
-- Idempotent: filters out files already recorded.

{{ config(
    materialized='incremental',
    unique_key='source_file',
    tags=['raw']
) }}

with stores_csv as (
    {{ read_landing_csv(
        'stores_*.csv',
        {'store_group':'VARCHAR', 'store_token':'VARCHAR', 'store_name':'VARCHAR'},
        exclude_header_predicate="store_group = 'store_group' and store_token = 'store_token' and store_name = 'store_name'"
    ) }}
),
sales_csv as (
    {{ read_landing_csv(
        'sales_*.csv',
        {
            'store_token':'VARCHAR',
            'transaction_id':'VARCHAR',
            'receipt_token':'VARCHAR',
            'transaction_time':'VARCHAR',
            'amount':'VARCHAR',
            'user_role':'VARCHAR'
        },
        exclude_header_predicate="store_token = 'store_token' and transaction_id = 'transaction_id' and receipt_token = 'receipt_token'"
    ) }}
),
stores_files as (
    select
        filename as source_file,
        'stores' as file_type,
        regexp_extract(filename, 'stores_(\d{8})\.csv', 1) as batch_date,
        count(*) as row_count
    from stores_csv
    where filename is not null
    group by filename
),
sales_files as (
    select
        filename as source_file,
        'sales' as file_type,
        regexp_extract(filename, 'sales_(\d{8})(?:_\d+)?\.csv', 1) as batch_date,
        count(*) as row_count
    from sales_csv
    where filename is not null
    group by filename
),
all_files as (
    select * from stores_files
    union all
    select * from sales_files
)
select
    source_file,
    file_type,
    batch_date,
    row_count,
    current_timestamp as processed_at
from all_files
{% if is_incremental() %}
where source_file not in (select source_file from {{ this }})
{% endif %}
