{% snapshot raw_sales_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='transaction_uid',
        strategy='check',
        check_cols=['receipt_token', 'transaction_time', 'amount', 'user_role']
    )
}}

select
    transaction_uid,
    row_uid,
    source_file,
    file_name,
    source_row_number,
    store_token,
    transaction_id,
    receipt_token,
    transaction_time,
    amount,
    user_role,
    batch_date,
    content_sha256,
    received_at as ingested_at
from {{ ref('raw_sales_current') }}

{% endsnapshot %}
