{% snapshot raw_stores_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='store_token',
        strategy='check',
        check_cols=['store_group', 'store_name']
    )
}}

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
    received_at as ingested_at
from {{ ref('raw_stores_current') }}

{% endsnapshot %}
