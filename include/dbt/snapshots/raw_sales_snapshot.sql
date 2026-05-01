{% snapshot raw_sales_snapshot %}
{{ config(
    target_schema='raw',
    unique_key='transaction_uid',
    strategy='check',
    check_cols=['receipt_token', 'transaction_time', 'amount', 'user_role'],
    invalidate_hard_deletes=False
) }}

with raw_csv as (
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
keyed as (
    select
        coalesce(store_token, '') || '|' || coalesce(transaction_id, '') as transaction_uid,
        store_token,
        transaction_id,
        receipt_token,
        transaction_time,
        amount,
        user_role,
        filename as source_file,
        regexp_extract(filename, 'sales_(\d{8})(?:_\d+)?\.csv', 1) as batch_date,
        current_timestamp as ingested_at
    from raw_csv
),
deduped as (
    select *
    from keyed
    qualify row_number() over (
        partition by transaction_uid
        order by source_file desc
    ) = 1
)
select * from deduped
where store_token is not null
  and transaction_id is not null
{% endsnapshot %}
