{% snapshot raw_stores_snapshot %}
{{ config(
    target_schema='raw',
    unique_key='store_token',
    strategy='check',
    check_cols=['store_group', 'store_name'],
    invalidate_hard_deletes=False
) }}

with raw_csv as (
    {{ read_landing_csv(
        'stores_*.csv',
        {'store_group':'VARCHAR', 'store_token':'VARCHAR', 'store_name':'VARCHAR'},
        exclude_header_predicate="store_group = 'store_group' and store_token = 'store_token' and store_name = 'store_name'"
    ) }}
),
deduped as (
    select
        store_token,
        store_group,
        store_name,
        filename as source_file,
        regexp_extract(filename, 'stores_(\d{8})\.csv', 1) as batch_date,
        current_timestamp as ingested_at
    from raw_csv
    qualify row_number() over (
        partition by store_token
        order by filename desc
    ) = 1
)
select * from deduped
where store_token is not null
{% endsnapshot %}
