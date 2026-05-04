{{ config(
    materialized='incremental',
    unique_key='store_token',
    incremental_strategy='merge',
    tags=['silver']
) }}

with current_snap as (
    select *
    from {{ ref('raw_stores_snapshot') }}
    where dbt_valid_to is null
),
validated as (
    select
        store_token,
        store_group,
        store_name,
        batch_date,
        source_file,
        ingested_at as valid_from,
        dbt_updated_at as snapshot_updated_at
    from current_snap
    where {{ matches_regex("coalesce(store_group, '')", "^[0-9A-F]{8}$") }}
      and {{ matches_regex("coalesce(store_token, '')", "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$") }}
      and store_name is not null
      and length(store_name) between 1 and 200
)
select * from validated
