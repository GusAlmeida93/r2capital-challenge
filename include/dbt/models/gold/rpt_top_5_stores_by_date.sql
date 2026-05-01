-- Output 3: Top 5 sales stores for each of the last 10 transaction dates.
-- Up to 5 rows per transaction_date.

{{ config(materialized='table', tags=['gold']) }}

with sales as (
    select
        cast(transaction_time as date) as transaction_date,
        store_token,
        amount
    from {{ ref('silver_sales') }}
),
last_dates as (
    select distinct transaction_date
    from sales
    qualify row_number() over (order by transaction_date desc) <= 10
),
daily_store as (
    select
        s.transaction_date,
        s.store_token,
        sum(s.amount) as store_total_sales
    from sales s
    join last_dates d on d.transaction_date = s.transaction_date
    group by s.transaction_date, s.store_token
),
ranked as (
    select
        transaction_date,
        store_token,
        store_total_sales,
        row_number() over (
            partition by transaction_date
            order by store_total_sales desc, store_token
        ) as top_rank_id
    from daily_store
)
select
    current_date as snapshot_date,
    r.transaction_date,
    r.top_rank_id,
    r.store_total_sales,
    r.store_token,
    s.store_name
from ranked r
left join {{ ref('silver_stores') }} s
    on s.store_token = r.store_token
where r.top_rank_id <= 5
order by r.transaction_date desc, r.top_rank_id
