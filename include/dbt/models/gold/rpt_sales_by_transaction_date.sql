{{ config(
    materialized='table',
    tags=['gold']
) }}

with sales as (
    select
        cast(transaction_time as date) as transaction_date,
        store_token,
        amount
    from {{ ref('silver_sales') }}
),
daily_store as (
    select
        transaction_date,
        store_token,
        sum(amount) as store_total
    from sales
    group by transaction_date, store_token
),
top_store_per_date as (
    select
        transaction_date,
        store_token as top_store_token
    from (
        select
            transaction_date,
            store_token,
            row_number() over (
                partition by transaction_date
                order by store_total desc, store_token
            ) as row_rank
        from daily_store
    )
    where row_rank = 1
),
daily as (
    select
        transaction_date,
        count(distinct store_token) as stores_with_transactions,
        sum(amount) as total_sales_amount,
        avg(amount) as total_sales_average
    from sales
    group by transaction_date
),
monthly as (
    select
        transaction_date,
        sum(total_sales_amount) over (
            partition by date_trunc('month', transaction_date)
            order by transaction_date
            rows between unbounded preceding and current row
        ) as month_accumulated_sales
    from daily
),
combined as (
    select
        d.transaction_date as transaction_date,
        d.stores_with_transactions as stores_with_transactions,
        d.total_sales_amount as total_sales_amount,
        d.total_sales_average as total_sales_average,
        m.month_accumulated_sales as month_accumulated_sales,
        t.top_store_token as top_store_token
    from daily d
    join monthly m on m.transaction_date = d.transaction_date
    left join top_store_per_date t on t.transaction_date = d.transaction_date
),
ranked as (
    select
        transaction_date,
        stores_with_transactions,
        total_sales_amount,
        total_sales_average,
        month_accumulated_sales,
        top_store_token,
        row_number() over (order by transaction_date desc) as row_rank
    from combined
)
select
    current_date as snapshot_date,
    transaction_date,
    stores_with_transactions,
    total_sales_amount,
    total_sales_average,
    month_accumulated_sales,
    top_store_token
from ranked
where row_rank <= 40
order by transaction_date desc
