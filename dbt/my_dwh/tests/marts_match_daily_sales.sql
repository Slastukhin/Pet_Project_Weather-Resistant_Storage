with expected as (
    select city_key, date_key, count(*) as sales_count,
           sum(total_amount_rub) as revenue
    from {{ ref('core_fct_sales') }}
    group by city_key, date_key
),
actual as (
    select 'hourly' as mart, city_key, date_key,
           sum(sales_count) as sales_count, sum(total_revenue_rub) as revenue
    from {{ ref('mart_sales_weather_hourly') }}
    group by city_key, date_key
    union all
    select 'daily', city_key, date_key, sales_count, total_revenue_rub
    from {{ ref('mart_sales_weather_daily') }}
),
marts as (
    select 'hourly' as mart union all select 'daily'
),
expected_per_mart as (
    select m.mart, e.* from expected e cross join marts m
)
select coalesce(e.mart, a.mart) as mart,
       coalesce(e.city_key, a.city_key) as city_key,
       coalesce(e.date_key, a.date_key) as date_key
from expected_per_mart e
full join actual a using (mart, city_key, date_key)
where coalesce(e.sales_count, 0) <> coalesce(a.sales_count, 0)
   or coalesce(e.revenue, 0) <> coalesce(a.revenue, 0)
