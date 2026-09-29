with sales_per_day as (
    select
        city_key,
        date_key,
        count(*) as sales_count,
        sum(total_amount_rub) as total_revenue_rub
    from {{ ref('core_fct_sales') }}
    group by city_key, date_key
),

weather_per_day as (
    select
        city_key,
        date_key,
        avg(temperature_c) as avg_temperature_c,
        min(temperature_c) as min_temperature_c,
        max(temperature_c) as max_temperature_c,
        sum(precipitation_mm) as total_precipitation_mm,
        count(temperature_c) as temperature_hours_count,
        count(precipitation_mm) as precipitation_hours_count
    from {{ ref('core_fct_weather_hour') }}
    group by city_key, date_key
),

joined as (
    select 
        s.city_key,
        s.date_key,
        s.sales_count,
        s.total_revenue_rub,
        w.avg_temperature_c,
        w.min_temperature_c,
        w.max_temperature_c,
        w.total_precipitation_mm,
        coalesce(w.temperature_hours_count, 0) as temperature_hours_count,
        coalesce(w.precipitation_hours_count, 0) as precipitation_hours_count,
        round(
            s.total_revenue_rub / nullif(s.sales_count, 0),
            2
        ) as average_check_rub
    from sales_per_day s left join weather_per_day w
    on s.city_key = w.city_key and s.date_key = w.date_key
)

select
    j.city_key,
    cities.city_slug,
    cities.city_name,

    j.date_key,
    dates.date as sales_date,
    dates.weekday,
    dates.is_weekend,

    j.sales_count,
    j.total_revenue_rub,
    j.average_check_rub,

    j.avg_temperature_c,
    j.min_temperature_c,
    j.max_temperature_c,
    j.total_precipitation_mm,
    j.temperature_hours_count,
    j.precipitation_hours_count

from joined j

left join {{ ref('core_dim_city') }} cities
    on j.city_key = cities.city_key

left join {{ ref('core_dim_date') }} dates
    on j.date_key = dates.date_key