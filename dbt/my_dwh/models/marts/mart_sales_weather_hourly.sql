with sales_per_hour as (
    select
    count(*) as sales_count,
        city_key,
        date_key,
        hour,
        sum(total_amount_rub) as total_revenue_rub,
        avg(total_amount_rub) as average_check_rub 
    from {{ ref('core_fct_sales') }}
    group by city_key, date_key, hour
),

sales_period as (
    select
        city_key,
        min(date_key) as first_date_key,
        max(date_key) as last_date_key
    from sales_per_hour
    group by city_key
),

weather as (
    select 
        city_key,
        date_key,
        hour,
        measured_at,
        temperature_c,
        apparent_temperature_c,
        precipitation_mm,
        surface_pressure_hpa
    from {{ ref('core_fct_weather_hour') }}
),

joined_1 as (
    select
        w.city_key,
        w.date_key,
        w.hour,
        w.measured_at,
        w.temperature_c,
        w.apparent_temperature_c,
        w.precipitation_mm,
        w.surface_pressure_hpa,
        coalesce(s.sales_count, 0) as sales_count,
        coalesce(s.total_revenue_rub, 0) as total_revenue_rub,
        s.average_check_rub
    from weather w

    join sales_period p
        on w.city_key = p.city_key
        and w.date_key >= p.first_date_key
        and w.date_key <= p.last_date_key

    left join sales_per_hour s
        on w.city_key = s.city_key
        and w.date_key = s.date_key
        and w.hour = s.hour
),

joined_2 as (
    select
        j.*,
        c.city_slug,
        c.city_name,
        d.date as sales_date
    from joined_1 j left join {{ ref('core_dim_city') }} c
    on j.city_key = c.city_key
    left join {{ ref('core_dim_date') }} d
    on j.date_key = d.date_key
)

select *
from joined_2