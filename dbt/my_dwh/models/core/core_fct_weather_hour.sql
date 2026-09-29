with weather_source as (
    select
        city_slug,
        period_start,
        period_end,
        loaded_at,
        measured_at,
        temperature_c,
        apparent_temperature_c,
        precipitation_mm,
        surface_pressure_hpa
    from {{ ref('stg_open_meteo__weather_hourly') }}
),

ranked_weather as (
    select
        *,
        row_number() over (
            partition by city_slug, measured_at
            order by loaded_at desc, period_end desc, period_start desc
        ) as row_number
    from weather_source
),

weather_deduplicated as (
    select
        city_slug,
        loaded_at,
        measured_at,
        temperature_c,
        apparent_temperature_c,
        precipitation_mm,
        surface_pressure_hpa
    from ranked_weather
    where row_number = 1
),

city_source as (
    select
        city_key,
        city_slug,
        timezone
    from {{ ref('core_dim_city') }}
),

date_source as (
    select
        date_key,
        date
    from {{ ref('core_dim_date') }}
),

joined as (
    select
        cities.city_key,
        weather.measured_at,
        dates.date_key,
        extract(
            hour from weather.measured_at at time zone cities.timezone
        )::integer as hour,
        weather.temperature_c,
        weather.apparent_temperature_c,
        weather.precipitation_mm,
        weather.surface_pressure_hpa,
        weather.loaded_at
    from weather_deduplicated weather
    left join city_source cities
        on weather.city_slug = cities.city_slug
    left join date_source dates
        on (weather.measured_at at time zone cities.timezone)::date = dates.date
)

select
    city_key,
    measured_at,
    date_key,
    hour,
    temperature_c,
    apparent_temperature_c,
    precipitation_mm,
    surface_pressure_hpa,
    loaded_at
from joined
