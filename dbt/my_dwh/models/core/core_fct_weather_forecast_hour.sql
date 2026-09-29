with forecast_source as (

    select
        city_slug,
        issued_at,
        forecast_for,
        forecast_days,
        temperature_c,
        apparent_temperature_c,
        precipitation_mm,
        surface_pressure_hpa,
        precipitation_probability_pct,
        loaded_at

    from {{ ref('stg_open_meteo__weather_forecast') }}

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
        forecast.issued_at,
        forecast.forecast_for,
        forecast.forecast_days,

        dates.date_key as forecast_date_key,

        extract(
            hour from forecast.forecast_for at time zone cities.timezone
        )::integer as forecast_hour,

        forecast.temperature_c,
        forecast.apparent_temperature_c,
        forecast.precipitation_mm,
        forecast.surface_pressure_hpa,
        forecast.precipitation_probability_pct,
        forecast.loaded_at

    from forecast_source forecast

    left join city_source cities
        on forecast.city_slug = cities.city_slug

    left join date_source dates
        on (
            forecast.forecast_for at time zone cities.timezone
        )::date = dates.date

)

select
    city_key,
    issued_at,
    forecast_for,
    forecast_days,
    forecast_date_key,
    forecast_hour,
    temperature_c,
    apparent_temperature_c,
    precipitation_mm,
    surface_pressure_hpa,
    precipitation_probability_pct,
    loaded_at

from joined