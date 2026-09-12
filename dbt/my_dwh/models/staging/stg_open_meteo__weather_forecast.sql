with

source as (
    select *
    from {{ source('open_meteo','weather_forecast') }}
    where payload is not null

),

with_indices as (
    select 
        city_slug,
        issued_at, 
        forecast_days,
        loaded_at,
        payload,
        payload->'hourly'->'time' as ts_arr,
        jsonb_array_length(payload->'hourly'->'time') as hours_count
    from source
),

exploded as (

    select 
        s.city_slug,
        s.issued_at,
        s.forecast_days,
        s.loaded_at,
        ((s.ts_arr->>g.idx)::timestamp at time zone 'UTC') as forecast_for,
        (s.payload->'hourly'->'precipitation'->>g.idx)::numeric(12,5) as precipitation_mm,
        (s.payload->'hourly'->'temperature_2m'->>g.idx)::numeric(6,2) as temperature_c,
        (s.payload->'hourly'->'surface_pressure'->>g.idx)::numeric(6,2) as surface_pressure_hpa,
        (s.payload->'hourly'->'apparent_temperature'->>g.idx)::numeric(6,2) as apparent_temperature_c,
        (s.payload->'hourly'->'precipitation_probability'->>g.idx)::numeric(6,2) as precipitation_probability_pct,
        (s.payload->>'latitude')::numeric(9,5) as latitude,
        (s.payload->>'longitude')::numeric(9,5) as longitude
    from with_indices s
    cross join lateral generate_series(0, (s.hours_count - 1)) as g(idx)
    where s.hours_count > 0

)

select * from exploded