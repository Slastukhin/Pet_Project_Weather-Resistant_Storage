with

source as (
    select *
    from {{ source('open_meteo', 'weather_archive') }}
    where payload is not null
),

with_indices as (
    select
        city_slug,
        period_start,
        period_end,
        loaded_at,
        payload,
        payload->'hourly'->'time' as ts_arr,
        jsonb_array_length(payload->'hourly'->'time') as hours_count
    from source
),

exploded as (
    select
        s.city_slug,
        s.period_start,
        s.period_end,
        s.loaded_at,
        ((s.ts_arr ->> g.idx)::timestamp at time zone 'UTC') as measured_at,
        (s.payload->'hourly'->'precipitation'->>g.idx)::numeric(12,5) as precipitation_mm,
        (s.payload->'hourly'->'temperature_2m'->>g.idx)::numeric(6,2) as temperature_c,
        (s.payload->'hourly'->'surface_pressure'->>g.idx)::numeric(8,2) as surface_pressure_hpa,
        (s.payload->'hourly'->'apparent_temperature'->>g.idx)::numeric(6,2) as apparent_temperature_c,
        (s.payload->>'latitude')::numeric(9,5) as latitude,
        (s.payload->>'longitude')::numeric(9,5) as longitude
    from with_indices s
    cross join lateral generate_series(0, greatest(s.hours_count - 1, 0)) as g(idx)
    where s.hours_count > 0
)

select * from exploded