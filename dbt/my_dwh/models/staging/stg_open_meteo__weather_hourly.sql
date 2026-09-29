with source as materialized (
    select
        s.city_slug,
        s.period_start,
        s.period_end,
        s.loaded_at,
        (s.payload->>'latitude')::numeric(9,5) as latitude,
        (s.payload->>'longitude')::numeric(9,5) as longitude,
        arrays.*
    from {{ source('open_meteo', 'weather_archive') }} s
    cross join lateral jsonb_to_record(s.payload->'hourly') as arrays(
        time text[], precipitation numeric[], temperature_2m numeric[],
        surface_pressure numeric[], apparent_temperature numeric[]
    )
    where s.payload is not null
)

select
    s.city_slug,
    s.period_start,
    s.period_end,
    s.loaded_at,
    h.time::timestamp at time zone 'UTC' as measured_at,
    h.precipitation::numeric(12,5) as precipitation_mm,
    h.temperature_2m::numeric(6,2) as temperature_c,
    h.surface_pressure::numeric(8,2) as surface_pressure_hpa,
    h.apparent_temperature::numeric(6,2) as apparent_temperature_c,
    s.latitude,
    s.longitude
from source s
cross join lateral unnest(
    s.time, s.precipitation, s.temperature_2m,
    s.surface_pressure, s.apparent_temperature
) as h(time, precipitation, temperature_2m, surface_pressure, apparent_temperature)
where h.time is not null
