with cities as (

    select
        city_slug,
        city_name,
        latitude,
        longitude

    from {{ ref('stg_open_meteo__cities') }}

),

city_timezones as (

    select distinct
        city_slug,
        timezone

    from {{ ref('stg_synthetic__coffee_sales') }}

),

joined as (

    select
        c.city_slug,
        c.city_name,
        c.latitude,
        c.longitude,
        t.timezone

    from cities c

    left join city_timezones t
        on c.city_slug = t.city_slug

)

select
    md5(city_slug) as city_key,
    city_slug,
    city_name,
    timezone,
    latitude,
    longitude

from joined