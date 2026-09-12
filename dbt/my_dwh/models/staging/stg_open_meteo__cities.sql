with source as (
    select * from {{ source('open_meteo', 'cities') }}
),

renamed as (
    select

        city_slug,
        city_name,
        latitude::numeric(9,5),
        longitude::numeric(9,5),
        updated_at

    from source

)

select * from renamed
