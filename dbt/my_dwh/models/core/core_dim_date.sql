with source as (

    select
        generate_series(
            date '2024-01-01',
            current_date + interval '3 years',
            interval '1 day'
        )::date as calendar_date

),

final as (

    select
        to_char(calendar_date, 'YYYYMMDD')::integer as date_key,
        calendar_date as date,
        extract(year from calendar_date)::integer as year,
        extract(month from calendar_date)::integer as month,
        trim(to_char(calendar_date, 'Day')) as weekday,
        extract(isodow from calendar_date) in (6, 7) as is_weekend

    from source

)

select *
from final
order by date
