with source as (

    select * from {{ source('kaggle', 'kaggle_coffee') }}

),

renamed as (

    select
        (date + time) at time zone 'UTC'    as ordered_at,
        coffee_name as product_name,
        money::numeric(10, 2)   as transaction_amount,
        cash_type   as payment_type,
        hour_of_day as order_hour,
        time_of_day as day_period,
        weekday as weekday_name,
        weekdaysort as weekday_number,
        month_name,
        monthsort   as month_number

    from source

)

select * from renamed