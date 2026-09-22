with source as (

    select * from {{ source('synthetically_from_spark', 'synthetic_coffee_sales') }}

),

renamed as (

    select
        source_sale_id,
        synthetic_sale_id,
        synthetic_sale_item_id,
        city_slug,
        timezone,
        date as sold_local_date,
        sold_at at time zone 'UTC' as sold_at,
        product_name,
        payment_type,
        amount_money as source_amount,
        price_multiplier,
        amount_rub::numeric(12,2),
        quantity,
        line_number,
        is_synthetic

    from source

)

select * from renamed