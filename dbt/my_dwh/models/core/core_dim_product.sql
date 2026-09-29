with source as (

    select
        trim(product_name) as product_name

    from {{ ref('stg_synthetic__coffee_sales') }}

    where product_name is not null

),

unique_products as (

    select distinct
        product_name

    from source

)

select
    md5(product_name) as product_key,
    product_name

from unique_products