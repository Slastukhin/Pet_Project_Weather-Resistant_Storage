with sales_items_source as (
    select
        synthetic_sale_item_id as sale_item_key,
        synthetic_sale_id as sale_key,
        line_number,
        quantity,
        ROUND((amount_rub / NULLIF(quantity::numeric(12,2), 0)), 2) as unit_price_rub,
        amount_rub as line_amount_rub,
        product_name
    from {{ ref('stg_synthetic__coffee_sales') }}
),

product_source as (
    select 
        product_key,
        product_name
    from {{ ref('core_dim_product') }}
),

joined as (
    select
        items.sale_item_key,
        items.sale_key,
        items.line_number,
        product.product_key,
        items.quantity,
        items.line_amount_rub,
        items.unit_price_rub
    from sales_items_source items
    left join  product_source product
    on trim(items.product_name) = product.product_name
)

select
    sale_item_key,
    sale_key,
    line_number,
    product_key,
    quantity,
    unit_price_rub,
    line_amount_rub
from joined