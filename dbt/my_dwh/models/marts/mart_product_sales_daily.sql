with items as (
    select
        sale_key,
        product_key,
        quantity,
        line_amount_rub
    from {{ ref('core_fct_sales_item') }}
),

sales as (
    select
        sale_key,
        city_key,
        date_key
    from {{ ref('core_fct_sales') }}
),

items_with_sales as (
    select
        i.sale_key,
        i.product_key,
        i.quantity,
        i.line_amount_rub,
        s.city_key,
        s.date_key
    from items i left join sales s
    on i.sale_key = s.sale_key
),

product_sales_per_day as (
    select
        city_key,
        date_key,
        product_key,
        sum(quantity) as units_sold,
        count(distinct sale_key) as receipts_count,
        sum(line_amount_rub) as product_revenue_rub
    from items_with_sales
    group by
        city_key,
        date_key,
        product_key
),

city_revenue_per_day as (
    select
        city_key,
        date_key,
        sum(product_revenue_rub) as city_revenue_rub
    from product_sales_per_day
    group by
        city_key,
        date_key
),

calculated as (
    select
        p.city_key,
        p.date_key,
        p.product_key,
        p.units_sold,
        p.receipts_count,
        p.product_revenue_rub,

        round(
            p.product_revenue_rub / nullif(p.units_sold, 0),
            2
        ) as average_unit_price_rub,

        round(
            p.product_revenue_rub * 100.0
            / nullif(c.city_revenue_rub, 0),
            2
        ) as revenue_share_pct

    from product_sales_per_day p

    left join city_revenue_per_day c
        on p.city_key = c.city_key
        and p.date_key = c.date_key
)

select
    c.city_key,
    cities.city_slug,
    cities.city_name,

    c.date_key,
    dates.date as sales_date,

    c.product_key,
    products.product_name,

    c.units_sold,
    c.receipts_count,
    c.product_revenue_rub,
    c.average_unit_price_rub,
    c.revenue_share_pct

from calculated c

left join {{ ref('core_dim_city') }} cities
    on c.city_key = cities.city_key

left join {{ ref('core_dim_product') }} products
    on c.product_key = products.product_key

left join {{ ref('core_dim_date') }} dates
    on c.date_key = dates.date_key