with sales_source as (
    select
        city_slug,
        synthetic_sale_id,
        source_sale_id,
        sold_at,
        sold_local_date,
        payment_type,
        is_synthetic,
        amount_rub
    from {{ ref('stg_synthetic__coffee_sales') }}
),

city_source as (
    select
        city_key,
        city_slug,
        timezone
    from {{ ref('core_dim_city') }}
),

date_source as (
    select
        date_key,
        date
    from {{ ref('core_dim_date') }}
),

joined as (
    select 
        sales.synthetic_sale_id,
        sales.source_sale_id,
        cities.city_key,
        sales.sold_at,
        dates.date_key,
        cities.timezone,
        sales.amount_rub,
        sales.payment_type,
        sales.is_synthetic
    from sales_source sales
    left join city_source cities
    on sales.city_slug = cities.city_slug
    left join date_source dates 
    on sales.sold_local_date = dates.date
)


select
    synthetic_sale_id as sale_key,
    extract(hour from sold_at at time zone timezone)::integer as hour,
    amount_rub as total_amount_rub,
    source_sale_id,
    city_key,
    sold_at,
    date_key,
    payment_type,
    is_synthetic
from joined