with products as (
    select city_key, date_key, sum(product_revenue_rub) as revenue
    from {{ ref('mart_product_sales_daily') }}
    group by city_key, date_key
),
sales as (
    select city_key, date_key, sum(total_amount_rub) as revenue
    from {{ ref('core_fct_sales') }}
    group by city_key, date_key
)
select coalesce(p.city_key, s.city_key) as city_key,
       coalesce(p.date_key, s.date_key) as date_key
from products p full join sales s using (city_key, date_key)
where p.revenue is distinct from s.revenue
