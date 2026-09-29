with items as (
    select sale_key, sum(line_amount_rub) as amount
    from {{ ref('core_fct_sales_item') }}
    group by sale_key
)
select coalesce(s.sale_key, i.sale_key) as sale_key
from {{ ref('core_fct_sales') }} s
full join items i on s.sale_key = i.sale_key
where s.total_amount_rub is distinct from i.amount
