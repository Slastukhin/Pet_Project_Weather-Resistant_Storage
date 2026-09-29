select 'sale' as source, sale_key as row_key
from {{ ref('core_fct_sales') }}
where total_amount_rub < 0 or hour not between 0 and 23
union all
select 'item', sale_item_key
from {{ ref('core_fct_sales_item') }}
where quantity <= 0 or line_number <= 0
   or unit_price_rub < 0 or line_amount_rub < 0
   or abs(line_amount_rub - quantity * unit_price_rub) > quantity * 0.01
