select s.sale_key
from {{ ref('core_fct_sales') }} s
join {{ ref('core_dim_city') }} c using (city_key)
join {{ ref('core_dim_date') }} d using (date_key)
where (s.sold_at at time zone c.timezone)::date <> d.date
   or extract(hour from s.sold_at at time zone c.timezone)::integer <> s.hour
