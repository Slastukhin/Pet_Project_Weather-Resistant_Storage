select 'archive' as source, city_key, measured_at as weather_time
from {{ ref('core_fct_weather_hour') }}
where hour not between 0 and 23 or precipitation_mm < 0
   or surface_pressure_hpa <= 0
union all
select 'forecast', city_key, forecast_for
from {{ ref('core_fct_weather_forecast_hour') }}
where forecast_hour not between 0 and 23 or forecast_days <= 0
   or precipitation_mm < 0 or surface_pressure_hpa <= 0
   or precipitation_probability_pct not between 0 and 100
