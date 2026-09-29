CREATE TABLE {table}
(
    city_key String,
    city_slug String,
    city_name String,
    date_key Int32,
    sales_date Date,
    weekday String,
    is_weekend Bool,
    sales_count UInt64,
    total_revenue_rub Decimal(20, 2),
    average_check_rub Nullable(Decimal(20, 2)),
    avg_temperature_c Nullable(Float64),
    min_temperature_c Nullable(Decimal(6, 2)),
    max_temperature_c Nullable(Decimal(6, 2)),
    total_precipitation_mm Nullable(Decimal(20, 5)),
    temperature_hours_count UInt16,
    precipitation_hours_count UInt16
)
ENGINE = MergeTree
ORDER BY (city_key, sales_date)
