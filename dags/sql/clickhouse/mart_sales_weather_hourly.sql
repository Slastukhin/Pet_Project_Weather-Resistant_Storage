CREATE TABLE {table}
(
    city_key String,
    date_key Int32,
    hour UInt8,
    measured_at DateTime64(6, 'UTC'),
    temperature_c Nullable(Decimal(6, 2)),
    apparent_temperature_c Nullable(Decimal(6, 2)),
    precipitation_mm Nullable(Decimal(12, 5)),
    surface_pressure_hpa Nullable(Decimal(8, 2)),
    sales_count UInt64,
    total_revenue_rub Decimal(20, 2),
    average_check_rub Nullable(Decimal(38, 16)),
    city_slug String,
    city_name String,
    sales_date Date
)
ENGINE = MergeTree
ORDER BY (city_key, sales_date, hour)
