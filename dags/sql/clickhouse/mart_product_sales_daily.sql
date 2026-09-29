CREATE TABLE {table}
(
    city_key String,
    city_slug String,
    city_name String,
    date_key Int32,
    sales_date Date,
    product_key String,
    product_name String,
    units_sold UInt64,
    receipts_count UInt64,
    product_revenue_rub Decimal(20, 2),
    average_unit_price_rub Nullable(Decimal(20, 2)),
    revenue_share_pct Nullable(Decimal(7, 2))
)
ENGINE = MergeTree
ORDER BY (city_key, sales_date, product_key)
