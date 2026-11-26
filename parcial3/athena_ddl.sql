-- DDL del esquema en estrella (ejecutar una vez en Athena / Glue)
-- dim_date, dim_route, dim_fare_class, fact_search, fact_booking_event

CREATE EXTERNAL TABLE IF NOT EXISTS dim_date (
  date_key INT,
  full_date DATE,
  year INT,
  month INT,
  quarter INT,
  weekday INT,
  weekday_name STRING,
  is_weekend BOOLEAN,
  is_holiday BOOLEAN,
  holiday_name STRING,
  is_long_weekend BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET/star/dim_date/';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_route (
  route_key INT,
  route STRING,
  origin STRING,
  destination STRING,
  region STRING,
  route_category STRING,
  valid_from DATE,
  valid_to DATE,
  is_current BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET/star/dim_route/';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_fare_class (
  fare_class_key INT,
  fare_class STRING
)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET/star/dim_fare_class/';

CREATE EXTERNAL TABLE IF NOT EXISTS fact_search (
  event_id STRING,
  date_key INT,
  route_key INT,
  results_count INT,
  searches INT
)
PARTITIONED BY (month STRING)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET/star/fact_search/';

CREATE EXTERNAL TABLE IF NOT EXISTS fact_booking_event (
  event_id STRING,
  date_key INT,
  route_key INT,
  fare_class_key INT,
  reservation_id STRING,
  event_type STRING,
  bookings INT,
  cancellations INT,
  revenue_cents BIGINT
)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET/star/fact_booking_event/';

-- Después de escribir nuevos Parquet de fact_search:
-- MSCK REPAIR TABLE fact_search;
