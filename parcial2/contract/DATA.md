# Data contract (EMR output + Glue/Athena tables)

Your raw data is produced by `python -m airline_data.generate --seed <YOUR_SEED> --rows 2000000 --out raw/`
(files `flights.csv`, `reservations.csv`) and uploaded to S3. It is deliberately dirty.

## Cleaning rules your EMR (PySpark) job must implement
* C1 `route`: remove all whitespace and upper-case it (`' bog - mde '` -> `BOG-MDE`).
* C2 `status`: strip whitespace and upper-case it.
* C3 keep only `status` in `{CONFIRMED, CANCELLED}`.
* C4 drop rows with missing `price_cents` or `price_cents <= 0`.
* C5 drop duplicate `reservation_id` (keep the first).

## Table 1 – `reservations_curated` (Parquet, partitioned by `month`)
Columns (in this order, exact names): `reservation_id`(string) `flight_id`(string) `route`(string)
`fare_class`(string) `price_cents`(bigint) `status`(string); partition column `month` (string, `YYYY-MM`,
the month of `flight_date`). Must be registered in the Glue Data Catalog, 24 partitions (or partition projection).

## Table 2 – `route_month_metrics` (Parquet, may be unpartitioned)
`route`, `month`, `seats_capacity`(bigint, sum of capacity of that route's flights in that month),
`seats_sold`(bigint, CONFIRMED reservations), `occupancy`(double = seats_sold/seats_capacity),
`revenue_cents`(bigint, CONFIRMED), `bookings`(bigint = CONFIRMED count), `cancellations`(bigint).

Both tables are queried by the harness with Athena in the database named in `config.yaml`.
