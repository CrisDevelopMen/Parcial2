# Star schema contract (Glue Data Catalog / Athena)

You receive two reference files (generate them with `python -m airline_data.generate_p3 --seed <SEED> --out ref/`):
`routes_ref.csv` (route, origin, destination, region) and `route_category_history.csv`
(route, route_category, effective_date). Some routes change category on 2025-02-01.

Database: `aws.athena_database`. Parquet. Extra columns are allowed (you design attributes and hierarchies);
the columns below are required, with these types (any integer type counts as int).

| Table | Required columns |
|---|---|
| `dim_date` | `date_key` int (yyyymmdd), `full_date` date, `year` int, `month` int, `quarter` int, `weekday` int (1=Monday..7=Sunday), `weekday_name` string (English, Monday..Sunday), `is_weekend` boolean, `is_holiday` boolean, `holiday_name` string (empty if none), `is_long_weekend` boolean. One row per day, 2023-01-01 to 2027-12-31. Holidays come from `holidays_ref.csv` (Colombian public holidays). `is_long_weekend` is true for a holiday that falls on a Friday or Monday, and for the Saturday and Sunday of that weekend (plus the Monday/Friday that completes the three days). |
| `dim_route` | `route_key` int (surrogate), `route` string (natural key), `origin`, `destination`, `region` string, `route_category` string, `valid_from` date, `valid_to` date, `is_current` boolean. **Type 2**: one row per version; `valid_from` inclusive, `valid_to` exclusive, current row has `valid_to = 9999-12-31` and `is_current = true`. |
| `dim_fare_class` | `fare_class_key` int, `fare_class` string |
| `fact_search` | `event_id` string, `date_key` int, `route_key` int, `results_count` int, `searches` int (=1). Grain: one row per distinct search event. **Partitioned by `month`** (string `YYYY-MM`, month of `event_ts`), partitions registered in Glue. |
| `fact_booking_event` | `event_id` string, `date_key` int, `route_key` int, `fare_class_key` int, `reservation_id` string, `event_type` string, `bookings` int, `cancellations` int, `revenue_cents` int. Grain: one row per distinct reservation event. |

Measures: for `reservation_created`: bookings=1, cancellations=0, revenue_cents=+price_cents.
For `reservation_cancelled`: bookings=0, cancellations=1, revenue_cents=-price_cents.

Rules
* `date_key` comes from `event_ts` (when it happened), not from arrival time.
* `route_key` must point to the version of the route that was valid on the event date (as-of join).
* Deduplicate by `event_id`. Late and out-of-order events must produce the same result as if they were on time.
* The harness only checks events whose `event_id` starts with `rp-` (its replay set), but your pipeline must process everything.
* Your Athena workgroup must define a per-query data usage limit of at most 1 GB.


## Business questions (design driver)

Q1 revenue per month and fare class; Q2 reservations and cancellations per route; Q3 reservations per weekday name; Q4 top 10 routes by net revenue; Q5 revenue per month by route category valid at the event date; Q6 searches and reservations per route; Q7 searches and reservations on holidays vs other days; Q8 reservations on long-weekend days vs other days.
