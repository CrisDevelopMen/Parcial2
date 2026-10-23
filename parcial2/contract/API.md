# API contract (backend)

The harness talks to your backend ONLY through these endpoints. JSON in, JSON out.

| Method & path | Request | Success | Failure |
|---|---|---|---|
| `POST /admin/flights` | `{"flight_id": str, "route": "AAA-BBB", "flight_date": "YYYY-MM-DD", "capacity": int, "price_cents": int}` | `201` (same body) | `409` if flight_id exists |
| `GET /flights/search?origin=AAA&destination=BBB&date=YYYY-MM-DD` | – | `200` JSON **list** of `{"flight_id", "route", "flight_date", "seats_available", "price_cents"}` (empty list if none; only flights with `seats_available > 0` are required, others may be included) | `422` on bad params |
| `POST /reservations` | `{"flight_id": str, "passenger_name": str, "fare_class": "ECONOMY"\|"BUSINESS"}` | `201` `{"reservation_id", "flight_id", "status": "CONFIRMED", "passenger_name"}` | `409` when the flight has no seats left; `404` unknown flight |
| `GET /reservations/{reservation_id}` | – | `200` same shape as above | `404` |
| `GET /analytics/occupancy?route=AAA-BBB&month=YYYY-MM` | – | `200` `{"route", "month", "occupancy": float 0..1, "seats_sold": int, "seats_capacity": int}` computed **from Athena** | `404` if no data |
| `GET /health` | – | `200` | – |

Rules the harness checks:
* Never return `5xx` under concurrency; "sold out" is `409`, not `500`.
* A flight with capacity N never has more than N reservations with status CONFIRMED.
* `/analytics/occupancy` must be served from the Athena table `route_month_metrics` (the harness inspects
  Athena's query history during the check). Caching is allowed only if the first request per (route, month) queries Athena.
