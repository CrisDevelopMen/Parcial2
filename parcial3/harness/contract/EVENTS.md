# Event contract (Kinesis)

One stream, JSON records (UTF-8). Every event has the common envelope:

| Field | Type | Meaning |
|---|---|---|
| `event_id` | string | Unique per event (UUID recommended). A retry of the same event reuses the id. |
| `event_type` | string | `search_performed`, `reservation_created` or `reservation_cancelled` |
| `event_ts` | string | When it **happened**, UTC, `YYYY-MM-DDTHH:MM:SSZ` (not when it arrived) |
| `schema_version` | int | `1` |

| `event_type` | Extra fields | Kinesis partition key |
|---|---|---|
| `search_performed` | `session_id` (string), `origin`, `destination` (airport codes), `travel_date` (`YYYY-MM-DD`), `results_count` (int) | `session_id` |
| `reservation_created` | `reservation_id`, `flight_id`, `route` (`AAA-BBB`), `flight_date`, `fare_class` (`ECONOMY`/`BUSINESS`), `price_cents` (int) | `flight_id` |
| `reservation_cancelled` | same fields as `reservation_created` | `flight_id` |

Rules
* `GET /flights/search` publishes one `search_performed` per call. The request may carry the header
  `X-Session-Id`; if present it is the event's `session_id`, otherwise generate one. Publishing must not slow
  the response down (publish asynchronously; a lost search event is acceptable, a slow search is not).
* `POST /reservations` publishes one `reservation_created` **after** the reservation is stored.
* `POST /reservations/{id}/cancel` marks it CANCELLED, frees the seat and publishes one `reservation_cancelled`.
* Do **not** put passenger names or other personal data in events.

Landing zone: all events must reach `aws.raw_events_uri` in S3 as JSON (one object per line or concatenated JSON;
gzip allowed), within 5 minutes. The landing zone is append-only: duplicates may appear there.
