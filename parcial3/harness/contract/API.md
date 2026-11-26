# API contract additions (Parcial 3)

Everything from the Parcial 2 API stays (`POST /admin/flights`, `GET /flights/search`, `POST /reservations`,
`GET /reservations/{id}`, `GET /health`). Parcial 2 requirements are NOT re-graded here, but the system must
keep working: slowing down or breaking them to publish events is a design error you will be asked about.

| Method & path | Behavior |
|---|---|
| `POST /reservations/{id}/cancel` | `200` with the reservation (`status: CANCELLED`); `404` unknown; `409` if already cancelled. Frees the seat. |
| `GET /analytics/live?window=5` | `200` `{"window_minutes": 5, "searches": n, "reservations": n, "cancellations": n, "conversion": float}` counting events whose **`event_ts`** is in the last `window` minutes. `conversion` = reservations / searches (0 if no searches). Must be fed by a consumer of the Kinesis stream, not by Athena, and reflect a new event in at most 10 s. |
| `POST /admin/refresh-star` | Starts the job that (re)builds the star schema from the landing zone. `202` `{"job_id": ...}` (or `200`). |
| `GET /admin/refresh-status` | `200` `{"state": "RUNNING"\|"SUCCEEDED"\|"FAILED"}` |
