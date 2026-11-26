# Cambios generados para Parcial 3

## Archivos
- `main_p3.py` — backend completo con:
  - Publicación de eventos a Kinesis (asíncrona en búsquedas, post-commit en reservas/cancelaciones)
  - `POST /reservations/{id}/cancel`
  - `GET /analytics/live`
  - `POST /admin/refresh-star` + `GET /admin/refresh-status`
  - Endpoints Athena para Q1–Q6 del panel gerencial

- `athena_ddl.sql` — DDL de las tablas del esquema en estrella
- `cost_projection.yaml` — proyección de costo (bajo el límite de R12)

## Qué falta implementar en AWS (no en código)
1. Stream Kinesis (1 shard).
2. Firehose → S3 landing zone (`RAW_EVENTS_URI`).
3. Job de refresh (Lambda o script) que:
   - Lee la landing zone
   - Deduplica por event_id
   - Construye dim_date enriquecida + dim_route SCD2
   - Escribe Parquet y registra particiones
4. Consumidor Kinesis para alimentar los contadores de `/analytics/live` (puede ser Lambda o proceso aparte).
5. Workgroup Athena con límite de 1 GB.

## Cómo usar
1. Reemplaza tu `backend/app/main.py` con `main_p3.py`.
2. Ajusta las variables de entorno (`KINESIS_STREAM`, `ATHENA_DB`, `ATHENA_OUTPUT`, etc.).
3. Despliega en AWS y corre el harness nuevo.

## Job de refresh (nuevo)
- `refresh_star_job.py` — esqueleto completo que:
  - Lee la landing zone S3 (JSON/gz)
  - Deduplica por event_id
  - Construye dim_date enriquecida (festivos + long weekends)
  - Construye dim_route SCD Type 2
  - Hace as-of join para route_key
  - Escribe fact_search (particionado por month) y fact_booking_event en Parquet
- `lambda_handler.py` — wrapper para desplegarlo como Lambda
- `requirements_job.txt` — dependencias del job

### Cómo invocarlo desde el backend
En `main_p3.py` el endpoint `POST /admin/refresh-star` puede lanzar este Lambda
(o un Glue job / ECS task) en lugar de la simulación actual.
