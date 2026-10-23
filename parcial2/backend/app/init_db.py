"""Crea el esquema (idempotente). Se ejecuta una vez antes de arrancar uvicorn."""
import os
import time

import psycopg

DDL = """
CREATE TABLE IF NOT EXISTS flights (
    flight_id       TEXT PRIMARY KEY,
    route           TEXT NOT NULL,
    flight_date     DATE NOT NULL,
    capacity        INTEGER NOT NULL CHECK (capacity > 0),
    seats_available INTEGER NOT NULL CHECK (seats_available >= 0),
    price_cents     BIGINT NOT NULL CHECK (price_cents >= 0)
);
CREATE INDEX IF NOT EXISTS ix_flights_route_date ON flights (route, flight_date);
CREATE TABLE IF NOT EXISTS reservations (
    reservation_id TEXT PRIMARY KEY,
    flight_id      TEXT NOT NULL REFERENCES flights(flight_id),
    passenger_name TEXT NOT NULL,
    fare_class     TEXT NOT NULL CHECK (fare_class IN ('ECONOMY','BUSINESS')),
    status         TEXT NOT NULL DEFAULT 'CONFIRMED',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_res_flight ON reservations (flight_id);
"""

url = os.environ.get("DATABASE_URL", "postgresql://airline:airline@localhost:5432/airline")
for attempt in range(30):
    try:
        with psycopg.connect(url, connect_timeout=10) as c:
            c.execute(DDL)
        print("schema ok")
        break
    except psycopg.OperationalError as e:
        print(f"db not ready ({e}); retry {attempt + 1}/30")
        time.sleep(10)
else:
    raise SystemExit("could not reach database")
