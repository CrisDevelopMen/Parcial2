"""Backend de reservas + endpoint analítico (Athena). Sirve también el frontend estático."""
import datetime as dt
import os
import re
import time
import uuid
from pathlib import Path
from typing import Literal

import boto3
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://airline:airline@localhost:5432/airline")
POOL_MAX = int(os.environ.get("POOL_MAX", "30"))
ATHENA_DB = os.environ.get("ATHENA_DB", "airline_analytics")
ATHENA_TABLE = os.environ.get("ATHENA_TABLE", "route_month_metrics")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")
ATHENA_OUTPUT = os.environ.get("ATHENA_OUTPUT")  # s3://bucket/athena-results/
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

STATIC = Path(__file__).parent / "static"
ROUTE_RE = re.compile(r"^[A-Z]{3}-[A-Z]{3}$")
MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
IATA = r"^[A-Za-z]{3}$"

pool = ConnectionPool(DATABASE_URL, min_size=2, max_size=POOL_MAX, timeout=30,
                      kwargs={"row_factory": dict_row}, open=False)
app = FastAPI(title="Airline reservations")


@app.on_event("startup")
def _startup():
    pool.open(wait=True)


@app.on_event("shutdown")
def _shutdown():
    pool.close()


class FlightIn(BaseModel):
    flight_id: str = Field(min_length=1, max_length=64)
    route: str = Field(pattern=r"^[A-Z]{3}-[A-Z]{3}$")
    flight_date: dt.date
    capacity: int = Field(gt=0)
    price_cents: int = Field(ge=0)


class ReservationIn(BaseModel):
    flight_id: str = Field(min_length=1)
    passenger_name: str = Field(min_length=1, max_length=200)
    fare_class: Literal["ECONOMY", "BUSINESS"]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/admin/flights", status_code=201)
def create_flight(f: FlightIn):
    try:
        with pool.connection() as c:
            c.execute(
                "INSERT INTO flights (flight_id, route, flight_date, capacity, seats_available, price_cents)"
                " VALUES (%s,%s,%s,%s,%s,%s)",
                (f.flight_id, f.route, f.flight_date, f.capacity, f.capacity, f.price_cents))
    except UniqueViolation:
        raise HTTPException(409, "flight_id already exists")
    return {**f.model_dump(), "flight_date": f.flight_date.isoformat()}


@app.get("/flights/search")
def search(origin: str = Query(pattern=IATA), destination: str = Query(pattern=IATA), date: dt.date = Query()):
    route = f"{origin.upper()}-{destination.upper()}"
    with pool.connection() as c:
        rows = c.execute(
            "SELECT flight_id, route, flight_date, seats_available, price_cents FROM flights"
            " WHERE route=%s AND flight_date=%s AND seats_available > 0 ORDER BY flight_id",
            (route, date)).fetchall()
    for r in rows:
        r["flight_date"] = r["flight_date"].isoformat()
    return rows


@app.post("/reservations", status_code=201)
def reserve(r: ReservationIn):
    rid = uuid.uuid4().hex
    with pool.connection() as c:  # una transacción: commit al salir, rollback si hay excepción
        # UPDATE atómico: Postgres bloquea la fila, así nunca se venden más asientos que la capacidad.
        hit = c.execute(
            "UPDATE flights SET seats_available = seats_available - 1"
            " WHERE flight_id=%s AND seats_available > 0 RETURNING flight_id", (r.flight_id,)).fetchone()
        if hit is None:
            exists = c.execute("SELECT 1 FROM flights WHERE flight_id=%s", (r.flight_id,)).fetchone()
            raise HTTPException(409 if exists else 404, "sold out" if exists else "unknown flight")
        c.execute(
            "INSERT INTO reservations (reservation_id, flight_id, passenger_name, fare_class, status)"
            " VALUES (%s,%s,%s,%s,'CONFIRMED')", (rid, r.flight_id, r.passenger_name, r.fare_class))
    return {"reservation_id": rid, "flight_id": r.flight_id, "status": "CONFIRMED",
            "passenger_name": r.passenger_name}


@app.get("/reservations/{reservation_id}")
def get_reservation(reservation_id: str):
    with pool.connection() as c:
        row = c.execute("SELECT reservation_id, flight_id, status, passenger_name FROM reservations"
                        " WHERE reservation_id=%s", (reservation_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "reservation not found")
    return row


_athena = None


def athena():
    global _athena
    if _athena is None:
        _athena = boto3.client("athena", region_name=AWS_REGION)
    return _athena


def run_athena(sql: str, timeout: float = 9.0):
    kw = {"QueryString": sql, "WorkGroup": ATHENA_WORKGROUP,
          "QueryExecutionContext": {"Database": ATHENA_DB}}
    if ATHENA_OUTPUT:
        kw["ResultConfiguration"] = {"OutputLocation": ATHENA_OUTPUT}
    qid = athena().start_query_execution(**kw)["QueryExecutionId"]
    t0, wait = time.time(), 0.2
    while True:
        st = athena().get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if st["State"] == "SUCCEEDED":
            break
        if st["State"] in ("FAILED", "CANCELLED"):
            raise RuntimeError(st.get("StateChangeReason", st["State"]))
        if time.time() - t0 > timeout:
            raise TimeoutError("athena timeout")
        time.sleep(wait)
        wait = min(wait * 1.5, 1.0)
    rows = athena().get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    if len(rows) < 2:
        return None
    head = [d.get("VarCharValue") for d in rows[0]["Data"]]
    return dict(zip(head, [d.get("VarCharValue") for d in rows[1]["Data"]]))


@app.get("/analytics/occupancy")
def occupancy(route: str = Query(), month: str = Query()):
    route = route.upper()
    if not ROUTE_RE.match(route) or not MONTH_RE.match(month):
        raise HTTPException(422, "route must be AAA-BBB and month YYYY-MM")
    # Valores ya validados por regex -> sin riesgo de inyección. Sin caché: cada llamada consulta Athena.
    sql = (f"SELECT route, month, occupancy, seats_sold, seats_capacity FROM {ATHENA_TABLE}"
           f" WHERE route='{route}' AND month='{month}' LIMIT 1")
    try:
        row = run_athena(sql)
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"detail": f"athena error: {e}"}, status_code=503)
    if row is None:
        raise HTTPException(404, "no data for route/month")
    return {"route": row["route"], "month": row["month"], "occupancy": float(row["occupancy"]),
            "seats_sold": int(row["seats_sold"]), "seats_capacity": int(row["seats_capacity"])}


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/manager")
def manager():
    return FileResponse(STATIC / "manager.html")
