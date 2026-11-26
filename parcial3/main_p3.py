"""Backend Parcial 3: reservas + eventos Kinesis + live analytics + star schema refresh + Athena.
Extiende el Parcial 2. Publica eventos de forma asíncrona (búsquedas) o post-commit (reservas/cancelaciones).
"""
import datetime as dt
import json
import os
import re
import time
import uuid
from pathlib import Path
from typing import Literal, Optional
from concurrent.futures import ThreadPoolExecutor

import boto3
from fastapi import FastAPI, HTTPException, Query, Request, Header
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
ATHENA_OUTPUT = os.environ.get("ATHENA_OUTPUT")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
KINESIS_STREAM = os.environ.get("KINESIS_STREAM", "airline-events")
RAW_EVENTS_URI = os.environ.get("RAW_EVENTS_URI", "s3://your-bucket/raw-events/")

STATIC = Path(__file__).parent / "static"
ROUTE_RE = re.compile(r"^[A-Z]{3}-[A-Z]{3}$")
MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
IATA = r"^[A-Za-z]{3}$"

pool = ConnectionPool(DATABASE_URL, min_size=2, max_size=POOL_MAX, timeout=30,
                      kwargs={"row_factory": dict_row}, open=False)
app = FastAPI(title="Airline reservations - Parcial 3")
executor = ThreadPoolExecutor(max_workers=4)

# --- Kinesis helper (publicación asíncrona para no ralentizar la API) ---
_kinesis = None

def kinesis():
    global _kinesis
    if _kinesis is None:
        _kinesis = boto3.client("kinesis", region_name=AWS_REGION)
    return _kinesis

def publish_event(event: dict, partition_key: str):
    """Publica de forma asíncrona. Una pérdida de search es aceptable; no bloquear al usuario."""
    def _put():
        try:
            kinesis().put_record(
                StreamName=KINESIS_STREAM,
                Data=json.dumps(event).encode("utf-8"),
                PartitionKey=partition_key,
            )
        except Exception:
            pass  # no fallar la API por un evento de search
    executor.submit(_put)

def make_event(event_type: str, **extra) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_ts": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "schema_version": 1,
        **extra,
    }

# --- Startup / Shutdown ---
@app.on_event("startup")
def _startup():
    pool.open(wait=True)

@app.on_event("shutdown")
def _shutdown():
    pool.close()
    executor.shutdown(wait=False)

# --- Models ---
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

# --- Endpoints existentes (Parcial 2) ---
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
def search(
    origin: str = Query(pattern=IATA),
    destination: str = Query(pattern=IATA),
    date: dt.date = Query(),
    x_session_id: Optional[str] = Header(None, alias="X-Session-Id"),
):
    route = f"{origin.upper()}-{destination.upper()}"
    with pool.connection() as c:
        rows = c.execute(
            "SELECT flight_id, route, flight_date, seats_available, price_cents FROM flights"
            " WHERE route=%s AND flight_date=%s AND seats_available > 0 ORDER BY flight_id",
            (route, date)).fetchall()
    for r in rows:
        r["flight_date"] = r["flight_date"].isoformat()

    # Publicar search_performed (asíncrono, no bloquea)
    session_id = x_session_id or str(uuid.uuid4())
    publish_event(
        make_event(
            "search_performed",
            session_id=session_id,
            origin=origin.upper(),
            destination=destination.upper(),
            travel_date=date.isoformat(),
            results_count=len(rows),
        ),
        partition_key=session_id,
    )
    return rows

@app.post("/reservations", status_code=201)
def reserve(r: ReservationIn):
    rid = uuid.uuid4().hex
    with pool.connection() as c:
        hit = c.execute(
            "UPDATE flights SET seats_available = seats_available - 1"
            " WHERE flight_id=%s AND seats_available > 0 RETURNING flight_id, route, flight_date, price_cents",
            (r.flight_id,)).fetchone()
        if hit is None:
            exists = c.execute("SELECT 1 FROM flights WHERE flight_id=%s", (r.flight_id,)).fetchone()
            raise HTTPException(409 if exists else 404, "sold out" if exists else "unknown flight")
        c.execute(
            "INSERT INTO reservations (reservation_id, flight_id, passenger_name, fare_class, status)"
            " VALUES (%s,%s,%s,%s,'CONFIRMED')",
            (rid, r.flight_id, r.passenger_name, r.fare_class))
        flight_info = hit

    # Publicar DESPUÉS del commit (transacción de negocio)
    publish_event(
        make_event(
            "reservation_created",
            reservation_id=rid,
            flight_id=r.flight_id,
            route=flight_info["route"],
            flight_date=flight_info["flight_date"].isoformat(),
            fare_class=r.fare_class,
            price_cents=flight_info["price_cents"],
        ),
        partition_key=r.flight_id,
    )
    return {"reservation_id": rid, "flight_id": r.flight_id, "status": "CONFIRMED",
            "passenger_name": r.passenger_name}

@app.get("/reservations/{reservation_id}")
def get_reservation(reservation_id: str):
    with pool.connection() as c:
        row = c.execute(
            "SELECT reservation_id, flight_id, status, passenger_name FROM reservations"
            " WHERE reservation_id=%s", (reservation_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "reservation not found")
    return row

# --- Nuevos endpoints Parcial 3 ---
@app.post("/reservations/{reservation_id}/cancel")
def cancel_reservation(reservation_id: str):
    with pool.connection() as c:
        row = c.execute(
            "SELECT r.reservation_id, r.flight_id, r.status, r.fare_class, "
            "f.route, f.flight_date, f.price_cents "
            "FROM reservations r JOIN flights f ON r.flight_id = f.flight_id "
            "WHERE r.reservation_id=%s FOR UPDATE",
            (reservation_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "reservation not found")
        if row["status"] == "CANCELLED":
            raise HTTPException(409, "already cancelled")
        c.execute(
            "UPDATE reservations SET status='CANCELLED' WHERE reservation_id=%s",
            (reservation_id,))
        c.execute(
            "UPDATE flights SET seats_available = seats_available + 1 WHERE flight_id=%s",
            (row["flight_id"],))

    publish_event(
        make_event(
            "reservation_cancelled",
            reservation_id=reservation_id,
            flight_id=row["flight_id"],
            route=row["route"],
            flight_date=row["flight_date"].isoformat(),
            fare_class=row["fare_class"],
            price_cents=row["price_cents"],
        ),
        partition_key=row["flight_id"],
    )
    return {"reservation_id": reservation_id, "status": "CANCELLED"}

# Live analytics: en producción un consumidor Kinesis mantiene estos contadores.
# Aquí un stub simple (reemplazar por el consumidor real que lee el stream).
_live_counters = {"searches": 0, "reservations": 0, "cancellations": 0, "window_start": time.time()}

@app.get("/analytics/live")
def analytics_live(window: int = Query(5, ge=1, le=60)):
    # En producción: el consumidor Kinesis actualiza _live_counters según event_ts
    # de los últimos `window` minutos. Aquí devolvemos el estado actual.
    searches = _live_counters["searches"]
    reservations = _live_counters["reservations"]
    cancellations = _live_counters["cancellations"]
    conversion = (reservations / searches) if searches else 0.0
    return {
        "window_minutes": window,
        "searches": searches,
        "reservations": reservations,
        "cancellations": cancellations,
        "conversion": round(conversion, 4),
    }

REFRESH_LAMBDA = os.environ.get("REFRESH_LAMBDA", "refresh-star-job")
_refresh_state = {"state": "SUCCEEDED", "job_id": None}

_lambda = None

def lambda_client():
    global _lambda
    if _lambda is None:
        _lambda = boto3.client("lambda", region_name=AWS_REGION)
    return _lambda

@app.post("/admin/refresh-star")
def refresh_star():
    """Lanza el Lambda de refresh de forma asíncrona."""
    job_id = str(uuid.uuid4())
    try:
        resp = lambda_client().invoke(
            FunctionName=REFRESH_LAMBDA,
            InvocationType="Event",  # asíncrono
            Payload=json.dumps({"job_id": job_id}).encode(),
        )
        status = resp.get("StatusCode", 0)
        if status not in (202, 200):
            raise RuntimeError(f"Lambda invoke status {status}")
        _refresh_state["state"] = "RUNNING"
        _refresh_state["job_id"] = job_id
        return JSONResponse({"job_id": job_id}, status_code=202)
    except Exception as e:
        _refresh_state["state"] = "FAILED"
        raise HTTPException(503, f"failed to start refresh job: {e}")

STATUS_TABLE = os.environ.get("STATUS_TABLE", "refresh_jobs")
_dynamo = None

def dynamo():
    global _dynamo
    if _dynamo is None:
        _dynamo = boto3.resource("dynamodb", region_name=AWS_REGION)
    return _dynamo

@app.get("/admin/refresh-status")
def refresh_status():
    """Lee el estado real desde DynamoDB (escrito por el Lambda)."""
    job_id = _refresh_state.get("job_id")
    if not job_id:
        return {"state": _refresh_state.get("state", "SUCCEEDED")}
    try:
        item = dynamo().Table(STATUS_TABLE).get_item(Key={"job_id": job_id}).get("Item")
        if item and "state" in item:
            return {"state": item["state"]}
    except Exception:
        pass
    return {"state": _refresh_state.get("state", "RUNNING")}

# --- Athena (existente + nuevas consultas de negocio) ---
_athena = None

def athena():
    global _athena
    if _athena is None:
        _athena = boto3.client("athena", region_name=AWS_REGION)
    return _athena

def run_athena(sql: str, timeout: float = 30.0):
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
        return []
    head = [d.get("VarCharValue") for d in rows[0]["Data"]]
    return [dict(zip(head, [d.get("VarCharValue") for d in r["Data"]])) for r in rows[1:]]

@app.get("/analytics/occupancy")
def occupancy(route: str = Query(), month: str = Query()):
    route = route.upper()
    if not ROUTE_RE.match(route) or not MONTH_RE.match(month):
        raise HTTPException(422, "route must be AAA-BBB and month YYYY-MM")
    sql = (f"SELECT route, month, occupancy, seats_sold, seats_capacity FROM {ATHENA_TABLE}"
           f" WHERE route='{route}' AND month='{month}' LIMIT 1")
    try:
        rows = run_athena(sql)
    except Exception as e:
        return JSONResponse({"detail": f"athena error: {e}"}, status_code=503)
    if not rows:
        raise HTTPException(404, "no data for route/month")
    row = rows[0]
    return {"route": row["route"], "month": row["month"], "occupancy": float(row["occupancy"]),
            "seats_sold": int(row["seats_sold"]), "seats_capacity": int(row["seats_capacity"])}

# Nuevos endpoints de analytics (Q1-Q6) para el panel
@app.get("/analytics/revenue-by-month-fare")
def revenue_by_month_fare():
    sql = """
    SELECT d.year, d.month, fc.fare_class,
           SUM(f.revenue_cents) AS net_revenue_cents
    FROM fact_booking_event f
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_fare_class fc ON f.fare_class_key = fc.fare_class_key
    GROUP BY d.year, d.month, fc.fare_class
    ORDER BY d.year, d.month, fc.fare_class
    """
    try:
        return run_athena(sql)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=503)

@app.get("/analytics/reservations-by-route")
def reservations_by_route():
    sql = """
    SELECT r.route,
           SUM(f.bookings) AS reservations,
           SUM(f.cancellations) AS cancellations
    FROM fact_booking_event f
    JOIN dim_route r ON f.route_key = r.route_key AND r.is_current = true
    GROUP BY r.route
    ORDER BY reservations DESC
    """
    try:
        return run_athena(sql)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=503)

@app.get("/analytics/reservations-by-weekday")
def reservations_by_weekday():
    sql = """
    SELECT d.weekday_name, SUM(f.bookings) AS reservations
    FROM fact_booking_event f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE f.bookings = 1
    GROUP BY d.weekday_name
    ORDER BY CASE d.weekday_name
        WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6
        WHEN 'Sunday' THEN 7 END
    """
    try:
        return run_athena(sql)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=503)

@app.get("/analytics/top-routes-revenue")
def top_routes_revenue():
    sql = """
    SELECT r.route, SUM(f.revenue_cents) AS net_revenue_cents
    FROM fact_booking_event f
    JOIN dim_route r ON f.route_key = r.route_key AND r.is_current = true
    GROUP BY r.route
    ORDER BY net_revenue_cents DESC
    LIMIT 10
    """
    try:
        return run_athena(sql)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=503)

@app.get("/analytics/revenue-by-month-category")
def revenue_by_month_category():
    sql = """
    SELECT d.year, d.month, r.route_category,
           SUM(f.revenue_cents) AS net_revenue_cents
    FROM fact_booking_event f
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_route r ON f.route_key = r.route_key
    GROUP BY d.year, d.month, r.route_category
    ORDER BY d.year, d.month, r.route_category
    """
    try:
        return run_athena(sql)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=503)

@app.get("/analytics/searches-vs-reservations")
def searches_vs_reservations():
    sql = """
    WITH s AS (
      SELECT r.route, SUM(fs.searches) AS searches
      FROM fact_search fs
      JOIN dim_route r ON fs.route_key = r.route_key AND r.is_current = true
      GROUP BY r.route
    ),
    b AS (
      SELECT r.route, SUM(fb.bookings) AS reservations
      FROM fact_booking_event fb
      JOIN dim_route r ON fb.route_key = r.route_key AND r.is_current = true
      GROUP BY r.route
    )
    SELECT COALESCE(s.route, b.route) AS route,
           COALESCE(s.searches, 0) AS searches,
           COALESCE(b.reservations, 0) AS reservations
    FROM s FULL OUTER JOIN b ON s.route = b.route
    ORDER BY searches DESC
    """
    try:
        return run_athena(sql)
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=503)

# --- Frontend ---
@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")

@app.get("/manager")
def manager():
    return FileResponse(STATIC / "manager.html")
