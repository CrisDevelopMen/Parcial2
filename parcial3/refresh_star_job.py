"""
Esqueleto del job que reconstruye el esquema en estrella a partir de la landing zone.
Puede correr como:
  - Lambda (con timeout alto o Step Functions)
  - Glue Python Shell
  - Script en ECS / EC2 / local contra S3

Entrada:  RAW_EVENTS_URI (s3://bucket/prefix/)  — JSON o JSON.gz
Salida:   Parquet de dim_date, dim_route, dim_fare_class, fact_search, fact_booking_event
          + registro de particiones en Glue
"""
import json
import os
import gzip
import io
from datetime import datetime, date, timedelta
from pathlib import Path

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# --- Config (ajustar o tomar de env / SSM) ---
RAW_EVENTS_URI = os.environ.get("RAW_EVENTS_URI", "s3://your-bucket/raw-events/")
STAR_OUTPUT = os.environ.get("STAR_OUTPUT", "s3://your-bucket/star/")
ATHENA_DB = os.environ.get("ATHENA_DB", "airline_analytics")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
HOLIDAYS_CSV = os.environ.get("HOLIDAYS_CSV", "holidays_ref.csv")  # o path en S3
ROUTES_REF = os.environ.get("ROUTES_REF", "routes_ref.csv")
CATEGORY_HISTORY = os.environ.get("CATEGORY_HISTORY", "route_category_history.csv")

s3 = boto3.client("s3", region_name=AWS_REGION)
glue = boto3.client("glue", region_name=AWS_REGION)

# --- Status callback (DynamoDB) ---
STATUS_TABLE = os.environ.get("STATUS_TABLE", "refresh_jobs")
_dynamo = None

def dynamo():
    global _dynamo
    if _dynamo is None:
        _dynamo = boto3.resource("dynamodb", region_name=AWS_REGION)
    return _dynamo

def set_job_status(job_id: str, state: str, detail: str = ""):
    if not job_id:
        return
    try:
        dynamo().Table(STATUS_TABLE).put_item(Item={
            "job_id": job_id,
            "state": state,
            "detail": detail,
            "updated_at": datetime.utcnow().isoformat() + "Z",
        })
    except Exception as e:
        print(f"No se pudo actualizar status: {e}")

# ---------------------------------------------------------------------------
# 1. Leer eventos de la landing zone
# ---------------------------------------------------------------------------
def list_s3_objects(uri: str):
    # uri = s3://bucket/prefix/
    assert uri.startswith("s3://")
    parts = uri[5:].split("/", 1)
    bucket, prefix = parts[0], parts[1] if len(parts) > 1 else ""
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return bucket, keys

def read_events_from_s3(uri: str) -> pd.DataFrame:
    bucket, keys = list_s3_objects(uri)
    records = []
    for key in keys:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        if key.endswith(".gz"):
            body = gzip.decompress(body)
        for line in body.decode("utf-8").splitlines():
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)

# ---------------------------------------------------------------------------
# 2. Deduplicar por event_id (último gana o primero, según preferencia)
# ---------------------------------------------------------------------------
def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df.drop_duplicates(subset=["event_id"], keep="last")

# ---------------------------------------------------------------------------
# 3. Construir dim_date enriquecida (2023-01-01 a 2027-12-31)
# ---------------------------------------------------------------------------
def build_dim_date(holidays_df: pd.DataFrame) -> pd.DataFrame:
    start = date(2023, 1, 1)
    end = date(2027, 12, 31)
    dates = pd.date_range(start, end, freq="D")
    dim = pd.DataFrame({"full_date": dates.date})
    dim["date_key"] = dim["full_date"].apply(lambda d: int(d.strftime("%Y%m%d")))
    dim["year"] = dim["full_date"].apply(lambda d: d.year)
    dim["month"] = dim["full_date"].apply(lambda d: d.month)
    dim["quarter"] = dim["full_date"].apply(lambda d: (d.month - 1) // 3 + 1)
    dim["weekday"] = dim["full_date"].apply(lambda d: d.weekday() + 1)  # 1=Mon
    names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    dim["weekday_name"] = dim["weekday"].apply(lambda w: names[w - 1])
    dim["is_weekend"] = dim["weekday"] >= 6

    hol_map = dict(zip(holidays_df["date"], holidays_df["holiday_name"]))
    dim["is_holiday"] = dim["full_date"].isin(holidays_df["date"])
    dim["holiday_name"] = dim["full_date"].map(hol_map).fillna("")

    dim["is_long_weekend"] = False
    for hdate in holidays_df["date"]:
        wd = hdate.weekday()
        if wd == 4:  # Friday
            for off in (0, 1, 2):
                d = hdate + timedelta(days=off)
                dim.loc[dim["full_date"] == d, "is_long_weekend"] = True
        elif wd == 0:  # Monday
            for off in (-2, -1, 0):
                d = hdate + timedelta(days=off)
                dim.loc[dim["full_date"] == d, "is_long_weekend"] = True
    return dim

# ---------------------------------------------------------------------------
# 4. Construir dim_route SCD Type 2
# ---------------------------------------------------------------------------
def build_dim_route(routes_df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    key = 1
    far_future = date(9999, 12, 31)
    start = date(2023, 1, 1)
    for _, r in routes_df.iterrows():
        route = r["route"]
        rh = history_df[history_df["route"] == route].sort_values("effective_date")
        if rh.empty:
            rows.append({
                "route_key": key, "route": route,
                "origin": r["origin"], "destination": r["destination"],
                "region": r.get("region", ""), "route_category": "STANDARD",
                "valid_from": start, "valid_to": far_future, "is_current": True,
            })
            key += 1
            continue
        prev = start
        for _, hr in rh.iterrows():
            eff = pd.to_datetime(hr["effective_date"]).date()
            rows.append({
                "route_key": key, "route": route,
                "origin": r["origin"], "destination": r["destination"],
                "region": r.get("region", ""), "route_category": hr["route_category"],
                "valid_from": prev, "valid_to": eff, "is_current": False,
            })
            key += 1
            prev = eff
        rows.append({
            "route_key": key, "route": route,
            "origin": r["origin"], "destination": r["destination"],
            "region": r.get("region", ""), "route_category": rh.iloc[-1]["route_category"],
            "valid_from": prev, "valid_to": far_future, "is_current": True,
        })
        key += 1
    return pd.DataFrame(rows)

# ---------------------------------------------------------------------------
# 5. As-of join: route_key vigente en la fecha del evento
# ---------------------------------------------------------------------------
def asof_route_key(dim_route: pd.DataFrame, route: str, event_date: date) -> int | None:
    m = dim_route[
        (dim_route["route"] == route)
        & (dim_route["valid_from"] <= event_date)
        & (dim_route["valid_to"] > event_date)
    ]
    return int(m.iloc[0]["route_key"]) if not m.empty else None

# ---------------------------------------------------------------------------
# 6. Construir fact tables
# ---------------------------------------------------------------------------
def build_facts(events: pd.DataFrame, dim_route: pd.DataFrame):
    fare_map = {"ECONOMY": 1, "BUSINESS": 2}
    searches, bookings = [], []
    for _, ev in events.iterrows():
        try:
            ts = datetime.strptime(ev["event_ts"], "%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            continue
        dkey = int(ts.strftime("%Y%m%d"))
        edate = ts.date()
        route = ev.get("route") or f"{ev.get('origin','')}-{ev.get('destination','')}"
        rk = asof_route_key(dim_route, route, edate)
        if rk is None:
            continue
        etype = ev["event_type"]
        if etype == "search_performed":
            searches.append({
                "event_id": ev["event_id"],
                "date_key": dkey,
                "route_key": rk,
                "results_count": int(ev.get("results_count", 0)),
                "searches": 1,
                "month": ts.strftime("%Y-%m"),
            })
        elif etype in ("reservation_created", "reservation_cancelled"):
            is_create = etype == "reservation_created"
            bookings.append({
                "event_id": ev["event_id"],
                "date_key": dkey,
                "route_key": rk,
                "fare_class_key": fare_map.get(ev.get("fare_class"), 1),
                "reservation_id": ev.get("reservation_id", ""),
                "event_type": etype,
                "bookings": 1 if is_create else 0,
                "cancellations": 0 if is_create else 1,
                "revenue_cents": int(ev.get("price_cents", 0)) * (1 if is_create else -1),
            })
    return pd.DataFrame(searches), pd.DataFrame(bookings)

# ---------------------------------------------------------------------------
# 7. Escribir Parquet a S3
# ---------------------------------------------------------------------------
def write_parquet_s3(df: pd.DataFrame, s3_uri: str, partition_col: str | None = None):
    if df.empty:
        return
    bucket, key = s3_uri[5:].split("/", 1)
    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

def write_partitioned(df: pd.DataFrame, base_uri: str, partition_col: str):
    """Escribe fact_search particionado por month=YYYY-MM"""
    if df.empty:
        return
    for val, part in df.groupby(partition_col):
        uri = f"{base_uri.rstrip('/')}/{partition_col}={val}/data.parquet"
        write_parquet_s3(part.drop(columns=[partition_col]), uri)

# ---------------------------------------------------------------------------
# 8. Registrar particiones en Glue (opcional, o usar MSCK REPAIR)
# ---------------------------------------------------------------------------
def register_partitions(database: str, table: str, partitions: list[dict]):
    # partitions = [{"Values": ["2025-05"], "StorageDescriptor": {...}}]
    # Simplificado: en la práctica se puede llamar MSCK REPAIR TABLE desde Athena
    pass

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(job_id: str | None = None):
    job_id = job_id or os.environ.get("JOB_ID", "")
    set_job_status(job_id, "RUNNING")
    try:
        print("Leyendo landing zone...")
        events = read_events_from_s3(RAW_EVENTS_URI)
        print(f"Eventos crudos: {len(events)}")
        events = deduplicate(events)
        print(f"Después de dedup: {len(events)}")

    # Cargar referencias (en producción pueden estar en S3)
    holidays = pd.read_csv(HOLIDAYS_CSV, parse_dates=["date"])
    holidays["date"] = holidays["date"].dt.date
    routes = pd.read_csv(ROUTES_REF)
    history = pd.read_csv(CATEGORY_HISTORY)

    print("Construyendo dimensiones...")
    dim_date = build_dim_date(holidays)
    dim_route = build_dim_route(routes, history)
    dim_fare = pd.DataFrame({
        "fare_class_key": [1, 2],
        "fare_class": ["ECONOMY", "BUSINESS"],
    })

    print("Construyendo facts...")
    fact_search, fact_booking = build_facts(events, dim_route)
    print(f"fact_search: {len(fact_search)} | fact_booking: {len(fact_booking)}")

    print("Escribiendo Parquet...")
    write_parquet_s3(dim_date, f"{STAR_OUTPUT}dim_date/data.parquet")
    write_parquet_s3(dim_route, f"{STAR_OUTPUT}dim_route/data.parquet")
    write_parquet_s3(dim_fare, f"{STAR_OUTPUT}dim_fare_class/data.parquet")
    write_partitioned(fact_search, f"{STAR_OUTPUT}fact_search/", "month")
    write_parquet_s3(fact_booking, f"{STAR_OUTPUT}fact_booking_event/data.parquet")

    # Opcional: MSCK REPAIR TABLE fact_search vía Athena
        print("Job completado.")
        set_job_status(job_id, "SUCCEEDED", f"searches={len(fact_search)} bookings={len(fact_booking)}")
        return {"status": "SUCCEEDED", "searches": len(fact_search), "bookings": len(fact_booking)}
    except Exception as e:
        set_job_status(job_id, "FAILED", str(e))
        raise

if __name__ == "__main__":
    run()

# Opcional: al terminar el job, actualizar el estado para que /admin/refresh-status lo refleje.
# Ejemplo con DynamoDB:
# dynamodb = boto3.resource("dynamodb")
# table = dynamodb.Table("refresh_jobs")
# table.put_item(Item={"job_id": os.environ.get("JOB_ID"), "state": "SUCCEEDED"})

