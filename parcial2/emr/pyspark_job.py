"""
Job PySpark para EMR — transforma los datos crudos en:
  1. reservations_curated   (Parquet, particionado por month)
  2. route_month_metrics    (Parquet, no particionado)

Reglas de limpieza:
  C1 route: sin espacios, mayúsculas
  C2 status: sin espacios, mayúsculas
  C3 conservar solo CONFIRMED / CANCELLED
  C4 eliminar price_cents nulo o <= 0
  C5 eliminar reservation_id duplicado (primera aparición)
"""
import sys
from pyspark.sql import SparkSession, functions as F, Window

BUCKET = sys.argv[1]       # nombre del bucket (en EMR) o ruta local con prefijo file:// (pruebas)
GLUE_DB = sys.argv[2] if len(sys.argv) > 2 else "airline_analytics"   # "none" = no registrar en Glue
BASE = BUCKET if "://" in BUCKET else f"s3://{BUCKET}"

builder = SparkSession.builder.appName("airline-curated")
if GLUE_DB != "none":
    builder = builder.enableHiveSupport()
spark = builder.getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.variable.substitute", "false")   # para que ${month} llegue literal a Glue
spark.sparkContext.setLogLevel("WARN")

# ── 1. Lectura de raw (todo como string) ─────────────────────────────────────
raw_flights = spark.read.option("header", True).csv(f"{BASE}/raw/flights/")
raw_res = spark.read.option("header", True).csv(f"{BASE}/raw/reservations/")

# ── 2. Limpiar vuelos: C1 sobre route (la ruta solo existe en flights.csv) ───
flights = (raw_flights
           .withColumn("route", F.upper(F.regexp_replace("route", r"\s+", "")))
           .withColumn("capacity", F.col("capacity").cast("long"))
           .withColumn("flight_date", F.to_date("flight_date")))

# ── 3. Limpiar reservas (C2-C5) ─────────────────────────────────────────────
res = (raw_res
       # C2
       .withColumn("status", F.upper(F.trim("status")))
       # C3
       .filter(F.col("status").isin("CONFIRMED", "CANCELLED"))
       # C4
       .withColumn("price_cents", F.col("price_cents").cast("long"))
       .filter(F.col("price_cents").isNotNull() & (F.col("price_cents") > 0))
       # C5: primera aparición por reservation_id (orden de llegada en el CSV)
       .withColumn("_rn", F.row_number().over(
           Window.partitionBy("reservation_id").orderBy(F.monotonically_increasing_id())))
       .filter(F.col("_rn") == 1)
       .drop("_rn"))

# ── 4. curated_detail: inner join reservas limpias con vuelos limpios ────────
flights_ref = (flights
               .withColumn("month", F.date_format("flight_date", "yyyy-MM"))
               .select("flight_id", "route", "month"))

detail = (res.join(flights_ref, on="flight_id", how="inner")
          .select("reservation_id", "flight_id",
                  "route",                # viene de flights (ya limpia con C1)
                  "fare_class", "price_cents", "status", "month"))

# ── 5. Guardar reservations_curated (particionado por month) ─────────────────
OUT_CURATED = f"{BASE}/curated/reservations_curated/"
(detail
 .repartition("month")
 .write.mode("overwrite")
 .partitionBy("month")
 .parquet(OUT_CURATED))

# ── 6. Métricas por ruta y mes ───────────────────────────────────────────────
flights_cap = (flights
               .withColumn("month", F.date_format("flight_date", "yyyy-MM"))
               .groupBy("route", "month")
               .agg(F.sum("capacity").alias("seats_capacity")))

conf = detail.filter(F.col("status") == "CONFIRMED")
sold = conf.groupBy("route", "month").agg(
    F.count("*").alias("seats_sold"),
    F.sum("price_cents").alias("revenue_cents"))
book = detail.groupBy("route", "month").agg(F.count("*").alias("bookings"))
canc = detail.filter(F.col("status") == "CANCELLED").groupBy("route", "month").agg(
    F.count("*").alias("cancellations"))

metrics = (flights_cap
           .join(sold,  ["route", "month"], "left")
           .join(book,  ["route", "month"], "left")
           .join(canc,  ["route", "month"], "left")
           .fillna(0, subset=["seats_sold", "revenue_cents", "bookings", "cancellations"])
           .withColumn("seats_sold",     F.col("seats_sold").cast("long"))
           .withColumn("revenue_cents",  F.col("revenue_cents").cast("long"))
           .withColumn("bookings",       F.col("bookings").cast("long"))
           .withColumn("cancellations",  F.col("cancellations").cast("long"))
           .withColumn("seats_capacity", F.col("seats_capacity").cast("long"))
           .withColumn("occupancy",      F.col("seats_sold").cast("double") / F.col("seats_capacity"))
           .select("route", "month", "seats_capacity", "seats_sold", "occupancy",
                   "revenue_cents", "bookings", "cancellations")
           .orderBy("route", "month"))

OUT_METRICS = f"{BASE}/curated/route_month_metrics/"
metrics.write.mode("overwrite").parquet(OUT_METRICS)

# ── 7. Registrar tablas en Glue ──────────────────────────────────────────────
if GLUE_DB == "none":
    print("=== JOB COMPLETADO (sin Glue) ===")
    spark.stop()
    sys.exit(0)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {GLUE_DB}")

spark.sql(f"DROP TABLE IF EXISTS {GLUE_DB}.reservations_curated")
spark.sql(f"""
CREATE EXTERNAL TABLE {GLUE_DB}.reservations_curated (
  reservation_id STRING,
  flight_id      STRING,
  route          STRING,
  fare_class     STRING,
  price_cents    BIGINT,
  status         STRING
)
PARTITIONED BY (month STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET}/curated/reservations_curated/'
TBLPROPERTIES (
  'parquet.compress'                             = 'SNAPPY',
  'projection.enabled'                           = 'true',
  'projection.month.type'                        = 'date',
  'projection.month.format'                      = 'yyyy-MM',
  'projection.month.range'                       = '2023-01,2024-12',
  'projection.month.interval'                    = '1',
  'projection.month.interval.unit'               = 'MONTHS',
  'storage.location.template'                    = 's3://{BUCKET}/curated/reservations_curated/month=${{month}}'
)
""")

spark.sql(f"DROP TABLE IF EXISTS {GLUE_DB}.route_month_metrics")
spark.sql(f"""
CREATE EXTERNAL TABLE {GLUE_DB}.route_month_metrics (
  route          STRING,
  month          STRING,
  seats_capacity BIGINT,
  seats_sold     BIGINT,
  occupancy      DOUBLE,
  revenue_cents  BIGINT,
  bookings       BIGINT,
  cancellations  BIGINT
)
STORED AS PARQUET
LOCATION 's3://{BUCKET}/curated/route_month_metrics/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

print("=== JOB COMPLETADO ===")
spark.stop()
