# Parcial 2 — Sistema de reservas de aerolínea + panel analítico (AWS)

Equipo semilla **77**. Despliegue en AWS Learner Lab (us-east-1). Harness: **70/70**.

## Arquitectura
- **EC2 t3.small**: FastAPI (`backend/app/main.py`) + frontend estático (`/` reservas, `/manager` panel gerencial).
- **RDS PostgreSQL db.t3.micro**: tablas `flights` y `reservations` (`backend/app/init_db.py`).
- **S3**: datos crudos (`raw/`), Parquet curado (`curated/`), resultados de Athena.
- **EMR** (auto-terminate): `emr/pyspark_job.py` limpia el CSV → Parquet particionado por `month` y registra las tablas en Glue.
- **Glue + Athena**: `reservations_curated` (partition projection) y `route_month_metrics`. Endpoint `GET /analytics/occupancy`.

## Estructura
| Carpeta / archivo | Contenido |
|---|---|
| `backend/` | API FastAPI, esquema de BD, frontend (`app/static/`) |
| `emr/` | Job PySpark |
| `deploy/` | Scripts de despliegue para CloudShell (02 backend, 03 EMR, 04 verificación) |
| `airline_data/` | Generador de datos y oráculo (del docente) |
| `harness/`, `tests/`, `conftest.py`, `thresholds.yaml`, `pricing.yaml` | Harness del docente (no editar) |
| `contract/` | Contratos de API, datos, frontend y costos |
| `config.template.yaml` / `config.yaml` | Configuración (semilla, URLs, Athena) |
| `cost_projection.yaml` | Proyección de costos mensual (~US$28.41) |
| `report.md` / `report.json` | Último reporte del harness |

## Despliegue (AWS CloudShell)
```bash
# 1. Datos
pip3 install numpy pandas
python3 -m airline_data.generate --seed 77 --rows 2000000 --out raw/
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET=airline-team77-$ACCOUNT
aws s3 mb s3://$BUCKET --region us-east-1
aws s3 cp raw/flights.csv      s3://$BUCKET/raw/flights/flights.csv
aws s3 cp raw/reservations.csv s3://$BUCKET/raw/reservations/reservations.csv

# 2. Backend (RDS + EC2)
(cd backend && zip -r ../backend.zip .)
bash deploy/02_backend.sh

# 3. EMR + Glue
bash deploy/03_emr.sh

# 4. Verificación + harness
bash deploy/04_verify.sh
```

## Harness completo
```bash
pip install -r requirements.txt && playwright install chromium
pytest            # genera report.md y report.json
```

## Notas
- La IP del EC2 cambia al reiniciar el Learner Lab: `04_verify.sh` regenera `config.yaml`.
- Credenciales: se usan las de AWS (env / ~/.aws). La contraseña de RDS se genera en `02_backend.sh` y se guarda en `~/airline.env` (fuera del repo).
- `raw/` no se versiona (79 MB); se regenera con el comando del paso 1.
