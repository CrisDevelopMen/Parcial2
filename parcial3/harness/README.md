# Parcial 3 - test harness
Setup (once):
```
pip install -r requirements.txt
cp config.example.yaml config.yaml      # seed, backend URL, Kinesis stream, landing zone, Athena database
python -m airline_data.generate_p3 --seed <YOUR_SEED> --out ref/   # reference files for your dimensions
```
Your Kinesis stream, landing zone and star tables must exist and your backend must be running.
Run the checks (several minutes; writes report.json and report.md):
```
pytest
```
Contracts: `contract/` (events, API, star schema, cost file). Do not edit `thresholds.yaml`, `harness/` or `tests/`.
Use your normal AWS credentials (environment or ~/.aws). Never commit them.
The harness injects test events into YOUR stream, so run it against your own account only.

## dim_date enriquecida
`generate_p3` también escribe `holidays_ref.csv` (festivos colombianos). `dim_date` debe incluir quarter, weekday (1=lunes..7=domingo), weekday_name, is_weekend, is_holiday, holiday_name, is_long_weekend (ver contract/STAR.md). Las preguntas de negocio Q1-Q8 están en el documento del parcial.
