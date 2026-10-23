#!/usr/bin/env bash
# Fase 4: completa config.yaml con la IP del EC2 y el bucket, prueba Athena + endpoint
# y corre el harness del profesor (R4-R7 y R10-R12) desde CloudShell.
# Ejecutar dentro de la carpeta parcial2/:  bash deploy/04_verify.sh
set -uo pipefail
export AWS_DEFAULT_REGION=us-east-1
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET=airline-team77-$ACCOUNT
IP=$(aws ec2 describe-instances --filters Name=tag:Name,Values=airline-backend Name=instance-state-name,Values=running \
     --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "BUCKET=$BUCKET  IP=$IP"

echo "== 1/4 Generar config.yaml"
sed -e "s#FILL_AFTER_DEPLOY_EC2_IP:8000#$IP:8000#" -e "s#FILL_AFTER_DEPLOY_EC2_IP:3000#$IP:8000#" \
    -e "s#FILL_AFTER_DEPLOY_BUCKET#$BUCKET#" config.template.yaml > config.yaml
grep -n "base_url\|athena_output" config.yaml

echo "== 2/4 Prueba rápida de Athena"
q() {
  local id st
  id=$(aws athena start-query-execution --work-group primary --query-execution-context Database=airline_analytics \
       --result-configuration OutputLocation=s3://$BUCKET/athena-results/ --query-string "$1" --query QueryExecutionId --output text)
  for i in $(seq 1 60); do
    st=$(aws athena get-query-execution --query-execution-id $id --query QueryExecution.Status.State --output text)
    [ "$st" = SUCCEEDED ] && break
    if [ "$st" = FAILED ] || [ "$st" = CANCELLED ]; then
      aws athena get-query-execution --query-execution-id $id --query QueryExecution.Status.StateChangeReason --output text; return 1
    fi
    sleep 2
  done
  aws athena get-query-results --query-execution-id $id --query 'ResultSet.Rows[1:][].Data[].VarCharValue' --output text
}
echo -n "filas curadas (esperado 2003012): "; q "SELECT count(*) FROM reservations_curated"
echo -n "filas 2024-01 (esperado 83568):  "; q "SELECT count(*) FROM reservations_curated WHERE month='2024-01'"
echo    "top 3 rutas (esperado LET-BOG 0.9514, ADZ-CTG 0.9372, BAQ-BGA 0.9191):"
q "SELECT route, round(CAST(sum(seats_sold) AS DOUBLE)/sum(seats_capacity),4) occ FROM route_month_metrics GROUP BY route ORDER BY occ DESC LIMIT 3"

echo "== 3/4 Endpoint analítico del backend (esperado occupancy 0.800661)"
curl -s -m 15 "http://$IP:8000/analytics/occupancy?route=CUC-PSO&month=2023-06"; echo

echo "== 4/4 Harness del profesor: R4-R7 (analítica) y R10-R12 (costos)"
python3 -m pip install -q pytest httpx pyyaml boto3 pandas pyarrow numpy 2>&1 | grep -v notice
python3 -m pytest tests/test_analytics.py tests/test_cost.py -q -rfE 2>&1 | tail -30
