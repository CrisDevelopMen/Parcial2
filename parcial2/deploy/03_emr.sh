#!/usr/bin/env bash
# Fase 3: lanza el job PySpark en EMR, espera que termine y verifica las tablas en Glue.
# Ejecutar en CloudShell: bash deploy/03_emr.sh
set -euo pipefail
export AWS_DEFAULT_REGION=us-east-1
ENVF=~/airline.env; source $ENVF

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET=${BUCKET:-airline-team77-$ACCOUNT}
GLUE_DB=airline_analytics

echo "== 1/5 Subir el job a S3"
aws s3 cp emr/pyspark_job.py s3://$BUCKET/scripts/pyspark_job.py

echo "== 2/5 (logs en s3://$BUCKET/emr-logs/)"

echo "== 3/5 Crear clúster EMR con el step incluido (se apaga solo al terminar)"
GLUECFG='[{"Classification":"spark-hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}},{"Classification":"hive-site","Properties":{"hive.metastore.client.factory.class":"com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"}}]'
CID=$(aws emr create-cluster \
  --name "airline-curated" \
  --release-label emr-6.15.0 \
  --applications Name=Spark Name=Hadoop Name=Hive \
  --instance-type m4.large \
  --instance-count 3 \
  --use-default-roles \
  --log-uri s3://$BUCKET/emr-logs/ \
  --auto-terminate \
  --configurations "$GLUECFG" \
  --steps Type=Spark,Name=curated,ActionOnFailure=TERMINATE_CLUSTER,Args=[--deploy-mode,cluster,s3://$BUCKET/scripts/pyspark_job.py,$BUCKET,$GLUE_DB] \
  --query ClusterId --output text)
echo "Cluster: $CID"
grep -v "^CLUSTER_ID=" $ENVF > $ENVF.tmp || true; echo "CLUSTER_ID=$CID" >> $ENVF.tmp; mv $ENVF.tmp $ENVF

echo "== 4/5 Obteniendo id del step"
sleep 10
SID=$(aws emr list-steps --cluster-id $CID --query 'Steps[0].Id' --output text)
echo "Step: $SID"

echo "== 5/5 Esperando COMPLETED (10-15 min)..."
while true; do
  STEP_STATE=$(aws emr describe-step --cluster-id $CID --step-id $SID \
    --query 'Step.Status.State' --output text)
  CLUSTER_STATE=$(aws emr describe-cluster --cluster-id $CID \
    --query 'Cluster.Status.State' --output text)
  echo "  cluster=$CLUSTER_STATE  step=$STEP_STATE"
  case "$STEP_STATE" in
    COMPLETED)
      echo "SUCCESS: step COMPLETED"
      echo "=== Verificando tablas en Glue ==="
      aws glue get-table --database-name $GLUE_DB --name reservations_curated \
        --query 'Table.Name' --output text && echo "  reservations_curated OK"
      aws glue get-table --database-name $GLUE_DB --name route_month_metrics \
        --query 'Table.Name' --output text && echo "  route_month_metrics OK"
      echo "=== Parquet en S3 ==="
      aws s3 ls s3://$BUCKET/curated/reservations_curated/ | head -5
      echo "Fase 3 completa."
      exit 0 ;;
    FAILED|CANCELLED|INTERRUPTED)
      echo "FAILED: step=$STEP_STATE"
      aws emr describe-step --cluster-id $CID --step-id $SID \
        --query 'Step.Status.FailureDetails'
      echo "Log: s3://$BUCKET/emr-logs/$CID/steps/$SID/"
      exit 1 ;;
  esac
  sleep 30
done
