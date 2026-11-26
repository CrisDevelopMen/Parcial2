#!/usr/bin/env bash
# Script de despliegue mínimo para Parcial 3
# Crea: Kinesis stream, S3 buckets, Firehose, DynamoDB, Lambda y roles básicos.
# Ajusta las variables al inicio y ejecuta con: bash deploy_aws.sh
# Requisitos: AWS CLI configurado (aws configure) y permisos de administrador o equivalentes.

set -euo pipefail

# ========== CONFIGURACIÓN (editar) ==========
REGION="us-east-1"
PROJECT="airline-p3"
STREAM_NAME="${PROJECT}-events"
LANDING_BUCKET="${PROJECT}-landing-$(date +%s)"
STAR_BUCKET="${PROJECT}-star-$(date +%s)"
LAMBDA_NAME="${PROJECT}-refresh-star"
STATUS_TABLE="refresh_jobs"
ATHENA_DB="airline_analytics"
# ============================================

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "Account: $ACCOUNT_ID | Region: $REGION"

# --- 1. Buckets S3 ---
echo ">>> Creando buckets S3..."
aws s3 mb "s3://${LANDING_BUCKET}" --region "$REGION"
aws s3 mb "s3://${STAR_BUCKET}" --region "$REGION"
echo "Landing: s3://${LANDING_BUCKET}"
echo "Star:    s3://${STAR_BUCKET}"

# --- 2. Kinesis Data Stream (1 shard) ---
echo ">>> Creando Kinesis stream..."
aws kinesis create-stream \
  --stream-name "$STREAM_NAME" \
  --shard-count 1 \
  --region "$REGION"

echo "Esperando a que el stream esté ACTIVE..."
aws kinesis wait stream-exists --stream-name "$STREAM_NAME" --region "$REGION"
echo "Stream listo: $STREAM_NAME"

# --- 3. DynamoDB tabla de estado ---
echo ">>> Creando tabla DynamoDB..."
aws dynamodb create-table \
  --table-name "$STATUS_TABLE" \
  --attribute-definitions AttributeName=job_id,AttributeType=S \
  --key-schema AttributeName=job_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region "$REGION" \
  >/dev/null

echo "Tabla $STATUS_TABLE creada."

# --- 4. Roles IAM ---
echo ">>> Creando roles IAM..."

# Role para Firehose
FIREHOSE_ROLE="${PROJECT}-firehose-role"
aws iam create-role \
  --role-name "$FIREHOSE_ROLE" \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "firehose.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }' >/dev/null || true

aws iam put-role-policy \
  --role-name "$FIREHOSE_ROLE" \
  --policy-name firehose-s3 \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
      {
        \"Effect\": \"Allow\",
        \"Action\": [\"s3:AbortMultipartUpload\",\"s3:GetBucketLocation\",\"s3:GetObject\",\"s3:ListBucket\",\"s3:ListBucketMultipartUploads\",\"s3:PutObject\"],
        \"Resource\": [\"arn:aws:s3:::${LANDING_BUCKET}\",\"arn:aws:s3:::${LANDING_BUCKET}/*\"]
      },
      {
        \"Effect\": \"Allow\",
        \"Action\": [\"kinesis:DescribeStream\",\"kinesis:GetShardIterator\",\"kinesis:GetRecords\",\"kinesis:ListShards\"],
        \"Resource\": \"arn:aws:kinesis:${REGION}:${ACCOUNT_ID}:stream/${STREAM_NAME}\"
      }
    ]
  }"

# Role para Lambda
LAMBDA_ROLE="${PROJECT}-lambda-role"
aws iam create-role \
  --role-name "$LAMBDA_ROLE" \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "lambda.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }' >/dev/null || true

aws iam attach-role-policy \
  --role-name "$LAMBDA_ROLE" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole || true

aws iam put-role-policy \
  --role-name "$LAMBDA_ROLE" \
  --policy-name lambda-star \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
      {
        \"Effect\": \"Allow\",
        \"Action\": [\"s3:GetObject\",\"s3:ListBucket\"],
        \"Resource\": [\"arn:aws:s3:::${LANDING_BUCKET}\",\"arn:aws:s3:::${LANDING_BUCKET}/*\"]
      },
      {
        \"Effect\": \"Allow\",
        \"Action\": [\"s3:PutObject\",\"s3:GetObject\",\"s3:ListBucket\"],
        \"Resource\": [\"arn:aws:s3:::${STAR_BUCKET}\",\"arn:aws:s3:::${STAR_BUCKET}/*\"]
      },
      {
        \"Effect\": \"Allow\",
        \"Action\": [\"dynamodb:PutItem\",\"dynamodb:GetItem\",\"dynamodb:UpdateItem\"],
        \"Resource\": \"arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/${STATUS_TABLE}\"
      }
    ]
  }"

echo "Roles creados. Esperando propagación..."
sleep 10

# --- 5. Firehose ---
echo ">>> Creando Firehose..."
aws firehose create-delivery-stream \
  --delivery-stream-name "${PROJECT}-to-s3" \
  --delivery-stream-type KinesisStreamAsSource \
  --kinesis-stream-source-configuration "KinesisStreamARN=arn:aws:kinesis:${REGION}:${ACCOUNT_ID}:stream/${STREAM_NAME},RoleARN=arn:aws:iam::${ACCOUNT_ID}:role/${FIREHOSE_ROLE}" \
  --s3-destination-configuration "{
    \"RoleARN\": \"arn:aws:iam::${ACCOUNT_ID}:role/${FIREHOSE_ROLE}\",
    \"BucketARN\": \"arn:aws:s3:::${LANDING_BUCKET}\",
    \"Prefix\": \"raw/\",
    \"BufferingHints\": {\"SizeInMBs\": 1, \"IntervalInSeconds\": 60},
    \"CompressionFormat\": \"GZIP\"
  }" \
  --region "$REGION" >/dev/null

echo "Firehose creado: ${PROJECT}-to-s3"

# --- 6. Empaquetar y desplegar Lambda ---
echo ">>> Empaquetando Lambda..."
TMPDIR=$(mktemp -d)
cp refresh_star_job.py lambda_handler.py "$TMPDIR/"
cd "$TMPDIR"
# Instalar dependencias ligeras (en un entorno real usa una layer o docker)
pip install pandas pyarrow boto3 -t . --quiet 2>/dev/null || echo "pip install falló (puedes usar una Lambda Layer)"
zip -r /tmp/refresh-star.zip . >/dev/null
cd - >/dev/null

echo ">>> Creando Lambda..."
aws lambda create-function \
  --function-name "$LAMBDA_NAME" \
  --runtime python3.11 \
  --role "arn:aws:iam::${ACCOUNT_ID}:role/${LAMBDA_ROLE}" \
  --handler lambda_handler.handler \
  --zip-file fileb:///tmp/refresh-star.zip \
  --timeout 900 \
  --memory-size 1024 \
  --environment "Variables={RAW_EVENTS_URI=s3://${LANDING_BUCKET}/raw/,STAR_OUTPUT=s3://${STAR_BUCKET}/star/,STATUS_TABLE=${STATUS_TABLE},ATHENA_DB=${ATHENA_DB},AWS_REGION=${REGION}}" \
  --region "$REGION" >/dev/null || \
aws lambda update-function-code \
  --function-name "$LAMBDA_NAME" \
  --zip-file fileb:///tmp/refresh-star.zip \
  --region "$REGION" >/dev/null

echo "Lambda desplegada: $LAMBDA_NAME"

# --- 7. Resumen ---
cat <<SUMMARY

========================================
DESPLIEGUE COMPLETADO
========================================
Stream Kinesis:     $STREAM_NAME
Landing zone:       s3://${LANDING_BUCKET}/raw/
Star schema output: s3://${STAR_BUCKET}/star/
Lambda refresh:     $LAMBDA_NAME
DynamoDB status:    $STATUS_TABLE
Athena DB:          $ATHENA_DB (créala manualmente si no existe)

Variables para el backend:
  KINESIS_STREAM=$STREAM_NAME
  REFRESH_LAMBDA=$LAMBDA_NAME
  STATUS_TABLE=$STATUS_TABLE
  RAW_EVENTS_URI=s3://${LANDING_BUCKET}/raw/
  ATHENA_DB=$ATHENA_DB
  ATHENA_OUTPUT=s3://${STAR_BUCKET}/athena-results/
  AWS_REGION=$REGION

Siguientes pasos:
1. Crea la base Athena: aws athena start-query-execution --query-string "CREATE DATABASE IF NOT EXISTS ${ATHENA_DB}" --result-configuration OutputLocation=s3://${STAR_BUCKET}/athena-results/
2. Ejecuta athena_ddl.sql ajustando los LOCATION.
3. Crea el workgroup con límite de 1 GB.
4. Despliega el backend con las variables de entorno de arriba.
5. Corre el harness.

SUMMARY
