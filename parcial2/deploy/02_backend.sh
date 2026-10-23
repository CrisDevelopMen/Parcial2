#!/usr/bin/env bash
# Fase 2: RDS PostgreSQL (db.t3.micro, single-AZ) + EC2 t3.small con el backend/frontend.
# Ejecutar en AWS CloudShell desde la carpeta donde se descomprimió el zip:  bash deploy/02_backend.sh
set -euo pipefail
export AWS_DEFAULT_REGION=us-east-1
ENVF=~/airline.env
touch $ENVF; source $ENVF

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET=${BUCKET:-airline-team77-$ACCOUNT}
DB_ID=airline-db
DB_PASS=${DB_PASS:-$(openssl rand -hex 16)}
save() { grep -v "^$1=" $ENVF > $ENVF.tmp || true; echo "$1=$2" >> $ENVF.tmp; mv $ENVF.tmp $ENVF; }
save BUCKET "$BUCKET"; save DB_PASS "$DB_PASS"

echo "== 1/5 Red y security groups"
VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
sg() {  # crea el SG si no existe y devuelve su id
  local id; id=$(aws ec2 describe-security-groups --filters Name=group-name,Values=$1 Name=vpc-id,Values=$VPC \
        --query 'SecurityGroups[0].GroupId' --output text)
  if [ "$id" = "None" ]; then id=$(aws ec2 create-security-group --group-name $1 --description "$2" --vpc-id $VPC --query GroupId --output text); fi
  echo $id
}
WEB_SG=$(sg airline-web-sg "backend+frontend")
DB_SG=$(sg airline-db-sg "postgres solo desde web")
aws ec2 authorize-security-group-ingress --group-id $WEB_SG --protocol tcp --port 8000 --cidr 0.0.0.0/0 2>/dev/null || true
aws ec2 authorize-security-group-ingress --group-id $WEB_SG --protocol tcp --port 22 --cidr 0.0.0.0/0 2>/dev/null || true
aws ec2 authorize-security-group-ingress --group-id $DB_SG --protocol tcp --port 5432 --source-group $WEB_SG 2>/dev/null || true
echo "web=$WEB_SG db=$DB_SG"

echo "== 2/5 RDS PostgreSQL db.t3.micro (tarda ~10 min)"
if ! aws rds describe-db-instances --db-instance-identifier $DB_ID >/dev/null 2>&1; then
  aws rds create-db-instance --db-instance-identifier $DB_ID --engine postgres \
    --db-instance-class db.t3.micro --allocated-storage 20 --storage-type gp2 \
    --master-username airline --master-user-password "$DB_PASS" --db-name airline \
    --vpc-security-group-ids $DB_SG --no-multi-az --no-publicly-accessible \
    --backup-retention-period 0 --no-auto-minor-version-upgrade >/dev/null
fi
aws rds wait db-instance-available --db-instance-identifier $DB_ID
DB_HOST=$(aws rds describe-db-instances --db-instance-identifier $DB_ID --query 'DBInstances[0].Endpoint.Address' --output text)
save DB_HOST "$DB_HOST"; echo "RDS listo: $DB_HOST"

echo "== 3/5 Subir código a S3"
aws s3 cp backend.zip s3://$BUCKET/app/backend.zip

echo "== 4/5 Lanzar EC2 t3.small"
AMI=$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 --query Parameter.Value --output text)
cat > /tmp/userdata.sh <<EOF
#!/bin/bash
set -ex
dnf install -y python3-pip unzip
mkdir -p /opt/airline && cd /opt/airline
aws s3 cp s3://$BUCKET/app/backend.zip . --region us-east-1 && unzip -o backend.zip
python3 -m venv venv && venv/bin/pip install -q -r requirements.txt
cat > /etc/airline.env <<ENV
DATABASE_URL=postgresql://airline:$DB_PASS@$DB_HOST:5432/airline
ATHENA_OUTPUT=s3://$BUCKET/athena-results/
ATHENA_DB=airline_analytics
ATHENA_WORKGROUP=primary
AWS_REGION=us-east-1
POOL_MAX=30
ENV
cat > /etc/systemd/system/airline.service <<UNIT
[Unit]
After=network-online.target
[Service]
WorkingDirectory=/opt/airline
EnvironmentFile=/etc/airline.env
ExecStartPre=/opt/airline/venv/bin/python app/init_db.py
ExecStart=/opt/airline/venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 2
Restart=always
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload && systemctl enable --now airline
EOF
IID=$(aws ec2 describe-instances --filters Name=tag:Name,Values=airline-backend Name=instance-state-name,Values=pending,running \
      --query 'Reservations[0].Instances[0].InstanceId' --output text)
if [ "$IID" = "None" ]; then
  IID=$(aws ec2 run-instances --image-id $AMI --instance-type t3.small --security-group-ids $WEB_SG \
    --iam-instance-profile Name=LabInstanceProfile --user-data file:///tmp/userdata.sh \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=airline-backend}]' \
    --query 'Instances[0].InstanceId' --output text)
fi
aws ec2 wait instance-running --instance-ids $IID
IP=$(aws ec2 describe-instances --instance-ids $IID --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
save EC2_ID "$IID"; save EC2_IP "$IP"

echo "== 5/5 Esperando que la app responda (2-4 min)"
for i in $(seq 1 40); do
  if curl -sf -m 5 http://$IP:8000/health >/dev/null; then
    echo; echo "LISTO -> backend/frontend: http://$IP:8000   panel: http://$IP:8000/manager"; exit 0
  fi
  printf .; sleep 10
done
echo; echo "La app aún no responde. Revisa: EC2 > instancia airline-backend > Actions > Monitor > Get system log"
exit 1
