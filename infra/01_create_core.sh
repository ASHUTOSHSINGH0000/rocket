#!/usr/bin/env bash
# =============================================================================
# 01_create_core.sh - S3 lakehouse, Glue Data Catalog, IAM role, SNS, Glue job
# Idempotent: safe to re-run. Requires AWS CLI v2 logged in (aws sts get-caller-identity).
# Usage:  ALERT_EMAIL=you@example.com ./infra/01_create_core.sh
# Optional: AWS_REGION (default ap-south-1), BUCKET (default novacart-lakehouse)
# Note: S3 bucket names are global. If novacart-lakehouse is taken, set
#       BUCKET=novacart-lakehouse-<suffix> and use the same value everywhere.
# =============================================================================
set -euo pipefail

REGION="${AWS_REGION:-ap-south-1}"
BUCKET="${BUCKET:-novacart-lakehouse}"
ALERT_EMAIL="${ALERT_EMAIL:?Set ALERT_EMAIL to receive SNS failure alerts}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
GLUE_ROLE="NovaCartGlueRole"
GLUE_JOB="novacart_run_sql"
TMP="$(mktemp -d)"
echo "Account ${ACCOUNT}  region ${REGION}  bucket s3://${BUCKET}"

fill() {  # replace placeholders in a policy template
  sed -e "s/__BUCKET__/${BUCKET}/g" -e "s/__REGION__/${REGION}/g" \
      -e "s/__ACCOUNT__/${ACCOUNT}/g" -e "s/__MWAA_ENV__/${MWAA_ENV:-novacart-mwaa}/g" "$1"
}

# ---------------------------------------------------------------- 1. S3 bucket
if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "Bucket exists"
else
  if [ "$REGION" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"
  else
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
      --create-bucket-configuration LocationConstraint="$REGION"
  fi
fi
aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-encryption --bucket "$BUCKET" --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
# send S3 object events to EventBridge (used by the _SUCCESS trigger)
aws s3api put-bucket-notification-configuration --bucket "$BUCKET" \
  --notification-configuration '{"EventBridgeConfiguration":{}}'

# folder markers so the layout is visible in the console (S3 has no real folders)
for prefix in input/batch_1/ input/batch_2/ input/reference/ bronze/ silver/ gold/ \
              quarantine/ reports/ scripts/sql/ scripts/glue/ temp/ sparkhistory/; do
  aws s3api put-object --bucket "$BUCKET" --key "$prefix" >/dev/null
done
echo "S3 layout created"

# ---------------------------------------------------------------- 2. Glue Data Catalog
for db in bronze silver gold quarantine; do
  if aws glue get-database --name "$db" --region "$REGION" >/dev/null 2>&1; then
    echo "Glue database $db exists"
  else
    aws glue create-database --region "$REGION" \
      --database-input "{\"Name\":\"$db\",\"Description\":\"NovaCart $db layer\",\"LocationUri\":\"s3://${BUCKET}/${db}/\"}"
    echo "Glue database $db created"
  fi
done

# ---------------------------------------------------------------- 3. IAM role for Glue
if ! aws iam get-role --role-name "$GLUE_ROLE" >/dev/null 2>&1; then
  aws iam create-role --role-name "$GLUE_ROLE" \
    --assume-role-policy-document "file://${ROOT}/infra/iam/glue-trust.json" >/dev/null
fi
aws iam attach-role-policy --role-name "$GLUE_ROLE" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
fill "${ROOT}/infra/iam/glue-lakehouse-policy.json" > "${TMP}/glue-policy.json"
aws iam put-role-policy --role-name "$GLUE_ROLE" --policy-name NovaCartLakehouseAccess \
  --policy-document "file://${TMP}/glue-policy.json"
echo "IAM role $GLUE_ROLE ready (new roles take ~10 s to propagate)"
sleep 10

# ---------------------------------------------------------------- 4. SNS topic for alerts
TOPIC_ARN="$(aws sns create-topic --name novacart-pipeline-alerts --region "$REGION" --query TopicArn --output text)"
aws sns subscribe --topic-arn "$TOPIC_ARN" --protocol email --notification-endpoint "$ALERT_EMAIL" \
  --region "$REGION" >/dev/null
echo "SNS topic $TOPIC_ARN (confirm the subscription email sent to $ALERT_EMAIL)"

# ---------------------------------------------------------------- 5. upload code and input data
aws s3 cp "${ROOT}/glue/run_sql.py" "s3://${BUCKET}/scripts/glue/run_sql.py"
aws s3 sync "${ROOT}/sql/" "s3://${BUCKET}/scripts/sql/" --exclude "*" --include "*.sql" --delete
aws s3 sync "${ROOT}/input/reference/" "s3://${BUCKET}/input/reference/"
# batch folders are uploaded by 03_upload_batch.sh so the _SUCCESS marker is written last

# ---------------------------------------------------------------- 6. Glue job
cat > "${TMP}/default-args.json" <<EOF
{
  "--job-language": "python",
  "--datalake-formats": "delta",
  "--conf": "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.sql.session.timeZone=UTC",
  "--enable-glue-datacatalog": "true",
  "--enable-continuous-cloudwatch-log": "true",
  "--enable-metrics": "true",
  "--enable-spark-ui": "true",
  "--spark-event-logs-path": "s3://${BUCKET}/sparkhistory/",
  "--TempDir": "s3://${BUCKET}/temp/",
  "--lake": "s3://${BUCKET}",
  "--input": "s3://${BUCKET}/input",
  "--sql_root": "s3://${BUCKET}/scripts/sql",
  "--batch_id": "1",
  "--steps": "all"
}
EOF
JOB_SPEC=$(cat <<EOF
{
  "Role": "arn:aws:iam::${ACCOUNT}:role/${GLUE_ROLE}",
  "Command": {"Name": "glueetl", "ScriptLocation": "s3://${BUCKET}/scripts/glue/run_sql.py", "PythonVersion": "3"},
  "DefaultArguments": $(cat "${TMP}/default-args.json"),
  "GlueVersion": "5.0",
  "WorkerType": "G.1X",
  "NumberOfWorkers": 2,
  "Timeout": 30,
  "MaxRetries": 0,
  "ExecutionProperty": {"MaxConcurrentRuns": 3}
}
EOF
)
if aws glue get-job --job-name "$GLUE_JOB" --region "$REGION" >/dev/null 2>&1; then
  aws glue update-job --job-name "$GLUE_JOB" --job-update "$JOB_SPEC" --region "$REGION" >/dev/null
  echo "Glue job $GLUE_JOB updated"
else
  echo "$JOB_SPEC" > "${TMP}/job.json"
  aws glue create-job --name "$GLUE_JOB" --region "$REGION" --cli-input-json "file://${TMP}/job.json" >/dev/null
  echo "Glue job $GLUE_JOB created"
fi

echo
echo "Done. Next:"
echo "  1) confirm the SNS email subscription"
echo "  2) ./infra/03_upload_batch.sh 1   (uploads batch 1; with the trigger in place it starts the DAG)"
echo "  3) quick test without Airflow:"
echo "     aws glue start-job-run --job-name $GLUE_JOB --region $REGION --arguments '{\"--batch_id\":\"1\",\"--steps\":\"all\"}'"
echo "SNS_TOPIC_ARN=${TOPIC_ARN}"
