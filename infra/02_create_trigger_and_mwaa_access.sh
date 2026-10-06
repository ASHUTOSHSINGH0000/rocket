#!/usr/bin/env bash
# =============================================================================
# 02_create_trigger_and_mwaa_access.sh
#   a) grants the existing MWAA execution role access to Glue + SNS and uploads the DAG
#   b) creates the Lambda + EventBridge rule:  input/batch_<n>/_SUCCESS  ->  DAG run
# Prerequisite: an MWAA environment (Airflow 2.x) created in the console (see README, step 4).
# Usage: MWAA_ENV=novacart-mwaa MWAA_DAGS_BUCKET=my-mwaa-bucket ./infra/02_create_trigger_and_mwaa_access.sh
# =============================================================================
set -euo pipefail

REGION="${AWS_REGION:-ap-south-1}"
BUCKET="${BUCKET:-novacart-lakehouse}"
MWAA_ENV="${MWAA_ENV:?Set MWAA_ENV to your MWAA environment name}"
MWAA_DAGS_BUCKET="${MWAA_DAGS_BUCKET:?Set MWAA_DAGS_BUCKET to the S3 bucket chosen for MWAA}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
TMP="$(mktemp -d)"
FN="novacart-trigger-dag"
LAMBDA_ROLE="NovaCartTriggerLambdaRole"

fill() {
  sed -e "s/__BUCKET__/${BUCKET}/g" -e "s/__REGION__/${REGION}/g" \
      -e "s/__ACCOUNT__/${ACCOUNT}/g" -e "s/__MWAA_ENV__/${MWAA_ENV}/g" "$1"
}

# ---------------------------------------------------------------- a) MWAA access + DAG upload
EXEC_ROLE_ARN="$(aws mwaa get-environment --name "$MWAA_ENV" --region "$REGION" --query Environment.ExecutionRoleArn --output text)"
EXEC_ROLE="${EXEC_ROLE_ARN##*/}"
fill "${ROOT}/infra/iam/mwaa-execution-extra-policy.json" > "${TMP}/mwaa.json"
aws iam put-role-policy --role-name "$EXEC_ROLE" --policy-name NovaCartGlueSnsAccess \
  --policy-document "file://${TMP}/mwaa.json"
aws s3 cp "${ROOT}/dags/novacart_medallion_dag.py" "s3://${MWAA_DAGS_BUCKET}/dags/novacart_medallion_dag.py"
echo "MWAA role ${EXEC_ROLE} updated, DAG uploaded (appears in the Airflow UI within ~1 min)"

# ---------------------------------------------------------------- b) Lambda role + function
if ! aws iam get-role --role-name "$LAMBDA_ROLE" >/dev/null 2>&1; then
  aws iam create-role --role-name "$LAMBDA_ROLE" \
    --assume-role-policy-document "file://${ROOT}/infra/iam/lambda-trust.json" >/dev/null
  sleep 10
fi
fill "${ROOT}/infra/iam/lambda-trigger-policy.json" > "${TMP}/lambda.json"
aws iam put-role-policy --role-name "$LAMBDA_ROLE" --policy-name NovaCartTriggerDag \
  --policy-document "file://${TMP}/lambda.json"

(cd "${ROOT}/lambda" && zip -q -j "${TMP}/fn.zip" trigger_dag_lambda.py)
if aws lambda get-function --function-name "$FN" --region "$REGION" >/dev/null 2>&1; then
  aws lambda update-function-code --function-name "$FN" --zip-file "fileb://${TMP}/fn.zip" --region "$REGION" >/dev/null
else
  aws lambda create-function --function-name "$FN" --region "$REGION" --runtime python3.12 \
    --handler trigger_dag_lambda.lambda_handler --timeout 30 \
    --role "arn:aws:iam::${ACCOUNT}:role/${LAMBDA_ROLE}" \
    --environment "Variables={MWAA_ENV_NAME=${MWAA_ENV},DAG_ID=novacart_medallion}" \
    --zip-file "fileb://${TMP}/fn.zip" >/dev/null
fi
echo "Lambda ${FN} deployed"

# ---------------------------------------------------------------- c) EventBridge rule
fill "${ROOT}/infra/iam/eventbridge-rule-pattern.json" > "${TMP}/pattern.json"
RULE_ARN="$(aws events put-rule --name novacart-batch-success --region "$REGION" \
  --event-pattern "file://${TMP}/pattern.json" --query RuleArn --output text)"
FN_ARN="$(aws lambda get-function --function-name "$FN" --region "$REGION" --query Configuration.FunctionArn --output text)"
aws events put-targets --rule novacart-batch-success --region "$REGION" \
  --targets "Id=trigger-dag,Arn=${FN_ARN}" >/dev/null
aws lambda add-permission --function-name "$FN" --region "$REGION" \
  --statement-id novacart-eventbridge --action lambda:InvokeFunction \
  --principal events.amazonaws.com --source-arn "$RULE_ARN" >/dev/null 2>&1 || true
echo "EventBridge rule novacart-batch-success -> ${FN} ready"
echo "Upload a batch with ./infra/03_upload_batch.sh <n>; its _SUCCESS marker starts the DAG."
