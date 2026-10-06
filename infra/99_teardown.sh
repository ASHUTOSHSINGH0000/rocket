#!/usr/bin/env bash
# Delete billable / created resources after grading. MWAA must be deleted in the console
# (or: aws mwaa delete-environment --name <env>) - it bills per hour while it exists.
# Usage: CONFIRM=yes ./infra/99_teardown.sh            (KEEP_DATA=yes keeps the S3 bucket)
set -euo pipefail
[ "${CONFIRM:-}" = "yes" ] || { echo "Set CONFIRM=yes to delete resources"; exit 1; }
REGION="${AWS_REGION:-ap-south-1}"
BUCKET="${BUCKET:-novacart-lakehouse}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
aws events remove-targets --rule novacart-batch-success --ids trigger-dag --region "$REGION" || true
aws events delete-rule --name novacart-batch-success --region "$REGION" || true
aws lambda delete-function --function-name novacart-trigger-dag --region "$REGION" || true
aws glue delete-job --job-name novacart_run_sql --region "$REGION" || true
aws glue delete-job --job-name novacart_evidence --region "$REGION" || true
for db in bronze silver gold quarantine; do aws glue delete-database --name "$db" --region "$REGION" || true; done
aws sns delete-topic --topic-arn "arn:aws:sns:${REGION}:${ACCOUNT}:novacart-pipeline-alerts" --region "$REGION" || true
aws iam delete-role-policy --role-name NovaCartTriggerLambdaRole --policy-name NovaCartTriggerDag || true
aws iam delete-role --role-name NovaCartTriggerLambdaRole || true
aws iam delete-role-policy --role-name NovaCartGlueRole --policy-name NovaCartLakehouseAccess || true
aws iam detach-role-policy --role-name NovaCartGlueRole --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole || true
aws iam delete-role --role-name NovaCartGlueRole || true
if [ "${KEEP_DATA:-}" != "yes" ]; then
  aws s3 rm "s3://${BUCKET}" --recursive && aws s3api delete-bucket --bucket "$BUCKET" --region "$REGION" || true
fi
echo "Teardown finished. Remember to delete the MWAA environment if you created one."
