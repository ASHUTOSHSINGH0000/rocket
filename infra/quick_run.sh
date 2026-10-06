#!/usr/bin/env bash
# =============================================================================
# quick_run.sh - FASTEST demo path (about 20 min, no MWAA needed)
#   1. creates bucket, Glue catalog, IAM role, SNS, Glue job   (01_create_core.sh)
#   2. runs batch 1, batch 2, batch 2 again as Glue job runs and waits for each
#   3. runs the Delta evidence job (history, time travel, schema test, OPTIMIZE, CDF)
#   4. downloads DQ reports + gold results to ./aws_results/
# Usage:  ALERT_EMAIL=you@example.com BUCKET=novacart-lakehouse-<yourname> ./infra/quick_run.sh
# =============================================================================
set -euo pipefail
REGION="${AWS_REGION:-ap-south-1}"
BUCKET="${BUCKET:-novacart-lakehouse}"
export AWS_REGION="$REGION" BUCKET
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
OUT="${ROOT}/aws_results"; mkdir -p "$OUT"

"${ROOT}/infra/01_create_core.sh"

# evidence job: same settings as the pipeline job, different script
aws s3 cp "${ROOT}/evidence/run_evidence.py" "s3://${BUCKET}/scripts/glue/run_evidence.py"
EVID_SPEC=$(aws glue get-job --job-name novacart_run_sql --query 'Job' --output json | python3 -c "
import json,sys; j=json.load(sys.stdin)
keep={k:j[k] for k in ['Role','Command','DefaultArguments','GlueVersion','WorkerType','NumberOfWorkers','Timeout','MaxRetries']}
keep['Command']['ScriptLocation']='s3://${BUCKET}/scripts/glue/run_evidence.py'
print(json.dumps(keep))")
if aws glue get-job --job-name novacart_evidence >/dev/null 2>&1; then
  aws glue update-job --job-name novacart_evidence --job-update "$EVID_SPEC" >/dev/null
else
  aws glue create-job --name novacart_evidence --cli-input-json "$EVID_SPEC" >/dev/null
fi

run_and_wait() {  # $1 job  $2 batch  $3 label
  local id state
  id=$(aws glue start-job-run --job-name "$1" --arguments "{\"--batch_id\":\"$2\",\"--steps\":\"all\"}" \
        --query JobRunId --output text)
  echo "[$3] started ${1} run ${id}"
  while true; do
    state=$(aws glue get-job-run --job-name "$1" --run-id "$id" --query JobRun.JobRunState --output text)
    case "$state" in
      SUCCEEDED) echo "[$3] SUCCEEDED"; echo "$id" >> "${OUT}/job_runs.txt"; return 0 ;;
      FAILED|ERROR|TIMEOUT|STOPPED)
        echo "[$3] ${state}: $(aws glue get-job-run --job-name "$1" --run-id "$id" --query JobRun.ErrorMessage --output text)"
        echo "Logs: CloudWatch > Log groups > /aws-glue/jobs/error  (stream ${id})"; exit 1 ;;
      *) sleep 20 ;;
    esac
  done
}

"${ROOT}/infra/03_upload_batch.sh" 1
run_and_wait novacart_run_sql 1 "batch 1"
"${ROOT}/infra/03_upload_batch.sh" 2
run_and_wait novacart_run_sql 2 "batch 2"
run_and_wait novacart_run_sql 2 "batch 2 re-run (idempotency)"
run_and_wait novacart_evidence 2 "Delta evidence"

aws s3 sync "s3://${BUCKET}/reports/" "${OUT}/reports/"
echo
echo "Done. Screenshots to take now:"
echo "  - Glue console > Jobs > novacart_run_sql > Runs  (3 succeeded runs)"
echo "  - Glue console > Jobs > novacart_evidence > latest run > Output logs: history, time travel, schema error, OPTIMIZE, CDF"
echo "  - DQ reports downloaded to ${OUT}/reports/"
echo "Compare numbers with local_test/expected_results/ (total revenue 295,585.58 USD, 14 payment mismatches)."
echo "Delete everything afterwards: CONFIRM=yes ./infra/99_teardown.sh"
