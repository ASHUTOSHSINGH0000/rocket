#!/usr/bin/env bash
# Upload one batch to the landing zone, then write the _SUCCESS marker LAST.
# The marker is what EventBridge listens for, so the DAG only starts once the batch is complete.
# Usage: ./infra/03_upload_batch.sh 1
set -euo pipefail
B="${1:?batch number, e.g. 1}"
BUCKET="${BUCKET:-novacart-lakehouse}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
aws s3 cp "${ROOT}/input/batch_${B}/orders_batch_${B}.csv"        "s3://${BUCKET}/input/batch_${B}/"
aws s3 cp "${ROOT}/input/batch_${B}/order_items_batch_${B}.jsonl" "s3://${BUCKET}/input/batch_${B}/"
aws s3 sync "${ROOT}/input/reference/" "s3://${BUCKET}/input/reference/"
printf '' | aws s3 cp - "s3://${BUCKET}/input/batch_${B}/_SUCCESS"
echo "Batch ${B} uploaded and marked complete."
