#!/usr/bin/env bash
# One command, about 5-8 minutes on a laptop. Mac prerequisites (one time):
#   brew install openjdk@17 python@3.12
#   export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
# Optional: BUCKET=novacart-lakehouse12 copies the finished lakehouse to S3 at the end.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY="${PYTHON:-python3.12}"
[ -d .venv ] || "$PY" -m venv .venv
.venv/bin/pip install -q --upgrade pip setuptools wheel
.venv/bin/pip install -q pyspark==3.5.3 delta-spark==3.2.1
rm -rf "$ROOT/lake"
.venv/bin/python local_delta/run_local_delta.py --lake "$ROOT/lake" 2>&1 | tee "$ROOT/pipeline_run_log.txt"
if [ -n "${BUCKET:-}" ]; then
  aws s3 sync "$ROOT/lake/" "s3://${BUCKET}/lakehouse/" --exclude "*.crc"
  echo "Lakehouse copied to s3://${BUCKET}/lakehouse/"
fi
echo "Full log (your evidence): $ROOT/pipeline_run_log.txt"
