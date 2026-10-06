#!/usr/bin/env bash
# Local logic test (no AWS, no Delta jar needed): batch 1, batch 2, batch 2 again,
# then export gold tables + DQ report to local_test/expected_results/.
# Needs: Java 17+, Python 3.9+, pip install pyspark==3.5.3 pandas
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LAKE="${LAKE:-/tmp/novacart-local-lake}"
rm -rf "$LAKE"
python "$ROOT/local_test/harness.py" --batches 1,2,2 --lake "$LAKE" --input "$ROOT/input" --sql_root "$ROOT/sql"
python "$ROOT/local_test/export_results.py" --lake "$LAKE" --out "$ROOT/local_test/expected_results"
