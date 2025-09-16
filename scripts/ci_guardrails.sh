#!/usr/bin/env bash
set -euo pipefail

# CI guardrail: run the smoke test which includes KPI checks.
# Fails fast if any KPI breaches (e.g., FP-after-p2 > 5).
scripts/phase1b_smoke.sh
echo "=== Golden regression check (Operation IRONCLAD) ==="
# Expect these to exist; you can refresh them when you intentionally change behavior.
BASE_ROLLUP="ci/baseline/rollup.json"
BASE_KPIS="ci/baseline/svr_kpis.csv"
if [[ -f "$BASE_ROLLUP" && -f "$BASE_KPIS" ]]; then
  python3 scripts/ci_compare_golden.py \
    --baseline-rollup "$BASE_ROLLUP" \
    --baseline-svr-kpis "$BASE_KPIS" \
    --current-rollup "out/svr/rollup.json" \
    --svr-dir "out/svr" \
    --tol 0.03
else
  echo "WARN: Golden files not found. Skipping strict comparison."
fi

echo "✅ CI guardrails passed."
