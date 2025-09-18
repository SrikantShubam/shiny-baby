# #!/usr/bin/env bash
# set -euo pipefail

# # CI guardrail: run the smoke test which includes KPI checks.
# # Fails fast if any KPI breaches (e.g., FP-after-p2 > 5).
# scripts/phase1b_smoke.sh
# echo "=== Golden regression check (Operation IRONCLAD) ==="
# # Expect these to exist; you can refresh them when you intentionally change behavior.
# BASE_ROLLUP="ci/baseline/rollup.json"
# BASE_KPIS="ci/baseline/svr_kpis.csv"
# if [[ -f "$BASE_ROLLUP" && -f "$BASE_KPIS" ]]; then
#   python3 scripts/ci_compare_golden.py \
#     --baseline-rollup "$BASE_ROLLUP" \
#     --baseline-svr-kpis "$BASE_KPIS" \
#     --current-rollup "out/svr/rollup.json" \
#     --svr-dir "out/svr" \
#     --tol 0.03
# else
#   echo "WARN: Golden files not found. Skipping strict comparison."
# fi

# echo "✅ CI guardrails passed."














#!/usr/bin/env bash
set -euo pipefail

# 1) Core pipeline (preproc + SVR + rollup)
scripts/run_phase1a.sh --indir data --svr --rollup --venv ./phoenix_env --outdir ./out

# 2) Phase-1B KPIs (front-page bleed etc.). Your existing script exits non-zero on failure.
scripts/phase1b_smoke.sh

# 3) Contacts (keep if you still want it in CI; harmless and fast)
python scripts/etl_contacts.py
python scripts/check_contacts_quality.py out/canon/contacts.parquet \
  --min_rows 30 --max_null_email 0.25 --max_dup_email_rate 0.10 --max_bad_phone_rate 0.20

# 4) Financials (NEW guardrail)
python scripts/etl_financials.py
python scripts/check_financials_quality.py out/canon/financials.parquet \
  --min_rows 300 --min_numeric_rate 0.80 --min_periods_per_table 2 --allow_single_period_bs

echo "✅ CI guardrails passed."
