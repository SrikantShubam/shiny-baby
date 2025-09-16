#!/usr/bin/env bash
set -euo pipefail

# 1) Run the pipeline
scripts/run_phase1a.sh --indir data --svr --rollup --venv ./phoenix_env --outdir ./out

echo "=== KPI: FRONT_PAGE after page 2 ==="
fail=0
for src in data/*.json; do
  base=$(basename "$src" .json)
  p="out/preproc/${base}.preproc_v6_2.json"
  if [[ ! -f "$p" ]]; then
    echo "WARN: missing preproc for $base"; fail=1; continue
  fi
  fp_after_p2=$(jq -r '.processed[] | select(.table_type=="FRONT_PAGE") | .page_number' "$p" \
                | awk '$1>2{c++} END{print c+0}')
  printf "%-50s  FP_after_p2=%s\n" "$base" "$fp_after_p2"
  if [[ "${fp_after_p2:-0}" -gt 5 ]]; then
    echo "FAIL: $base FP_after_p2=$fp_after_p2 (>5)"; fail=1
  fi
done

echo "=== KPI: SI/SC/AIQ_v2 snapshot ==="
for src in data/*.json; do
  base=$(basename "$src" .json)
  f="out/svr/${base}/SVR_blind_faith.json"
  if [[ -f "$f" ]]; then
    jq -r '"\(.dossier_source)\tSI=\(.accords.accord_I_structural_integrity.avg)\tSC=\(.accords.accord_II_semantic_clarity.score)\tAIQ_v2=\(.accords.accord_III_aiq.aiq_v2)\tproc=\(.tallies.processed)\tskip=\(.tallies.skipped)"' "$f"
  else
    echo "WARN: missing SVR for $base"
  fi
done

if [[ $fail -ne 0 ]]; then
  echo "Smoke test failed."
  exit 1
fi
echo "All KPIs within guardrails."
