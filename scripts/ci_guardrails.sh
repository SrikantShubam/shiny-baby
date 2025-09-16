#!/usr/bin/env bash
set -euo pipefail

# CI guardrail: run the smoke test which includes KPI checks.
# Fails fast if any KPI breaches (e.g., FP-after-p2 > 5).
scripts/phase1b_smoke.sh

echo "✅ CI guardrails passed."
