# Phase-1B: Pattern Tuning & Hardening

**Goals**
- Keep FP bleed after page 2 per dossier ≤ 5.
- Preserve SI on tabular-rich dossiers (≥ 0.80 on Agrolife/Motisons/Sah).
- Maintain honest SI=0 for single-column contact slabs; value carried by SC/AIQ.

**Levers**
1. Pattern thresholds (FRONT_CONTACT_STRICT/KV).
2. KV label coverage (aliases/synonyms).
3. Header composition (period headers).
4. Deduping near-identical FP contact slabs.
5. KPI smoke tests that fail the run if guardrails break.

**KPIs**
- `FP_after_p2` per dossier
- SI/SC/AIQ_v2 per dossier
- Processed vs Skipped counts (watch for catastrophic drops)

**Exit criteria**
- All KPIs green on current 5 dossiers.
- Patterns/pipeline stable across 2 consecutive runs.
