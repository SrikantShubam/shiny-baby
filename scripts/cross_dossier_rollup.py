#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cross-dossier SVR roll-up
Usage:
  python scripts/cross_dossier_rollup.py --inputs SVR1.json SVR2.json ... --out rollup.json
"""
import argparse, json
from pathlib import Path
from statistics import mean

def load(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as e:
        return {"error": str(e), "path": str(path)}

def pick_aiq(accords):
    a = accords.get("accord_III_aiq", {})
    # Prefer v2, then v1; else average whatever components exist.
    if "aiq_v2" in a:
        return float(a["aiq_v2"])
    if "aiq_v1" in a:
        return float(a["aiq_v1"])
    comps = []
    for k in ("numeric_density_avg", "header_salience_avg", "frontpage_entity_rate", "anchor_density_avg"):
        if k in a:
            comps.append(float(a[k]))
    return float(mean(comps)) if comps else 0.0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", nargs="+", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    svrs = [load(p) for p in args.inputs]
    ok = [s for s in svrs if "accords" in s and "tallies" in s]
    if not ok:
        Path(args.out).write_text(json.dumps({"error":"no valid svrs"}, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"ok": False, "count": 0}, ensure_ascii=False))
        return

    comp = [float(s.get("composite_score", 0.0)) for s in ok]
    si   = [float(s["accords"]["accord_I_structural_integrity"]["avg"]) for s in ok]
    sc   = [float(s["accords"]["accord_II_semantic_clarity"]["score"]) for s in ok]
    aiq  = [pick_aiq(s["accords"]) for s in ok]
    proc = [int(s["tallies"]["processed"]) for s in ok]
    skip = [int(s["tallies"]["skipped"]) for s in ok]

    roll = {
        "docs": [s.get("dossier_source","unknown") for s in ok],
        "composite_avg": round(mean(comp), 4),
        "SI_avg": round(mean(si), 4),
        "SC_avg": round(mean(sc), 4),
        "AIQ_avg": round(mean(aiq), 4),
        "processed_total": int(sum(proc)),
        "skipped_total": int(sum(skip)),
        "kpis": {
            "target_composite": 0.78,
            "meets_target": round(mean(comp), 4) >= 0.78
        }
    }
    Path(args.out).write_text(json.dumps(roll, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"ok": True, "out": args.out, "docs": len(ok)}, ensure_ascii=False))

if __name__ == "__main__":
    main()
