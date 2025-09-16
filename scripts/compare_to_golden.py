#!/usr/bin/env python3
import argparse, csv, json, math, sys, re
from pathlib import Path

DEF_TOL = 0.03

def die(msg): print(msg); sys.exit(1)

def load_csv(path):
    rows = {}
    with open(path, newline="", encoding="utf-8") as f:
        for i, r in enumerate(csv.DictReader(f)):
            rows[r["dossier"]] = {k: r[k] for k in r}
    return rows

def load_current():
    cur = {}
    for f in Path("out/svr").glob("*/SVR_blind_faith.json"):
        j = json.loads(f.read_text(encoding="utf-8"))
        d = j.get("dossier_source","unknown")
        cur[d] = {
            "SI": float(j["accords"]["accord_I_structural_integrity"]["avg"]),
            "SC": float(j["accords"]["accord_II_semantic_clarity"]["score"]),
            "AIQ_v2": float(j["accords"]["accord_III_aiq"]["aiq_v2"]),
            "processed": int(j["tallies"]["processed"]),
            "skipped": int(j["tallies"]["skipped"]),
        }
    return cur

def fp_after_p2_map():
    mp = {}
    for f in Path("out/preproc").glob("*.preproc_v6_2.json"):
        k = re.sub(r"\.preproc_v6_2\.json$", "", f.name)
        j = json.loads(f.read_text(encoding="utf-8"))
        cnt = sum(1 for t in j.get("processed", [])
                  if t.get("table_type")=="FRONT_PAGE" and int(t.get("page_number") or 0) > 2)
        mp[k] = cnt
    return mp

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kpi_golden", default="ci/baseline/svr_kpis_golden.csv")
    ap.add_argument("--fp_golden", default="ci/baseline/fp_after_p2_golden.json")
    ap.add_argument("--tol", type=float, default=DEF_TOL)
    args = ap.parse_args()

    gold = load_csv(args.kpi_golden)
    cur = load_current()

    # Compare SVR KPIs
    bad = []
    for d, g in gold.items():
        if d not in cur:
            bad.append(f"[MISSING] dossier now absent: {d}")
            continue
        for m in ("SI","SC","AIQ_v2"):
            gv = float(g[m]); cv = float(cur[d][m])
            if not math.isfinite(cv) or abs(cv - gv) > args.tol:
                bad.append(f"[REGRESS] {d} {m}: cur={cv:.4f} vs gold={gv:.4f} tol={args.tol}")

    # Compare FP_after_p2 exact
    fp_gold = json.loads(Path(args.fp_golden).read_text(encoding="utf-8"))
    fp_cur = fp_after_p2_map()
    for k, gv in fp_gold.items():
        cv = fp_cur.get(k)
        if cv is None:
            bad.append(f"[MISSING] preproc now absent: {k}")
        elif cv != gv:
            bad.append(f"[FP_DIFF] {k}: cur={cv} vs gold={gv} (must match)")

    if bad:
        print("=== REGRESSION DETECTED ===")
        print("\n".join(bad))
        sys.exit(2)
    print("Golden comparison: PASS")
if __name__ == "__main__":
    main()
