#!/usr/bin/env python3
import argparse, json, csv, re
from pathlib import Path
from datetime import datetime

def fp_after_p2(preproc_path: Path) -> int:
    data = json.loads(preproc_path.read_text(encoding="utf-8"))
    c = 0
    for t in data.get("processed", []):
        if t.get("table_type") == "FRONT_PAGE" and int(t.get("page_number") or 0) > 2:
            c += 1
    return c

def load_svr(svr_path: Path):
    j = json.loads(svr_path.read_text(encoding="utf-8"))
    d = {
        "dossier": j.get("dossier_source","unknown"),
        "SI": float(j["accords"]["accord_I_structural_integrity"]["avg"]),
        "SC": float(j["accords"]["accord_II_semantic_clarity"]["score"]),
        "AIQ_v2": float(j["accords"]["accord_III_aiq"]["aiq_v2"]),
        "processed": int(j["tallies"]["processed"]),
        "skipped": int(j["tallies"]["skipped"]),
    }
    return d

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="ci/baseline")
    ap.add_argument("--stamp", action="store_true")
    args = ap.parse_args()

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    stamp = f"_{ts}" if args.stamp else ""

    # Collect current KPIs
    svr_rows = []
    for f in Path("out/svr").glob("*/SVR_blind_faith.json"):
        svr_rows.append(load_svr(f))

    # Write KPIs CSV (stamped + golden alias)
    kpi_csv_stamped = outdir / f"svr_kpis{stamp}.csv"
    with kpi_csv_stamped.open("w", newline="", encoding="utf-8") as fo:
        w = csv.DictWriter(fo, fieldnames=["dossier","SI","SC","AIQ_v2","processed","skipped"])
        w.writeheader(); w.writerows(svr_rows)
    kpi_csv_golden = outdir / "svr_kpis_golden.csv"
    kpi_csv_golden.write_text(kpi_csv_stamped.read_text(encoding="utf-8"), encoding="utf-8")

    # FP after p2 JSON (stamped + golden alias)
    fp_map = {}
    for f in Path("out/preproc").glob("*.preproc_v6_2.json"):
        key = re.sub(r"\.preproc_v6_2\.json$", "", f.name)
        fp_map[key] = fp_after_p2(f)
    fp_json_stamped = outdir / f"fp_after_p2{stamp}.json"
    fp_json_stamped.write_text(json.dumps(fp_map, ensure_ascii=False, indent=2), encoding="utf-8")
    fp_json_golden = outdir / "fp_after_p2_golden.json"
    fp_json_golden.write_text(fp_json_stamped.read_text(encoding="utf-8"), encoding="utf-8")

    # Rollup snapshot if present
    rollup = Path("out/svr/rollup.json")
    if rollup.exists():
        r_stamped = outdir / f"rollup{stamp}.json"
        r_stamped.write_text(rollup.read_text(encoding="utf-8"), encoding="utf-8")
        (outdir / "rollup_golden.json").write_text(rollup.read_text(encoding="utf-8"), encoding="utf-8")

    print(json.dumps({"ok": True, "golden_dir": str(outdir), "stamp": ts if args.stamp else None}))
if __name__ == "__main__":
    main()
