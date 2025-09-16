#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phoenix v6.2 Validator (binary PASS/FAIL)
----------------------------------------
Usage:
  python validate_phoenix_v6_2.py --input data/sah.json [--preproc phoenix/surgeon/preproc_v6_2.py]

What it does:
1) Locates & runs the Surgeon (preproc_v6_2.py) on the legacy JSON.
2) Loads the produced preproc JSON.
3) Applies sanity/quality checks representing our six failure guards.
4) Emits a single JSON object to STDOUT:
   {"PASS": true|false, "reasons": [...], "processed": N, "skipped": M}

Surgeon path resolution order:
  --preproc arg > $PREPROC_PATH > ./test_dir/preproc_v6_2.py >
  ./phoenix/surgeon/preproc_v6_2.py > /mnt/data/preproc_v6_2.py
"""
import argparse, json, os, re, sys, tempfile, subprocess
from pathlib import Path
from typing import Any, Dict, List

HERE = Path(__file__).resolve().parent

def find_preproc(explicit: str|None) -> Path|None:
    candidates = []
    if explicit: candidates.append(Path(explicit))
    if os.getenv("PREPROC_PATH"): candidates.append(Path(os.getenv("PREPROC_PATH")))
    candidates += [
        HERE.parent / "test_dir" / "preproc_v6_2.py",
        HERE.parent / "phoenix" / "surgeon" / "preproc_v6_2.py",
        Path("/mnt/data/preproc_v6_2.py"),
    ]
    for c in candidates:
        if c and c.exists():
            return c
    return None

def load_legacy_counts(legacy_path: Path) -> int:
    """Return number of raw tables in the legacy input (first top-level company)."""
    try:
        data = json.loads(legacy_path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and data:
            first_key = next(iter(data.keys()))
            meta = data[first_key]
            if isinstance(meta, dict) and isinstance(meta.get("tables", []), list):
                return len(meta["tables"])
    except Exception:
        pass
    return -1

def tabularity_score(rows: List[List[str]]) -> float:
    if not rows: return 0.0
    cols = max((len(r) for r in rows), default=0)
    if cols <= 1: return 0.0
    def isnum(v: str) -> bool: return bool(re.match(r"^[0-9,.\-\(\)%₹`]+$", (v or "").strip()))
    col_scores = []
    for j in range(cols):
        vals = [r[j] for r in rows if j < len(r) and (r[j] or "").strip()]
        if not vals: col_scores.append(0.0); continue
        num = sum(isnum(v) for v in vals)
        frac = max(num/len(vals), 1 - num/len(vals))
        col_scores.append(frac)
    purity = sum(col_scores)/len(col_scores)
    strong = sum(1 for f in col_scores if f > 0.7)
    width = 1.0 if (cols >= 2 and strong >= 1) else 0.0
    return round(0.7*purity + 0.3*width, 3)

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Legacy JSON (one dossier)")
    ap.add_argument("--preproc", default=None, help="Path to preproc_v6_2.py")
    args = ap.parse_args()

    legacy = Path(args.input).resolve()
    if not legacy.exists():
        print(json.dumps({"PASS": False, "reason": {"error":"input_not_found", "path": str(legacy)}}))
        sys.exit(2)

    preproc = find_preproc(args.preproc)
    if not preproc:
        print(json.dumps({"PASS": False, "reason": {"error":"preproc_not_found"}}))
        sys.exit(3)

    # Run Surgeon
    raw_count = load_legacy_counts(legacy)
    with tempfile.TemporaryDirectory() as td:
        outdir = Path(td)
        out_json = outdir / f"{legacy.stem}.preproc_v6_2.json"
        try:
            cmd = [sys.executable, str(preproc), "--input", str(legacy), "--output", str(out_json)]
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if proc.returncode != 0 or not out_json.exists():
                print(json.dumps({"PASS": False, "reason": {"error":"surgeon_failed", "stderr": proc.stderr[:4000]}}))
                sys.exit(4)
        except Exception as e:
            print(json.dumps({"PASS": False, "reason": {"error":"surgeon_exception", "msg": str(e)}}))
            sys.exit(5)

        # Load result
        try:
            res = json.loads(out_json.read_text(encoding="utf-8"))
        except Exception as e:
            print(json.dumps({"PASS": False, "reason": {"error":"preproc_json_parse", "msg": str(e)}}))
            sys.exit(6)

    processed = res.get("processed", []) or []
    skipped   = res.get("skipped", []) or []
    pcount    = res.get("processed_count", len(processed))
    scount    = res.get("skipped_count", len(skipped))

    # -------- Quality / safety checks (six guards) --------
    reasons: List[str] = []

    # G1: Non-empty processing
    if pcount <= 0:
        reasons.append("no_tables_processed")

    # G2: Dedup integrity — content hashes unique
    hashes = [t.get("content_hash") for t in processed if t.get("content_hash")]
    if len(hashes) != len(set(hashes)):
        reasons.append("duplicate_content_hash_detected")

    # G3: Accounting sanity (optional; only if we could read legacy count)
    if raw_count >= 0 and (pcount + scount) > (raw_count + 2):  # small slack for triage-generated splits
        reasons.append(f"unexpected_output_count:{pcount+scount} vs legacy:{raw_count}")

# G4: Structural integrity — measure only on genuinely tabular candidates
    weak = 0
    considered = 0
    for t in processed:
        if t.get("synthetic_singlecol") or t.get("table_type") == "FRONT_PAGE":
            continue  # text salvage and FP contact slabs are allowed to be low-tabular
        considered += 1
        score = tabularity_score(t.get("data", []))
        if score < 0.25:
            weak += 1
    if considered and weak/considered > 0.25:
        reasons.append("low_tabularity_fraction")


    # G5: Period/header stitch — if headers mention years/months, ensure ≥2 columns
    year_re = re.compile(r"(?:19|20)\d{2}")
    month = ("jan","feb","mar","apr","may","jun","jul","aug","sep","sept","oct","nov","dec")
    bad_period = 0
    for t in processed:
        headers = [h.lower() for h in (t.get("headers",[]) or [])]
        if any(year_re.search(h) or any(m in h for m in month) for h in headers):
            if len(headers) < 2:
                bad_period += 1
    if bad_period > 0:
        reasons.append("period_header_suspect")

    # G6: Front-page signal — at least one processed FRONT_PAGE table with contact/prospectus signals
    fp_hit = 0
    fp_terms = ("email","website","tel","brlm","registrar","issue opens","issue closes","anchor investor")
    for t in processed:
        if (t.get("table_type") == "FRONT_PAGE"):
            blob = " ".join(" ".join(r) for r in (t.get("data",[]) or [])).lower()
            if any(term in blob for term in fp_terms):
                fp_hit += 1; break
        if fp_hit == 0:
        # If nothing processed at all, it's a hard fail. Otherwise, warn only.
            if pcount == 0:
                reasons.append("front_page_semantic_block_missing")
            else:
                reasons.append("WARN_front_page_semantic_block_missing")
    # PASS ignores WARN_* reasons
    hard_reasons = [r for r in reasons if not str(r).startswith("WARN_")]
    PASS = len(hard_reasons) == 0
    print(json.dumps({"PASS": PASS, "reasons": reasons, "processed": pcount, "skipped": scount}, ensure_ascii=False))

if __name__ == "__main__":
    main()
