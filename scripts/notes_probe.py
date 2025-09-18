#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Notes Recon (Phase-2, Step C – parallel track):
- Scan preproc for pages that look like Notes / Accounting Policies / Related Parties etc.
- Produce a simple index with (dossier, page_number, note_ref, topic, snippet_len).

Writes: out/canon/notes_index.parquet
"""

from __future__ import annotations
import json, re, sys
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd

PREPROC_DIR = Path("out/preproc")
OUT_DIR = Path("out/canon"); OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PARQUET = OUT_DIR / "notes_index.parquet"

TOPIC_MAP = [
    (re.compile(r"\b(significant accounting policies|basis of preparation)\b", re.I), "Accounting policies"),
    (re.compile(r"\b(related party|related parties|KMP)\b", re.I), "Related parties"),
    (re.compile(r"\b(contingent liabilities?|contingencies|commitments)\b", re.I), "Contingent liabilities"),
    (re.compile(r"\b(revenue recognition|ind as 115|ifrs 15)\b", re.I), "Revenue recognition"),
    (re.compile(r"\b(leases?|ind as 116|ifrs 16)\b", re.I), "Leases"),
    (re.compile(r"\b(tax(es)?|income tax|deferred tax)\b", re.I), "Taxes"),
    (re.compile(r"\b(segment information|operating segments?)\b", re.I), "Segments"),
    (re.compile(r"\b(impairment|expected credit loss|ECL)\b", re.I), "Impairment/ECL"),
    (re.compile(r"\b(borrowings?|loans?|interest)\b", re.I), "Borrowings"),
    (re.compile(r"\b(earnings per share|EPS)\b", re.I), "Earnings per share"),
]

NOTE_REF_RE = re.compile(r"\bNote\s+(\d+[A-Z]?)\b", re.I)
NOTES_PAGE_HINT = re.compile(r"\b(notes?\s+to\s+the\s+financial\s+statements?|notes?\s*\(\s*continued\s*\))\b", re.I)

def norm(x: Any) -> str:
    if x is None: return ""
    return re.sub(r"\s+", " ", str(x).strip())

def iter_preproc_files() -> List[Path]:
    return sorted(PREPROC_DIR.glob("*.preproc_v6_2.json"))

def guess_topic(blob: str) -> Optional[str]:
    for rx, label in TOPIC_MAP:
        if rx.search(blob):
            return label
    return None

def main():
    rows: List[Dict[str, Any]] = []
    for fp in iter_preproc_files():
        try:
            obj = json.loads(fp.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[WARN] {fp}: {e}", file=sys.stderr)
            continue

        dossier = obj.get("filename") or obj.get("source") or fp.stem
        for t in obj.get("processed", []):
            # Notes often live in GENERIC tables or text slabs, frequently after page 2
            pg = t.get("page_number") or 0
            if pg <= 2:
                continue
            headers = t.get("headers") or []
            data = t.get("data") or []
            band = " ".join([norm(h) for h in headers])
            # flatten a few first rows for sniffing
            flat = " ".join(" ".join(norm(c) for c in r[:6]) for r in data[:8])
            blob = (band + " " + flat).lower()

            if NOTESPAGE := (NOTES_PAGE_HINT.search(blob) or "significant accounting policies" in blob):
                # try to extract multiple note refs on the page; fallback to None
                refs = set(m.group(1) for m in NOTE_REF_RE.finditer(blob))
                topic = guess_topic(blob)
                snippet_len = len(flat)
                if not refs:
                    rows.append({
                        "dossier": dossier,
                        "page_number": pg,
                        "note_ref": None,
                        "topic": topic or "Notes (unspecified)",
                        "snippet_len": snippet_len,
                    })
                else:
                    for ref in sorted(refs):
                        rows.append({
                            "dossier": dossier,
                            "page_number": pg,
                            "note_ref": ref,
                            "topic": topic or "Notes",
                            "snippet_len": snippet_len,
                        })
            else:
                # Opportunistic capture by strong topics even if “Notes” not spelled out
                topic = guess_topic(blob)
                if topic:
                    rows.append({
                        "dossier": dossier,
                        "page_number": pg,
                        "note_ref": None,
                        "topic": topic,
                        "snippet_len": len(flat),
                    })

    if not rows:
        print("No notes rows found; nothing written.")
        return

    df = pd.DataFrame(rows)
    cols = ["dossier","page_number","note_ref","topic","snippet_len"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    df.to_parquet(OUT_PARQUET, index=False)
    print(f"Wrote {len(df):,} rows -> {OUT_PARQUET}")

if __name__ == "__main__":
    main()
