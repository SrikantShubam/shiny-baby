#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Operation Blind Faith — Canonical Strategic Value Report (SVR)
Authoritative, self-contained audit over a preprocessed dossier (Surgeon v6.2 output).

Usage:
  python phoenix/audit/blind_faith_svr.py --input out/preproc/<name>.preproc_v6_2.json --outdir out/svr/<name>
Outputs:
  - <outdir>/SVR_blind_faith.json      # canonical SVR (ground truth)
  - <outdir>/SVR_methodology.md        # "show your work" doc

Accords implemented (Grok Accords):
I.  Structural Integrity (SI): tabularity_score averaged over processed tables
II. Semantic Clarity (SC): tiering (Tier 0–3) compressed to [0,1]
III.Analytical Information Quotient (AIQ): mix of numeric density, header salience, FP entities, and anchor density

Composite_v2 = 0.5*SI + 0.3*SC + 0.2*AIQ_v2
"""

import argparse, json, re
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Tuple

# -------------------------
# Regex / tokens
# -------------------------

NUM_RE     = re.compile(r"^[0-9,.\-\(\)%₹`]+$")
EMAIL_RE   = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
URL_RE     = re.compile(r"(?:(?:https?://)?(?:www\.)?[A-Za-z0-9.-]+\.[A-Za-z]{2,}(?:/[^\s]*)?)", re.I)
PHONE_RE   = re.compile(r"(?:\+?\d[\d\s\-]{7,}\d)")

MONTH_TOKENS = ("jan","feb","mar","apr","may","jun","jul","aug","sep","sept","oct","nov","dec")
YEAR_RE      = re.compile(r"(?:19|20)\d{2}")

FRONTPAGE_TERMS = (
  "brlm","registrar","email","e-mail","website","tel","telephone",
  "issue opens","issue closes","anchor investor","bid / issue","rta","kfin","link intime"
)

HEADER_HINTS = (
  "revenue","income","expenses","ebitda","profit","loss","equity","assets","liabilities",
  "cash","year","period","as on","for the year","nav","eps","shares","note","particulars",
  "sr. no.","description","details","total"
)

# -------------------------
# Helpers (pure / deterministic)
# -------------------------

def norm_text(x: Any) -> str:
    return re.sub(r"\s+"," ", ("" if x is None else str(x)).strip().lower())

def is_numeric_cell(s: str) -> bool:
    return bool(NUM_RE.match((s or "").strip()))

def tokenized(s: str) -> List[str]:
    s = norm_text(s)
    return [t for t in re.split(r"[^a-z0-9%]+", s) if t]

def tabularity_score(rows: List[List[str]]) -> float:
    """Measures column consistency (numeric vs non-numeric) + minimal width requirement."""
    if not rows: return 0.0
    cols = max((len(r) for r in rows), default=0)
    if cols <= 1: return 0.0
    col_scores = []
    for j in range(cols):
        vals = [norm_text(r[j]) for r in rows if j < len(r)]
        vals = [v for v in vals if v]
        if not vals: col_scores.append(0.0); continue
        num = sum(is_numeric_cell(v) for v in vals)
        frac = max(num/len(vals), 1 - num/len(vals))  # purity of a column
        col_scores.append(frac)
    purity = sum(col_scores)/len(col_scores)
    strong = sum(1 for f in col_scores if f > 0.7)
    width  = 1.0 if (cols >= 2 and strong >= 1) else 0.0
    return round(0.7*purity + 0.3*width, 3)

def is_frontpage_semantic_blob(table: Dict[str,Any]) -> bool:
    text = " ".join(" ".join(r) for r in (table.get("data",[]) or []))
    text = norm_text(text)
    return any(term in text for term in FRONTPAGE_TERMS)

def header_has_period_tokens(headers: List[str]) -> bool:
    H = " ".join(norm_text(h) for h in headers or [])
    return (any(m in H for m in MONTH_TOKENS) or bool(YEAR_RE.search(H)))

def header_salience_score(headers: List[str]) -> float:
    """Fraction of header cells containing known salient tokens or period tokens."""
    if not headers: return 0.0
    heads = [norm_text(h) for h in headers]
    hits  = 0
    for h in heads:
        if any(k in h for k in HEADER_HINTS) or header_has_period_tokens([h]):
            hits += 1
    return hits / max(1, len(heads))

def numeric_density(rows: List[List[str]]) -> float:
    cells = sum(len(r) for r in (rows or []))
    if cells == 0: return 0.0
    nums  = sum(is_numeric_cell(c) for r in rows for c in r)
    return nums / cells

def tier_semantics(table: Dict[str,Any]) -> int:
    """Tier 0..3 heuristic:
       3: Multi-period (headers with years/months) or classic FS keywords and decent numeric density
       2: Good structure (tabularity ≥ 0.5) OR front-page semantic block
       1: Weak table (tabularity in [0.25, 0.5))
       0: Likely non-table/noisy (tabularity < 0.25)
    """
    headers = table.get("headers", []) or []
    data    = table.get("data", []) or []
    tscore  = tabularity_score(data)
    if header_has_period_tokens(headers) and numeric_density(data) >= 0.25:
        return 3
    blob = " ".join(tokenized(" ".join(headers)))
    if any(k in blob for k in ("balance","profit","loss","cash","equity","assets","liabilities","statement")) and numeric_density(data) >= 0.25:
        return 3
    if tscore >= 0.5 or is_frontpage_semantic_blob(table):
        return 2
    if tscore >= 0.25:
        return 1
    return 0

# -------------------------
# SVR computation
# -------------------------

def compute_svr(doc: Dict[str,Any], source: str) -> Dict[str,Any]:
    processed = doc.get("processed", []) or []
    skipped   = doc.get("skipped",   []) or []
    tallies   = {"processed": len(processed), "skipped": len(skipped)}

    # Accord I: Structural Integrity
    per_si = [{"page": t.get("page_number"), "idx": t.get("table_index"),
               "tabularity": tabularity_score(t.get("data", []))} for t in processed]
    si_avg = round(mean([x["tabularity"] for x in per_si]) if per_si else 0.0, 4)

    # Accord II: Semantic Clarity (tiered)
    tiers = [tier_semantics(t) for t in processed]
    t0 = tiers.count(0); t1 = tiers.count(1); t2 = tiers.count(2); t3 = tiers.count(3)
    sc_score = round(((0*t0 + 1*t1 + 2*t2 + 3*t3) / max(1, 3*len(processed))), 4)

    # Accord III: AIQ — include anchor density for front-page style contact slabs
    def anchor_density(tbl: Dict[str,Any]) -> float:
        txt = " ".join(" ".join(r) for r in (tbl.get("data",[]) or [])).lower() + " " + " ".join(tbl.get("headers",[]) or [])
        anchors = 0
        anchors += 1 if EMAIL_RE.search(txt) else 0
        anchors += 1 if URL_RE.search(txt)   else 0
        anchors += 1 if PHONE_RE.search(txt) else 0
        return anchors/3.0  # normalize to [0,1]

    nd = [numeric_density(t.get("data",[])) for t in processed] or [0.0]
    hs = [header_salience_score(t.get("headers",[])) for t in processed] or [0.0]
    fp = [1.0 if is_frontpage_semantic_blob(t) else 0.0 for t in processed] or [0.0]
    ad = [anchor_density(t) for t in processed] or [0.0]

    nd_avg = round(mean(nd),4); hs_avg = round(mean(hs),4); fp_avg = round(mean(fp),4); ad_avg = round(mean(ad),4)
    aiq_v1 = round(mean([nd_avg, hs_avg, fp_avg]), 4)
    aiq_v2 = round(mean([nd_avg, hs_avg, fp_avg, ad_avg]), 4)

    # Composite v1 (backward compat) and v2 (with anchor_density)
    composite_v1 = round(0.5*si_avg + 0.3*sc_score + 0.2*aiq_v1, 4)
    composite_v2 = round(0.5*si_avg + 0.3*sc_score + 0.2*aiq_v2, 4)

    accords = {
        "accord_I_structural_integrity": {"avg": si_avg, "samples": per_si[: min(6, len(per_si))]},
        "accord_II_semantic_clarity": {"tiers": {"t0": t0, "t1": t1, "t2": t2, "t3": t3}, "score": sc_score},
        "accord_III_aiq": {
            "numeric_density_avg": nd_avg,
            "header_salience_avg": hs_avg,
            "frontpage_entity_rate": fp_avg,
            "anchor_density_avg": ad_avg,
            "aiq_v1": aiq_v1,
            "aiq_v2": aiq_v2
        }
    }

    methodology = {
        "accord_I":  {"definition": "tabularity_score truthful", "weights_in_composite": 0.5},
        "accord_II": {"definition": "Tiering via period headers / FS lexicon / FP semantics", "weights_in_composite": 0.3},
        "accord_III":{"definition": "AIQ_v2 = mean(numeric_density, header_salience, FP_entity_rate, anchor_density)", "weights_in_composite": 0.2},
        "composite_v1": "0.5*SI + 0.3*SC + 0.2*AIQ_v1",
        "composite_v2": "0.5*SI + 0.3*SC + 0.2*AIQ_v2"
    }

    return {
        "dossier_source": source,
        "tallies": tallies,
        "accords": accords,
        "composite_score": composite_v2,   # headline
        "composite_legacy": composite_v1,  # continuity
        "methodology": methodology
    }

def write_methodology_md(svr: Dict[str,Any], out_md: Path) -> None:
    a1 = svr["accords"]["accord_I_structural_integrity"]
    a2 = svr["accords"]["accord_II_semantic_clarity"]
    a3 = svr["accords"]["accord_III_aiq"]
    lines = []
    lines.append("# Operation Blind Faith — Methodology\n")
    lines.append("## Accord I — Structural Integrity (SI)\n")
    lines.append("- **Definition:** `tabularity_score = 0.7*avg(column_purity) + 0.3*width_guard`.\n")
    lines.append("- **Average SI:** **{:.4f}**\n".format(a1["avg"]))
    lines.append("- **Samples:**\n")
    for s in a1.get("samples", []):
        lines.append(f"  - Page {s['page']}, Table {s['idx']}: tabularity={s['tabularity']:.3f}\n")
    lines.append("\n## Accord II — Semantic Clarity (SC)\n")
    lines.append("- **Tiering:** 0..3 via period headers, FS lexicon, front-page semantics, structure gates.\n")
    tiers = a2["tiers"]
    lines.append(f"- **Counts:** T0={tiers['t0']} T1={tiers['t1']} T2={tiers['t2']} T3={tiers['t3']}\n")
    lines.append("- **SC Score:** **{:.4f}** (=(1*T1 + 2*T2 + 3*T3) / (3 * total_tables))\n".format(a2["score"]))
    lines.append("\n## Accord III — AIQ\n")
    lines.append("- **Numeric density avg:** {:.4f}\n".format(a3["numeric_density_avg"]))
    lines.append("- **Header salience avg:** {:.4f}\n".format(a3["header_salience_avg"]))
    lines.append("- **Front-page entity rate:** {:.4f}\n".format(a3["frontpage_entity_rate"]))
    lines.append("- **Anchor density avg:** {:.4f}\n".format(a3["anchor_density_avg"]))
    lines.append("- **AIQ (v2):** **{:.4f}**\n".format(a3["aiq_v2"]))
    lines.append("\n## Composite Score\n")
    lines.append("- **Composite (v2):** **{:.4f}** = 0.5*SI + 0.3*SC + 0.2*AIQ_v2\n".format(svr["composite_score"]))
    out_md.write_text("".join(lines), encoding="utf-8")

# -------------------------
# CLI
# -------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Surgeon v6.2 preproc JSON")
    ap.add_argument("--outdir", required=True, help="Output directory for SVR & methodology")
    args = ap.parse_args()

    in_path = Path(args.input)
    out_dir = Path(args.outdir)
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        doc = json.loads(in_path.read_text(encoding="utf-8"))
    except Exception as e:
        out = {"error":"preproc_json_parse", "msg": str(e)}
        (out_dir/"SVR_blind_faith.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"ok": False, "error": "parse"}))
        return

    source = doc.get("source") or doc.get("filename") or in_path.stem
    svr = compute_svr(doc, source=source)

    svr_path = out_dir / "SVR_blind_faith.json"
    svr_path.write_text(json.dumps(svr, ensure_ascii=False, indent=2), encoding="utf-8")

    write_methodology_md(svr, out_dir / "SVR_methodology.md")

    print(json.dumps({"ok": True, "svr": str(svr_path), "composite": svr["composite_score"]}, ensure_ascii=False))

if __name__ == "__main__":
    main()
