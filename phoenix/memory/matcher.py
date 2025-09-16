from __future__ import annotations
from typing import Dict, Any, List, Tuple
import math, re, hashlib
from .schemas import Signature

_num_pat = re.compile(r"^[0-9,.\-\(\)%₹`]+$")

def _frac(items: int, total: int) -> float:
    return 0.0 if total <= 0 else items/total

def _entropy(counts: List[int]) -> float:
    total = sum(counts) or 1
    ps = [c/total for c in counts if c>0]
    return -sum(p*math.log2(p) for p in ps) if ps else 0.0

def _ngrams(tokens: List[str], n: int = 2, limit: int = 16) -> List[str]:
    grams = []
    for i in range(len(tokens)-n+1):
        grams.append(" ".join(tokens[i:i+n]))
        if len(grams) >= limit: break
    return grams

def compute_signature(table: Dict[str, Any]) -> Signature:
    headers: List[str] = table.get("headers", []) or []
    data: List[List[str]] = table.get("data", []) or []
    cols = max((len(r) for r in data), default=len(headers))
    rows = len(data)

    # column fractions
    num_frac, alpha_frac = [], []
    for j in range(cols):
        col = [ (r[j] if j < len(r) else "") for r in data ]
        non_empty = [c for c in col if (c or "").strip()]
        if not non_empty:
            num_frac.append(0.0); alpha_frac.append(0.0); continue
        nums = sum(1 for c in non_empty if _num_pat.match(c.strip()))
        alps = sum(1 for c in non_empty if any(t.isalpha() for t in c))
        total = len(non_empty)
        num_frac.append(nums/total)
        alpha_frac.append(alps/total)

    # cue counts
    h_text = " ".join(h.lower() for h in headers)
    contact_terms = ("email","website","tel","phone","contact","brlm","registrar","anchor investor")
    period_terms = ("fy","fiscal","year","as on","ended","mar","jun","sep","dec","20","19")
    cc = sum(h_text.count(t) for t in contact_terms)
    pc = sum(h_text.count(t) for t in period_terms)

    # row-label density (left-most stringy stubs)
    left = [ (r[0] if r else "") for r in data ]
    lbl = sum(1 for v in left if isinstance(v,str) and len(v.strip())>0 and not _num_pat.match(v.strip()))
    row_label_density = _frac(lbl, rows)

    # linebreak entropy (does this look like prose?)
    lb_counts = [ (r[j].count("\n") if j < len(r) and isinstance(r[j],str) else 0) for r in data for j in range(min(3, cols)) ]
    linebreak_entropy = _entropy([c for c in lb_counts if c>=0])

    # proto-grid score: balance of numeric/textive columns
    strong = sum(1 for f in num_frac if f>0.7) + sum(1 for f in alpha_frac if f>0.7)
    proto_grid = strong / (cols or 1)

    # header ngrams
    toks = re.findall(r"[a-zA-Z]{2,}", h_text)
    hgrams = _ngrams(toks, 2, limit=16)

    # tiny minhash-ish sketch: take first 16 token hashes
    mh = []
    for t in toks[:32]:
        mh.append(int(hashlib.md5(t.encode("utf-8")).hexdigest()[:8], 16))

    return Signature(
        page_no=table.get("page_number", -1),
        table_idx=table.get("table_index", -1),
        cols=cols, rows=rows,
        num_frac_per_col=num_frac,
        alpha_frac_per_col=alpha_frac,
        header_ngrams=hgrams,
        contact_cues=cc,
        period_cues=pc,
        row_label_density=row_label_density,
        linebreak_entropy=linebreak_entropy,
        proto_grid_score=proto_grid,
        minhash=mh,
        source_extractor=str(table.get("source_extractor","")),
        ocr_flag=bool(table.get("ocr_flag", False))
    )

def family_hints(sig: Signature) -> List[str]:
    hints = []
    if sig.contact_cues >= 1 and sig.proto_grid_score < 0.6:
        hints.append("contact_slab")
    if sig.period_cues >= 2 and sig.cols >= 3:
        hints.append("period_grid")
    if sig.row_label_density > 0.5 and (sig.num_frac_per_col[:1] or [0])[0] < 0.4:
        hints.append("ledger_stub")
    if sig.rows <= 3 and sig.cols <= 3:
        hints.append("micro_tables")
    return hints or ["generic"]

def jaccard_overlap(a: List[int], b: List[int]) -> float:
    if not a or not b: return 0.0
    A,B=set(a),set(b)
    inter=len(A&B); union=len(A|B) or 1
    return inter/union

def match_patterns(sig: Signature, patterns: List[Dict[str, Any]], topk: int = 3) -> List[Tuple[str,float]]:
    scored = []
    for p in patterns:
        fam = p.get("family","generic")
        sketch = p.get("signature_sketch",{})
        mh = sketch.get("minhash",[])
        score = 0.5 * jaccard_overlap(sig.minhash, mh)
        score += 0.25 * min(1.0, abs(sig.cols - sketch.get("cols", sig.cols)) / max(1, sig.cols))
        score += 0.25 * (1.0 if fam in family_hints(sig) else 0.0)
        scored.append((fam, round(score,3)))
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:topk]
