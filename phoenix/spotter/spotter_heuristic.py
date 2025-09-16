#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phoenix Heuristic Spotter (stub/MVP)
- For PDFs with word coordinates, groups lines by Y, merges into regions by X-span IoU,
  then emits candidate boxes. Gate candidates with tabularity_score before passing to Surgeon.
"""
import argparse, json, re
from pathlib import Path
from typing import List, Dict, Any

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

def propose_regions(words: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not words: return []
    words = sorted(words, key=lambda w: (w["y"], w["x"]))
    lines, cur = [], []
    for w in words:
        if not cur: cur=[w]; continue
        if abs(w["y"] - cur[-1]["y"]) <= max(2, 0.25*cur[-1]["h"]):
            cur.append(w)
        else:
            lines.append(cur); cur=[w]
    if cur: lines.append(cur)

    def span(line): return (min(t["x"] for t in line), max(t["x"]+t["w"] for t in line))
    regions, bucket, last_span = [], [], None
    for ln in lines:
        sp = span(ln)
        if not bucket: bucket=[ln]; last_span=sp; continue
        overlap = min(last_span[1], sp[1]) - max(last_span[0], sp[0])
        width = max(last_span[1]-last_span[0], sp[1]-sp[0])
        iou = overlap/width if width>0 else 0
        if iou > 0.6:
            bucket.append(ln)
            last_span=(max(last_span[0], sp[0]), min(last_span[1], sp[1]))
        else:
            regions.append(bucket); bucket=[ln]; last_span=sp
    if bucket: regions.append(bucket)

    boxes=[]
    for reg in regions:
        xs=[t["x"] for ln in reg for t in ln]
        ys=[t["y"] for ln in reg for t in ln]
        x2s=[t["x"]+t["w"] for ln in reg for t in ln]
        y2s=[t["y"]+t["h"] for ln in reg for t in ln]
        boxes.append({"x1": min(xs), "y1": min(ys), "x2": max(x2s), "y2": max(y2s), "score": 0.5})
    return boxes

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--words_json", help="Path to JSON: list of word dicts with x,y,w,h,text for one page")
    ap.add_argument("--debug_out", help="Optional JSON to write proposed boxes")
    args = ap.parse_args()

    if not args.words_json:
        print(json.dumps({"regions": [], "note":"no coords"})); return
    words = json.loads(Path(args.words_json).read_text(encoding="utf-8"))
    boxes = propose_regions(words)
    out = {"regions": boxes, "count": len(boxes)}
    if args.debug_out:
        Path(args.debug_out).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False))

if __name__ == "__main__":
    main()
