#!/usr/bin/env python3
import json, re
from pathlib import Path
import pandas as pd

EMAIL_RE  = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
URL_RE    = re.compile(r"(?:(?:https?://)?(?:www\.)?[A-Za-z0-9.\-]+\.[A-Za-z]{2,}(?:/[^\s]*)?)", re.I)
PHONE_RE  = re.compile(r"(?:\+?\d[\d\s\-]{7,}\d)")
KV_RE     = re.compile(r"^\s*([A-Za-z][A-Za-z\s./()\-]{1,40}):\s*(.+)$")

def norm_email(s): return s.strip().lower()
def norm_url(s):
    s = s.strip()
    return s if s.startswith(("http://","https://")) else f"https://{s}" if s else s
def norm_phone(s): return re.sub(r"[^\d+]", "", s).strip()

def load_aliases():
    p = Path("phoenix/memory/recipes/kv_aliases.json")
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return {}

ALIASES = load_aliases()
def alias_key(k: str) -> str:
    lk = k.strip().lower()
    for canon, alist in ALIASES.items():
        if lk == canon.lower() or lk in [a.lower() for a in alist]:
            return canon
    return lk

def harvest_row(row_text: str):
    row_text = row_text.strip()
    m = KV_RE.match(row_text)
    if not m:
        return []
    key = alias_key(m.group(1))
    val = m.group(2).strip()
    out = []
    # Split obvious multi-values by separators
    parts = re.split(r"[;,|]", val) if any(sep in val for sep in ";,|") else [val]
    for p in parts:
        p = p.strip()
        emails = EMAIL_RE.findall(p)
        phones = PHONE_RE.findall(p)
        urls   = URL_RE.findall(p)
        if not any([emails, phones, urls]):
            out.append((key, p, None, None, None))
        else:
            if emails:
                for e in emails:
                    out.append((key, p, norm_email(e), None, None))
            if phones:
                for ph in phones:
                    out.append((key, p, None, norm_phone(ph), None))
            if urls:
                for u in urls:
                    out.append((key, p, None, None, u))
    return out

def main():
    rows = []
    for pre in Path("out/preproc").glob("*.preproc_v6_2.json"):
        j = json.loads(pre.read_text(encoding="utf-8"))
        source = j.get("filename") or j.get("source") or pre.name
        for t in j.get("processed", []):
            if t.get("table_type") != "FRONT_PAGE":
                continue
            page = t.get("page_number")
            idx  = t.get("table_index")
            headers = t.get("headers") or []
            data = t.get("data") or []
            # 2 modes: single text column with KV lines; or multi-col with KV-like pairs
            if len(headers) == 1:
                for r in data:
                    txt = " ".join(x for x in r if x).strip()
                    for key, raw, em, ph, url in harvest_row(txt):
                        rows.append({
                            "source_doc": source, "page": page, "table_index": idx,
                            "org": None, "role": key if "manager" in key.lower() or "registrar" in key.lower() else None,
                            "person": None, "email": em, "phone": ph, "url": norm_url(url) if url else None,
                            "address_raw": raw if "address" in key.lower() else None,
                            "notes": raw if "address" not in key.lower() else None
                        })
            else:
                # try basic KV in first column, value in second+
                for r in data:
                    key = alias_key((r[0] or "").strip())
                    val = " ".join([c for c in r[1:] if c]).strip()
                    if not key or not val:
                        continue
                    for key2, raw, em, ph, url in harvest_row(f"{key}: {val}"):
                        rows.append({
                            "source_doc": source, "page": page, "table_index": idx,
                            "org": None, "role": key2 if "manager" in key2.lower() or "registrar" in key2.lower() else None,
                            "person": None, "email": em, "phone": ph, "url": norm_url(url) if url else None,
                            "address_raw": raw if "address" in key2.lower() else None,
                            "notes": raw if "address" not in key2.lower() else None
                        })
    df = pd.DataFrame(rows)
    outdir = Path("out/canon"); outdir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(outdir / "contacts.parquet", index=False)
    print(f"Wrote {len(df)} rows -> {outdir/'contacts.parquet'}")

if __name__ == "__main__":
    main()
