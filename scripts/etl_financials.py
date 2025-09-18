# # #!/usr/bin/env python3
# # # -*- coding: utf-8 -*-
# # """
# # ETL: Convert preproc FINANCIAL_STATEMENT tables to canonical long-format parquet.

# # Input:  out/preproc/*.preproc_v6_2.json
# # Output: out/canon/financials.parquet  (schema: schemas/financials.schema.json)

# # Rules:
# # - First column = line_item; remaining columns map to headers as period_label(s).
# # - Units: detect from headers/title (₹ in crore/lakh/million/billion); scale values to INR.
# # - Statement type: infer from table_title/headers ('balance sheet'->BS, 'profit'/'loss'->PL, 'cash flow'/'cash flows'->CF).
# # - Consolidated flag: true if 'consolidated' in title/headers; else false if 'standalone' in title/headers; else false.
# # """

# # import json, re, sys
# # from pathlib import Path
# # from typing import Dict, List, Optional, Tuple

# # import pandas as pd

# # PREPROC_DIR = Path("out/preproc")
# # OUT_DIR = Path("out/canon")
# # OUT_DIR.mkdir(parents=True, exist_ok=True)
# # OUT_PATH = OUT_DIR / "financials.parquet"

# # # --- Helpers -----------------------------------------------------------------

# # UNIT_PATTERNS = [
# #     (re.compile(r"(₹|rs\.?|inr).{0,12}in\s*crore", re.I), 1e7, "₹ in crore"),
# #     (re.compile(r"(₹|rs\.?|inr).{0,12}crore", re.I),       1e7, "₹ crore"),
# #     (re.compile(r"(₹|rs\.?|inr).{0,12}in\s*lakh", re.I),   1e5, "₹ in lakh"),
# #     (re.compile(r"(₹|rs\.?|inr).{0,12}lakh", re.I),        1e5, "₹ lakh"),
# #     (re.compile(r"(₹|rs\.?|inr).{0,12}(mn|million)", re.I),1e6, "₹ million"),
# #     (re.compile(r"(₹|rs\.?|inr).{0,12}(bn|billion)", re.I),1e9, "₹ billion"),
# # ]

# # STMT_KEYWORDS = {
# #     "BS": [r"balance\s+sheet", r"financial\s+position"],
# #     "PL": [r"profit\s*&?\s*loss", r"statement\s+of\s+operations", r"p&l"],
# #     "CF": [r"cash\s*flow", r"cash\s*flows"]
# # }

# # def detect_units(text: str) -> Tuple[float, str]:
# #     for pat, scale, label in UNIT_PATTERNS:
# #         if pat.search(text):
# #             return scale, label
# #     return 1.0, "unknown"

# # def infer_stmt_type(text: str) -> Optional[str]:
# #     t = text.lower()
# #     for code, pats in STMT_KEYWORDS.items():
# #         for p in pats:
# #             if re.search(p, t):
# #                 return code
# #     # weak heuristics by line items
# #     if re.search(r"\bassets?\b|\bliabilit", t): return "BS"
# #     if re.search(r"\brevenue|income|expenses?|ebit|profit|loss\b", t): return "PL"
# #     if re.search(r"\bcash\b.*\bflow", t): return "CF"
# #     return None

# # def to_float(cell: str) -> Optional[float]:
# #     if cell is None:
# #         return None
# #     s = str(cell).strip()
# #     if s in ("", "-", "—", "NA", "N/A", "nil", "Nil"):
# #         return None
# #     # remove footnote markers like (1), *, †
# #     s = re.sub(r"\*+|†+|\(\d+\)$", "", s).strip()

# #     # handle parentheses negatives
# #     neg = False
# #     if s.startswith("(") and s.endswith(")"):
# #         neg = True
# #         s = s[1:-1].strip()

# #     # percent? treat as null for financials
# #     if "%" in s:
# #         return None

# #     # remove thousand separators and stray symbols
# #     s = s.replace(",", "")
# #     s = re.sub(r"[^\d.\-]", "", s)

# #     if s in ("", "-", "."):
# #         return None
# #     try:
# #         v = float(s)
# #         return -v if neg else v
# #     except ValueError:
# #         return None

# # def is_consolidated(text: str) -> Optional[bool]:
# #     t = text.lower()
# #     if "consolidated" in t: return True
# #     if "standalone" in t or "stand alone" in t: return False
# #     return None

# # def row_iter(preproc_obj: Dict, dossier_source: str):
# #     processed = preproc_obj.get("processed", [])
# #     for t in processed:
# #         if t.get("table_type") != "FINANCIAL_STATEMENT":
# #             continue
# #         headers: List[str] = t.get("headers") or []
# #         data: List[List[str]] = t.get("data") or []
# #         page = t.get("page_number")
# #         idx  = t.get("table_index")
# #         title = t.get("table_title") or ""

# #         # build a context blob for unit/type detection
# #         head_blob = " ".join(h for h in headers if h)
# #         ctx = f"{title} || {head_blob}"

# #         scale, unit_src = detect_units(ctx)
# #         stmt = infer_stmt_type(ctx) or "PL"  # safe default if unknown
# #         cons = is_consolidated(ctx)
# #         if cons is None: cons = False

# #         if not headers or len(headers) < 2:
# #             # Not enough columns to pivot (must have line_item + ≥1 period column)
# #             continue

# #         period_labels = headers[1:]
# #         for r in data:
# #             if not r: 
# #                 continue
# #             line_item = str(r[0]).strip()
# #             if not line_item:
# #                 continue
# #             # iterate over period columns
# #             for j, per in enumerate(period_labels, start=1):
# #                 per_lbl = str(per or "").strip()
# #                 if not per_lbl:
# #                     continue
# #                 val = to_float(r[j]) if j < len(r) else None
# #                 if val is None:
# #                     yield {
# #                         "dossier_source": dossier_source,
# #                         "page_number": page,
# #                         "table_index": idx,
# #                         "statement_type": stmt,
# #                         "line_item": line_item,
# #                         "period_label": per_lbl,
# #                         "value": None,
# #                         "unit_src": unit_src,
# #                         "currency": "INR",
# #                         "consolidated": cons,
# #                         "notes": None
# #                     }
# #                 else:
# #                     yield {
# #                         "dossier_source": dossier_source,
# #                         "page_number": page,
# #                         "table_index": idx,
# #                         "statement_type": stmt,
# #                         "line_item": line_item,
# #                         "period_label": per_lbl,
# #                         "value": val * scale,
# #                         "unit_src": unit_src,
# #                         "currency": "INR",
# #                         "consolidated": cons,
# #                         "notes": None
# #                     }

# # def main():
# #     inputs = sorted(PREPROC_DIR.glob("*.preproc_v6_2.json"))
# #     rows = []
# #     for p in inputs:
# #         with p.open("r", encoding="utf-8") as f:
# #             obj = json.load(f)
# #         dossier = obj.get("filename") or obj.get("source") or p.stem
# #         rows.extend(row_iter(obj, dossier))

# #     if not rows:
# #         print("No financial rows found; nothing to write.")
# #         return

# #     df = pd.DataFrame(rows, columns=[
# #         "dossier_source","page_number","table_index","statement_type",
# #         "line_item","period_label","value","unit_src","currency",
# #         "consolidated","notes"
# #     ])

# #     # Basic cleanup: drop all-null value rows if no numeric content at all
# #     # (we still keep nulls to preserve structure, but if *everything* is null, drop)
# #     if df["value"].notna().sum() == 0:
# #         print("All financial values are null; writing an empty file to avoid downstream errors.")
# #         df = df.iloc[0:0]

# #     OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
# #     df.to_parquet(OUT_PATH, index=False)
# #     print(f"Wrote {len(df):,} rows -> {OUT_PATH}")

# # if __name__ == "__main__":
# #     sys.exit(main())
























# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# ETL: Convert preproc FINANCIAL_STATEMENT tables to canonical long-format parquet.

# Input:  out/preproc/*.preproc_v6_2.json
# Output: out/canon/financials.parquet  (schema: schemas/financials.schema.json)

# Upgrades vs v1:
# - Period-aware column selection (filters out Notes/Particulars/meta columns).
# - Stricter numeric heuristics per column.
# - Drops rows with no numeric across selected period columns.
# - Broader unit detection (₹ in (crore|lakh|million|billion), with parentheses/variants).

# Schema: schemas/financials.schema.json
# """

# import json, re, sys
# from pathlib import Path
# from typing import Dict, List, Optional, Tuple

# import pandas as pd

# PREPROC_DIR = Path("out/preproc")
# OUT_DIR = Path("out/canon")
# OUT_DIR.mkdir(parents=True, exist_ok=True)
# OUT_PATH = OUT_DIR / "financials.parquet"

# # ----------------- Patterns -----------------

# UNIT_PATTERNS = [
#     # variants incl. parentheses and spacing
#     (re.compile(r"(₹|rs\.?|inr)[^a-zA-Z0-9]{0,10}\(?\s*in\s*crore\s*\)?", re.I), 1e7, "₹ in crore"),
#     (re.compile(r"(₹|rs\.?|inr)[^a-zA-Z0-9]{0,10}crore", re.I),                  1e7, "₹ crore"),
#     (re.compile(r"(₹|rs\.?|inr)[^a-zA-Z0-9]{0,10}\(?\s*in\s*lakh\s*\)?", re.I), 1e5, "₹ in lakh"),
#     (re.compile(r"(₹|rs\.?|inr)[^a-zA-Z0-9]{0,10}lakh", re.I),                   1e5, "₹ lakh"),
#     (re.compile(r"(₹|rs\.?|inr)[^a-zA-Z0-9]{0,10}(mn|million)", re.I),           1e6, "₹ million"),
#     (re.compile(r"(₹|rs\.?|inr)[^a-zA-Z0-9]{0,10}(bn|billion)", re.I),           1e9, "₹ billion"),
# ]

# # common period label cues
# PERIOD_PATTERNS = [
#     re.compile(r"\bfy\s*\d{2}\s*[-/]\s*\d{2}\b", re.I),     # FY 22-23
#     re.compile(r"\bfy\s*20\d{2}\b", re.I),                  # FY 2023
#     re.compile(r"\b(20|19)\d{2}\b"),                        # standalone year
#     re.compile(r"\bq[1-4]\b", re.I),                        # Q1..Q4
#     re.compile(r"\b(quarter|half[-\s]?year|h[12])\b", re.I),
#     re.compile(r"\b(as at|as on|as of|for the|ended|ending)\b", re.I),
#     re.compile(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", re.I)
# ]

# STMT_KEYWORDS = {
#     "BS": [r"balance\s+sheet", r"financial\s+position"],
#     "PL": [r"profit\s*&?\s*loss", r"statement\s+of\s+operations", r"\bp&l\b"],
#     "CF": [r"cash\s*flow", r"cash\s*flows"]
# }

# # ----------------- Helpers -----------------

# def detect_units(text: str) -> Tuple[float, str]:
#     for pat, scale, label in UNIT_PATTERNS:
#         if pat.search(text):
#             return scale, label
#     return 1.0, "unknown"

# def infer_stmt_type(text: str) -> Optional[str]:
#     t = (text or "").lower()
#     for code, pats in STMT_KEYWORDS.items():
#         for p in pats:
#             if re.search(p, t):
#                 return code
#     if re.search(r"\bassets?\b|\bliabilit", t): return "BS"
#     if re.search(r"\brevenue|income|expenses?|ebit|profit|loss\b", t): return "PL"
#     if re.search(r"\bcash\b.*\bflow", t): return "CF"
#     return None

# def to_float(cell: str) -> Optional[float]:
#     if cell is None:
#         return None
#     s = str(cell).strip()
#     if s in ("", "-", "–", "—", "NA", "N/A", "Nil", "nil"):
#         return None
#     # remove footnote markers like *, †, and trailing (1)
#     s = re.sub(r"[*†]+|\(\d+\)$", "", s).strip()

#     # parentheses negatives
#     neg = False
#     if s.startswith("(") and s.endswith(")"):
#         neg = True
#         s = s[1:-1].strip()

#     # percent? ignore for this ETL
#     if "%" in s:
#         return None

#     s = s.replace(",", "")
#     s = re.sub(r"[^\d.\-]", "", s)
#     if s in ("", "-", "."):
#         return None
#     try:
#         v = float(s)
#         return -v if neg else v
#     except ValueError:
#         return None

# def score_period_label(h: str) -> int:
#     if not h:
#         return 0
#     s = str(h).lower()
#     return sum(bool(p.search(s)) for p in PERIOD_PATTERNS)

# def select_period_columns(headers: List[str], data: List[List[str]]) -> List[int]:
#     """
#     Decide which columns (index >=1) are 'period' columns.
#     Blend header cues + per-column numeric fraction.
#     """
#     if not headers or not data:
#         return []

#     # number of columns present in data
#     ncols = max(len(r) for r in data)
#     # column numeric fraction (excluding leftmost)
#     numeric_fracs = []
#     for j in range(1, ncols):
#         vals = [to_float(r[j]) for r in data if j < len(r)]
#         total = len(vals)
#         frac = (sum(v is not None for v in vals) / total) if total else 0.0
#         numeric_fracs.append((j, frac))

#     # header scores
#     header_scores = []
#     for j in range(1, min(ncols, len(headers))):
#         header_scores.append((j, score_period_label(headers[j])))

#     # combine: keep if header indicates period OR numeric frac is decent
#     candidates = set()
#     for j, sc in header_scores:
#         if sc >= 1:
#             candidates.add(j)
#     for j, frac in numeric_fracs:
#         if frac >= 0.25:
#             candidates.add(j)

#     # fallback: pick top 3 numeric columns if nothing matched
#     if not candidates:
#         top = sorted(numeric_fracs, key=lambda x: x[1], reverse=True)[:3]
#         candidates = {j for j, frac in top if frac >= 0.10}

#     return sorted(candidates)

# def is_consolidated(text: str) -> Optional[bool]:
#     t = (text or "").lower()
#     if "consolidated" in t: return True
#     if "standalone" in t or "stand alone" in t: return False
#     return None

# def iter_financial_rows(preproc_obj: Dict, dossier_source: str):
#     processed = preproc_obj.get("processed", [])
#     for t in processed:
#         if t.get("table_type") != "FINANCIAL_STATEMENT":
#             continue
#         headers: List[str] = t.get("headers") or []
#         data: List[List[str]] = t.get("data") or []
#         page = t.get("page_number")
#         idx  = t.get("table_index")
#         title = t.get("table_title") or ""

#         # context for units/type detection
#         head_blob = " ".join(h for h in headers if h)
#         ctx = f"{title} || {head_blob}"

#         scale, unit_src = detect_units(ctx)
#         stmt = infer_stmt_type(ctx) or "PL"
#         cons = is_consolidated(ctx)
#         if cons is None: cons = False

#         # determine period columns
#         period_cols = select_period_columns(headers, data)
#         if len(period_cols) == 0:
#             # nothing useful to emit from this table
#             continue

#         # map labels for chosen columns
#         def plabel(j: int) -> str:
#             if j < len(headers) and headers[j]:
#                 return str(headers[j]).strip()
#             return f"Period_{j}"

#         # emit only rows with at least one numeric among selected period columns
#         for r in data:
#             if not r:
#                 continue
#             line_item = str(r[0]).strip() if len(r) else ""
#             if not line_item:
#                 continue

#             any_numeric = False
#             parsed_vals = []
#             for j in period_cols:
#                 v = to_float(r[j]) if j < len(r) else None
#                 parsed_vals.append((j, v))
#                 if v is not None:
#                     any_numeric = True

#             if not any_numeric:
#                 continue  # drop all-null financial rows

#             for j, v in parsed_vals:
#                 yield {
#                     "dossier_source": dossier_source,
#                     "page_number": page,
#                     "table_index": idx,
#                     "statement_type": stmt,
#                     "line_item": line_item,
#                     "period_label": plabel(j),
#                     "value": (v * scale) if v is not None else None,
#                     "unit_src": unit_src,
#                     "currency": "INR",
#                     "consolidated": cons,
#                     "notes": None
#                 }

# def main():
#     inputs = sorted(PREPROC_DIR.glob("*.preproc_v6_2.json"))
#     rows = []
#     for p in inputs:
#         with p.open("r", encoding="utf-8") as f:
#             obj = json.load(f)
#         dossier = obj.get("filename") or obj.get("source") or p.stem
#         rows.extend(iter_financial_rows(obj, dossier))

#     if not rows:
#         print("No financial rows found; nothing to write.")
#         return

#     df = pd.DataFrame(rows, columns=[
#         "dossier_source","page_number","table_index","statement_type",
#         "line_item","period_label","value","unit_src","currency",
#         "consolidated","notes"
#     ])

#     OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
#     df.to_parquet(OUT_PATH, index=False)
#     print(f"Wrote {len(df):,} rows -> {OUT_PATH}")

# if __name__ == "__main__":
#     sys.exit(main())

































# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# ETL: Convert preproc FINANCIAL_STATEMENT tables to canonical long-format parquet.

# Input:  out/preproc/*.preproc_v6_2.json
# Output: out/canon/financials.parquet

# Upgrades:
# - Uses expanded FS lexicon (BS/PL/CF).
# - Period allowlist + meta denylist (from LLM).
# - Slightly more permissive numeric threshold to improve coverage.
# - Still drops rows with no numeric in selected period columns.
# """

# import json, re, sys
# from pathlib import Path
# from typing import Dict, List, Optional, Tuple
# import pandas as pd

# PREPROC_DIR = Path("out/preproc")
# OUT_DIR = Path("out/canon")
# OUT_DIR.mkdir(parents=True, exist_ok=True)
# OUT_PATH = OUT_DIR / "financials.parquet"

# # ----------------- Expanded FS lexicon (typing) -----------------
# STMT_KEYWORDS = {
#     "BS": [
#         "Balance Sheet",
#         "Consolidated Balance Sheet",
#         "Statement of Assets and Liabilities",
#         "Consolidated statement of assets and liabilities",
#         "Restated Consolidated statement of assets and liabilities",
#         "Statement of Financial Position",
#         "Consolidated Statement of Financial Position",
#         "EQUITY AND LIABILITIES",
#         "ASSETS"
#     ],
#     "PL": [
#         "Statement of Profit and Loss",
#         "Consolidated Statement of Profit and Loss",
#         "Restated Consolidated Statement of Profit and Loss",
#         "Profit and Loss Account",
#         "Statement of profit or loss",
#         "Income Statement",
#         "Statement of Operations",
#         "Consolidated Statement of Operations",
#         "Statement of Profit and Loss for the year ended",
#         "Statement of Profit and Loss for the period ended",
#         "Statement of Profit and Loss for the quarter ended",
#         "Statement of Profit and Loss for the half year ended"
#     ],
#     "CF": [
#         "Statement of Cash Flows",
#         "Consolidated Statement of Cash Flows",
#         "Restated Consolidated statement of cash flows",
#         "Cash Flow Statement",
#         "Consolidated Cash Flow Statement",
#         "Cash flow from operating activities",
#         "Cash flow from investing activities",
#         "Cash flow from financing activities"
#     ]
# }

# # compile for fast matching
# STMT_RE = {k: [re.compile(re.escape(x), re.I) for x in v] for k, v in STMT_KEYWORDS.items()}

# # ----------------- Units -----------------
# UNIT_PATTERNS = [
#     (re.compile(r"(₹|rs\.?|inr)[^a-zA-Z0-9]{0,10}\(?\s*in\s*crore\s*\)?", re.I), 1e7, "₹ in crore"),
#     (re.compile(r"(₹|rs\.?|inr)[^a-zA-Z0-9]{0,10}crore", re.I),                  1e7, "₹ crore"),
#     (re.compile(r"(₹|rs\.?|inr)[^a-zA-Z0-9]{0,10}\(?\s*in\s*lakh\s*\)?", re.I), 1e5, "₹ in lakh"),
#     (re.compile(r"(₹|rs\.?|inr)[^a-zA-Z0-9]{0,10}lakh", re.I),                   1e5, "₹ lakh"),
#     (re.compile(r"(₹|rs\.?|inr)[^a-zA-Z0-9]{0,10}(mn|million)", re.I),           1e6, "₹ million"),
#     (re.compile(r"(₹|rs\.?|inr)[^a-zA-Z0-9]{0,10}(bn|billion)", re.I),           1e9, "₹ billion"),
# ]

# # ----------------- Period vs Meta (from LLM) -----------------
# PERIOD_ALLOWLIST_STRINGS = [
#     "As at 31 March 2021",
#     "As at 31 March 2020",
#     "As at 31 March 2019",
#     "Year ended 31 March 2021",
#     "Year ended 31 March 2020",
#     "Year ended 31 March 2019",
#     "For the Financial Year ended March 31",
#     "Financial year"
# ]
# PERIOD_ALLOWLIST = [re.compile(re.escape(s), re.I) for s in PERIOD_ALLOWLIST_STRINGS]

# META_DENYLIST_STRINGS = [
#     "Particulars", "Note", "Amount", "Description"
# ]
# META_DENYLIST = [re.compile(re.escape(s), re.I) for s in META_DENYLIST_STRINGS]
# # Also treat explicit unit headers as meta
# UNIT_HEADER_RE = re.compile(r"\(\s*rs\.?|₹|inr|million|mn|billion|bn|crore|lakh", re.I)

# # Generic period cues (kept from v1, still useful)
# PERIOD_PATTERNS = [
#     re.compile(r"\bfy\s*\d{2}\s*[-/]\s*\d{2}\b", re.I),     # FY 22-23
#     re.compile(r"\bfy\s*20\d{2}\b", re.I),                  # FY 2023
#     re.compile(r"\b(20|19)\d{2}\b"),                        # standalone year
#     re.compile(r"\bq[1-4]\b", re.I),                        # Q1..Q4
#     re.compile(r"\b(quarter|half[-\s]?year|h[12])\b", re.I),
#     re.compile(r"\b(as at|as on|as of|for the|ended|ending)\b", re.I),
#     re.compile(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", re.I)
# ]

# def detect_units(text: str) -> Tuple[float, str]:
#     for pat, scale, label in UNIT_PATTERNS:
#         if pat.search(text or ""):
#             return scale, label
#     return 1.0, "unknown"

# def infer_stmt_type(text: str) -> Optional[str]:
#     t = (text or "")
#     for code, regs in STMT_RE.items():
#         if any(r.search(t) for r in regs):
#             return code
#     tl = t.lower()
#     if "consolidated" in tl or "financial position" in tl or "assets" in tl or "liabilit" in tl:
#         return "BS"
#     if "cash" in tl and "flow" in tl:
#         return "CF"
#     if "profit" in tl or "loss" in tl or "income" in tl or "operations" in tl:
#         return "PL"
#     return None

# def is_consolidated(text: str) -> Optional[bool]:
#     t = (text or "").lower()
#     if "consolidated" in t: return True
#     if "standalone" in t or "stand alone" in t: return False
#     return None

# def to_float(cell: str) -> Optional[float]:
#     if cell is None:
#         return None
#     s = str(cell).strip()
#     if s in ("", "-", "–", "—", "NA", "N/A", "Nil", "nil"):
#         return None
#     s = re.sub(r"[*†]+|\(\d+\)$", "", s).strip()
#     neg = False
#     if s.startswith("(") and s.endswith(")"):
#         neg = True
#         s = s[1:-1].strip()
#     if "%" in s:
#         return None
#     s = s.replace(",", "")
#     s = re.sub(r"[^\d.\-]", "", s)
#     if s in ("", "-", "."):
#         return None
#     try:
#         v = float(s)
#         return -v if neg else v
#     except ValueError:
#         return None

# def header_is_meta(h: str) -> bool:
#     if not h: return True
#     for r in META_DENYLIST:
#         if r.search(h): return True
#     if UNIT_HEADER_RE.search(h or ""): return True
#     return False

# def score_period_label(h: str) -> int:
#     if not h: return 0
#     s = str(h)
#     score = 0
#     for r in PERIOD_ALLOWLIST:
#         if r.search(s): score += 2
#     for p in PERIOD_PATTERNS:
#         if p.search(s): score += 1
#     return score

# def select_period_columns(headers: List[str], data: List[List[str]]) -> List[int]:
#     if not headers or not data:
#         return []
#     ncols = max(len(r) for r in data)
#     # numeric fraction per col (exclude leftmost)
#     numeric_fracs = []
#     for j in range(1, ncols):
#         vals = [to_float(r[j]) for r in data if j < len(r)]
#         total = len(vals)
#         frac = (sum(v is not None for v in vals) / total) if total else 0.0
#         numeric_fracs.append((j, frac))
#     # header scores + meta filter
#     header_scores = []
#     for j in range(1, min(ncols, len(headers))):
#         h = headers[j]
#         if header_is_meta(h):
#             continue
#         header_scores.append((j, score_period_label(h)))

#     candidates = set()
#     for j, sc in header_scores:
#         if sc >= 1:
#             candidates.add(j)
#     # slightly more permissive numeric cutoff to boost coverage
#     for j, frac in numeric_fracs:
#         if frac >= 0.15:
#             candidates.add(j)

#     if not candidates:
#         # fallback: top 4 numeric-ish columns
#         top = sorted(numeric_fracs, key=lambda x: x[1], reverse=True)[:4]
#         candidates = {j for j, frac in top if frac >= 0.10}

#     return sorted(candidates)

# def iter_financial_rows(preproc_obj: Dict, dossier_source: str):
#     processed = preproc_obj.get("processed", [])
#     for t in processed:
#         if t.get("table_type") != "FINANCIAL_STATEMENT":
#             continue
#         headers: List[str] = t.get("headers") or []
#         data: List[List[str]] = t.get("data") or []
#         page = t.get("page_number")
#         idx  = t.get("table_index")
#         title = t.get("table_title") or ""
#         head_blob = " ".join(h for h in headers if h)

#         scale, unit_src = detect_units(f"{title} || {head_blob}")
#         stmt = infer_stmt_type(f"{title} || {head_blob}") or "PL"
#         cons = is_consolidated(f"{title} || {head_blob}")
#         if cons is None: cons = False

#         period_cols = select_period_columns(headers, data)
#         if len(period_cols) == 0:
#             continue

#         def plabel(j: int) -> str:
#             if j < len(headers) and headers[j]:
#                 return str(headers[j]).strip()
#             return f"Period_{j}"

#         for r in data:
#             if not r:
#                 continue
#             line_item = str(r[0]).strip() if len(r) else ""
#             if not line_item:
#                 continue

#             any_numeric = False
#             parsed_vals = []
#             for j in period_cols:
#                 v = to_float(r[j]) if j < len(r) else None
#                 parsed_vals.append((j, v))
#                 if v is not None:
#                     any_numeric = True
#             if not any_numeric:
#                 continue

#             for j, v in parsed_vals:
#                 yield {
#                     "dossier_source": dossier_source,
#                     "page_number": page,
#                     "table_index": idx,
#                     "statement_type": stmt,
#                     "line_item": line_item,
#                     "period_label": plabel(j),
#                     "value": (v * scale) if v is not None else None,
#                     "unit_src": unit_src,
#                     "currency": "INR",
#                     "consolidated": cons,
#                     "notes": None
#                 }

# def main():
#     inputs = sorted(PREPROC_DIR.glob("*.preproc_v6_2.json"))
#     rows = []
#     for p in inputs:
#         with p.open("r", encoding="utf-8") as f:
#             obj = json.load(f)
#         dossier = obj.get("filename") or obj.get("source") or p.stem
#         rows.extend(iter_financial_rows(obj, dossier))

#     if not rows:
#         print("No financial rows found; nothing to write.")
#         return

#     df = pd.DataFrame(rows, columns=[
#         "dossier_source","page_number","table_index","statement_type",
#         "line_item","period_label","value","unit_src","currency",
#         "consolidated","notes"
#     ])

#     OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
#     df.to_parquet(OUT_PATH, index=False)
#     print(f"Wrote {len(df):,} rows -> {OUT_PATH}")

# if __name__ == "__main__":
#     sys.exit(main())




























# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# ETL: Convert preproc FINANCIAL_STATEMENT tables to canonical long-format parquet.

# Input:  out/preproc/*.preproc_v6_2.json
# Output: out/canon/financials.parquet

# Changes vs previous:
# - Expanded FS lexicon and period regex (dates like 31-03-2021, FY2020-21, Q3FY21, H1FY22).
# - Stronger meta/UOM/notes/particulars filtering for headers.
# - Candidate period columns: allow if (header looks like period) OR (numeric_frac >= 0.25); fallback top 6 >= 0.20.
# - Keeps single-period BS rows (quality gate can allow with flag).
# """

# import json, re, sys
# from pathlib import Path
# from typing import Dict, List, Optional, Tuple
# import pandas as pd

# PREPROC_DIR = Path("out/preproc")
# OUT_DIR = Path("out/canon")
# OUT_DIR.mkdir(parents=True, exist_ok=True)
# OUT_PATH = OUT_DIR / "financials.parquet"

# # ----------------- FS lexicon -----------------
# STMT_KEYWORDS = {
#     "BS": [
#         "Balance Sheet",
#         "Consolidated Balance Sheet",
#         "Statement of Assets and Liabilities",
#         "Consolidated statement of assets and liabilities",
#         "Restated Consolidated statement of assets and liabilities",
#         "Statement of Financial Position",
#         "Consolidated Statement of Financial Position",
#         "EQUITY AND LIABILITIES",
#         "ASSETS"
#     ],
#     "PL": [
#         "Statement of Profit and Loss",
#         "Consolidated Statement of Profit and Loss",
#         "Restated Consolidated Statement of Profit and Loss",
#         "Profit and Loss Account",
#         "Statement of profit or loss",
#         "Income Statement",
#         "Statement of Operations",
#         "Consolidated Statement of Operations",
#         "Statement of Profit and Loss for the year ended",
#         "Statement of Profit and Loss for the period ended",
#         "Statement of Profit and Loss for the quarter ended",
#         "Statement of Profit and Loss for the half year ended"
#     ],
#     "CF": [
#         "Statement of Cash Flows",
#         "Consolidated Statement of Cash Flows",
#         "Restated Consolidated statement of cash flows",
#         "Cash Flow Statement",
#         "Consolidated Cash Flow Statement",
#         "Cash flow from operating activities",
#         "Cash flow from investing activities",
#         "Cash flow from financing activities"
#     ]
# }
# STMT_RE = {k: [re.compile(re.escape(x), re.I) for x in v] for k, v in STMT_KEYWORDS.items()}

# # ----------------- Units -----------------
# UNIT_PATTERNS = [
#     (re.compile(r"(₹|rs\.?|inr)[^a-zA-Z0-9]{0,10}\(?\s*in\s*crore\s*\)?", re.I), 1e7, "₹ in crore"),
#     (re.compile(r"(₹|rs\.?|inr)[^a-zA-Z0-9]{0,10}crore", re.I),                  1e7, "₹ crore"),
#     (re.compile(r"(₹|rs\.?|inr)[^a-zA-Z0-9]{0,10}\(?\s*in\s*lakh\s*\)?", re.I), 1e5, "₹ in lakh"),
#     (re.compile(r"(₹|rs\.?|inr)[^a-zA-Z0-9]{0,10}lakh", re.I),                   1e5, "₹ lakh"),
#     (re.compile(r"(₹|rs\.?|inr)[^a-zA-Z0-9]{0,10}(mn|million)", re.I),           1e6, "₹ million"),
#     (re.compile(r"(₹|rs\.?|inr)[^a-zA-Z0-9]{0,10}(bn|billion)", re.I),           1e9, "₹ billion"),
# ]

# # ----------------- Period vs Meta -----------------
# PERIOD_ALLOWLIST_STRINGS = [
#     "As at 31 March 2021","As at 31 March 2020","As at 31 March 2019",
#     "Year ended 31 March 2021","Year ended 31 March 2020","Year ended 31 March 2019",
#     "For the Financial Year ended March 31","Financial year"
# ]
# PERIOD_ALLOWLIST = [re.compile(re.escape(s), re.I) for s in PERIOD_ALLOWLIST_STRINGS]

# META_DENYLIST_STRINGS = [
#     "Particulars","Particulars of","Note","Notes","Schedule","UOM","Unit",
#     "Amount","Description","Regd.","CIN","ISIN","FVTPL*",
#     "Amortised cost"

# ]
# META_DENYLIST = [re.compile(re.escape(s), re.I) for s in META_DENYLIST_STRINGS]
# UNIT_HEADER_RE = re.compile(r"\(\s*rs\.?|₹|inr|million|mn|billion|bn|crore|lakh", re.I)

# # Generic + compact period cues
# PERIOD_PATTERNS = [
#     re.compile(r"\bfy\s*\d{2}\s*[-/]\s*\d{2}\b", re.I),         # FY 22/23
#     re.compile(r"\bfy\s*(?:20)?\d{2}\b", re.I),                 # FY 2023 / FY23
#     re.compile(r"\b(?:FY)?\s?(?:20)?\d{2}\s?[–-]\s?(?:20)?\d{2}\b", re.I),  # FY2020-21 / 2020-21
#     re.compile(r"\bQ[1-4]\s*FY(?:20)?\d{2}\b", re.I),           # Q3FY21
#     re.compile(r"\bH[12]\s*FY(?:20)?\d{2}\b", re.I),            # H1FY22
#     re.compile(r"\b(20|19)\d{2}\b"),                            # bare year
#     re.compile(r"\b(as at|as on|as of|for the|ended|ending)\b", re.I),
#     re.compile(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", re.I),
#     re.compile(r"\b(?:31|30|29|28)[-/\. ]?(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[-/\. ]?(?:20|19)\d{2}\b", re.I),
#     re.compile(r"\b(?:0?[1-9]|1[0-2])[-/\.](?:0?[1-9]|[12]\d|3[01])[-/\.](?:20|19)\d{2}\b"), # MM/DD/YYYY
#     re.compile(r"\b(?:31|30|29|28)[-/\.](?:0?[1-9]|1[0-2])[-/\.](?:20|19)\d{2}\b")           # DD/MM/YYYY
# ]

# def detect_units(text: str) -> Tuple[float, str]:
#     for pat, scale, label in UNIT_PATTERNS:
#         if pat.search(text or ""):
#             return scale, label
#     return 1.0, "unknown"

# def infer_stmt_type(text: str) -> Optional[str]:
#     t = (text or "")
#     for code, regs in STMT_RE.items():
#         if any(r.search(t) for r in regs):
#             return code
#     tl = t.lower()
#     if "consolidated" in tl or "financial position" in tl or "assets" in tl or "liabilit" in tl:
#         return "BS"
#     if "cash" in tl and "flow" in tl:
#         return "CF"
#     if "profit" in tl or "loss" in tl or "income" in tl or "operations" in tl:
#         return "PL"
#     return None

# def is_consolidated(text: str) -> Optional[bool]:
#     t = (text or "").lower()
#     if "consolidated" in t: return True
#     if "standalone" in t or "stand alone" in t: return False
#     return None

# def to_float(cell: str) -> Optional[float]:
#     if cell is None:
#         return None
#     s = str(cell).strip()
#     if s in ("", "-", "–", "—", "NA", "N/A", "Nil", "nil"):
#         return None
#     s = re.sub(r"[*†]+|\(\d+\)$", "", s).strip()
#     neg = False
#     if s.startswith("(") and s.endswith(")"):
#         neg = True
#         s = s[1:-1].strip()
#     if "%" in s:
#         return None
#     s = s.replace(",", "")
#     s = re.sub(r"[^\d.\-]", "", s)
#     if s in ("", "-", "."):
#         return None
#     try:
#         v = float(s)
#         return -v if neg else v
#     except ValueError:
#         return None

# def header_is_meta(h: str) -> bool:
#     if not h: return True
#     for r in META_DENYLIST:
#         if r.search(h): return True
#     if UNIT_HEADER_RE.search(h or ""): return True
#     return False

# def score_period_label(h: str) -> int:
#     if not h: return 0
#     s = str(h)
#     score = 0
#     for r in PERIOD_ALLOWLIST:
#         if r.search(s): score += 2
#     for p in PERIOD_PATTERNS:
#         if p.search(s): score += 1
#     return score

# def select_period_columns(headers: List[str], data: List[List[str]]) -> List[int]:
#     if not headers or not data:
#         return []
#     ncols = max(len(r) for r in data)

#     # numeric fraction per col (exclude leftmost)
#     numeric_fracs = []
#     for j in range(1, ncols):
#         vals = [to_float(r[j]) for r in data if j < len(r)]
#         total = len(vals)
#         frac = (sum(v is not None for v in vals) / total) if total else 0.0
#         numeric_fracs.append((j, frac))

#     # header scores + meta filter
#     header_scores = []
#     for j in range(1, min(ncols, len(headers))):
#         h = headers[j]
#         if header_is_meta(h):
#             continue
#         header_scores.append((j, score_period_label(h)))

#     candidates = set()
#     # prefer true period headers
#     for j, sc in header_scores:
#         if sc >= 1:
#             candidates.add(j)
#     # allow numeric-ish columns (slightly higher than before)
#     for j, frac in numeric_fracs:
#         if frac >= 0.25:
#             candidates.add(j)

#     if not candidates:
#         # fallback: top 6 numeric-ish columns
#         top = sorted(numeric_fracs, key=lambda x: x[1], reverse=True)[:6]
#         candidates = {j for j, frac in top if frac >= 0.20}

#     return sorted(candidates)

# def iter_financial_rows(preproc_obj: Dict, dossier_source: str):
#     processed = preproc_obj.get("processed", [])
#     for t in processed:
#         if t.get("table_type") != "FINANCIAL_STATEMENT":
#             continue
#         headers: List[str] = t.get("headers") or []
#         data: List[List[str]] = t.get("data") or []
#         page = t.get("page_number")
#         idx  = t.get("table_index")
#         title = t.get("table_title") or ""
#         head_blob = " ".join(h for h in headers if h)

#         scale, unit_src = detect_units(f"{title} || {head_blob}")
#         stmt = infer_stmt_type(f"{title} || {head_blob}") or "PL"
#         cons = is_consolidated(f"{title} || {head_blob}")
#         if cons is None: cons = False

#         period_cols = select_period_columns(headers, data)
#         if len(period_cols) == 0:
#             continue

#         def plabel(j: int) -> str:
#             if j < len(headers) and headers[j]:
#                 return str(headers[j]).strip()
#             return f"Period_{j}"

#         for r in data:
#             if not r:
#                 continue
#             line_item = str(r[0]).strip() if len(r) else ""
#             if len(line_item) < 2:
#                 continue

#             any_numeric = False
#             parsed_vals = []
#             for j in period_cols:
#                 v = to_float(r[j]) if j < len(r) else None
#                 parsed_vals.append((j, v))
#                 if v is not None:
#                     any_numeric = True
#             if not any_numeric:
#                 continue

#             for j, v in parsed_vals:
#                 yield {
#                     "dossier_source": dossier_source,
#                     "page_number": page,
#                     "table_index": idx,
#                     "statement_type": stmt,
#                     "line_item": line_item,
#                     "period_label": plabel(j),
#                     "value": (v * scale) if v is not None else None,
#                     "unit_src": unit_src,
#                     "currency": "INR",
#                     "consolidated": cons,
#                     "notes": None
#                 }

# def main():
#     inputs = sorted(PREPROC_DIR.glob("*.preproc_v6_2.json"))
#     rows = []
#     for p in inputs:
#         with p.open("r", encoding="utf-8") as f:
#             obj = json.load(f)
#         dossier = obj.get("filename") or obj.get("source") or p.stem
#         rows.extend(iter_financial_rows(obj, dossier))

#     if not rows:
#         print("No financial rows found; nothing to write.")
#         return

#     df = pd.DataFrame(rows, columns=[
#         "dossier_source","page_number","table_index","statement_type",
#         "line_item","period_label","value","unit_src","currency",
#         "consolidated","notes"
#     ])

#     OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
#     df.to_parquet(OUT_PATH, index=False)
#     print(f"Wrote {len(df):,} rows -> {OUT_PATH}")

# if __name__ == "__main__":
#     sys.exit(main())




































#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
ETL: canonicalize FINANCIAL_STATEMENT tables from preproc_v6_2 outputs
- Reads:  out/preproc/*.preproc_v6_2.json
- Selects: tables with table_type == "FINANCIAL_STATEMENT"
- Identifies 'period' columns and melts values into long format
- Writes: out/canon/financials.parquet

This version includes an expanded denylist/regex set (ACME tuning):
- Denylist terms: "Amortised cost", "FVTPL*" (handled as regex FVTPL.*)
- Denylist regex: r"Private Limited$", r"^Total of"
- Extra period regex: r"^\d{1,2}\s+\w+\s+\d{4}"
"""

from __future__ import annotations
import json, re, sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd

PREPROC_DIR = Path("out/preproc")
OUT_DIR = Path("out/canon")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PARQUET = OUT_DIR / "financials.parquet"

# ---------------- lexicon & patterns ----------------

# Meta headers (labels/notes/units/aggregate columns) we do NOT want as period cols
META_DENYLIST_STRINGS = [
    "Particulars",
    "Note",
    "Schedule",
    "UOM",
    "Amount",
    "Description",
    # ACME/LLM additions
    "Amortised cost",
]

# Metric-ish headers that are NOT periods (ratios/margins/%/EPS etc)
METRIC_HINTS = re.compile(
    r"\b(ratio|margin|percent|percentage|per\s+share|eps|coverage|turnover|times)\b|\%",
    re.I,
)

# Regex denylist (apply before deciding period columns)
META_DENYLIST_REGEX = [
    r"Private Limited$",   # company-name columns
    r"^Total of",          # aggregation description columns
]

# Your LLM “FVTPL*” suggestion is a wildcard; treat it as regex:
METRIC_DENYLIST_REGEX = [
    r"FVTPL.*",            # e.g., "FVTPL financial assets"
]

# Period header patterns (base set + LLM addition)
PERIOD_PATTERNS: List[re.Pattern] = [
    re.compile(r"\bfy\s*\d{2}\s*[-/]\s*\d{2}\b", re.I),
    re.compile(r"\bfy\s*(?:20)?\d{2}\b", re.I),
    re.compile(r"\b(?:FY)?\s?(?:20)?\d{2}\s?[–-]\s?(?:20)?\d{2}\b", re.I),
    re.compile(r"\bQ[1-4]\s*FY(?:20)?\d{2}\b", re.I),
    re.compile(r"\bH[12]\s*FY(?:20)?\d{2}\b", re.I),
    re.compile(r"\b(20|19)\d{2}\b"),
    re.compile(r"\b(as at|as on|as of|for the|ended|ending)\b", re.I),
    re.compile(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", re.I),
    # LLM addition – date at start like "31 March 2021"
    re.compile(r"^\d{1,2}\s+\w+\s+\d{4}", re.I),
]

# Map some table titles to statement type
FS_TITLE_LEXICON = {
    "BS": [
        "Balance Sheet",
        "Consolidated Balance Sheet",
        "Statement of Assets and Liabilities",
        "Consolidated statement of assets and liabilities",
        "Restated Consolidated statement of assets and liabilities",
        "Statement of Financial Position",
        "Consolidated Statement of Financial Position",
        "EQUITY AND LIABILITIES",
        "ASSETS",
    ],
    "PL": [
        "Statement of Profit and Loss",
        "Consolidated Statement of Profit and Loss",
        "Restated Consolidated Statement of Profit and Loss",
        "Profit and Loss Account",
        "Statement of profit or loss",
        "Income Statement",
        "Statement of Operations",
        "Consolidated Statement of Operations",
        "Statement of Profit and Loss for the year ended",
        "Statement of Profit and Loss for the period ended",
        "Statement of Profit and Loss for the quarter ended",
        "Statement of Profit and Loss for the half year ended",
    ],
    "CF": [
        "Statement of Cash Flows",
        "Consolidated Statement of Cash Flows",
        "Restated Consolidated statement of cash flows",
        "Cash Flow Statement",
        "Consolidated Cash Flow Statement",
        "Cash flow from operating activities",
        "Cash flow from investing activities",
        "Cash flow from financing activities",
    ],
}

# ---------------- helpers ----------------

def norm(s: Any) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).strip())

def to_float(cell: Any) -> Optional[float]:
    s = norm(cell)
    if s in ("", "-", "–", "—", "−", "NA", "N/A", "Nil", "nil"):
        return None
    if "%" in s:
        return None
    # normalize spaces
    s = s.replace("\u00A0", " ").replace("\u2009", " ")
    s2 = s.replace(",", "")
    s2 = re.sub(r"[^\d.\-()]", "", s2)
    neg = s2.startswith("(") and s2.endswith(")")
    s2 = s2.strip("()")
    try:
        v = float(s2)
        return -v if neg else v
    except Exception:
        return None


def header_matches_any_regex(header: str, patterns: List[str]) -> bool:
    h = norm(header)
    return any(re.search(rx, h, re.I) for rx in patterns)

def is_meta_header(header: str) -> bool:
    h = norm(header)
    if any(h.lower() == t.lower() for t in META_DENYLIST_STRINGS):
        return True
    if header_matches_any_regex(h, META_DENYLIST_REGEX):
        return True
    return False

def is_metric_header(header: str) -> bool:
    h = norm(header)
    if METRIC_HINTS.search(h):
        return True
    if header_matches_any_regex(h, METRIC_DENYLIST_REGEX):
        return True
    return False

def looks_like_period(header: str) -> bool:
    h = norm(header)
    if not h:
        return False
    return any(p.search(h) for p in PERIOD_PATTERNS)

def select_period_columns(headers: List[str]) -> List[int]:
    """
    Period columns = headers that match date/period patterns AND are not meta/metric.
    """
    pcols: List[int] = []
    for j, h in enumerate(headers):
        if not h:
            continue
        if is_meta_header(h) or is_metric_header(h):
            continue
        if looks_like_period(h):
            pcols.append(j)
    # Fallback: if nothing matched and we see classic 2+ numeric trailing columns,
    # keep last 2 columns unless they are metric/meta.
    if not pcols and len(headers) >= 2:
        cand = [len(headers) - 2, len(headers) - 1]
        pcols = [
            j for j in cand
            if not is_meta_header(headers[j]) and not is_metric_header(headers[j])
        ]
    return pcols

def classify_statement_type(table_title: Optional[str], headers: List[str]) -> str:
    title = norm(table_title).lower()
    for st, phrases in FS_TITLE_LEXICON.items():
        for p in phrases:
            if p.lower() in title:
                return st
            # also scan header band
            for h in headers:
                if p.lower() in norm(h).lower():
                    return st
    return "UNK"

# ---------------- core ----------------

def iter_preproc_files() -> List[Path]:
    return sorted(PREPROC_DIR.glob("*.preproc_v6_2.json"))
def build_rows_from_table(dossier: str, t: Dict[str, Any]) -> List[Dict[str, Any]]:
    headers: List[str] = t.get("headers") or []
    data: List[List[str]] = t.get("data") or []
    if not headers or not data:
        return []

    stmt_type = classify_statement_type(t.get("table_title"), headers)
    pcols = select_period_columns(headers)
    if not pcols:
        return []

    label_col = 0  # left-most label/particulars

    out_rows: List[Dict[str, Any]] = []
    for r in data:
        label = norm(r[label_col]) if label_col < len(r) else ""
        if not label:
            continue

        # --- NEW: precompute numeric values across all period columns
        vals_num = []
        for j in pcols:
            cell = r[j] if j < len(r) else ""
            vals_num.append(to_float(cell))

        # Skip this row entirely if NONE of the period cells are numeric
        if not any(v is not None for v in vals_num):
            continue

        # Melt after passing the gate
        for j, vnum in zip(pcols, vals_num):
            hdr = headers[j] if j < len(headers) else ""
            cell = r[j] if j < len(r) else ""
            out_rows.append({
                "dossier": dossier,
                "page_number": t.get("page_number"),
                "table_index": t.get("table_index"),
                "table_title": t.get("table_title"),
                "statement_type": stmt_type,  # BS / PL / CF / UNK
                "row_label": label,
                "period_header": hdr,
                "value_raw": norm(cell),
                "value_num": vnum,
            })
    return out_rows


def main():
    all_rows: List[Dict[str, Any]] = []
    files = iter_preproc_files()
    for fp in files:
        try:
            obj = json.loads(fp.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[WARN] Failed to read {fp}: {e}", file=sys.stderr)
            continue
        dossier = obj.get("filename") or obj.get("source") or fp.stem
        for t in obj.get("processed", []):
            if t.get("table_type") != "FINANCIAL_STATEMENT":
                continue
            rows = build_rows_from_table(dossier, t)
            if rows:
                all_rows.extend(rows)

    if not all_rows:
        print("No financial rows found; nothing written.")
        return

    df = pd.DataFrame(all_rows)
    # canonical order
    cols = [
        "dossier","page_number","table_index","table_title","statement_type",
        "row_label","period_header","value_raw","value_num"
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]

    df.to_parquet(OUT_PARQUET, index=False)
    print(f"Wrote {len(df):,} rows -> {OUT_PARQUET}")

if __name__ == "__main__":
    main()


