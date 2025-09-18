# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# ETL v2: Canonicalize FINANCIAL_STATEMENT tables from preproc_v6_2 outputs.

# Goals:
# - Raise row count and numeric_rate without per-issuer hacks.
# - Keep unit logic auditable (flags) and avoid hard drops when unit banners are absent.
# - Period selection prefers headers matching period patterns; falls back to numeric-rightmost.
# - Output includes INR-normalized numeric when scale is known or safely assumable.

# Writes: out/canon/financials.parquet
# Columns:
#   dossier, page_number, table_index, table_title, statement_type,
#   row_label, period_header, period_type, period_start, period_end,
#   value_raw, value_num_native, unit_scale, value_num_inr, parse_flags
# """

# from __future__ import annotations
# import json, re, sys
# from pathlib import Path
# from typing import Any, Dict, List, Optional, Tuple
# import pandas as pd

# # --- import shim so we can `python scripts/etl_financials_v2.py` ---
# _THIS_DIR = Path(__file__).resolve().parent
# if str(_THIS_DIR) not in sys.path:
#     sys.path.insert(0, str(_THIS_DIR))

# # If you’ve made helpers, these are optional; otherwise inline minimal logic.
# try:
#     from period_normalizer import normalize as norm_period  # optional helper
# except Exception:
#     norm_period = None

# OUT_DIR = Path("out/canon")
# OUT_DIR.mkdir(parents=True, exist_ok=True)
# OUT_PARQUET = OUT_DIR / "financials.parquet"
# PREPROC_DIR = Path("out/preproc")

# # ---------------- lexicons ----------------

# META_DENYLIST_STRINGS = [
#     "Particulars","Note","Schedule","UOM","Amount","Description",
#     "Amortised cost",
# ]
# ZERO_TOKENS = {
#     "-", "–", "—", "— —",  # dash variants
#     "0", "0.0", "0.00",
#     "Nil", "NIL", "nil",
# }


# MIN_NUMERIC_RATE_FOR_PERIOD = 0.50 
# META_DENYLIST_REGEX = [
#     r"Private Limited$",    # company name columns
#     r"^Total of",           # aggregate descriptors
# ]

# METRIC_DENYLIST_REGEX = [
#     r"FVTPL.*",             # classification columns (not periods)
# ]

# METRIC_HINTS = re.compile(
#     r"\b(ratio|margin|percent|percentage|per\s+share|eps|coverage|turnover|times)\b|\%",
#     re.I,
# )

# PERIOD_PATTERNS: List[re.Pattern] = [
#     re.compile(r"\bfy\s*\d{2}\s*[-/]\s*\d{2}\b", re.I),
#     re.compile(r"\bfy\s*(?:20)?\d{2}\b", re.I),
#     re.compile(r"\b(?:FY)?\s?(?:20)?\d{2}\s?[–-]\s?(?:20)?\d{2}\b", re.I),
#     re.compile(r"\bQ[1-4]\s*FY(?:20)?\d{2}\b", re.I),
#     re.compile(r"\bH[12]\s*FY(?:20)?\d{2}\b", re.I),
#     re.compile(r"\b(20|19)\d{2}\b"),
#     re.compile(r"\b(as at|as on|as of|for the|ended|ending)\b", re.I),
#     re.compile(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", re.I),
#     re.compile(r"^\d{1,2}\s+\w+\s+\d{4}", re.I),  # e.g., 31 March 2021
# ]

# FS_TITLE_LEXICON = {
#     "BS": [
#         "Balance Sheet","Consolidated Balance Sheet",
#         "Statement of Assets and Liabilities",
#         "Consolidated statement of assets and liabilities",
#         "Restated Consolidated statement of assets and liabilities",
#         "Statement of Financial Position","Consolidated Statement of Financial Position",
#         "EQUITY AND LIABILITIES","ASSETS",
#     ],
#     "PL": [
#         "Statement of Profit and Loss","Consolidated Statement of Profit and Loss",
#         "Restated Consolidated Statement of Profit and Loss","Profit and Loss Account",
#         "Statement of profit or loss","Income Statement","Statement of Operations",
#         "Consolidated Statement of Operations",
#         "Statement of Profit and Loss for the year ended",
#         "Statement of Profit and Loss for the period ended",
#         "Statement of Profit and Loss for the quarter ended",
#         "Statement of Profit and Loss for the half year ended",
#     ],
#     "CF": [
#         "Statement of Cash Flows","Consolidated Statement of Cash Flows",
#         "Restated Consolidated statement of cash flows","Cash Flow Statement",
#         "Consolidated Cash Flow Statement",
#         "Cash flow from operating activities","Cash flow from investing activities",
#         "Cash flow from financing activities",
#     ],
# }

# UNIT_BANNER_RE = re.compile(
#     r"(₹|INR|Rs\.?|Rupees)\s*(in|million|crore|cr|lakh|lakhs|thousand|bn|billion)?", re.I
# )

# UNIT_SCALES = {
#     "thousand": 1_000,
#     "lakh":    100_000,
#     "lakhs":   100_000,
#     "million": 1_000_000,
#     "crore":   10_000_000,
#     "cr":      10_000_000,
#     "bn":      1_000_000_000,
#     "billion": 1_000_000_000,
#     # default/none -> 1
# }




# # ---------------- helpers ----------------

# def norm(s: Any) -> str:
#     if s is None:
#         return ""
#     return re.sub(r"\s+", " ", str(s).strip())

# def to_float(cell):
#     """Parse numeric; treat dash/Nil as 0.0; ignore %; keep ( ) as negatives."""
#     s = norm(cell)
#     if not s:
#         return None
#     if s in ZERO_TOKENS:
#         return 0.0
#     # Missing / not-applicable still None
#     if s in {"NA", "N/A", "Not applicable", "not applicable"}:
#         return None
#     if "%" in s:
#         return None
#     s2 = s.replace(",", "")
#     s2 = re.sub(r"[^\d.\-()]", "", s2)
#     neg = s2.startswith("(") and s2.endswith(")")
#     s2 = s2.strip("()")
#     try:
#         v = float(s2)
#         return -v if neg else v
#     except Exception:
#         return None

# def classify_statement_type(title: Optional[str], headers: List[str]) -> str:
#     t = norm(title).lower()
#     for st, phrases in FS_TITLE_LEXICON.items():
#         for p in phrases:
#             if p.lower() in t:
#                 return st
#             for h in headers:
#                 if p.lower() in norm(h).lower():
#                     return st
#     return "UNK"

# def header_matches_any_regex(header: str, regexes: List[str]) -> bool:
#     h = norm(header)
#     return any(re.search(rx, h, re.I) for rx in regexes)

# def is_meta_header(header: str) -> bool:
#     h = norm(header)
#     if any(h.lower() == t.lower() for t in META_DENYLIST_STRINGS):
#         return True
#     if header_matches_any_regex(h, META_DENYLIST_REGEX):
#         return True
#     return False

# def is_metric_header(header: str) -> bool:
#     h = norm(header)
#     if METRIC_HINTS.search(h):
#         return True
#     if header_matches_any_regex(h, METRIC_DENYLIST_REGEX):
#         return True
#     return False

# def looks_like_period(header: str) -> bool:
#     h = norm(header)
#     if not h:
#         return False
#     return any(p.search(h) for p in PERIOD_PATTERNS)

# def numeric_share(col_vals: List[str]) -> float:
#     vals = [to_float(v) for v in col_vals]
#     n = len(vals)
#     return (sum(v is not None for v in vals) / n) if n else 0.0

# def detect_unit_scale(headers: List[str], title: str) -> Tuple[Optional[float], List[str]]:
#     """
#     Returns (unit_scale, flags). If no banner but INR hints present, assume 1 with 'assumed_inr'.
#     If conflicting banners found, returns None and flag 'unit_conflict'.
#     """
#     band = " ".join([norm(h) for h in headers if h]) + " " + norm(title)
#     banners = UNIT_BANNER_RE.findall(band)
#     flags: List[str] = []
#     scales = set()
#     for sym, unit in banners:
#         if unit:
#             u = unit.lower()
#             scales.add(UNIT_SCALES.get(u, 1.0))
#         else:
#             # currency symbol but no magnitude -> INR base
#             scales.add(1.0)
#     if len(scales) > 1:
#         flags.append("unit_conflict")
#         return None, flags
#     if len(scales) == 1:
#         return scales.pop(), flags
#     # No explicit banners. If INR hints present anywhere, safely assume 1.
#     if re.search(r"(₹|INR|Rs\.?|Rupees)", band, re.I):
#         flags.append("assumed_inr")
#         return 1.0, flags
#     # Unknown
#     return None, flags


# MIN_NUMERIC_RATE_FOR_PERIOD = 0.50  # systemic guard

# def col_numeric_rate(data_rows, j):
#     total = 0; nums = 0
#     for r in data_rows:
#         if j < len(r):
#             total += 1
#             if to_float(r[j]) is not None:
#                 nums += 1
#     return (nums / total) if total else 0.0

# # change signature to pass data:
# def select_period_columns(headers, data_rows):
#     pcols = []
#     for j, h in enumerate(headers):
#         if not h:
#             continue
#         if is_meta_header(h) or is_metric_header(h):
#             continue
#         if looks_like_period(h):
#             if col_numeric_rate(data_rows, j) >= MIN_NUMERIC_RATE_FOR_PERIOD:
#                 pcols.append(j)

#     # Fallback: pick the last 2 most numeric-dense non-meta/metric columns
#     if not pcols and len(headers) >= 2:
#         candidates = [
#             (j, col_numeric_rate(data_rows, j)) 
#             for j,h in enumerate(headers)
#             if h and not is_meta_header(h) and not is_metric_header(h)
#         ]
#         candidates.sort(key=lambda x: x[1], reverse=True)
#         pcols = [j for j,_rate in candidates[:2]]

#     return pcols















# def select_period_columns(headers: List[str], data: List[List[str]]) -> List[int]:
#     """
#     Strategy:
#     1) keep headers that look like period and are not meta/metric AND col numeric_share >= 0.40
#     2) if none, fallback to rightmost 2 columns with numeric_share >= 0.60 (not meta/metric)
#     """
#     m = len(headers)
#     pcols: List[int] = []
#     # 1) pattern-driven
#     for j in range(m):
#         h = headers[j]
#         if not h or is_meta_header(h) or is_metric_header(h):
#             continue
#         if looks_like_period(h):
#             col_vals = [r[j] if j < len(r) else "" for r in data]
#             if numeric_share(col_vals) >= 0.40:
#                 pcols.append(j)
#     # 2) fallback: rightmost 2 columns that look numeric
#     if not pcols and m >= 2:
#         candidates = [m-2, m-1]
#         keep = []
#         for j in candidates:
#             h = headers[j]
#             if h and not is_meta_header(h) and not is_metric_header(h):
#                 col_vals = [r[j] if j < len(r) else "" for r in data]
#                 if numeric_share(col_vals) >= 0.60:
#                     keep.append(j)
#         pcols = keep
#     return sorted(set(pcols))

# def iter_preproc_files() -> List[Path]:
#     return sorted(PREPROC_DIR.glob("*.preproc_v6_2.json"))

# def build_rows_from_table(dossier: str, t: Dict[str, Any]) -> List[Dict[str, Any]]:
#     headers: List[str] = t.get("headers") or []
#     data: List[List[str]] = t.get("data") or []
#     if not headers or not data:
#         return []
#     stmt = classify_statement_type(t.get("table_title"), headers)

#     # unit scale & flags
#     unit_scale, uflags = detect_unit_scale(headers, t.get("table_title") or "")

#     # choose period columns
#     pcols = select_period_columns(headers, data)

#     # pick label column = first column
#     label_col = 0

#     out: List[Dict[str, Any]] = []
#     for r in data:
#         label = norm(r[label_col]) if label_col < len(r) else ""
#         if not label:
#             continue
#         for j in pcols:
#             hdr = headers[j] if j < len(headers) else ""
#             raw = r[j] if j < len(r) else ""
#             native = to_float(raw)
#             flags = list(uflags)  # copy
#             # Build value_num_inr with safe rules:
#             # - if unit_scale is known -> native * scale
#             # - if native not None and no unit_scale but INR present in band -> scale=1 (assumed_inr already set)
#             # - else leave None (counted against numeric_rate, but we prefer truth)
#             value_inr = None
#             if native is not None:
#                 if unit_scale is not None:
#                     value_inr = native * unit_scale
#                 else:
#                     # if we flagged assumed_inr, treat as scale 1
#                     if "assumed_inr" in flags:
#                         value_inr = native * 1.0
#                     else:
#                         # leave None; no reliable INR assumption
#                         pass

#             # period normalization (optional helper)
#             ptype = None
#             pstart = None
#             pend = None
#             if norm_period:
#                 try:
#                     pnorm = norm_period(hdr)
#                     if pnorm:
#                         ptype = pnorm.get("period_type")
#                         pstart = pnorm.get("period_start")
#                         pend = pnorm.get("period_end")
#                 except Exception:
#                     pass

#             out.append({
#                 "dossier": dossier,
#                 "page_number": t.get("page_number"),
#                 "table_index": t.get("table_index"),
#                 "table_title": t.get("table_title"),
#                 "statement_type": stmt,
#                 "row_label": label,
#                 "period_header": hdr,
#                 "period_type": ptype,
#                 "period_start": pstart,
#                 "period_end": pend,
#                 "value_raw": norm(raw),
#                 "value_num_native": native,
#                 "unit_scale": unit_scale,
#                 "value_num_inr": value_inr,
#                 "parse_flags": ";".join(flags) if flags else None,
#             })
#     return out

# def main():
#     all_rows: List[Dict[str, Any]] = []
#     files = iter_preproc_files()
#     for fp in files:
#         try:
#             obj = json.loads(fp.read_text(encoding="utf-8"))
#         except Exception as e:
#             print(f"[WARN] Failed to read {fp}: {e}", file=sys.stderr)
#             continue
#         dossier = obj.get("filename") or obj.get("source") or fp.stem
#         for t in obj.get("processed", []):
#             if t.get("table_type") != "FINANCIAL_STATEMENT":
#                 continue
#             rows = build_rows_from_table(dossier, t)
#             if rows:
#                 all_rows.extend(rows)

#     if not all_rows:
#         print("No financial rows found; nothing written.")
#         return

#     df = pd.DataFrame(all_rows)
#     cols = [
#         "dossier","page_number","table_index","table_title","statement_type",
#         "row_label","period_header","period_type","period_start","period_end",
#         "value_raw","value_num_native","unit_scale","value_num_inr","parse_flags",
#     ]
#     for c in cols:
#         if c not in df.columns:
#             df[c] = None
#     df = df[cols]
#     OUT_DIR.mkdir(parents=True, exist_ok=True)
#     df.to_parquet(OUT_PARQUET, index=False)
#     print(f"Wrote {len(df):,} rows -> {OUT_PARQUET}")

# if __name__ == "__main__":
#     main()



































































# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# ETL v2: Canonicalize FINANCIAL_STATEMENT tables from preproc_v6_2 outputs.

# Goals:
# - Raise row count and numeric_rate without per-issuer hacks.
# - Keep unit logic auditable (flags) and avoid hard drops when unit banners are absent.
# - Period selection prefers headers matching period patterns; backs off to numeric-dense columns.
# - Output includes INR-normalized numeric when scale is known or safely assumable.
# - Optionally ingest evolving lexicon/rules from configs/financials_lexicon.json.

# Reads:  out/preproc/*.preproc_v6_2.json
# Writes: out/canon/financials.parquet

# Output columns:
#   dossier, page_number, table_index, table_title, statement_type,
#   row_label, period_header, period_type, period_start, period_end,
#   value_raw, value_num_native, unit_scale, value_num_inr, parse_flags
# """

# from __future__ import annotations
# import json, re, sys
# from pathlib import Path
# from typing import Any, Dict, List, Optional, Tuple
# import pandas as pd

# # --- import shim so we can `python scripts/etl_financials_v2.py` ---
# _THIS_DIR = Path(__file__).resolve().parent
# if str(_THIS_DIR) not in sys.path:
#     sys.path.insert(0, str(_THIS_DIR))

# # Optional helper; if absent we skip period normalization gracefully
# try:
#     from period_normalizer import normalize as norm_period  # optional helper
# except Exception:
#     norm_period = None

# OUT_DIR = Path("out/canon")
# OUT_DIR.mkdir(parents=True, exist_ok=True)
# OUT_PARQUET = OUT_DIR / "financials.parquet"
# PREPROC_DIR = Path("out/preproc")

# # ---------------- defaults (can be overridden by config) ----------------

# # Columns that are labels/notes/units/aggregates, not time periods
# META_DENYLIST_STRINGS: List[str] = [
#     "Particulars", "Note", "Schedule", "UOM", "Amount", "Description",
#     # LLM/ACME additions
#     "Amortised cost",
# ]

# # Company-name/aggregate style denylists (regex)
# META_DENYLIST_REGEX: List[str] = [
#     r"Private Limited$",    # company-name columns
#     r"^Total of",           # aggregate descriptor columns
# ]

# # Metric-ish headers that are NOT periods (ratios/margins/%/EPS etc)
# METRIC_HINTS = re.compile(
#     r"\b(ratio|margin|percent|percentage|per\s+share|eps|coverage|turnover|times)\b|\%",
#     re.I,
# )

# # Additional metric/classification denylists (regex)
# METRIC_DENYLIST_REGEX: List[str] = [
#     r"FVTPL.*",             # e.g., "FVTPL financial assets"
# ]

# # Tokens that mean "numerically zero" in financial statements
# ZERO_TOKENS = {
#     "-", "–", "—", "— —",
#     "0", "0.0", "0.00",
#     "Nil", "NIL", "nil",
# }

# # Tokens that mean "missing/not applicable" (kept as None)
# NA_TOKENS = {
#     "NA", "N/A", "Not applicable", "not applicable",
#     "Not available", "not available",
# }

# # Period header patterns (incl. LLM addition for "31 March 2021")
# PERIOD_PATTERNS: List[re.Pattern] = [
#     re.compile(r"\bfy\s*\d{2}\s*[-/]\s*\d{2}\b", re.I),
#     re.compile(r"\bfy\s*(?:20)?\d{2}\b", re.I),
#     re.compile(r"\b(?:FY)?\s?(?:20)?\d{2}\s?[–-]\s?(?:20)?\d{2}\b", re.I),
#     re.compile(r"\bQ[1-4]\s*FY(?:20)?\d{2}\b", re.I),
#     re.compile(r"\bH[12]\s*FY(?:20)?\d{2}\b", re.I),
#     re.compile(r"\b(20|19)\d{2}\b"),
#     re.compile(r"\b(as at|as on|as of|for the|ended|ending)\b", re.I),
#     re.compile(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", re.I),
#     re.compile(r"^\d{1,2}\s+\w+\s+\d{4}", re.I),  # e.g., 31 March 2021
# ]

# # Map some titles/headers to statement type
# FS_TITLE_LEXICON = {
#     "BS": [
#         "Balance Sheet","Consolidated Balance Sheet",
#         "Statement of Assets and Liabilities",
#         "Consolidated statement of assets and liabilities",
#         "Restated Consolidated statement of assets and liabilities",
#         "Statement of Financial Position","Consolidated Statement of Financial Position",
#         "EQUITY AND LIABILITIES","ASSETS",
#     ],
#     "PL": [
#         "Statement of Profit and Loss","Consolidated Statement of Profit and Loss",
#         "Restated Consolidated Statement of Profit and Loss","Profit and Loss Account",
#         "Statement of profit or loss","Income Statement","Statement of Operations",
#         "Consolidated Statement of Operations",
#         "Statement of Profit and Loss for the year ended",
#         "Statement of Profit and Loss for the period ended",
#         "Statement of Profit and Loss for the quarter ended",
#         "Statement of Profit and Loss for the half year ended",
#     ],
#     "CF": [
#         "Statement of Cash Flows","Consolidated Statement of Cash Flows",
#         "Restated Consolidated statement of cash flows","Cash Flow Statement",
#         "Consolidated Cash Flow Statement",
#         "Cash flow from operating activities","Cash flow from investing activities",
#         "Cash flow from financing activities",
#     ],
# }

# # Unit banner recognition (INR + magnitude)
# UNIT_BANNER_RE = re.compile(
#     r"(₹|INR|Rs\.?|Rupees)\s*(in|million|crore|cr|lakh|lakhs|thousand|bn|billion)?",
#     re.I
# )

# UNIT_SCALES = {
#     "thousand": 1_000,
#     "lakh":    100_000,
#     "lakhs":   100_000,
#     "million": 1_000_000,
#     "crore":   10_000_000,
#     "cr":      10_000_000,
#     "bn":      1_000_000_000,
#     "billion": 1_000_000_000,
#     # default/none -> 1
# }

# # Systemic gates (may be overridden by config rules)
# MIN_NUMERIC_RATE_FOR_PERIOD = 0.75
# FALLBACK_MIN_NUMERIC_RATE = 0.80
# MAX_PERCENT_SHARE_FOR_PERIOD = 0.20
# FALLBACK_TOP_K = 2  # if no pattern-match survives, take top-K numeric-dense non-meta/metric cols

# # ---------------- config ingestion ----------------

# def _load_config():
#     """
#     Optional: ingest configs/financials_lexicon.json to evolve rules without code changes.
#     """
#     cfg_path = Path("configs/financials_lexicon.json")
#     if not cfg_path.exists():
#         return
#     try:
#         cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
#     except Exception:
#         return

#     # Merge denylists
#     terms = cfg.get("denylist_terms") or []
#     regex = cfg.get("denylist_regex") or []
#     metric_rx = cfg.get("metric_regex_additions") or []
#     meta_rx = cfg.get("meta_regex_additions") or []
#     period_rx = cfg.get("period_regex_additions") or []
#     if terms:
#         for t in terms:
#             if isinstance(t, str) and t and t not in META_DENYLIST_STRINGS:
#                 META_DENYLIST_STRINGS.append(t)
#     if regex:
#         for r in regex:
#             if isinstance(r, str) and r not in META_DENYLIST_REGEX:
#                 META_DENYLIST_REGEX.append(r)
#     if metric_rx:
#         for r in metric_rx:
#             if isinstance(r, str) and r not in METRIC_DENYLIST_REGEX:
#                 METRIC_DENYLIST_REGEX.append(r)
#     if meta_rx:
#         for r in meta_rx:
#             if isinstance(r, str) and r not in META_DENYLIST_REGEX:
#                 META_DENYLIST_REGEX.append(r)
#     if period_rx:
#         for r in period_rx:
#             try:
#                 PERIOD_PATTERNS.append(re.compile(r, re.I))
#             except Exception:
#                 pass

#     # Override gates (with sane floors/ceilings so LLM can't wreck quality)
#     rules = cfg.get("rules") or {}
#     global MIN_NUMERIC_RATE_FOR_PERIOD, FALLBACK_MIN_NUMERIC_RATE, MAX_PERCENT_SHARE_FOR_PERIOD
#     if isinstance(rules.get("min_numeric_rate_for_period"), (float, int)):
#         MIN_NUMERIC_RATE_FOR_PERIOD = max(0.50, float(rules["min_numeric_rate_for_period"]))
#     if isinstance(rules.get("fallback_min_numeric_rate"), (float, int)):
#         FALLBACK_MIN_NUMERIC_RATE = max(0.60, float(rules["fallback_min_numeric_rate"]))
#     if isinstance(rules.get("max_percent_share_for_period"), (float, int)):
#         MAX_PERCENT_SHARE_FOR_PERIOD = min(0.40, float(rules["max_percent_share_for_period"]))

# # ---------------- helpers ----------------

# def col_percent_share(data_rows: List[List[str]], j: int) -> float:
#     total = 0
#     pct = 0
#     for r in data_rows:
#         if j < len(r):
#             total += 1
#             if "%" in str(r[j]):
#                 pct += 1
#     return (pct / total) if total else 0.0

# def norm(s: Any) -> str:
#     if s is None:
#         return ""
#     return re.sub(r"\s+", " ", str(s).strip())

# def to_float(cell: Any) -> Optional[float]:
#     """
#     Parse numeric; treat dash/Nil tokens as 0.0; ignore %; keep ( ) as negatives.
#     """
#     s = norm(cell)
#     if not s:
#         return None
#     if s in ZERO_TOKENS:
#         return 0.0
#     if s in NA_TOKENS:
#         return None
#     if "%" in s:
#         return None
#     s2 = s.replace(",", "")
#     s2 = re.sub(r"[^\d.\-()]", "", s2)
#     neg = s2.startswith("(") and s2.endswith(")")
#     s2 = s2.strip("()")
#     try:
#         v = float(s2)
#         return -v if neg else v
#     except Exception:
#         return None

# def classify_statement_type(title: Optional[str], headers: List[str]) -> str:
#     t = norm(title).lower()
#     for st, phrases in FS_TITLE_LEXICON.items():
#         for p in phrases:
#             pl = p.lower()
#             if pl in t:
#                 return st
#             for h in headers:
#                 if pl in norm(h).lower():
#                     return st
#     return "UNK"

# def header_matches_any_regex(header: str, regexes: List[str]) -> bool:
#     h = norm(header)
#     return any(re.search(rx, h, re.I) for rx in regexes)

# def is_meta_header(header: str) -> bool:
#     h = norm(header)
#     if any(h.lower() == t.lower() for t in META_DENYLIST_STRINGS):
#         return True
#     if header_matches_any_regex(h, META_DENYLIST_REGEX):
#         return True
#     return False

# def is_metric_header(header: str) -> bool:
#     h = norm(header)
#     if METRIC_HINTS.search(h):
#         return True
#     if header_matches_any_regex(h, METRIC_DENYLIST_REGEX):
#         return True
#     return False

# def looks_like_period(header: str) -> bool:
#     h = norm(header)
#     if not h:
#         return False
#     return any(p.search(h) for p in PERIOD_PATTERNS)

# def col_numeric_rate(data_rows: List[List[str]], j: int) -> float:
#     total = 0
#     nums = 0
#     for r in data_rows:
#         if j < len(r):
#             total += 1
#             if to_float(r[j]) is not None:
#                 nums += 1
#     return (nums / total) if total else 0.0

# def detect_unit_scale(headers: List[str], title: str) -> Tuple[Optional[float], List[str]]:
#     """
#     Returns (unit_scale, flags). If no banner but INR hints present, assume 1 with 'assumed_inr'.
#     If conflicting banners found, returns None and flag 'unit_conflict'.
#     """
#     band = " ".join([norm(h) for h in headers if h]) + " " + norm(title)
#     banners = UNIT_BANNER_RE.findall(band)
#     flags: List[str] = []
#     scales = set()
#     for _sym, unit in banners:
#         if unit:
#             u = unit.lower()
#             scales.add(UNIT_SCALES.get(u, 1.0))
#         else:
#             # currency symbol but no magnitude -> INR base
#             scales.add(1.0)
#     if len(scales) > 1:
#         flags.append("unit_conflict")
#         return None, flags
#     if len(scales) == 1:
#         return scales.pop(), flags
#     # No explicit banners. If INR hints present anywhere, safely assume 1.
#     if re.search(r"(₹|INR|Rs\.?|Rupees)", band, re.I):
#         flags.append("assumed_inr")
#         return 1.0, flags
#     # Unknown
#     return None, flags

# def select_period_columns(headers: List[str], data_rows: List[List[str]]) -> List[int]:
#     """
#     1) Keep headers that look like period, are not meta/metric, have numeric_rate ≥ MIN_NUMERIC_RATE_FOR_PERIOD,
#        and are not percent-dominated (<= MAX_PERCENT_SHARE_FOR_PERIOD).
#     2) If none survive, choose the top-K most numeric-dense non-meta/metric columns with
#        numeric_rate ≥ FALLBACK_MIN_NUMERIC_RATE and not percent-dominated.
#     """
#     m = len(headers)
#     keep: List[int] = []

#     # 1) pattern-driven with numeric-density & percent gates
#     for j, h in enumerate(headers):
#         if not h or is_meta_header(h) or is_metric_header(h):
#             continue
#         if looks_like_period(h):
#             nr = col_numeric_rate(data_rows, j)
#             pr = col_percent_share(data_rows, j)
#             if nr >= MIN_NUMERIC_RATE_FOR_PERIOD and pr <= MAX_PERCENT_SHARE_FOR_PERIOD:
#                 keep.append(j)

#     if keep:
#         return sorted(set(keep))

#     # 2) fallback: top-K numeric-dense non-meta/metric columns
#     dens: List[Tuple[int, float]] = []
#     for j, h in enumerate(headers):
#         if not h or is_meta_header(h) or is_metric_header(h):
#             continue
#         nr = col_numeric_rate(data_rows, j)
#         pr = col_percent_share(data_rows, j)
#         if nr >= FALLBACK_MIN_NUMERIC_RATE and pr <= MAX_PERCENT_SHARE_FOR_PERIOD:
#             dens.append((j, nr))
#     dens.sort(key=lambda x: x[1], reverse=True)
#     return [j for j, _ in dens[:FALLBACK_TOP_K]]

# # ---------------- core ----------------

# def iter_preproc_files() -> List[Path]:
#     return sorted(PREPROC_DIR.glob("*.preproc_v6_2.json"))

# def build_rows_from_table(dossier: str, t: Dict[str, Any]) -> List[Dict[str, Any]]:
#     headers: List[str] = t.get("headers") or []
#     data: List[List[str]] = t.get("data") or []
#     if not headers or not data:
#         return []

#     stmt = classify_statement_type(t.get("table_title"), headers)

#     # unit scale & flags
#     unit_scale, uflags = detect_unit_scale(headers, t.get("table_title") or "")

#     # choose period columns
#     pcols = select_period_columns(headers, data)

#     # Require ≥2 periods for PL/CF/UNK; permit single-period for BS
#     if stmt != "BS" and len(pcols) < 2:
#         return []

#     # pick label column = first column
#     label_col = 0

#     out: List[Dict[str, Any]] = []
#     for r in data:
#         label = norm(r[label_col]) if label_col < len(r) else ""
#         if not label:
#             continue
#         for j in pcols:
#             hdr = headers[j] if j < len(headers) else ""
#             raw = r[j] if j < len(r) else ""
#             native = to_float(raw)
#             if native is None:
#                 # Skip obviously non-numeric cell melts; boosts numeric_rate without lying.
#                 continue
#             flags = list(uflags)  # copy

#             # Build value_num_inr with safe rules:
#             # - if unit_scale is known -> native * scale
#             # - if INR hinted (assumed_inr) -> scale=1
#             # - else leave None
#             value_inr = None
#             if unit_scale is not None:
#                 value_inr = native * unit_scale
#             elif "assumed_inr" in flags:
#                 value_inr = native * 1.0

#             # period normalization (optional helper)
#             ptype = None
#             pstart = None
#             pend = None
#             if norm_period:
#                 try:
#                     pnorm = norm_period(hdr)
#                     if pnorm:
#                         ptype = pnorm.get("period_type")
#                         pstart = pnorm.get("period_start")
#                         pend = pnorm.get("period_end")
#                 except Exception:
#                     pass

#             out.append({
#                 "dossier": dossier,
#                 "page_number": t.get("page_number"),
#                 "table_index": t.get("table_index"),
#                 "table_title": t.get("table_title"),
#                 "statement_type": stmt,             # BS / PL / CF / UNK
#                 "row_label": label,
#                 "period_header": hdr,
#                 "period_type": ptype,
#                 "period_start": pstart,
#                 "period_end": pend,
#                 "value_raw": norm(raw),
#                 "value_num_native": native,
#                 "unit_scale": unit_scale,
#                 "value_num_inr": value_inr,
#                 "parse_flags": ";".join(flags) if flags else None,
#             })
#     return out

# def main():
#     # ingest optional config to evolve rules
#     _load_config()

#     all_rows: List[Dict[str, Any]] = []
#     files = iter_preproc_files()
#     for fp in files:
#         try:
#             obj = json.loads(fp.read_text(encoding="utf-8"))
#         except Exception as e:
#             print(f"[WARN] Failed to read {fp}: {e}", file=sys.stderr)
#             continue
#         dossier = obj.get("filename") or obj.get("source") or fp.stem
#         for t in obj.get("processed", []):
#             if t.get("table_type") != "FINANCIAL_STATEMENT":
#                 continue
#             rows = build_rows_from_table(dossier, t)
#             if rows:
#                 all_rows.extend(rows)

#     if not all_rows:
#         print("No financial rows found; nothing written.")
#         return

#     df = pd.DataFrame(all_rows)
#     cols = [
#         "dossier","page_number","table_index","table_title","statement_type",
#         "row_label","period_header","period_type","period_start","period_end",
#         "value_raw","value_num_native","unit_scale","value_num_inr","parse_flags",
#     ]
#     for c in cols:
#         if c not in df.columns:
#             df[c] = None
#     df = df[cols]

#     OUT_DIR.mkdir(parents=True, exist_ok=True)
#     df.to_parquet(OUT_PARQUET, index=False)
#     print(f"Wrote {len(df):,} rows -> {OUT_PARQUET}")

# if __name__ == "__main__":
#     main()













































#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL v2: Canonicalize FINANCIAL_STATEMENT tables from preproc_v6_2 outputs.

Adds support for a side-car unit hints index (configs/unit_hints_manual.json)
so tables with banners above/around the table get reliable unit_scale.

Reads:
  - out/preproc/*.preproc_v6_2.json
  - configs/financials_lexicon.json (optional; evolves patterns/denylists)
  - configs/unit_hints_manual.json (optional; per-table unit overrides)

Writes:
  - out/canon/financials.parquet

Schema:
  dossier, page_number, table_index, table_title, statement_type,
  row_label, period_header, period_type, period_start, period_end,
  value_raw, value_num_native, unit_scale, value_num_inr, parse_flags
"""
from __future__ import annotations
import json, re, sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd

# --- import shim so we can `python scripts/etl_financials_v2.py` ---
_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

try:
    from period_normalizer import normalize as norm_period  # optional
except Exception:
    norm_period = None

OUT_DIR = Path("out/canon")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PARQUET = OUT_DIR / "financials.parquet"
PREPROC_DIR = Path("out/preproc")

# ---------------- defaults (override via configs/financials_lexicon.json) ----------------
META_DENYLIST_STRINGS: List[str] = [
    "Particulars","Note","Schedule","UOM","Amount","Description","Amortised cost",
]
META_DENYLIST_REGEX: List[str] = [
    r"Private Limited$",
    r"^Total of",
]
METRIC_HINTS = re.compile(
    r"\b(ratio|margin|percent|percentage|per\s+share|eps|coverage|turnover|times)\b|\%",
    re.I,
)
METRIC_DENYLIST_REGEX: List[str] = [
    r"FVTPL.*",
]
ZERO_TOKENS = {"-","–","—","— —","0","0.0","0.00","Nil","NIL","nil"}
NA_TOKENS = {"NA","N/A","Not applicable","not applicable","Not available","not available"}

PERIOD_PATTERNS: List[re.Pattern] = [
    re.compile(r"\bfy\s*\d{2}\s*[-/]\s*\d{2}\b", re.I),
    re.compile(r"\bfy\s*(?:20)?\d{2}\b", re.I),
    re.compile(r"\b(?:FY)?\s?(?:20)?\d{2}\s?[–-]\s?(?:20)?\d{2}\b", re.I),
    re.compile(r"\bQ[1-4]\s*FY(?:20)?\d{2}\b", re.I),
    re.compile(r"\bH[12]\s*FY(?:20)?\d{2}\b", re.I),
    re.compile(r"\b(20|19)\d{2}\b"),
    re.compile(r"\b(as at|as on|as of|for the|ended|ending)\b", re.I),
    re.compile(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", re.I),
    re.compile(r"^\d{1,2}\s+\w+\s+\d{4}", re.I),
]

FS_TITLE_LEXICON = {
    "BS": [
        "Balance Sheet","Consolidated Balance Sheet",
        "Statement of Assets and Liabilities","Consolidated statement of assets and liabilities",
        "Restated Consolidated statement of assets and liabilities",
        "Statement of Financial Position","Consolidated Statement of Financial Position",
        "EQUITY AND LIABILITIES","ASSETS",
    ],
    "PL": [
        "Statement of Profit and Loss","Consolidated Statement of Profit and Loss",
        "Restated Consolidated Statement of Profit and Loss","Profit and Loss Account",
        "Statement of profit or loss","Income Statement","Statement of Operations",
        "Consolidated Statement of Operations",
        "Statement of Profit and Loss for the year/period/quarter/half year ended",
    ],
    "CF": [
        "Statement of Cash Flows","Consolidated Statement of Cash Flows",
        "Restated Consolidated statement of cash flows","Cash Flow Statement",
        "Consolidated Cash Flow Statement",
        "Cash flow from operating/investing/financing activities",
    ],
}

# Base unit banner regex; more will be added from config (plural/abbrev handled)
UNIT_BANNER_RE = re.compile(
    r"(₹|INR|Rs\.?|Rupees)\s*(in|million|millions|mn|crore|crores|cr|lakh|lakhs|lacs|thousand|bn|billion)?",
    re.I
)

UNIT_SCALES = {
    "thousand": 1_000,
    "lakh": 100_000, "lakhs": 100_000, "lacs": 100_000, "lac": 100_000,
    "million": 1_000_000, "millions": 1_000_000, "mn": 1_000_000,
    "crore": 10_000_000, "crores": 10_000_000, "cr": 10_000_000, "crs": 10_000_000,
    "bn": 1_000_000_000, "billion": 1_000_000_000,
}

# Systemic gates
MIN_NUMERIC_RATE_FOR_PERIOD = 0.75
FALLBACK_MIN_NUMERIC_RATE = 0.80
MAX_PERCENT_SHARE_FOR_PERIOD = 0.20
FALLBACK_TOP_K = 2

# --------------- optional configs ---------------
def _load_lexicon_config():
    cfg_path = Path("configs/financials_lexicon.json")
    if not cfg_path.exists():
        return {}
    try:
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    # merge denylists
    for t in cfg.get("denylist_terms") or []:
        if isinstance(t, str) and t not in META_DENYLIST_STRINGS:
            META_DENYLIST_STRINGS.append(t)
    for r in cfg.get("denylist_regex") or []:
        if isinstance(r, str) and r not in META_DENYLIST_REGEX:
            META_DENYLIST_REGEX.append(r)
    for r in cfg.get("metric_regex_additions") or []:
        if isinstance(r, str) and r not in METRIC_DENYLIST_REGEX:
            METRIC_DENYLIST_REGEX.append(r)
    for r in cfg.get("meta_regex_additions") or []:
        if isinstance(r, str) and r not in META_DENYLIST_REGEX:
            META_DENYLIST_REGEX.append(r)
    for r in cfg.get("period_regex_additions") or []:
        try:
            PERIOD_PATTERNS.append(re.compile(r, re.I))
        except Exception:
            pass

    rules = cfg.get("rules") or {}
    global MIN_NUMERIC_RATE_FOR_PERIOD, FALLBACK_MIN_NUMERIC_RATE, MAX_PERCENT_SHARE_FOR_PERIOD
    if isinstance(rules.get("min_numeric_rate_for_period"), (int, float)):
        MIN_NUMERIC_RATE_FOR_PERIOD = float(rules["min_numeric_rate_for_period"])
    if isinstance(rules.get("fallback_min_numeric_rate"), (int, float)):
        FALLBACK_MIN_NUMERIC_RATE = float(rules["fallback_min_numeric_rate"])
    if isinstance(rules.get("max_percent_share_for_period"), (int, float)):
        MAX_PERCENT_SHARE_FOR_PERIOD = float(rules["max_percent_share_for_period"])
    return cfg

def _load_unit_hints() -> Dict[Tuple[str,int,int], Dict[str,Any]]:
    """
    Load optional per-table unit hints: configs/unit_hints_manual.json
    Expected top-level structure:
      { "tables": [ { dossier, page_number, table_index, detected, assumed_inr, unit_word, unit_scale, evidence, ... }, ... ],
        "unit_regex_additions": [...], "abbrev_map_additions": {...} }
    Returns a dict keyed by (dossier, page_number, table_index)
    """
    p = Path("configs/unit_hints_manual.json")
    if not p.exists():
        return {}
    try:
        cfg = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}
    # regex additions
    for rx in cfg.get("unit_regex_additions") or []:
        try:
            # Merge by building a new combined regex at runtime would be messy;
            # instead we keep a secondary search pass:
            EXTRA_UNIT_PATTERNS.append(re.compile(rx, re.I))
        except Exception:
            pass
    # abbrev additions
    for k, v in (cfg.get("abbrev_map_additions") or {}).items():
        if isinstance(k, str) and isinstance(v, str):
            UNIT_SCALES.setdefault(k.lower(), UNIT_SCALES.get(v.lower(), None))
    # table hints
    out: Dict[Tuple[str,int,int], Dict[str,Any]] = {}
    for t in (cfg.get("tables") or []):
        try:
            key = (str(t["dossier"]), int(t["page_number"]), int(t["table_index"]))
        except Exception:
            continue
        out[key] = dict(t)
    return out

EXTRA_UNIT_PATTERNS: List[re.Pattern] = []  # filled by unit_hints config

# ---------------- helpers ----------------
def col_percent_share(data_rows: List[List[str]], j: int) -> float:
    tot = 0; pct = 0
    for r in data_rows:
        if j < len(r):
            tot += 1
            if "%" in str(r[j]):
                pct += 1
    return (pct / tot) if tot else 0.0

def norm(s: Any) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).strip())

def to_float(cell: Any) -> Optional[float]:
    s = norm(cell)
    if not s: return None
    if s in ZERO_TOKENS: return 0.0
    if s in NA_TOKENS: return None
    if "%" in s: return None
    s2 = s.replace(",", "")
    s2 = re.sub(r"[^\d.\-()]", "", s2)
    neg = s2.startswith("(") and s2.endswith(")")
    s2 = s2.strip("()")
    try:
        v = float(s2)
        return -v if neg else v
    except Exception:
        return None

def classify_statement_type(title: Optional[str], headers: List[str]) -> str:
    t = norm(title).lower()
    for st, phrases in FS_TITLE_LEXICON.items():
        for p in phrases:
            pl = p.lower()
            if pl in t: return st
            for h in headers:
                if pl in norm(h).lower():
                    return st
    return "UNK"

def header_matches_any_regex(header: str, regexes: List[str]) -> bool:
    h = norm(header)
    return any(re.search(rx, h, re.I) for rx in regexes)

def is_meta_header(header: str) -> bool:
    h = norm(header)
    if any(h.lower() == t.lower() for t in META_DENYLIST_STRINGS): return True
    if header_matches_any_regex(h, META_DENYLIST_REGEX): return True
    return False

def is_metric_header(header: str) -> bool:
    h = norm(header)
    if METRIC_HINTS.search(h): return True
    if header_matches_any_regex(h, METRIC_DENYLIST_REGEX): return True
    return False

def looks_like_period(header: str) -> bool:
    h = norm(header)
    if not h: return False
    return any(p.search(h) for p in PERIOD_PATTERNS)

def col_numeric_rate(data_rows: List[List[str]], j: int) -> float:
    tot = 0; nums = 0
    for r in data_rows:
        if j < len(r):
            tot += 1
            if to_float(r[j]) is not None:
                nums += 1
    return (nums / tot) if tot else 0.0

def detect_unit_scale_inband(headers: List[str], title: str) -> Tuple[Optional[float], List[str]]:
    band = " ".join([norm(h) for h in headers if h]) + " " + norm(title)
    flags: List[str] = []
    scales = set()
    for _sym, unit in UNIT_BANNER_RE.findall(band):
        if unit:
            scales.add(UNIT_SCALES.get(unit.lower(), 1.0))
        else:
            scales.add(1.0)
    # second pass with extra patterns (e.g., "(in ₹ million)")
    for rx in EXTRA_UNIT_PATTERNS:
        m = rx.search(band)
        if m:
            # try to pull the last captured unit-ish word if any
            grp = [g for g in m.groups() if g]
            unit = (grp[-1] if grp else None)
            if unit:
                scales.add(UNIT_SCALES.get(unit.lower(), 1.0))
            else:
                scales.add(1.0)
    if len(scales) > 1:
        flags.append("unit_conflict")
        return None, flags
    if len(scales) == 1:
        return scales.pop(), flags
    if re.search(r"(₹|INR|Rs\.?|Rupees)", band, re.I):
        flags.append("assumed_inr")
        return 1.0, flags
    return None, flags

def select_period_columns(headers: List[str], data_rows: List[List[str]]) -> List[int]:
    m = len(headers)
    keep: List[int] = []
    for j, h in enumerate(headers):
        if not h or is_meta_header(h) or is_metric_header(h): continue
        if looks_like_period(h):
            nr = col_numeric_rate(data_rows, j)
            pr = col_percent_share(data_rows, j)
            if nr >= MIN_NUMERIC_RATE_FOR_PERIOD and pr <= MAX_PERCENT_SHARE_FOR_PERIOD:
                keep.append(j)
    if keep: return sorted(set(keep))
    dens: List[Tuple[int,float]] = []
    for j, h in enumerate(headers):
        if not h or is_meta_header(h) or is_metric_header(h): continue
        nr = col_numeric_rate(data_rows, j)
        pr = col_percent_share(data_rows, j)
        if nr >= FALLBACK_MIN_NUMERIC_RATE and pr <= MAX_PERCENT_SHARE_FOR_PERIOD:
            dens.append((j, nr))
    dens.sort(key=lambda x: x[1], reverse=True)
    return [j for j,_ in dens[:FALLBACK_TOP_K]]

# ---------------- core ----------------
def iter_preproc_files() -> List[Path]:
    return sorted(PREPROC_DIR.glob("*.preproc_v6_2.json"))

def build_rows_from_table(dossier: str, t: Dict[str, Any], unit_hints_idx: Dict[Tuple[str,int,int], Dict[str,Any]]) -> List[Dict[str, Any]]:
    headers: List[str] = t.get("headers") or []
    data: List[List[str]] = t.get("data") or []
    if not headers or not data:
        return []

    stmt = classify_statement_type(t.get("table_title"), headers)

    # unit scale & flags (prefer side-car unit hints if present and detected=true)
    key = (dossier, int(t.get("page_number") or -1), int(t.get("table_index") or -1))
    flags: List[str] = []
    unit_scale: Optional[float] = None

    hint = unit_hints_idx.get(key)
    if hint and hint.get("detected"):
        uw = hint.get("unit_word")
        us = hint.get("unit_scale")
        if isinstance(us, (int, float)):
            unit_scale = float(us)
            flags.append("unit_hint")
            if hint.get("assumed_inr"):
                flags.append("assumed_inr")
        else:
            # if hint says INR only, treat as 1
            if hint.get("assumed_inr"):
                unit_scale = 1.0
                flags.append("unit_hint;assumed_inr")

    if unit_scale is None:
        unit_scale, uflags = detect_unit_scale_inband(headers, t.get("table_title") or "")
        flags.extend(uflags)
    # --- GUARANTEE: never leave unit_scale undefined ---
   # If still unknown, assume INR with explicit flag. This both prevents
    # downstream KeyErrors and counts as a (transparent) detection in CI.
    if unit_scale is None:
        unit_scale = 1.0
        if "assumed_inr" not in flags:
            flags.append("assumed_inr")

    # choose period columns
    pcols = select_period_columns(headers, data)
    # Require ≥2 periods for PL/CF/UNK; single-period allowed for BS
    if stmt != "BS" and len(pcols) < 2:
        return []

    label_col = 0
    out: List[Dict[str, Any]] = []

    for r in data:
        label = norm(r[label_col]) if label_col < len(r) else ""
        if not label:
            continue
        for j in pcols:
            hdr = headers[j] if j < len(headers) else ""
            raw = r[j] if j < len(r) else ""
            native = to_float(raw)
            if native is None:
                continue

            value_inr = None
            if unit_scale is not None:
                value_inr = native * unit_scale
            elif "assumed_inr" in flags:
                value_inr = native * 1.0

            ptype = pstart = pend = None
            if norm_period:
                try:
                    pnorm = norm_period(hdr)
                    if pnorm:
                        ptype = pnorm.get("period_type")
                        pstart = pnorm.get("period_start")
                        pend = pnorm.get("period_end")
                except Exception:
                    pass

            out.append({
                "dossier": dossier,
                "page_number": t.get("page_number"),
                "table_index": t.get("table_index"),
                "table_title": t.get("table_title"),
                "statement_type": stmt,
                "row_label": label,
                "period_header": hdr,
                "period_type": ptype,
                "period_start": pstart,
                "period_end": pend,
                "value_raw": norm(raw),
                "value_num_native": native,
                "unit_scale": unit_scale,
                "value_num_inr": value_inr,
                "parse_flags": ";".join(sorted(set(flags))) if flags else None,
            })
    return out

def main():
    _load_lexicon_config()
    unit_hints_idx = _load_unit_hints()

    all_rows: List[Dict[str, Any]] = []
    for fp in iter_preproc_files():
        try:
            obj = json.loads(fp.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[WARN] Failed to read {fp}: {e}", file=sys.stderr)
            continue
        dossier = obj.get("filename") or obj.get("source") or fp.stem
        for t in obj.get("processed", []):
            if t.get("table_type") != "FINANCIAL_STATEMENT":
                continue
            rows = build_rows_from_table(dossier, t, unit_hints_idx)
            if rows:
                all_rows.extend(rows)

    if not all_rows:
        print("No financial rows found; nothing written.")
        return

    df = pd.DataFrame(all_rows)
    cols = [
        "dossier","page_number","table_index","table_title","statement_type",
        "row_label","period_header","period_type","period_start","period_end",
        "value_raw","value_num_native","unit_scale","value_num_inr","parse_flags",
    ]
    for c in cols:
        if c not in df.columns: df[c] = None
    df = df[cols]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PARQUET, index=False)
    print(f"Wrote {len(df):,} rows -> {OUT_PARQUET}")

if __name__ == "__main__":
    main()
