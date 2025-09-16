# # #!/usr/bin/env python3
# # # -*- coding: utf-8 -*-
# # """
# # Phoenix Surgeon v6.2 (Patched)
# # Implements Phase-1A directives over legacy JSON extracts.

# # Directives implemented:
# # 1) Structural triage via tabularity_score (primary shield against prose-as-table).
# # 2) Cease financial sentence-splitting (only split FRONT_PAGE).
# # 3) Header fortification:
# #    3a) Guard against row-label slabs (looks_like_row_label_band).
# #    3b) Compose multirow period headers (try_compose_period_headers).
# # 4) Front-page semantic promotion via controlled regex FRONT_FIELD_MAP.
# # 5) Stronger hashing includes page_no and table_title (compute_table_hash).

# # I/O:
# # - Input: legacy JSON in the form:
# #   {
# #     "<Key>": {
# #       "filename": "...pdf",
# #       "file_size_mb": 4.9,
# #       "tables": [
# #         { "page_number": 1, "table_data": [ {"col_0": "...", "col_1": "..."} , ... ] },
# #         ...
# #       ]
# #     }
# #   }
# # - Output: gz/json written by caller; main() prints a short summary to stdout.
# # """
# # from __future__ import annotations

# # import json, gzip, re, time, hashlib, sys
# # from dataclasses import dataclass
# # from typing import Any, Dict, List, Optional, Tuple

# # # -------------------- helpers --------------------

# # def normalize_cell(x: Any) -> str:
# #     if x is None:
# #         return ""
# #     return re.sub(r"\s+", " ", str(x).strip())

# # def safe_get_max_cols(table_data: List[Dict[str,str]]) -> int:
# #     max_cols = 0
# #     for row in table_data:
# #         for k in row.keys():
# #             if k.lower().startswith("col_"):
# #                 try:
# #                     i = int(k.split("_")[1])
# #                     max_cols = max(max_cols, i + 1)
# #                 except Exception:
# #                     pass
# #     return max_cols

# # def pad_to_len(arr: List[str], n: int) -> List[str]:
# #     return arr + [""] * (n - len(arr)) if len(arr) < n else arr[:n]

# # # If a table row doesn't have col_* keys, concatenate string-like fields
# # def concat_row_fields(row: Dict[str, Any]) -> str:
# #     vals: List[str] = []
# #     for k, v in row.items():
# #         if v is None:
# #             continue
# #         if isinstance(v, (int, float)):
# #             vals.append(str(v))
# #         elif isinstance(v, str):
# #             s = v.strip()
# #             if s:
# #                 vals.append(s)
# #     return normalize_cell(" ".join(vals))

# # # -------------------- Directives --------------------

# # # (1) Structural triage
# # def tabularity_score(rows: List[List[str]]) -> float:
# #     if not rows:
# #         return 0.0
# #     cols = max((len(r) for r in rows), default=0)
# #     if cols <= 1:
# #         return 0.0

# #     numeric_col_frac = []
# #     for j in range(cols):
# #         vals = [normalize_cell(r[j]) for r in rows if j < len(r) and normalize_cell(r[j])]
# #         if not vals:
# #             numeric_col_frac.append(0.0)
# #             continue
# #         num = sum(bool(re.fullmatch(r"[0-9,.\-()%₹]+", v)) for v in vals)
# #         frac = max(num/len(vals), 1 - num/len(vals))  # purity of type
# #         numeric_col_frac.append(frac)

# #     purity = sum(numeric_col_frac)/len(numeric_col_frac)

# #     left = [normalize_cell(r[0]) for r in rows if r and normalize_cell(r[0])]
# #     label_diversity = len(set(left))/max(1, len(left))
# #     label_signal = 1 - min(1.0, label_diversity) * 0.5

# #     width = 1.0 if (cols >= 2 and any(f > 0.7 for f in numeric_col_frac)) else 0.0

# #     return 0.5*purity + 0.3*label_signal + 0.2*width

# # # (3a) Row-label slab guard
# # ROW_LABEL_TOKENS = {"assets","liabilities","equity","income","expenses","particulars","notes"}

# # def looks_like_row_label_band(row: Dict[str,str]) -> bool:
# #     cells = [normalize_cell(v) for k,v in row.items() if k.lower().startswith("col_")]
# #     if not cells:
# #         return False
# #     left = normalize_cell(row.get("col_0",""))
# #     other_text = " ".join(normalize_cell(row.get(f"col_{j}","")) for j in range(1, min(6, len(cells))))
# #     left_heavy = len(left) > 20 and len(other_text) < 10
# #     has_fin_labels = sum(t in left.lower() for t in ROW_LABEL_TOKENS) >= 2
# #     return left_heavy and has_fin_labels

# # # (3b) Multirow period header composer
# # PERIOD_ROW_PATTS = [
# #     re.compile(r"\b(as on|as at|as of|for the|nine months|half year|quarter|q[1-4]|h[12]|fy)\b", re.I),
# #     re.compile(r"\b(ended|ending)\b", re.I),
# #     re.compile(r"\b(20\d{2}|19\d{2})\b"),
# #     re.compile(r"\b₹\s*(in|million|crore|lakh)\b", re.I),
# #     re.compile(r"\bfy\s*\d{2}\s*[-/]\s*\d{2}\b", re.I),
# # ]
# # FP_TOKENS = (
# #     "registrar", "rta", "brlm", "book running lead manager",
# #     "global coordinator", "syndicate member", "contact person",
# #     "company secretary", "compliance officer", "investor relations",
# #     "email", "e-mail", "website", "web site", "tel", "telephone", "phone", "mobile", "mob.", "fax",
# #     "link intime", "kfin", "kfintech", "bigshare", "mas services", "cameo",
# #     "issue opens", "issue closes", "price band", "isin", "cin", "pan", "sebi"
# # )

# # def try_compose_period_headers(table_data: List[Dict[str,str]], kept_cols: List[int], search_rows: int = 4) -> Optional[List[str]]:
# #     if not table_data or not kept_cols:
# #         return None
# #     rows = [[normalize_cell(table_data[r].get(f"col_{c}","")) 
# #              for c in kept_cols] for r in range(min(search_rows, len(table_data)))]
# #     if not rows:
# #         return None
# #     cols = list(zip(*rows))
# #     headers: List[str] = []
# #     any_hit = 0
# #     for col in cols:
# #         band = " ".join([t for t in col if t]).strip()
# #         if any(p.search(band) for p in PERIOD_ROW_PATTS):
# #             headers.append(re.sub(r"\s+"," ",band))
# #             any_hit += 1
# #         else:
# #             headers.append("")
# #     if any_hit >= max(2, len(headers)//3):
# #         return headers
# #     return None

# # # (4) Controlled front-page labels
# # FRONT_FIELD_MAP = {
# #    r"\bregistrar(\s+to\s+the)?\s+(issue|offer)\b": "Registrar",
# #    r"\b(lead\s+manager|merchant\s+banker|brlm)\b": "Lead Manager",
# #    r"\b(contact\s+person|compliance\s+officer|company\s+secretary)\b": "Contact",
# #    r"\bemail|e-?mail\b": "Email",
# #    r"\bwebsite|web\s*site|url\b": "Website",
# #    r"\btelephone|tel|phone|fax\b": "Telephone",
# # }

# # def infer_semantic_headers_for_front_page(data_rows: List[List[str]], original_headers: List[str]) -> Tuple[List[str], List[List[str]], str]:
# #     if not data_rows:
# #         return original_headers, data_rows, "No data rows to infer from."
# #     first_row = data_rows[0]
# #     tokens = " ".join([normalize_cell(c) for c in first_row]).lower()
# #     labels = [name for pat,name in FRONT_FIELD_MAP.items() if re.search(pat, tokens)]
# #     if 2 <= len(labels) <= len(original_headers):
# #         new_headers = labels + [f"Field_{i}" for i in range(len(original_headers)-len(labels))]
# #         return pad_to_len(new_headers, len(original_headers)), data_rows[1:], "Promoted compact front-page labels."
# #     return original_headers, data_rows, "No compact label set found."

# # # (5) Stronger hashing
# # def compute_table_hash(display_headers: List[str], data_rows: List[List[str]], table_index: int, raw_header_parts: Optional[List[List[str]]] = None, sample_rows: int = 40, page_no: Optional[int] = None, title: Optional[str] = None) -> str:
# #     h = hashlib.md5()
# #     prefix = f"idx:{table_index}|pg:{page_no if page_no is not None else -1}|title:{(title or '')[:64]}|"
# #     text = prefix + "".join(display_headers or [])
# #     if raw_header_parts:
# #         text += "".join("".join(p) for p in raw_header_parts)
# #     h.update(text.encode("utf-8"))
# #     for r in (data_rows or [])[:sample_rows]:
# #         h.update("\\u241F".join([normalize_cell(c) for c in r]).encode("utf-8"))
# #     return h.hexdigest()

# # # -------------------- Core Surgeon --------------------

# # @dataclass
# # class PreprocessorConfig:
# #     min_content_threshold: float = 0.30
# #     max_header_rows: int = 4
# #     min_data_rows: int = 1
# #     max_header_length: int = 100
# #     dedupe: bool = True
# #     output_compression: bool = False  # handled by caller
# #     prose_split_char_threshold: int = 250

# # HEADER_HINTS  = ['total','amount','year','period','march','december','fy','q1','q2','q3','q4','half year','h1','h2','as on','as at','as of','revenue','assets','liabilities','equity','cash flow','profit','loss','income','expenses','notes','particulars','sr. no.','details','description','metric','₹ in crore','₹ in lakh']
# # FINANCIAL_STATEMENT_KEYWORDS = ['balance sheet','profit and loss','statement of profit and loss','cash flow','statement of operations','consolidated financial','shareholder equity','financial position','notes to accounts']
# # FRONT_PAGE_KEYWORDS = ['offer','issue','price band','registrar','lead manager','merchant banker','rta','ipo','equity share','fresh issue','offer for sale','contact person','telephone','tel','phone','fax','website','email','e-mail']

# # class TableType:
# #     FRONT_PAGE = "FRONT_PAGE"
# #     FINANCIAL_STATEMENT = "FINANCIAL_STATEMENT"
# #     GENERIC = "GENERIC"

# # class Surgeon:
# #     def __init__(self, cfg: PreprocessorConfig):
# #         self.cfg = cfg
# #         # --- universal anchors for front-page “contact slab” salvage ---
# #         self.EMAIL_RE  = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
# #         self.URL_RE    = re.compile(r"(?:(?:https?://)?(?:www\.)?[A-Za-z0-9.-]+\.[A-Za-z]{2,}(?:/[^\s]*)?)", re.I)
# #         self.PHONE_RE  = re.compile(r"(?:\+?\d[\d\s\-]{7,}\d)")
# #         self.ROLE_TOKS = (
# #              "registrar","lead manager","merchant banker","brlm",
# #              "contact person","compliance officer","company secretary",
# #              "telephone","tel","phone","fax","website","email","e-mail",
# #              "bid","issue opens","issue closes","anchor investor","rta"
# #          )

# #     # --- classification ---
# #     def classify(self, table_data: List[Dict[str, Any]], page_number: Optional[int]) -> str:
# #         if page_number is not None and page_number <= 2:
# #             return TableType.FRONT_PAGE
# #         all_text = " ".join(normalize_cell(v).lower() for row in table_data for v in row.values())
# #         if any(k in all_text for k in FINANCIAL_STATEMENT_KEYWORDS):
# #             return TableType.FINANCIAL_STATEMENT
# #         # broadened FP signals so we can classify FP-like slabs beyond page 1–2
# #         if any(k in all_text for k in FRONT_PAGE_KEYWORDS) or any(rt in all_text for rt in self.ROLE_TOKS):
# #             return TableType.FRONT_PAGE
# #         return TableType.GENERIC

# #     # --- header helpers ---
# #     def detect_header_rows(self, table_data: List[Dict[str,str]], max_header_rows: int) -> List[int]:
# #         def score_header_row(row: Dict[str,str]) -> float:
# #             hits, labels, numerics, unique_labels = 0, 0, 0, set()
# #             for k, v in row.items():
# #                 s = normalize_cell(v); sl = s.lower()
# #                 if not s: continue
# #                 if any(h in sl for h in HEADER_HINTS): hits += 2
# #                 if re.search(r"\b(19\d{2}|20\d{2})\b", sl) or re.search(r"^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)", sl, re.I):
# #                     hits += 1
# #                 for t in s.split():
# #                     if t.isalpha(): 
# #                         labels += 1; unique_labels.add(t.lower())
# #                     elif re.fullmatch(r"[\d,.%₹-]+", t):
# #                         numerics += 1
# #             ratio = labels / (labels + numerics) if (labels + numerics) > 0 else 0.0
# #             return hits + 0.5 * ratio - (0.1 if hits == 0 and len(unique_labels) <= 1 else 0)

# #         header_indices: List[int] = []
# #         for i in range(min(max_header_rows, len(table_data))):
# #             row = table_data[i]
# #             if looks_like_row_label_band(row):
# #                 continue  # Directive 3a
# #             sc = score_header_row(row)
# #             # numeric-light, header-like
# #             numerics = sum(bool(re.search(r"[\d,.%₹-]", normalize_cell(v))) for v in row.values())
# #             labels = sum(bool(re.search(r"[A-Za-z]", normalize_cell(v))) for v in row.values())
# #             nr = numerics / max(1, (numerics + labels))
# #             if sc >= 0.5 and nr < 0.25:
# #                 header_indices.append(i)
# #         return header_indices

# #     # --- extraction ---
# #     def extract_headers(self, table_data: List[Dict[str,str]], header_rows: List[int], kept_cols: List[int], max_len: int) -> Tuple[List[str], List[List[str]], Optional[str], List[int]]:
# #         table_title = None
# #         raw_header_parts: List[List[str]] = []
# #         used_indices: List[int] = []

# #         # Directive 3b: try compose period headers first
# #         period_headers = try_compose_period_headers(table_data, kept_cols)
# #         if period_headers:
# #             display_headers = [h if h else f"Column_{i}" for i,h in enumerate(period_headers)]
# #             raw_header_parts = [[h] if h else [] for h in display_headers]
# #             used_indices = list(range(min(4, len(table_data))))
# #         else:
# #             if not header_rows:
# #                 display_headers = [f"Column_{i}" for i in range(len(kept_cols))]
# #                 raw_header_parts = [[h] for h in display_headers]
# #             else:
# #                 usable = [idx for idx in header_rows]
# #                 raw_parts = [[normalize_cell(table_data[hr].get(f"col_{c}","")) for hr in usable] for c in kept_cols]
# #                 raw_header_parts = [[p for p in parts if p] for parts in raw_parts]
# #                 display_headers = [" ".join(parts) if parts else f"Column_{idx}" for idx, parts in enumerate(raw_header_parts)]
# #                 used_indices = sorted(list(set(usable)))

# #         display_headers = [(h[:max_len-3] + "..." if len(h) > max_len else h) for h in display_headers]
# #         return display_headers, raw_header_parts, table_title, used_indices

# #     # --- triage ---
# #     def is_contextually_valuable(self, data_rows: List[List[str]], table_type: str) -> bool:
# #         # Structural gate first (Directive 1)
# #         ts = tabularity_score(data_rows)
# #              # --- Semantic salvage for FRONT_PAGE “contact slabs” ---
# #        # Accept even if low tabularity when ≥2 universal anchors hit OR density is high.
# #         if table_type == TableType.FRONT_PAGE and ts < 0.35:
# #            flat = " ".join(" ".join(r) for r in (data_rows or []))
# #            lflat = flat.lower()
# #            hits = 0
# #            if self.EMAIL_RE.search(flat): hits += 1
# #            if self.URL_RE.search(flat):   hits += 1
# #            if self.PHONE_RE.search(flat): hits += 1
# #            hits += sum(1 for t in self.ROLE_TOKS if t in lflat)
# #            non_empty = sum(1 for r in data_rows for c in r if (c or "").strip())
# #            total     = sum(len(r) for r in data_rows) or 1
# #            density   = non_empty / total
# #            # Key:value pattern: at least 3 “label:” occurrences (Email:, Website:, Tel:, etc.)
# #            kv_labels = len(re.findall(r"[A-Za-z][A-Za-z\s]{1,30}:", flat))
# #            if hits >= 2 or density >= 0.70 or kv_labels >= 3:
# #                return True
# #            # else fall through → reject
# #         if table_type != TableType.FINANCIAL_STATEMENT and ts < 0.35:
# #            return False

        
        

# #         # Allow financial statements to be looser; still require at least one numeric column present
# #         if table_type == TableType.FINANCIAL_STATEMENT:
# #             if max((len(r) for r in data_rows), default=0) <= 1:
# #                 return False
# #             # at least one numeric-looking value in first row
# #             if not any(re.search(r"\d", c or "") for c in (data_rows[0] if data_rows else [])):
# #                 return False
# #         return True

# #     # --- main per-table ---
# #     def preprocess_table(self, table: Dict[str,Any], table_index: int, seen_hashes: set[str]) -> Tuple[Optional[Dict[str,Any]], Optional[Dict[str,Any]]]:
# #         page_no = table.get("page_number")
# #         table_data = table.get("table_data") or []
# #         if not table_data:
# #             return None, {"page_number": page_no, "table_index": table_index, "reason": "empty_table"}

# #         table_type = self.classify(table_data, page_no)

# #         # Detect whether extraction provided explicit columns
# #         has_col_keys = any(any(k.lower().startswith("col_") for k in row.keys()) for row in table_data)

# #         # column pruning: keep all for FRONT_PAGE; else drop empty columns
# #         cols = safe_get_max_cols(table_data) if has_col_keys else 1 
# #         empty_cols = []
# #         if table_type != TableType.FRONT_PAGE  and has_col_keys:
# #             for j in range(cols):
# #                 non_empty = sum(1 for row in table_data if normalize_cell(row.get(f"col_{j}", "")) not in {"","-"})
# #                 if len(table_data) and (non_empty/len(table_data)) < 0.30:
# #                     empty_cols.append(j)
# #         kept_cols = [j for j in range(cols) if j not in empty_cols] or list(range(cols))

# #         # header_rows = self.detect_header_rows(table_data, max_header_rows=self.cfg.max_header_rows)
# #         # display_headers, raw_header_parts, table_title, used_header_rows = self.extract_headers(table_data, header_rows, kept_cols, self.cfg.max_header_length)
# #         if has_col_keys:
# #            header_rows = self.detect_header_rows(table_data, max_header_rows=self.cfg.max_header_rows)
# #            display_headers, raw_header_parts, table_title, used_header_rows = self.extract_headers(
# #                table_data, header_rows, kept_cols, self.cfg.max_header_length
# #            )
# #            # Build data rows (omit header rows)
# #            raw_data_rows_initial = [
# #                [normalize_cell(row.get(f"col_{j}", "")) for j in kept_cols]
# #                for i, row in enumerate(table_data) if i not in set(used_header_rows)
# #            ]
# #         else:
# #            # Fallback: synthesize a single text column from row fields
# #            display_headers = ["Column_0"]
# #            raw_header_parts = [["Column_0"]]
# #            table_title = None
# #            used_header_rows = []
# #            raw_data_rows_initial = [[concat_row_fields(row)] for row in table_data]
       

# #         # Directive 2: only split FRONT_PAGE
# #         force_split = (table_type == TableType.FRONT_PAGE)
# #         data_rows: List[List[str]] = []
# #         for r in raw_data_rows_initial:
# #             has_newlines = any('\n' in x for x in r)  # FIX: real newline char
# #             if force_split and has_newlines and len(raw_data_rows_initial) < 10:
# #                 parts = [x.split('\n') for x in r]
# #                 max_lines = max(len(p) for p in parts)
# #                 for i in range(max_lines):
# #                     data_rows.append([normalize_cell(p[i]) if i < len(p) else "" for p in parts])
# #             else:
# #                 data_rows.append([normalize_cell(x) for x in r])

# #         # FRONT_PAGE semantic compact labels (Directive 4)
# #         if table_type == TableType.FRONT_PAGE and all(h.startswith("Column_") for h in display_headers):
# #             new_headers, new_data_rows, note = infer_semantic_headers_for_front_page(data_rows, display_headers)
# #             display_headers, data_rows = new_headers, new_data_rows

# #         # Context value gate
# #         if not self.is_contextually_valuable(data_rows, table_type):
# #             return None, {"page_number": page_no, "table_index": table_index, "reason":"skipped_non_valuable"}

# #         # pad
# #         target_cols = len(kept_cols)
# #         display_headers = pad_to_len(display_headers, target_cols)
# #         data_rows = [pad_to_len(r, target_cols) for r in data_rows if any(c.strip() for c in r)]

# #         # hash (Directive 5)
# #         t_hash = compute_table_hash(display_headers, data_rows, table_index, raw_header_parts, sample_rows=40, page_no=page_no, title=table_title)
# #         if self.cfg.dedupe and t_hash in seen_hashes:
# #             return None, {"page_number": page_no, "table_index": table_index, "reason":"duplicate"}
# #         seen_hashes.add(t_hash)

# #         # simple table-type confidence hints (Phase-1A)
# #         numeric_cols = 0
# #         for j in range(target_cols):
# #             vals = [r[j] for r in data_rows if j < len(r)]
# #             if not vals: 
# #                 continue
# #             num = sum(bool(re.fullmatch(r"[0-9,.\-()%₹]+", v or "")) for v in vals)
# #             if num >= max(1, int(0.5*len(vals))):
# #                 numeric_cols += 1
# #         period_detected = any(re.search(r"(19|20)\d{2}", h) or "ended" in h.lower() for h in display_headers)
# #         confidence = min(1.0, 0.15 + 0.35*(numeric_cols/max(1,target_cols)) + (0.25 if period_detected else 0))

# #         proc_table = {
# #             "page_number": page_no,
# #             "table_index": table_index,
# #             "table_type": table_type,
# #             "headers": display_headers,
# #             "data": data_rows,
# #             "table_title": table_title,
# #             "content_hash": t_hash,
# #             "confidence": round(confidence, 3),
# #         }

# #         # --- Phoenix Learning Hook (safe no-op if missing/disabled) ---
# #         try:
# #             from phoenix.memory import memory_layer
# #             # Minimal inline config so learning works without plumbed YAML:
# #             learn_cfg = {
# #                 "enabled": True,
# #                 "events_path": "out/review/learning_events.jsonl",
# #                 "patterns_path": "out/patterns/patterns.jsonl",
# #                 "min_gain": 0.08,
# #                 "candidate_limit": 6,
# #                 "exploration_rate": 0.15,
# #             }
# #             dossier_name = str(table.get("source_file", "")) or "unknown"
# #             proc_table, _learn_evt = memory_layer.apply(
# #                 table_dict=proc_table,
# #                 dossier_name=dossier_name,
# #                 config=learn_cfg
# #             )
# #         except Exception:
# #             # never break core flow due to learning layer
# #             pass
# #         # --- end Learning Hook ---

# #         return proc_table, None

# # # -------------------- Runner --------------------

# # def run_on_legacy(input_json_path: str) -> Dict[str, Any]:
# #     with open(input_json_path, "r", encoding="utf-8") as f:
# #         legacy = json.load(f)

# #     key = next(iter(legacy.keys()))
# #     meta = legacy[key]
# #     tables = meta.get("tables", [])

# #     surgeon = Surgeon(PreprocessorConfig())
# #     seen = set()
# #     processed, skipped = [], []

# #     # ensure learning attribution
# #     dossier_name = (meta.get("filename") or key or "unknown")
# #     for idx, t in enumerate(tables):
# #         t = dict(t)  # shallow copy to annotate
# #         t["source_file"] = dossier_name
# #         proc, skip = surgeon.preprocess_table(t, idx, seen)
# #         if proc:
# #             processed.append(proc)
# #         elif skip:
# #             skipped.append(skip)

# #     result = {
# #         "source": key,
# #         "filename": meta.get("filename"),
# #         "processed_count": len(processed),
# #         "skipped_count": len(skipped),
# #         "processed": processed,
# #         "skipped": skipped
# #     }
# #     return result

# # def main():
# #     import argparse, os
# #     ap = argparse.ArgumentParser(description="Phoenix Surgeon v6.2 — run on legacy JSON")
# #     ap.add_argument("--input", required=True, help="Path to legacy JSON file")
# #     ap.add_argument("--output", required=False, help="Output JSON path")
# #     args = ap.parse_args()

# #     res = run_on_legacy(args.input)
# #     out = args.output or (os.path.splitext(args.input)[0] + ".preproc_v6_2.json")
# #     with open(out, "w", encoding="utf-8") as f:
# #         json.dump(res, f, ensure_ascii=False, indent=2)
# #     print(json.dumps({
# #         "ok": True,
# #         "processed": res["processed_count"],
# #         "skipped": res["skipped_count"],
# #         "out": out
# #     }))

# # if __name__ == "__main__":
# #     main()



# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# Phoenix Surgeon v6.2 (Patched)
# Implements Phase-1A directives over legacy JSON extracts.

# Directives implemented:
# 1) Structural triage via tabularity_score (primary shield against prose-as-table).
# 2) Cease financial sentence-splitting (only split FRONT_PAGE).
# 3) Header fortification:
#    3a) Guard against row-label slabs (looks_like_row_label_band).
#    3b) Compose multirow period headers (try_compose_period_headers).
# 4) Front-page semantic promotion via controlled regex FRONT_FIELD_MAP.
# 5) Stronger hashing includes page_no and table_title (compute_table_hash).
# 6) Schema-agnostic salvage: synthesize single text column for rows lacking col_* or with sparse columns.

# I/O:
# - Input: legacy JSON in the form:
#   {
#     "<Key>": {
#       "filename": "...pdf",
#       "file_size_mb": 4.9,
#       "tables": [
#         { "page_number": 1, "table_data": [ {"col_0": "...", "col_1": "..."} , ... ] },
#         ...
#       ]
#     }
#   }
# - Output: gz/json written by caller; main() prints a short summary to stdout.
# """
# from __future__ import annotations

# import json, re, hashlib, sys
# from dataclasses import dataclass
# from typing import Any, Dict, List, Optional, Tuple

# # -------------------- helpers --------------------

# def normalize_cell(x: Any) -> str:
#     if x is None:
#         return ""
#     return re.sub(r"\s+", " ", str(x).strip())

# def safe_get_max_cols(table_data: List[Dict[str,str]]) -> int:
#     max_cols = 0
#     for row in table_data:
#         for k in row.keys():
#             if k.lower().startswith("col_"):
#                 try:
#                     i = int(k.split("_")[1])
#                     max_cols = max(max_cols, i + 1)
#                 except Exception:
#                     pass
#     return max_cols

# def pad_to_len(arr: List[str], n: int) -> List[str]:
#     return arr + [""] * (n - len(arr)) if len(arr) < n else arr[:n]

# # If a table row doesn't have col_* keys, concatenate string-like fields
# def concat_row_fields(row: Dict[str, Any]) -> str:
#     vals: List[str] = []
#     for k, v in row.items():
#         if k.startswith("IGNORE_WHEN_COPYING"):
#             # explicit noise markers in some dumps
#             continue
#         if v is None:
#             continue
#         if isinstance(v, (int, float)):
#             vals.append(str(v))
#         elif isinstance(v, str):
#             s = v.strip()
#             if s:
#                 vals.append(s)
#         else:
#             s = str(v).strip()
#             if s:
#                 vals.append(s)
#     return normalize_cell(" ".join(vals))

# # -------------------- Directives --------------------

# # (1) Structural triage
# def tabularity_score(rows: List[List[str]]) -> float:
#     if not rows:
#         return 0.0
#     cols = max((len(r) for r in rows), default=0)
#     if cols <= 1:
#         return 0.0

#     numeric_col_frac: List[float] = []
#     for j in range(cols):
#         vals = [normalize_cell(r[j]) for r in rows if j < len(r) and normalize_cell(r[j])]
#         if not vals:
#             numeric_col_frac.append(0.0)
#             continue
#         num = sum(bool(re.fullmatch(r"[0-9,.\-()%₹]+", v)) for v in vals)
#         frac = max(num/len(vals), 1 - num/len(vals))  # purity of type
#         numeric_col_frac.append(frac)

#     purity = sum(numeric_col_frac)/len(numeric_col_frac)

#     left = [normalize_cell(r[0]) for r in rows if r and normalize_cell(r[0])]
#     label_diversity = len(set(left))/max(1, len(left))
#     label_signal = 1 - min(1.0, label_diversity) * 0.5

#     width = 1.0 if (cols >= 2 and any(f > 0.7 for f in numeric_col_frac)) else 0.0

#     return 0.5*purity + 0.3*label_signal + 0.2*width

# # (3a) Row-label slab guard
# ROW_LABEL_TOKENS = {"assets","liabilities","equity","income","expenses","particulars","notes"}

# def looks_like_row_label_band(row: Dict[str,str]) -> bool:
#     cells = [normalize_cell(v) for k,v in row.items() if k.lower().startswith("col_")]
#     if not cells:
#         return False
#     left = normalize_cell(row.get("col_0",""))
#     other_text = " ".join(normalize_cell(row.get(f"col_{j}","")) for j in range(1, min(6, len(cells))))
#     left_heavy = len(left) > 20 and len(other_text) < 10
#     has_fin_labels = sum(t in left.lower() for t in ROW_LABEL_TOKENS) >= 2
#     return left_heavy and has_fin_labels

# # (3b) Multirow period header composer
# PERIOD_ROW_PATTS = [
#     re.compile(r"\b(as on|as at|as of|for the|nine months|half year|quarter|q[1-4]|h[12]|fy)\b", re.I),
#     re.compile(r"\b(ended|ending)\b", re.I),
#     re.compile(r"\b(20\d{2}|19\d{2})\b"),
#     re.compile(r"\b₹\s*(in|million|crore|lakh)\b", re.I),
#     re.compile(r"\bfy\s*\d{2}\s*[-/]\s*\d{2}\b", re.I),
# ]

# FP_TOKENS = (
#     "registrar", "rta", "brlm", "book running lead manager",
#     "global coordinator", "syndicate member", "contact person",
#     "company secretary", "compliance officer", "investor relations",
#     "email", "e-mail", "website", "web site", "tel", "telephone", "phone", "mobile", "mob.", "fax",
#     "link intime", "kfin", "kfintech", "bigshare", "mas services", "cameo",
#     "issue opens", "issue closes", "price band", "isin", "cin", "pan", "sebi"
# )

# def try_compose_period_headers(table_data: List[Dict[str,str]], kept_cols: List[int], search_rows: int = 4) -> Optional[List[str]]:
#     if not table_data or not kept_cols:
#         return None
#     rows = [[normalize_cell(table_data[r].get(f"col_{c}","")) 
#              for c in kept_cols] for r in range(min(search_rows, len(table_data)))]
#     if not rows:
#         return None
#     cols = list(zip(*rows))
#     headers: List[str] = []
#     any_hit = 0
#     for col in cols:
#         band = " ".join([t for t in col if t]).strip()
#         if any(p.search(band) for p in PERIOD_ROW_PATTS):
#             headers.append(re.sub(r"\s+"," ",band))
#             any_hit += 1
#         else:
#             headers.append("")
#     if any_hit >= max(2, len(headers)//3):
#         return headers
#     return None

# # (4) Controlled front-page labels
# FRONT_FIELD_MAP = {
#    r"\bregistrar(\s+to\s+the)?\s+(issue|offer)\b": "Registrar",
#    r"\b(lead\s+manager|merchant\s+banker|brlm)\b": "Lead Manager",
#    r"\b(contact\s+person|compliance\s+officer|company\s+secretary)\b": "Contact",
#    r"\bemail|e-?mail\b": "Email",
#    r"\bwebsite|web\s*site|url\b": "Website",
#    r"\btelephone|tel|phone|fax\b": "Telephone",
# }

# def infer_semantic_headers_for_front_page(data_rows: List[List[str]], original_headers: List[str]) -> Tuple[List[str], List[List[str]], str]:
#     if not data_rows:
#         return original_headers, data_rows, "No data rows to infer from."
#     first_row = data_rows[0]
#     tokens = " ".join([normalize_cell(c) for c in first_row]).lower()
#     labels = [name for pat,name in FRONT_FIELD_MAP.items() if re.search(pat, tokens)]
#     if 2 <= len(labels) <= len(original_headers):
#         new_headers = labels + [f"Field_{i}" for i in range(len(original_headers)-len(labels))]
#         return pad_to_len(new_headers, len(original_headers)), data_rows[1:], "Promoted compact front-page labels."
#     return original_headers, data_rows, "No compact label set found."

# # (5) Stronger hashing
# def compute_table_hash(
#     display_headers: List[str],
#     data_rows: List[List[str]],
#     table_index: int,
#     raw_header_parts: Optional[List[List[str]]] = None,
#     sample_rows: int = 40,
#     page_no: Optional[int] = None,
#     title: Optional[str] = None
# ) -> str:
#     h = hashlib.md5()
#     prefix = f"idx:{table_index}|pg:{page_no if page_no is not None else -1}|title:{(title or '')[:64]}|"
#     text = prefix + "".join(display_headers or [])
#     if raw_header_parts:
#         text += "".join("".join(p) for p in raw_header_parts)
#     h.update(text.encode("utf-8"))
#     for r in (data_rows or [])[:sample_rows]:
#         h.update("\u241F".join([normalize_cell(c) for c in r]).encode("utf-8"))
#     return h.hexdigest()

# # -------------------- Core Surgeon --------------------

# @dataclass
# class PreprocessorConfig:
#     min_content_threshold: float = 0.30
#     max_header_rows: int = 4
#     min_data_rows: int = 1
#     max_header_length: int = 100
#     dedupe: bool = True
#     output_compression: bool = False  # handled by caller
#     prose_split_char_threshold: int = 250

# HEADER_HINTS  = [
#     'total','amount','year','period','march','december','fy','q1','q2','q3','q4','half year','h1','h2',
#     'as on','as at','as of','revenue','assets','liabilities','equity','cash flow','profit','loss',
#     'income','expenses','notes','particulars','sr. no.','details','description','metric','₹ in crore','₹ in lakh'
# ]
# FINANCIAL_STATEMENT_KEYWORDS = [
#     'balance sheet','profit and loss','statement of profit and loss','cash flow','statement of operations',
#     'consolidated financial','shareholder equity','financial position','notes to accounts'
# ]
# FRONT_PAGE_KEYWORDS = [
#     'offer','issue','price band','registrar','lead manager','merchant banker','rta','ipo','equity share',
#     'fresh issue','offer for sale','contact person','telephone','tel','phone','fax','website','email','e-mail'
# ]

# class TableType:
#     FRONT_PAGE = "FRONT_PAGE"
#     FINANCIAL_STATEMENT = "FINANCIAL_STATEMENT"
#     GENERIC = "GENERIC"

# class Surgeon:
#     def __init__(self, cfg: PreprocessorConfig):
#         self.cfg = cfg
#         # universal anchors for front-page “contact slab” salvage
#         self.EMAIL_RE  = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
#         self.URL_RE    = re.compile(r"(?:(?:https?://)?(?:www\.)?[A-Za-z0-9.-]+\.[A-Za-z]{2,}(?:/[^\s]*)?)", re.I)
#         self.PHONE_RE  = re.compile(r"(?:\+?\d[\d\s\-]{7,}\d)")
#         self.ROLE_TOKS = (
#             "registrar","lead manager","merchant banker","brlm",
#             "contact person","compliance officer","company secretary",
#             "telephone","tel","phone","fax","website","email","e-mail",
#             "bid","issue opens","issue closes","anchor investor","rta"
#         )

#     # --- classification ---
#     def classify(self, table_data: List[Dict[str, Any]], page_number: Optional[int]) -> str:
#         if page_number is not None and page_number <= 2:
#             return TableType.FRONT_PAGE
#         all_text = " ".join(normalize_cell(v).lower() for row in table_data for v in row.values())
#         if any(k in all_text for k in FINANCIAL_STATEMENT_KEYWORDS):
#             return TableType.FINANCIAL_STATEMENT
#         # broadened FP signals so we can classify FP-like slabs beyond page 1–2
#         if any(k in all_text for k in FRONT_PAGE_KEYWORDS) or any(rt in all_text for rt in self.ROLE_TOKS):
#             return TableType.FRONT_PAGE
#         return TableType.GENERIC

#     # --- header helpers ---
#     def detect_header_rows(self, table_data: List[Dict[str,str]], max_header_rows: int) -> List[int]:
#         def score_header_row(row: Dict[str,str]) -> float:
#             hits, labels, numerics, unique_labels = 0, 0, 0, set()
#             for _, v in row.items():
#                 s = normalize_cell(v); sl = s.lower()
#                 if not s:
#                     continue
#                 if any(h in sl for h in HEADER_HINTS):
#                     hits += 2
#                 if re.search(r"\b(19\d{2}|20\d{2})\b", sl) or re.search(r"^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)", sl, re.I):
#                     hits += 1
#                 for t in s.split():
#                     if t.isalpha():
#                         labels += 1; unique_labels.add(t.lower())
#                     elif re.fullmatch(r"[\d,.%₹-]+", t):
#                         numerics += 1
#             ratio = labels / (labels + numerics) if (labels + numerics) > 0 else 0.0
#             return hits + 0.5 * ratio - (0.1 if hits == 0 and len(unique_labels) <= 1 else 0)

#         header_indices: List[int] = []
#         for i in range(min(max_header_rows, len(table_data))):
#             row = table_data[i]
#             if looks_like_row_label_band(row):
#                 continue  # Directive 3a
#             sc = score_header_row(row)
#             numerics = sum(bool(re.search(r"[\d,.%₹-]", normalize_cell(v))) for v in row.values())
#             labels = sum(bool(re.search(r"[A-Za-z]", normalize_cell(v))) for v in row.values())
#             nr = numerics / max(1, (numerics + labels))
#             if sc >= 0.5 and nr < 0.25:
#                 header_indices.append(i)
#         return header_indices

#     # --- extraction ---
#     def extract_headers(
#         self,
#         table_data: List[Dict[str,str]],
#         header_rows: List[int],
#         kept_cols: List[int],
#         max_len: int
#     ) -> Tuple[List[str], List[List[str]], Optional[str], List[int]]:
#         table_title = None
#         raw_header_parts: List[List[str]] = []
#         used_indices: List[int] = []

#         # Directive 3b: try compose period headers first
#         period_headers = try_compose_period_headers(table_data, kept_cols)
#         if period_headers:
#             display_headers = [h if h else f"Column_{i}" for i,h in enumerate(period_headers)]
#             raw_header_parts = [[h] if h else [] for h in display_headers]
#             used_indices = list(range(min(4, len(table_data))))
#         else:
#             if not header_rows:
#                 display_headers = [f"Column_{i}" for i in range(len(kept_cols))]
#                 raw_header_parts = [[h] for h in display_headers]
#             else:
#                 usable = [idx for idx in header_rows]
#                 raw_parts = [[normalize_cell(table_data[hr].get(f"col_{c}","")) for hr in usable] for c in kept_cols]
#                 raw_header_parts = [[p for p in parts if p] for parts in raw_parts]
#                 display_headers = [" ".join(parts) if parts else f"Column_{idx}" for idx, parts in enumerate(raw_header_parts)]
#                 used_indices = sorted(list(set(usable)))

#         display_headers = [(h[:max_len-3] + "..." if len(h) > max_len else h) for h in display_headers]
#         return display_headers, raw_header_parts, table_title, used_indices

#     # --- triage ---
#     def is_contextually_valuable(self, data_rows: List[List[str]], table_type: str) -> bool:
#         # Structural gate first (Directive 1)
#         ts = tabularity_score(data_rows)

#         # Semantic salvage for FRONT_PAGE “contact slabs”
#         # Accept even if low tabularity when ≥2 universal anchors hit OR density is high OR ≥3 key:value labels.
#         if table_type == TableType.FRONT_PAGE and ts < 0.35:
#             flat = " ".join(" ".join(r) for r in (data_rows or []))
#             lflat = flat.lower()
#             hits = 0
#             if self.EMAIL_RE.search(flat): hits += 1
#             if self.URL_RE.search(flat):   hits += 1
#             if self.PHONE_RE.search(flat): hits += 1
#             hits += sum(1 for t in self.ROLE_TOKS if t in lflat)
#             non_empty = sum(1 for r in data_rows for c in r if (c or "").strip())
#             total     = sum(len(r) for r in data_rows) or 1
#             density   = non_empty / total
#             kv_labels = len(re.findall(r"[A-Za-z][A-Za-z\s]{1,30}:", flat))
#             if hits >= 2 or density >= 0.70 or kv_labels >= 3:
#                 return True  # accept semantic value despite low tabularity

#         if table_type != TableType.FINANCIAL_STATEMENT and ts < 0.35:
#             return False

#         # Allow financial statements to be looser; still require at least one numeric column present
#         if table_type == TableType.FINANCIAL_STATEMENT:
#             if max((len(r) for r in data_rows), default=0) <= 1:
#                 return False
#             # at least one numeric-looking value in first row
#             if not any(re.search(r"\d", c or "") for c in (data_rows[0] if data_rows else [])):
#                 return False
#         return True

#     # --- main per-table ---
#     def preprocess_table(
#         self,
#         table: Dict[str,Any],
#         table_index: int,
#         seen_hashes: set[str]
#     ) -> Tuple[Optional[Dict[str,Any]], Optional[Dict[str,Any]]]:

#         page_no = table.get("page_number")
#         table_data = table.get("table_data") or []
#         if not table_data:
#             return None, {"page_number": page_no, "table_index": table_index, "reason": "empty_table"}

#         table_type = self.classify(table_data, page_no)

#         # --- Schema-agnostic salvage: synthesize single text column when col_* are missing/sparse ---
#         salvage = False
#         raw_keys = list(table_data[0].keys()) if table_data else []
#         col_keys = [k for k in raw_keys if re.fullmatch(r"(?i)col_\d+", k)]
#         nonempty_colvals = sum(
#             1 for row in table_data for k in col_keys
#             if normalize_cell(row.get(k, "")) not in {"", "-"}
#         )
#         texty_keys = [k for k in raw_keys if k not in col_keys]

#         def row_to_text(row: Dict[str, Any]) -> str:
#             return concat_row_fields(row)

#         display_headers: List[str] = []
#         raw_header_parts: Optional[List[List[str]]] = None
#         used_header_rows: List[int] = []
#         kept_cols: List[int] = []

#         if (len(col_keys) == 0) or (nonempty_colvals <= 1) or (texty_keys and nonempty_colvals == 0):
#             cat_rows = [[row_to_text(r)] for r in table_data if row_to_text(r)]
#             cat_rows = [r for r in cat_rows if len(r[0]) >= 40]  # discard trivial lines
#             if cat_rows:
#                 keys_lc = {k.strip().lower() for k in raw_keys}
#                 blob = " ".join(r[0].lower() for r in cat_rows[:6])
#                 is_fp = any(tok in blob for tok in FP_TOKENS) or bool(re.search(r'@|https?://|www\.', blob))
#                 has_def = {"term", "description"} <= keys_lc

#                 if is_fp:
#                     table_type = TableType.FRONT_PAGE
#                     display_headers = ["Contact Block"]
#                 elif has_def and len(blob) >= 120:
#                     display_headers = ["Definition"]
#                 else:
#                     display_headers = ["Text"]

#                 kept_cols = [0]
#                 used_header_rows = []
#                 raw_header_parts = None
#                 data_rows = cat_rows
#                 salvage = True

#         # If not salvaged, follow the normal pipeline
#         if not salvage:
#             has_col_keys = len(col_keys) > 0
#             cols = safe_get_max_cols(table_data) if has_col_keys else 1
#             empty_cols: List[int] = []
#             if table_type != TableType.FRONT_PAGE and has_col_keys:
#                 for j in range(cols):
#                     non_empty = sum(1 for row in table_data if normalize_cell(row.get(f"col_{j}", "")) not in {"","-"})
#                     if len(table_data) and (non_empty/len(table_data)) < 0.30:
#                         empty_cols.append(j)
#             kept_cols = [j for j in range(cols) if j not in empty_cols] or list(range(cols))

#             if has_col_keys:
#                 header_rows = self.detect_header_rows(table_data, max_header_rows=self.cfg.max_header_rows)
#                 display_headers, raw_header_parts, table_title, used_header_rows = self.extract_headers(
#                     table_data, header_rows, kept_cols, self.cfg.max_header_length
#                 )
#                 # Build data rows (omit header rows)
#                 raw_data_rows_initial = [[normalize_cell(row.get(f"col_{j}", "")) for j in kept_cols]
#                                  for i, row in enumerate(table_data) if i not in set(used_header_rows)]
#             else:
#                 # Fallback (rare): synthesize a single text column
#                 display_headers = ["Column_0"]
#                 raw_header_parts = [["Column_0"]]
#                 table_title = None
#                 used_header_rows = []
#                 raw_data_rows_initial = [[concat_row_fields(row)] for row in table_data]

#             # Directive 2: only split FRONT_PAGE; use real newline char
#             force_split = (table_type == TableType.FRONT_PAGE)
#             data_rows: List[List[str]] = []
#             for r in raw_data_rows_initial:
#                 has_newlines = any('\n' in x for x in r)
#                 if force_split and has_newlines and len(raw_data_rows_initial) < 10:
#                     parts = [x.split('\n') for x in r]
#                     max_lines = max(len(p) for p in parts)
#                     for i in range(max_lines):
#                         data_rows.append([normalize_cell(p[i]) if i < len(p) else "" for p in parts])
#                 else:
#                     data_rows.append([normalize_cell(x) for x in r])

#             # FRONT_PAGE semantic compact labels (Directive 4)
#             if table_type == TableType.FRONT_PAGE and all(h.startswith("Column_") for h in display_headers):
#                 new_headers, new_data_rows, _note = infer_semantic_headers_for_front_page(data_rows, display_headers)
#                 display_headers, data_rows = new_headers, new_data_rows

#         # If salvaged, data_rows already set; else it's set by the normal path above
#         if salvage:
#             table_title = None  # not available
#             # data_rows set earlier
#         # Context value gate
#         if not self.is_contextually_valuable(data_rows, table_type):
#             return None, {"page_number": page_no, "table_index": table_index, "reason":"skipped_non_valuable"}

#         # pad
#         target_cols = len(kept_cols) if kept_cols else 1
#         display_headers = pad_to_len(display_headers, target_cols)
#         data_rows = [pad_to_len(r, target_cols) for r in data_rows if any((c or "").strip() for c in r)]

#         # hash (Directive 5)
#         t_hash = compute_table_hash(display_headers, data_rows, table_index, raw_header_parts, sample_rows=40, page_no=page_no, title=table_title)
#         if self.cfg.dedupe and t_hash in seen_hashes:
#             return None, {"page_number": page_no, "table_index": table_index, "reason":"duplicate"}
#         seen_hashes.add(t_hash)

#         # simple table-type confidence hints (Phase-1A)
#         numeric_cols = 0
#         for j in range(target_cols):
#             vals = [r[j] for r in data_rows if j < len(r)]
#             if not vals:
#                 continue
#             num = sum(bool(re.fullmatch(r"[0-9,.\-()%₹]+", v or "")) for v in vals)
#             if num >= max(1, int(0.5*len(vals))):
#                 numeric_cols += 1
#         period_detected = any(re.search(r"(19|20)\d{2}", h) or "ended" in h.lower() for h in display_headers)
#         confidence = min(1.0, 0.15 + 0.35*(numeric_cols/max(1,target_cols)) + (0.25 if period_detected else 0))

#                 # --- PRE-GATE: hand table to Policy (bandit) for optional salvage ---
#         proc_table_peek = {
#             "page_number": page_no,
#             "table_index": table_index,
#             "table_type": table_type,
#             "headers": display_headers,
#             "data": data_rows,
#             "table_title": table_title,
#         }
#         accept_override = False
#         try:
#             from phoenix.memory import memory_layer
#             learn_cfg = {
#                 "enabled": True,
#                 "events_path": "out/review/learning_events.jsonl",
#                 "patterns_path": "out/patterns/patterns.jsonl",
#                 "exploration_rate": 0.15,
#             }
#             dossier_name = str(table.get("source_file","")) or "unknown"
#             proc_table_peek, decision = memory_layer.apply(proc_table_peek, dossier_name, learn_cfg, stage="pre_gate")
#             accept_override = bool(decision.get("accept_override", False))
#             display_headers = proc_table_peek.get("headers", display_headers)
#             data_rows = proc_table_peek.get("data", data_rows)
#         except Exception:
#             pass

#         # --- Honest context gate (policy may override) ---
#         if not accept_override and not self.is_contextually_valuable(data_rows, table_type):
#             return None, {"page_number": page_no, "table_index": table_index, "reason": "skipped_non_valuable"}

#         # pad / normalize
#         target_cols = len(kept_cols) if kept_cols else 1
#         display_headers = pad_to_len(display_headers, target_cols)
#         data_rows = [pad_to_len(r, target_cols) for r in data_rows if any((c or "").strip() for c in r)]

#         # hash + dedupe
#         t_hash = compute_table_hash(display_headers, data_rows, table_index, raw_header_parts,
#                                     sample_rows=40, page_no=page_no, title=table_title)
#         if self.cfg.dedupe and t_hash in seen_hashes:
#             return None, {"page_number": page_no, "table_index": table_index, "reason": "duplicate"}
#         seen_hashes.add(t_hash)

#         # confidence (simple, unchanged)
#         numeric_cols = 0
#         for j in range(target_cols):
#             vals = [r[j] for r in data_rows if j < len(r)]
#             if not vals:
#                 continue
#             num = sum(bool(re.fullmatch(r"[0-9,.\-()%₹]+", v or "")) for v in vals)
#             if num >= max(1, int(0.5*len(vals))):
#                 numeric_cols += 1
#         period_detected = any(re.search(r"(19|20)\d{2}", h) or "ended" in h.lower() for h in display_headers)
#         confidence = min(1.0, 0.15 + 0.35*(numeric_cols/max(1,target_cols)) + (0.25 if period_detected else 0))

#         proc_table = {
#             "page_number": page_no,
#             "table_index": table_index,
#             "table_type": table_type,
#             "headers": display_headers,
#             "data": data_rows,
#             "table_title": table_title,
#             "content_hash": t_hash,
#             "confidence": round(confidence, 3),
#         }

#         try:
#             from phoenix.memory import memory_layer
#             learn_cfg = {
#                 "enabled": True,
#                 "events_path": "out/review/learning_events.jsonl",
#                 "patterns_path": "out/patterns/patterns.jsonl",
#                 "exploration_rate": 0.15,
#             }
#             dossier_name = str(table.get("source_file","")) or "unknown"
#             proc_table, _ = memory_layer.apply(proc_table, dossier_name, learn_cfg, stage="post_gate")
#         except Exception:
#             pass

#         return proc_table, None
# # -------------------- Runner --------------------

# def run_on_legacy(input_json_path: str) -> Dict[str, Any]:
#     with open(input_json_path, "r", encoding="utf-8") as f:
#         legacy = json.load(f)

#     key = next(iter(legacy.keys()))
#     meta = legacy[key]
#     tables = meta.get("tables", [])

#     surgeon = Surgeon(PreprocessorConfig())
#     seen = set()
#     processed, skipped = [], []

#     # ensure learning attribution
#     dossier_name = (meta.get("filename") or key or "unknown")
#     for idx, t in enumerate(tables):
#         t = dict(t)  # shallow copy to annotate
#         t["source_file"] = dossier_name
#         proc, skip = surgeon.preprocess_table(t, idx, seen)
#         if proc:
#             processed.append(proc)
#         elif skip:
#             skipped.append(skip)

#     result = {
#         "source": key,
#         "filename": meta.get("filename"),
#         "processed_count": len(processed),
#         "skipped_count": len(skipped),
#         "processed": processed,
#         "skipped": skipped
#     }
#     return result

# def main():
#     import argparse, os
#     ap = argparse.ArgumentParser(description="Phoenix Surgeon v6.2 — run on legacy JSON")
#     ap.add_argument("--input", required=True, help="Path to legacy JSON file")
#     ap.add_argument("--output", required=False, help="Output JSON path")
#     args = ap.parse_args()

#     res = run_on_legacy(args.input)
#     out = args.output or (os.path.splitext(args.input)[0] + ".preproc_v6_2.json")
#     with open(out, "w", encoding="utf-8") as f:
#         json.dump(res, f, ensure_ascii=False, indent=2)
#     print(json.dumps({
#         "ok": True,
#         "processed": res["processed_count"],
#         "skipped": res["skipped_count"],
#         "out": out
#     }))

# if __name__ == "__main__":
#     main()

































































# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# Phoenix Surgeon v6.2 (Patched)
# Implements Phase-1A directives over legacy JSON extracts.

# Directives implemented:
# 1) Structural triage via tabularity_score (primary shield against prose-as-table).
# 2) Cease financial sentence-splitting (only split FRONT_PAGE).
# 3) Header fortification:
#    3a) Guard against row-label slabs (looks_like_row_label_band).
#    3b) Compose multirow period headers (try_compose_period_headers).
# 4) Front-page semantic promotion via controlled regex FRONT_FIELD_MAP.
# 5) Stronger hashing includes page_no and table_title (compute_table_hash).
# 6) Schema-agnostic salvage: synthesize single text column for rows lacking col_* or with sparse columns.
# """

# from __future__ import annotations

# import json, re, hashlib
# from dataclasses import dataclass
# from typing import Any, Dict, List, Optional, Tuple

# # -------------------- helpers --------------------

# def normalize_cell(x: Any) -> str:
#     if x is None:
#         return ""
#     return re.sub(r"\s+", " ", str(x).strip())

# def safe_get_max_cols(table_data: List[Dict[str,str]]) -> int:
#     max_cols = 0
#     for row in table_data:
#         for k in row.keys():
#             if k.lower().startswith("col_"):
#                 try:
#                     i = int(k.split("_")[1])
#                     max_cols = max(max_cols, i + 1)
#                 except Exception:
#                     pass
#     return max_cols

# def pad_to_len(arr: List[str], n: int) -> List[str]:
#     return arr + [""] * (n - len(arr)) if len(arr) < n else arr[:n]

# # If a table row doesn't have col_* keys, concatenate string-like fields
# def concat_row_fields(row: Dict[str, Any]) -> str:
#     vals: List[str] = []
#     for k, v in row.items():
#         if k.startswith("IGNORE_WHEN_COPYING"):
#             continue
#         if v is None:
#             continue
#         if isinstance(v, (int, float)):
#             vals.append(str(v))
#         elif isinstance(v, str):
#             s = v.strip()
#             if s:
#                 vals.append(s)
#         else:
#             s = str(v).strip()
#             if s:
#                 vals.append(s)
#     return normalize_cell(" ".join(vals))

# # -------------------- Directives --------------------

# # (1) Structural triage
# def tabularity_score(rows: List[List[str]]) -> float:
#     if not rows:
#         return 0.0
#     cols = max((len(r) for r in rows), default=0)
#     if cols <= 1:
#         return 0.0

#     numeric_col_frac: List[float] = []
#     for j in range(cols):
#         vals = [normalize_cell(r[j]) for r in rows if j < len(r) and normalize_cell(r[j])]
#         if not vals:
#             numeric_col_frac.append(0.0)
#             continue
#         num = sum(bool(re.fullmatch(r"[0-9,.\-()%₹]+", v)) for v in vals)
#         frac = max(num/len(vals), 1 - num/len(vals))  # purity of type
#         numeric_col_frac.append(frac)

#     purity = sum(numeric_col_frac)/len(numeric_col_frac)
#     left = [normalize_cell(r[0]) for r in rows if r and normalize_cell(r[0])]
#     label_diversity = len(set(left))/max(1, len(left))
#     label_signal = 1 - min(1.0, label_diversity) * 0.5
#     width = 1.0 if (cols >= 2 and any(f > 0.7 for f in numeric_col_frac)) else 0.0
#     return 0.5*purity + 0.3*label_signal + 0.2*width

# # (3a) Row-label slab guard
# ROW_LABEL_TOKENS = {"assets","liabilities","equity","income","expenses","particulars","notes"}

# def looks_like_row_label_band(row: Dict[str,str]) -> bool:
#     cells = [normalize_cell(v) for k,v in row.items() if k.lower().startswith("col_")]
#     if not cells:
#         return False
#     left = normalize_cell(row.get("col_0",""))
#     other_text = " ".join(normalize_cell(row.get(f"col_{j}","")) for j in range(1, min(6, len(cells))))
#     left_heavy = len(left) > 20 and len(other_text) < 10
#     has_fin_labels = sum(t in left.lower() for t in ROW_LABEL_TOKENS) >= 2
#     return left_heavy and has_fin_labels

# # (3b) Multirow period header composer
# PERIOD_ROW_PATTS = [
#     re.compile(r"\b(as on|as at|as of|for the|nine months|half year|quarter|q[1-4]|h[12]|fy)\b", re.I),
#     re.compile(r"\b(ended|ending)\b", re.I),
#     re.compile(r"\b(20\d{2}|19\d{2})\b"),
#     re.compile(r"\b₹\s*(in|million|crore|lakh)\b", re.I),
#     re.compile(r"\bfy\s*\d{2}\s*[-/]\s*\d{2}\b", re.I),
# ]

# FP_TOKENS = (
#     "registrar", "rta", "brlm", "book running lead manager",
#     "global coordinator", "syndicate member", "contact person",
#     "company secretary", "compliance officer", "investor relations",
#     "email", "e-mail", "website", "web site", "tel", "telephone", "phone", "mobile", "mob.", "fax",
#     "link intime", "kfin", "kfintech", "bigshare", "mas services", "cameo",
#     "issue opens", "issue closes", "price band", "isin", "cin", "pan", "sebi"
# )

# def try_compose_period_headers(table_data: List[Dict[str,str]], kept_cols: List[int], search_rows: int = 4) -> Optional[List[str]]:
#     if not table_data or not kept_cols:
#         return None
#     rows = [[normalize_cell(table_data[r].get(f"col_{c}","")) 
#              for c in kept_cols] for r in range(min(search_rows, len(table_data)))]
#     if not rows:
#         return None
#     cols = list(zip(*rows))
#     headers: List[str] = []
#     any_hit = 0
#     for col in cols:
#         band = " ".join([t for t in col if t]).strip()
#         if any(p.search(band) for p in PERIOD_ROW_PATTS):
#             headers.append(re.sub(r"\s+"," ",band))
#             any_hit += 1
#         else:
#             headers.append("")
#     if any_hit >= max(2, len(headers)//3):
#         return headers
#     return None

# # (4) Controlled front-page labels
# FRONT_FIELD_MAP = {
#    r"\bregistrar(\s+to\s+the)?\s+(issue|offer)\b": "Registrar",
#    r"\b(lead\s+manager|merchant\s+banker|brlm)\b": "Lead Manager",
#    r"\b(contact\s+person|compliance\s+officer|company\s+secretary)\b": "Contact",
#    r"\bemail|e-?mail\b": "Email",
#    r"\bwebsite|web\s*site|url\b": "Website",
#    r"\btelephone|tel|phone|fax\b": "Telephone",
# }

# def infer_semantic_headers_for_front_page(data_rows: List[List[str]], original_headers: List[str]) -> Tuple[List[str], List[List[str]], str]:
#     if not data_rows:
#         return original_headers, data_rows, "No data rows to infer from."
#     first_row = data_rows[0]
#     tokens = " ".join([normalize_cell(c) for c in first_row]).lower()
#     labels = [name for pat,name in FRONT_FIELD_MAP.items() if re.search(pat, tokens)]
#     if 2 <= len(labels) <= len(original_headers):
#         new_headers = labels + [f"Field_{i}" for i in range(len(original_headers)-len(labels))]
#         return pad_to_len(new_headers, len(original_headers)), data_rows[1:], "Promoted compact front-page labels."
#     return original_headers, data_rows, "No compact label set found."

# # (5) Stronger hashing
# def compute_table_hash(
#     display_headers: List[str],
#     data_rows: List[List[str]],
#     table_index: int,
#     raw_header_parts: Optional[List[List[str]]] = None,
#     sample_rows: int = 40,
#     page_no: Optional[int] = None,
#     title: Optional[str] = None
# ) -> str:
#     h = hashlib.md5()
#     prefix = f"idx:{table_index}|pg:{page_no if page_no is not None else -1}|title:{(title or '')[:64]}|"
#     text = prefix + "".join(display_headers or [])
#     if raw_header_parts:
#         text += "".join("".join(p) for p in raw_header_parts)
#     h.update(text.encode("utf-8"))
#     for r in (data_rows or [])[:sample_rows]:
#         h.update("\u241F".join([normalize_cell(c) for c in r]).encode("utf-8"))
#     return h.hexdigest()

# # -------------------- Core Surgeon --------------------

# @dataclass
# class PreprocessorConfig:
#     min_content_threshold: float = 0.30
#     max_header_rows: int = 4
#     min_data_rows: int = 1
#     max_header_length: int = 100
#     dedupe: bool = True
#     output_compression: bool = False  # handled by caller
#     prose_split_char_threshold: int = 250

# HEADER_HINTS  = [
#     'total','amount','year','period','march','december','fy','q1','q2','q3','q4','half year','h1','h2',
#     'as on','as at','as of','revenue','assets','liabilities','equity','cash flow','profit','loss',
#     'income','expenses','notes','particulars','sr. no.','details','description','metric','₹ in crore','₹ in lakh'
# ]
# FINANCIAL_STATEMENT_KEYWORDS = [
#     'balance sheet','profit and loss','statement of profit and loss','cash flow','statement of operations',
#     'consolidated financial','shareholder equity','financial position','notes to accounts'
# ]
# FRONT_PAGE_KEYWORDS = [
#     'offer','issue','price band','registrar','lead manager','merchant banker','rta','ipo','equity share',
#     'fresh issue','offer for sale','contact person','telephone','tel','phone','fax','website','email','e-mail'
# ]

# class TableType:
#     FRONT_PAGE = "FRONT_PAGE"
#     FINANCIAL_STATEMENT = "FINANCIAL_STATEMENT"
#     GENERIC = "GENERIC"

# class Surgeon:
#     def __init__(self, cfg: PreprocessorConfig):
#         self.cfg = cfg
#         # universal anchors for front-page “contact slab” salvage
#         self.EMAIL_RE  = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
        
#         self.URL_RE    = re.compile(r"(?:(?:https?://)?(?:www\.)?[A-Za-z0-9.-]+\.[A-Za-z]{2,}(?:/[^\s]*)?)", re.I)
#         self.PHONE_RE  = re.compile(r"(?:\+?\d[\d\s\-]{7,}\d)")
#         self.ROLE_TOKS = (
#             "registrar","lead manager","merchant banker","brlm",
#             "contact person","compliance officer","company secretary",
#             "telephone","tel","phone","fax","website","email","e-mail",
#             "bid","issue opens","issue closes","anchor investor","rta"
#         )
    

#     def _anchor_kv_stats(self, text: str) -> tuple[int,int,int]:
#         """Return (anchors, kv_labels, role_hits) computed from a flat lowercase blob."""
#         anchors = 0
#         anchors += 1 if self.EMAIL_RE.search(text) else 0
#         anchors += 1 if self.URL_RE.search(text)   else 0
#         anchors += 1 if self.PHONE_RE.search(text) else 0
#         kv_labels = len(re.findall(r"[A-Za-z][A-Za-z\s]{1,30}:", text))
#         role_hits = sum(1 for t in self.ROLE_TOKS if t in text)
#         return anchors, kv_labels, role_hits

#     # --- classification ---
#     def classify(self, table_data: List[Dict[str, Any]], page_number: Optional[int]) -> str:
#         # Early pages are front-page by default
#         if page_number is not None and page_number <= 2:
#             return TableType.FRONT_PAGE

#         all_text_raw = " ".join(str(v) for row in table_data for v in row.values())
#         all_text = normalize_cell(all_text_raw).lower()

#         # Financial statements get priority classification
#         if any(k in all_text for k in FINANCIAL_STATEMENT_KEYWORDS):
#             return TableType.FINANCIAL_STATEMENT

#         anchors, kv, roles = self._anchor_kv_stats(all_text)
#         fp_words = any(k in all_text for k in FRONT_PAGE_KEYWORDS) or roles > 0

#         # Allow FRONT_PAGE up to p3 with moderate signals
#         if page_number is not None and page_number <= 3:
#             if anchors >= 2 or kv >= 3 or roles >= 2:
#                 return TableType.FRONT_PAGE

#         # Beyond p3: require strong *structured* front-page signals
#         if anchors >= 3 and kv >= 3:
#             return TableType.FRONT_PAGE

#         # Soft hint words alone shouldn’t flip later pages to FRONT_PAGE
#         if fp_words:
#             return TableType.GENERIC

#         return TableType.GENERIC


#     # --- header helpers ---
#     def detect_header_rows(self, table_data: List[Dict[str,str]], max_header_rows: int) -> List[int]:
#         def score_header_row(row: Dict[str,str]) -> float:
#             hits, labels, numerics, unique_labels = 0, 0, 0, set()
#             for _, v in row.items():
#                 s = normalize_cell(v); sl = s.lower()
#                 if not s:
#                     continue
#                 if any(h in sl for h in HEADER_HINTS):
#                     hits += 2
#                 if re.search(r"\b(19\d{2}|20\d{2})\b", sl) or re.search(r"^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)", sl, re.I):
#                     hits += 1
#                 for t in s.split():
#                     if t.isalpha():
#                         labels += 1; unique_labels.add(t.lower())
#                     elif re.fullmatch(r"[\d,.%₹-]+", t):
#                         numerics += 1
#             ratio = labels / (labels + numerics) if (labels + numerics) > 0 else 0.0
#             return hits + 0.5 * ratio - (0.1 if hits == 0 and len(unique_labels) <= 1 else 0)

#         header_indices: List[int] = []
#         for i in range(min(max_header_rows, len(table_data))):
#             row = table_data[i]
#             if looks_like_row_label_band(row):
#                 continue  # Directive 3a
#             sc = score_header_row(row)
#             numerics = sum(bool(re.search(r"[\d,.%₹-]", normalize_cell(v))) for v in row.values())
#             labels = sum(bool(re.search(r"[A-Za-z]", normalize_cell(v))) for v in row.values())
#             nr = numerics / max(1, (numerics + labels))
#             if sc >= 0.5 and nr < 0.25:
#                 header_indices.append(i)
#         return header_indices

#     # --- extraction ---
#     def extract_headers(
#         self,
#         table_data: List[Dict[str,str]],
#         header_rows: List[int],
#         kept_cols: List[int],
#         max_len: int
#     ) -> Tuple[List[str], List[List[str]], Optional[str], List[int]]:
#         table_title = None
#         raw_header_parts: List[List[str]] = []
#         used_indices: List[int] = []

#         # Directive 3b: try compose period headers first
#         period_headers = try_compose_period_headers(table_data, kept_cols)
#         if period_headers:
#             display_headers = [h if h else f"Column_{i}" for i,h in enumerate(period_headers)]
#             raw_header_parts = [[h] if h else [] for h in display_headers]
#             used_indices = list(range(min(4, len(table_data))))
#         else:
#             if not header_rows:
#                 display_headers = [f"Column_{i}" for i in range(len(kept_cols))]
#                 raw_header_parts = [[h] for h in display_headers]
#             else:
#                 usable = [idx for idx in header_rows]
#                 raw_parts = [[normalize_cell(table_data[hr].get(f"col_{c}","")) for hr in usable] for c in kept_cols]
#                 raw_header_parts = [[p for p in parts if p] for parts in raw_parts]
#                 display_headers = [" ".join(parts) if parts else f"Column_{idx}" for idx, parts in enumerate(raw_header_parts)]
#                 used_indices = sorted(list(set(usable)))

#         display_headers = [(h[:max_len-3] + "..." if len(h) > max_len else h) for h in display_headers]
#         return display_headers, raw_header_parts, table_title, used_indices

#     # --- triage ---
#     def is_contextually_valuable(self, data_rows: List[List[str]], table_type: str, page_no: Optional[int] = None) -> bool:
#         ts = tabularity_score(data_rows)

#         # FRONT_PAGE salvage: early pages lenient, later pages strict
#         if table_type == TableType.FRONT_PAGE and ts < 0.35:
#             flat = " ".join(" ".join(r) for r in (data_rows or []))
#             lflat = flat.lower()
#             hits = 0
#             # universal anchors
#             if self.EMAIL_RE.search(flat): hits += 1
#             if self.URL_RE.search(flat):   hits += 1
#             if self.PHONE_RE.search(flat): hits += 1
#             hits += sum(1 for t in self.ROLE_TOKS if t in lflat)
#             non_empty = sum(1 for r in data_rows for c in r if (c or "").strip())
#             total     = sum(len(r) for r in data_rows) or 1
#             density   = non_empty / total
#             kv_labels = len(re.findall(r"[A-Za-z][A-Za-z\s]{1,30}:", flat))

#             if page_no is None or page_no <= 3:
#                 # original early-page salvage
#                 if hits >= 2 or density >= 0.70 or kv_labels >= 3:
#                     return True
#             else:
#                 # late-page salvage requires strong structured signals
#                 if (hits >= 3 and kv_labels >= 3) or density >= 0.85:
#                     return True

#         if table_type != TableType.FINANCIAL_STATEMENT and ts < 0.35:
#             return False

#         if table_type == TableType.FINANCIAL_STATEMENT:
#             if max((len(r) for r in data_rows), default=0) <= 1:
#                 return False
#             if not any(re.search(r"\d", c or "") for c in (data_rows[0] if data_rows else [])):
#                 return False
#         return True


#     # --- main per-table ---
#     def preprocess_table(
#         self,
#         table: Dict[str,Any],
#         table_index: int,
#         seen_hashes: set[str]
#     ) -> Tuple[Optional[Dict[str,Any]], Optional[Dict[str,Any]]]:

#         page_no = table.get("page_number")
#         table_data = table.get("table_data") or []
#         if not table_data:
#             return None, {"page_number": page_no, "table_index": table_index, "reason": "empty_table"}

#         table_type = self.classify(table_data, page_no)

#         # --- Schema-agnostic salvage: synthesize single text column when col_* are missing/sparse ---
#         salvage = False
#         raw_keys = list(table_data[0].keys()) if table_data else []
#         col_keys = [k for k in raw_keys if re.fullmatch(r"(?i)col_\d+", k)]
#         nonempty_colvals = sum(
#             1 for row in table_data for k in col_keys
#             if normalize_cell(row.get(k, "")) not in {"", "-"}
#         )
#         texty_keys = [k for k in raw_keys if k not in col_keys]

#         def row_to_text(row: Dict[str, Any]) -> str:
#             return concat_row_fields(row)

#         display_headers: List[str] = []
#         raw_header_parts: Optional[List[List[str]]] = None
#         used_header_rows: List[int] = []
#         kept_cols: List[int] = []
#         table_title: Optional[str] = None

#         if (len(col_keys) == 0) or (nonempty_colvals <= 1) or (texty_keys and nonempty_colvals == 0):
#             cat_rows = [[row_to_text(r)] for r in table_data if row_to_text(r)]
#             cat_rows = [r for r in cat_rows if len(r[0]) >= 40]  # discard trivial lines
#             if cat_rows:
#                 keys_lc = {k.strip().lower() for k in raw_keys}
#                 blob = " ".join(r[0].lower() for r in cat_rows[:6])
#                 is_fp = any(tok in blob for tok in FP_TOKENS) or bool(re.search(r'@|https?://|www\.', blob))
#                 has_def = {"term", "description"} <= keys_lc

#                 if is_fp:
#                     table_type = TableType.FRONT_PAGE
#                     display_headers = ["Contact Block"]
#                 elif has_def and len(blob) >= 120:
#                     display_headers = ["Definition"]
#                 else:
#                     display_headers = ["Text"]

#                 kept_cols = [0]
#                 used_header_rows = []
#                 raw_header_parts = None
#                 data_rows = cat_rows
#                 salvage = True

#         # If not salvaged, follow the normal pipeline
#         if not salvage:
#             has_col_keys = len(col_keys) > 0
#             cols = safe_get_max_cols(table_data) if has_col_keys else 1
#             empty_cols: List[int] = []
#             if table_type != TableType.FRONT_PAGE and has_col_keys:
#                 for j in range(cols):
#                     non_empty = sum(1 for row in table_data if normalize_cell(row.get(f"col_{j}", "")) not in {"","-"})
#                     if len(table_data) and (non_empty/len(table_data)) < 0.30:
#                         empty_cols.append(j)
#             kept_cols = [j for j in range(cols) if j not in empty_cols] or list(range(cols))

#             if has_col_keys:
#                 header_rows = self.detect_header_rows(table_data, max_header_rows=self.cfg.max_header_rows)
#                 display_headers, raw_header_parts, table_title, used_header_rows = self.extract_headers(
#                     table_data, header_rows, kept_cols, self.cfg.max_header_length
#                 )
#                 # Build data rows (omit header rows)
#                 raw_data_rows_initial = [
#                     [normalize_cell(row.get(f"col_{j}", "")) for j in kept_cols]
#                     for i, row in enumerate(table_data) if i not in set(used_header_rows)
#                 ]
#             else:
#                 # Fallback (rare): synthesize a single text column
#                 display_headers = ["Column_0"]
#                 raw_header_parts = [["Column_0"]]
#                 table_title = None
#                 used_header_rows = []
#                 raw_data_rows_initial = [[concat_row_fields(row)] for row in table_data]

#             # Directive 2: only split FRONT_PAGE; use real newline char
#             force_split = (table_type == TableType.FRONT_PAGE)
#             data_rows: List[List[str]] = []
#             for r in raw_data_rows_initial:
#                 has_newlines = any('\n' in x for x in r)
#                 if force_split and has_newlines and len(raw_data_rows_initial) < 10:
#                     parts = [x.split('\n') for x in r]
#                     max_lines = max(len(p) for p in parts)
#                     for i in range(max_lines):
#                         data_rows.append([normalize_cell(p[i]) if i < len(p) else "" for p in parts])
#                 else:
#                     data_rows.append([normalize_cell(x) for x in r])

#             # FRONT_PAGE semantic compact labels (Directive 4)
#             if table_type == TableType.FRONT_PAGE and all(h.startswith("Column_") for h in display_headers):
#                 new_headers, new_data_rows, _note = infer_semantic_headers_for_front_page(data_rows, display_headers)
#                 display_headers, data_rows = new_headers, new_data_rows

#         # --------------------
#         # POLICY PRE-GATE (before any rejection)
#         # --------------------
#         proc_table_peek = {
#             "page_number": page_no,
#             "table_index": table_index,
#             "table_type": table_type,
#             "headers": display_headers,
#             "data": data_rows,
#             "table_title": table_title,
#         }
#         accept_override = False
#         try:
#             from phoenix.memory import memory_layer
#             learn_cfg = {
#                 "enabled": True,
#                 "events_path": "out/review/learning_events.jsonl",
#                 "patterns_path": "out/patterns/patterns.jsonl",
#                 "exploration_rate": 0.15,
#             }
#             dossier_name = str(table.get("source_file","")) or "unknown"
#             proc_table_peek, decision = memory_layer.apply(proc_table_peek, dossier_name, learn_cfg, stage="pre_gate")
#             accept_override = bool(decision.get("accept_override", False))
#             display_headers = proc_table_peek.get("headers", display_headers)
#             data_rows = proc_table_peek.get("data", data_rows)
#         except Exception:
#             pass

#         # Honest context gate (policy may override)
#         if not accept_override and not self.is_contextually_valuable(data_rows, table_type, page_no):
#             return None, {"page_number": page_no, "table_index": table_index, "reason":"skipped_non_valuable"}

#         # pad / normalize
#         target_cols = len(kept_cols) if kept_cols else 1
#         display_headers = pad_to_len(display_headers, target_cols)
#         data_rows = [pad_to_len(r, target_cols) for r in data_rows if any((c or "").strip() for c in r)]

#         # hash + dedupe
#         t_hash = compute_table_hash(display_headers, data_rows, table_index, raw_header_parts,
#                                     sample_rows=40, page_no=page_no, title=table_title)
#         if self.cfg.dedupe and t_hash in seen_hashes:
#             return None, {"page_number": page_no, "table_index": table_index, "reason": "duplicate"}
#         seen_hashes.add(t_hash)

#         # confidence (simple, unchanged)
#         numeric_cols = 0
#         for j in range(target_cols):
#             vals = [r[j] for r in data_rows if j < len(r)]
#             if not vals:
#                 continue
#             num = sum(bool(re.fullmatch(r"[0-9,.\-()%₹]+", v or "")) for v in vals)
#             if num >= max(1, int(0.5*len(vals))):
#                 numeric_cols += 1
#         period_detected = any(re.search(r"(19|20)\d{2}", h) or "ended" in h.lower() for h in display_headers)
#         confidence = min(1.0, 0.15 + 0.35*(numeric_cols/max(1,target_cols)) + (0.25 if period_detected else 0))

#         proc_table = {
#             "page_number": page_no,
#             "table_index": table_index,
#             "table_type": table_type,
#             "headers": display_headers,
#             "data": data_rows,
#             "table_title": table_title,
#             "content_hash": t_hash,
#             "confidence": round(confidence, 3),
#         }

#         # Optional post-gate learning hook
#         try:
#             from phoenix.memory import memory_layer
#             learn_cfg = {
#                 "enabled": True,
#                 "events_path": "out/review/learning_events.jsonl",
#                 "patterns_path": "out/patterns/patterns.jsonl",
#                 "exploration_rate": 0.15,
#             }
#             dossier_name = str(table.get("source_file","")) or "unknown"
#             proc_table, _ = memory_layer.apply(proc_table, dossier_name, learn_cfg, stage="post_gate")
#         except Exception:
#             pass

#         return proc_table, None

# # -------------------- Runner --------------------

# def run_on_legacy(input_json_path: str) -> Dict[str, Any]:
#     with open(input_json_path, "r", encoding="utf-8") as f:
#         legacy = json.load(f)

#     key = next(iter(legacy.keys()))
#     meta = legacy[key]
#     tables = meta.get("tables", [])

#     surgeon = Surgeon(PreprocessorConfig())
#     seen = set()
#     processed, skipped = [], []

#     dossier_name = (meta.get("filename") or key or "unknown")
#     for idx, t in enumerate(tables):
#         t = dict(t)  # shallow copy to annotate
#         t["source_file"] = dossier_name
#         proc, skip = surgeon.preprocess_table(t, idx, seen)
#         if proc:
#             processed.append(proc)
#         elif skip:
#             skipped.append(skip)

#     result = {
#         "source": key,
#         "filename": meta.get("filename"),
#         "processed_count": len(processed),
#         "skipped_count": len(skipped),
#         "processed": processed,
#         "skipped": skipped
#     }
#     return result

# def main():
#     import argparse, os
#     ap = argparse.ArgumentParser(description="Phoenix Surgeon v6.2 — run on legacy JSON")
#     ap.add_argument("--input", required=True, help="Path to legacy JSON file")
#     ap.add_argument("--output", required=False, help="Output JSON path")
#     args = ap.parse_args()

#     res = run_on_legacy(args.input)
#     out = args.output or (os.path.splitext(args.input)[0] + ".preproc_v6_2.json")
#     with open(out, "w", encoding="utf-8") as f:
#         json.dump(res, f, ensure_ascii=False, indent=2)
#     print(json.dumps({
#         "ok": True,
#         "processed": res["processed_count"],
#         "skipped": res["skipped_count"],
#         "out": out
#     }))

# if __name__ == "__main__":
#     main()




































#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

SOME MORE CHANGES
Phoenix Surgeon v6.2 (Patched)
Implements Phase-1A directives over legacy JSON extracts.

Directives implemented:
1) Structural triage via tabularity_score (primary shield against prose-as-table).
2) Cease financial sentence-splitting (only split FRONT_PAGE, and only early pages).
3) Header fortification:
   3a) Guard against row-label slabs (looks_like_row_label_band).
   3b) Compose multirow period headers (try_compose_period_headers).
4) Front-page semantic promotion via controlled regex FRONT_FIELD_MAP.
5) Stronger hashing includes page_no and table_title (compute_table_hash).
6) Schema-agnostic salvage: synthesize single text column for rows lacking col_* or with sparse columns.
7) Page-aware FP gating: late-page FP needs strong anchors+KV; weak late FP is demoted to GENERIC.
"""

from __future__ import annotations

import json, re, hashlib
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# -------------------- helpers --------------------

def normalize_cell(x: Any) -> str:
    if x is None:
        return ""
    return re.sub(r"\s+", " ", str(x).strip())

def safe_get_max_cols(table_data: List[Dict[str,str]]) -> int:
    max_cols = 0
    for row in table_data:
        for k in row.keys():
            if k.lower().startswith("col_"):
                try:
                    i = int(k.split("_")[1])
                    max_cols = max(max_cols, i + 1)
                except Exception:
                    pass
    return max_cols

def pad_to_len(arr: List[str], n: int) -> List[str]:
    return arr + [""] * (n - len(arr)) if len(arr) < n else arr[:n]

# If a table row doesn't have col_* keys, concatenate string-like fields
def concat_row_fields(row: Dict[str, Any]) -> str:
    vals: List[str] = []
    for k, v in row.items():
        if k.startswith("IGNORE_WHEN_COPYING"):
            continue
        if v is None:
            continue
        if isinstance(v, (int, float)):
            vals.append(str(v))
        elif isinstance(v, str):
            s = v.strip()
            if s:
                vals.append(s)
        else:
            s = str(v).strip()
            if s:
                vals.append(s)
    return normalize_cell(" ".join(vals))

# -------------------- Directives --------------------

# (1) Structural triage
def tabularity_score(rows: List[List[str]]) -> float:
    if not rows:
        return 0.0
    cols = max((len(r) for r in rows), default=0)
    if cols <= 1:
        return 0.0

    numeric_col_frac: List[float] = []
    for j in range(cols):
        vals = [normalize_cell(r[j]) for r in rows if j < len(r) and normalize_cell(r[j])]
        if not vals:
            numeric_col_frac.append(0.0)
            continue
        num = sum(bool(re.fullmatch(r"[0-9,.\-()%₹]+", v)) for v in vals)
        frac = max(num/len(vals), 1 - num/len(vals))  # purity of type
        numeric_col_frac.append(frac)

    purity = sum(numeric_col_frac)/len(numeric_col_frac)
    left = [normalize_cell(r[0]) for r in rows if r and normalize_cell(r[0])]
    label_diversity = len(set(left))/max(1, len(left))
    label_signal = 1 - min(1.0, label_diversity) * 0.5
    width = 1.0 if (cols >= 2 and any(f > 0.7 for f in numeric_col_frac)) else 0.0
    return 0.5*purity + 0.3*label_signal + 0.2*width

# (3a) Row-label slab guard
ROW_LABEL_TOKENS = {"assets","liabilities","equity","income","expenses","particulars","notes"}

def looks_like_row_label_band(row: Dict[str,str]) -> bool:
    cells = [normalize_cell(v) for k,v in row.items() if k.lower().startswith("col_")]
    if not cells:
        return False
    left = normalize_cell(row.get("col_0",""))
    other_text = " ".join(normalize_cell(row.get(f"col_{j}","")) for j in range(1, min(6, len(cells))))
    left_heavy = len(left) > 20 and len(other_text) < 10
    has_fin_labels = sum(t in left.lower() for t in ROW_LABEL_TOKENS) >= 2
    return left_heavy and has_fin_labels

# (3b) Multirow period header composer
PERIOD_ROW_PATTS = [
    re.compile(r"\b(as on|as at|as of|for the|nine months|half year|quarter|q[1-4]|h[12]|fy)\b", re.I),
    re.compile(r"\b(ended|ending)\b", re.I),
    re.compile(r"\b(20\d{2}|19\d{2})\b"),
    re.compile(r"\b₹\s*(in|million|crore|lakh)\b", re.I),
    re.compile(r"\bfy\s*\d{2}\s*[-/]\s*\d{2}\b", re.I),
]

FP_TOKENS = (
    "registrar", "rta", "brlm", "book running lead manager",
    "global coordinator", "syndicate member", "contact person",
    "company secretary", "compliance officer", "investor relations",
    "email", "e-mail", "website", "web site", "tel", "telephone", "phone", "mobile", "mob.", "fax",
    "link intime", "kfin", "kfintech", "bigshare", "mas services", "cameo",
    "issue opens", "issue closes", "price band", "isin", "cin", "pan", "sebi"
)

def try_compose_period_headers(table_data: List[Dict[str,str]], kept_cols: List[int], search_rows: int = 4) -> Optional[List[str]]:
    if not table_data or not kept_cols:
        return None
    rows = [[normalize_cell(table_data[r].get(f"col_{c}","")) 
             for c in kept_cols] for r in range(min(search_rows, len(table_data)))]
    if not rows:
        return None
    cols = list(zip(*rows))
    headers: List[str] = []
    any_hit = 0
    for col in cols:
        band = " ".join([t for t in col if t]).strip()
        if any(p.search(band) for p in PERIOD_ROW_PATTS):
            headers.append(re.sub(r"\s+"," ",band))
            any_hit += 1
        else:
            headers.append("")
    if any_hit >= max(2, len(headers)//3):
        return headers
    return None

# (4) Controlled front-page labels
FRONT_FIELD_MAP = {
   r"\bregistrar(\s+to\s+the)?\s+(issue|offer)\b": "Registrar",
   r"\b(lead\s+manager|merchant\s+banker|brlm)\b": "Lead Manager",
   r"\b(contact\s+person|compliance\s+officer|company\s+secretary)\b": "Contact",
   r"\bemail|e-?mail\b": "Email",
   r"\bwebsite|web\s*site|url\b": "Website",
   r"\btelephone|tel|phone|fax\b": "Telephone",
}

def infer_semantic_headers_for_front_page(data_rows: List[List[str]], original_headers: List[str]) -> Tuple[List[str], List[List[str]], str]:
    if not data_rows:
        return original_headers, data_rows, "No data rows to infer from."
    first_row = data_rows[0]
    tokens = " ".join([normalize_cell(c) for c in first_row]).lower()
    labels = [name for pat,name in FRONT_FIELD_MAP.items() if re.search(pat, tokens)]
    if 2 <= len(labels) <= len(original_headers):
        new_headers = labels + [f"Field_{i}" for i in range(len(original_headers)-len(labels))]
        return pad_to_len(new_headers, len(original_headers)), data_rows[1:], "Promoted compact front-page labels."
    return original_headers, data_rows, "No compact label set found."

# (5) Stronger hashing
def compute_table_hash(
    display_headers: List[str],
    data_rows: List[List[str]],
    table_index: int,
    raw_header_parts: Optional[List[List[str]]] = None,
    sample_rows: int = 40,
    page_no: Optional[int] = None,
    title: Optional[str] = None
) -> str:
    h = hashlib.md5()
    prefix = f"idx:{table_index}|pg:{page_no if page_no is not None else -1}|title:{(title or '')[:64]}|"
    text = prefix + "".join(display_headers or [])
    if raw_header_parts:
        text += "".join("".join(p) for p in raw_header_parts)
    h.update(text.encode("utf-8"))
    for r in (data_rows or [])[:sample_rows]:
        h.update("\u241F".join([normalize_cell(c) for c in r]).encode("utf-8"))
    return h.hexdigest()

# -------------------- Core Surgeon --------------------

@dataclass
class PreprocessorConfig:
    min_content_threshold: float = 0.30
    max_header_rows: int = 4
    min_data_rows: int = 1
    max_header_length: int = 100
    dedupe: bool = True
    output_compression: bool = False  # handled by caller
    prose_split_char_threshold: int = 250

HEADER_HINTS  = [
    'total','amount','year','period','march','december','fy','q1','q2','q3','q4','half year','h1','h2',
    'as on','as at','as of','revenue','assets','liabilities','equity','cash flow','profit','loss',
    'income','expenses','notes','particulars','sr. no.','details','description','metric','₹ in crore','₹ in lakh'
]
FINANCIAL_STATEMENT_KEYWORDS = [
    'balance sheet','profit and loss','statement of profit and loss','cash flow','statement of operations',
    'consolidated financial','shareholder equity','financial position','notes to accounts'
]
FRONT_PAGE_KEYWORDS = [
    'offer','issue','price band','registrar','lead manager','merchant banker','rta','ipo','equity share',
    'fresh issue','offer for sale','contact person','telephone','tel','phone','fax','website','email','e-mail'
]

class TableType:
    FRONT_PAGE = "FRONT_PAGE"
    FINANCIAL_STATEMENT = "FINANCIAL_STATEMENT"
    GENERIC = "GENERIC"

class Surgeon:
    def __init__(self, cfg: PreprocessorConfig):
        self.cfg = cfg
        # universal anchors for front-page “contact slab” salvage
        self.EMAIL_RE  = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
        self.URL_RE    = re.compile(r"(?:(?:https?://)?(?:www\.)?[A-Za-z0-9.-]+\.[A-Za-z]{2,}(?:/[^\s]*)?)", re.I)
        self.PHONE_RE  = re.compile(r"(?:\+?\d[\d\s\-]{7,}\d)")
        self.ROLE_TOKS = (
            "registrar","lead manager","merchant banker","brlm",
            "contact person","compliance officer","company secretary",
            "telephone","tel","phone","fax","website","email","e-mail",
            "bid","issue opens","issue closes","anchor investor","rta"
        )

    def _anchor_kv_stats(self, text: str) -> tuple[int,int,int]:
        """Return (anchors, kv_labels, role_hits) computed from a flat lowercase blob."""
        anchors = 0
        anchors += 1 if self.EMAIL_RE.search(text) else 0
        anchors += 1 if self.URL_RE.search(text)   else 0
        anchors += 1 if self.PHONE_RE.search(text) else 0
        kv_labels = len(re.findall(r"[A-Za-z][A-Za-z\s]{1,30}:", text))
        role_hits = sum(1 for t in self.ROLE_TOKS if t in text)
        return anchors, kv_labels, role_hits

    # --- classification ---
    def classify(self, table_data: List[Dict[str, Any]], page_number: Optional[int]) -> str:
        # Early pages are front-page by default
        if page_number is not None and page_number <= 2:
            return TableType.FRONT_PAGE

        all_text_raw = " ".join(str(v) for row in table_data for v in row.values())
        all_text = normalize_cell(all_text_raw).lower()

        # Financial statements get priority classification
        if any(k in all_text for k in FINANCIAL_STATEMENT_KEYWORDS):
            return TableType.FINANCIAL_STATEMENT

        anchors, kv, roles = self._anchor_kv_stats(all_text)

        # Page 3: moderately strict
        if page_number == 3:
            if (anchors >= 2 and kv >= 2) or roles >= 3:
                return TableType.FRONT_PAGE

        # Beyond p3: require strong structured FP signals
        if page_number and page_number > 3:
            if anchors >= 3 and kv >= 3 and roles >= 1:
                return TableType.FRONT_PAGE
            return TableType.GENERIC

        # Fallback for unknown page number (treat like early-ish)
        if anchors >= 2 and kv >= 2:
            return TableType.FRONT_PAGE

        return TableType.GENERIC

    # --- header helpers ---
    def detect_header_rows(self, table_data: List[Dict[str,str]], max_header_rows: int) -> List[int]:
        def score_header_row(row: Dict[str,str]) -> float:
            hits, labels, numerics, unique_labels = 0, 0, 0, set()
            for _, v in row.items():
                s = normalize_cell(v); sl = s.lower()
                if not s:
                    continue
                if any(h in sl for h in HEADER_HINTS):
                    hits += 2
                if re.search(r"\b(19\d{2}|20\d{2})\b", sl) or re.search(r"^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)", sl, re.I):
                    hits += 1
                for t in s.split():
                    if t.isalpha():
                        labels += 1; unique_labels.add(t.lower())
                    elif re.fullmatch(r"[\d,.%₹-]+", t):
                        numerics += 1
            ratio = labels / (labels + numerics) if (labels + numerics) > 0 else 0.0
            return hits + 0.5 * ratio - (0.1 if hits == 0 and len(unique_labels) <= 1 else 0)

        header_indices: List[int] = []
        for i in range(min(max_header_rows, len(table_data))):
            row = table_data[i]
            if looks_like_row_label_band(row):
                continue  # Directive 3a
            sc = score_header_row(row)
            numerics = sum(bool(re.search(r"[\d,.%₹-]", normalize_cell(v))) for v in row.values())
            labels = sum(bool(re.search(r"[A-Za-z]", normalize_cell(v))) for v in row.values())
            nr = numerics / max(1, (numerics + labels))
            if sc >= 0.5 and nr < 0.25:
                header_indices.append(i)
        return header_indices

    # --- extraction ---
    def extract_headers(
        self,
        table_data: List[Dict[str,str]],
        header_rows: List[int],
        kept_cols: List[int],
        max_len: int
    ) -> Tuple[List[str], List[List[str]], Optional[str], List[int]]:
        table_title = None
        raw_header_parts: List[List[str]] = []
        used_indices: List[int] = []

        # Directive 3b: try compose period headers first
        period_headers = try_compose_period_headers(table_data, kept_cols)
        if period_headers:
            display_headers = [h if h else f"Column_{i}" for i,h in enumerate(period_headers)]
            raw_header_parts = [[h] if h else [] for h in display_headers]
            used_indices = list(range(min(4, len(table_data))))
        else:
            if not header_rows:
                display_headers = [f"Column_{i}" for i in range(len(kept_cols))]
                raw_header_parts = [[h] for h in display_headers]
            else:
                usable = [idx for idx in header_rows]
                raw_parts = [[normalize_cell(table_data[hr].get(f"col_{c}","")) for hr in usable] for c in kept_cols]
                raw_header_parts = [[p for p in parts if p] for parts in raw_parts]
                display_headers = [" ".join(parts) if parts else f"Column_{idx}" for idx, parts in enumerate(raw_header_parts)]
                used_indices = sorted(list(set(usable)))

        display_headers = [(h[:max_len-3] + "..." if len(h) > max_len else h) for h in display_headers]
        return display_headers, raw_header_parts, table_title, used_indices

    # --- triage ---
    def is_contextually_valuable(self, data_rows: List[List[str]], table_type: str, page_no: Optional[int] = None) -> bool:
        ts = tabularity_score(data_rows)

        # FRONT_PAGE salvage: early pages lenient, later pages strict
        if table_type == TableType.FRONT_PAGE and ts < 0.35:
            flat = " ".join(" ".join(r) for r in (data_rows or []))
            lflat = flat.lower()
            hits = 0
            if self.EMAIL_RE.search(flat): hits += 1
            if self.URL_RE.search(flat):   hits += 1
            if self.PHONE_RE.search(flat): hits += 1
            hits += sum(1 for t in self.ROLE_TOKS if t in lflat)
            non_empty = sum(1 for r in data_rows for c in r if (c or "").strip())
            total     = sum(len(r) for r in data_rows) or 1
            density   = non_empty / total
            kv_labels = len(re.findall(r"[A-Za-z][A-Za-z\s]{1,30}:", flat))

            if page_no is None or page_no <= 2:
                if hits >= 2 or density >= 0.70 or kv_labels >= 3:
                    return True
            elif page_no == 3:
                if (hits >= 3 and kv_labels >= 2) or density >= 0.80:
                    return True
            else:
                if (hits >= 3 and kv_labels >= 3) or density >= 0.85:
                    return True

        if table_type != TableType.FINANCIAL_STATEMENT and ts < 0.35:
            return False

        if table_type == TableType.FINANCIAL_STATEMENT:
            if max((len(r) for r in data_rows), default=0) <= 1:
                return False
            if not any(re.search(r"\d", c or "") for c in (data_rows[0] if data_rows else [])):
                return False
        return True

    # --- main per-table ---
    def preprocess_table(
        self,
        table: Dict[str,Any],
        table_index: int,
        seen_hashes: set[str]
    ) -> Tuple[Optional[Dict[str,Any]], Optional[Dict[str,Any]]]:

        page_no = table.get("page_number")
        table_data = table.get("table_data") or []
        if not table_data:
            return None, {"page_number": page_no, "table_index": table_index, "reason": "empty_table"}

        table_type = self.classify(table_data, page_no)

        # --- Schema-agnostic salvage: synthesize single text column when col_* are missing/sparse ---
        salvage = False
        raw_keys = list(table_data[0].keys()) if table_data else []
        col_keys = [k for k in raw_keys if re.fullmatch(r"(?i)col_\d+", k)]
        nonempty_colvals = sum(
            1 for row in table_data for k in col_keys
            if normalize_cell(row.get(k, "")) not in {"", "-"}
        )
        texty_keys = [k for k in raw_keys if k not in col_keys]

        def row_to_text(row: Dict[str, Any]) -> str:
            return concat_row_fields(row)

        display_headers: List[str] = []
        raw_header_parts: Optional[List[List[str]]] = None
        used_header_rows: List[int] = []
        kept_cols: List[int] = []
        table_title: Optional[str] = None

        if (len(col_keys) == 0) or (nonempty_colvals <= 1) or (texty_keys and nonempty_colvals == 0):
            cat_rows = [[row_to_text(r)] for r in table_data if row_to_text(r)]
            cat_rows = [r for r in cat_rows if len(r[0]) >= 40]  # discard trivial lines
            if cat_rows:
                blob = " ".join(r[0].lower() for r in cat_rows[:6])
                is_fp_soft = any(tok in blob for tok in FP_TOKENS) or bool(re.search(r'@|https?://|www\.', blob))
                anchors, kv, roles = self._anchor_kv_stats(blob)

                if is_fp_soft:
                    # Gate by page number + strength
                    strong_early = (page_no is None or page_no <= 2)
                    strong_mid   = (page_no == 3 and anchors >= 2 and kv >= 2)
                    strong_late  = (page_no and page_no > 3 and anchors >= 3 and kv >= 3)
                    if strong_early or strong_mid or strong_late:
                        table_type = TableType.FRONT_PAGE
                        display_headers = ["Contact Block"]
                    else:
                        display_headers = ["Text"]
                else:
                    display_headers = ["Text"]

                kept_cols = [0]
                used_header_rows = []
                raw_header_parts = None
                data_rows = cat_rows
                salvage = True

        # If not salvaged, follow the normal pipeline
        if not salvage:
            has_col_keys = len(col_keys) > 0
            cols = safe_get_max_cols(table_data) if has_col_keys else 1
            empty_cols: List[int] = []
            if table_type != TableType.FRONT_PAGE and has_col_keys:
                for j in range(cols):
                    non_empty = sum(1 for row in table_data if normalize_cell(row.get(f"col_{j}", "")) not in {"","-"})
                    if len(table_data) and (non_empty/len(table_data)) < 0.30:
                        empty_cols.append(j)
            kept_cols = [j for j in range(cols) if j not in empty_cols] or list(range(cols))

            if has_col_keys:
                header_rows = self.detect_header_rows(table_data, max_header_rows=self.cfg.max_header_rows)
                display_headers, raw_header_parts, table_title, used_header_rows = self.extract_headers(
                    table_data, header_rows, kept_cols, self.cfg.max_header_length
                )
                # Build data rows (omit header rows)
                raw_data_rows_initial = [
                    [normalize_cell(row.get(f"col_{j}", "")) for j in kept_cols]
                    for i, row in enumerate(table_data) if i not in set(used_header_rows)
                ]
            else:
                # Fallback (rare): synthesize a single text column
                display_headers = ["Column_0"]
                raw_header_parts = [["Column_0"]]
                table_title = None
                used_header_rows = []
                raw_data_rows_initial = [[concat_row_fields(row)] for row in table_data]

            # Directive 2: only split FRONT_PAGE; restrict to early pages
            force_split = (table_type == TableType.FRONT_PAGE and (page_no is None or page_no <= 3))
            data_rows: List[List[str]] = []
            for r in raw_data_rows_initial:
                has_newlines = any('\n' in x for x in r)
                if force_split and has_newlines and len(raw_data_rows_initial) < 10:
                    parts = [x.split('\n') for x in r]
                    max_lines = max(len(p) for p in parts)
                    for i in range(max_lines):
                        data_rows.append([normalize_cell(p[i]) if i < len(p) else "" for p in parts])
                else:
                    data_rows.append([normalize_cell(x) for x in r])

            # FRONT_PAGE semantic compact labels (Directive 4)
            if table_type == TableType.FRONT_PAGE and all(h.startswith("Column_") for h in display_headers):
                new_headers, new_data_rows, _note = infer_semantic_headers_for_front_page(data_rows, display_headers)
                display_headers, data_rows = new_headers, new_data_rows

        # --------------------
        # POLICY PRE-GATE (before any rejection)
        # --------------------
        proc_table_peek = {
            "page_number": page_no,
            "table_index": table_index,
            "table_type": table_type,
            "headers": display_headers,
            "data": data_rows,
            "table_title": table_title,
        }
        accept_override = False
        try:
            from phoenix.memory import memory_layer
            learn_cfg = {
                "enabled": True,
                "events_path": "out/review/learning_events.jsonl",
                "patterns_path": "out/patterns/patterns.jsonl",
                "exploration_rate": 0.15,
            }
            dossier_name = str(table.get("source_file","")) or "unknown"
            proc_table_peek, decision = memory_layer.apply(proc_table_peek, dossier_name, learn_cfg, stage="pre_gate")
            accept_override = bool(decision.get("accept_override", False))
            display_headers = proc_table_peek.get("headers", display_headers)
            data_rows = proc_table_peek.get("data", data_rows)
        except Exception:
            pass

        # Honest context gate (policy may override)
        if not accept_override and not self.is_contextually_valuable(data_rows, table_type, page_no):
            return None, {"page_number": page_no, "table_index": table_index, "reason":"skipped_non_valuable"}

        # Demote weak late FRONT_PAGE to GENERIC to avoid inflating counts
        if (table_type == TableType.FRONT_PAGE) and (page_no is not None and page_no > 3):
            flat = " ".join(" ".join(r) for r in (data_rows or [])).lower()
            a, k, roles = self._anchor_kv_stats(flat)
            if not (a >= 3 and k >= 3):
                table_type = TableType.GENERIC

        # pad / normalize
        target_cols = len(kept_cols) if kept_cols else 1
        display_headers = pad_to_len(display_headers, target_cols)
        data_rows = [pad_to_len(r, target_cols) for r in data_rows if any((c or "").strip() for c in r)]

        # hash + dedupe
        t_hash = compute_table_hash(display_headers, data_rows, table_index, raw_header_parts,
                                    sample_rows=40, page_no=page_no, title=table_title)
        if self.cfg.dedupe and t_hash in seen_hashes:
            return None, {"page_number": page_no, "table_index": table_index, "reason": "duplicate"}
        seen_hashes.add(t_hash)

        # confidence (simple)
        numeric_cols = 0
        for j in range(target_cols):
            vals = [r[j] for r in data_rows if j < len(r)]
            if not vals:
                continue
            num = sum(bool(re.fullmatch(r"[0-9,.\-()%₹]+", v or "")) for v in vals)
            if num >= max(1, int(0.5*len(vals))):
                numeric_cols += 1
        period_detected = any(re.search(r"(19|20)\d{2}", h) or "ended" in h.lower() for h in display_headers)
        confidence = min(1.0, 0.15 + 0.35*(numeric_cols/max(1,target_cols)) + (0.25 if period_detected else 0))

        proc_table = {
            "page_number": page_no,
            "table_index": table_index,
            "table_type": table_type,
            "headers": display_headers,
            "data": data_rows,
            "table_title": table_title,
            "content_hash": t_hash,
            "confidence": round(confidence, 3),
        }

        # Optional post-gate learning hook
        try:
            from phoenix.memory import memory_layer
            learn_cfg = {
                "enabled": True,
                "events_path": "out/review/learning_events.jsonl",
                "patterns_path": "out/patterns/patterns.jsonl",
                "exploration_rate": 0.15,
            }
            dossier_name = str(table.get("source_file","")) or "unknown"
            proc_table, _ = memory_layer.apply(proc_table, dossier_name, learn_cfg, stage="post_gate")
        except Exception:
            pass

        return proc_table, None

# -------------------- Runner --------------------

def run_on_legacy(input_json_path: str) -> Dict[str, Any]:
    with open(input_json_path, "r", encoding="utf-8") as f:
        legacy = json.load(f)

    key = next(iter(legacy.keys()))
    meta = legacy[key]
    tables = meta.get("tables", [])

    surgeon = Surgeon(PreprocessorConfig())
    seen = set()
    processed, skipped = [], []

    dossier_name = (meta.get("filename") or key or "unknown")
    for idx, t in enumerate(tables):
        t = dict(t)  # shallow copy to annotate
        t["source_file"] = dossier_name
        proc, skip = surgeon.preprocess_table(t, idx, seen)
        if proc:
            processed.append(proc)
        elif skip:
            skipped.append(skip)

    result = {
        "source": key,
        "filename": meta.get("filename"),
        "processed_count": len(processed),
        "skipped_count": len(skipped),
        "processed": processed,
        "skipped": skipped
    }
    return result

def main():
    import argparse, os
    ap = argparse.ArgumentParser(description="Phoenix Surgeon v6.2 — run on legacy JSON")
    ap.add_argument("--input", required=True, help="Path to legacy JSON file")
    ap.add_argument("--output", required=False, help="Output JSON path")
    args = ap.parse_args()

    res = run_on_legacy(args.input)
    out = args.output or (os.path.splitext(args.input)[0] + ".preproc_v6_2.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False, indent=2)
    print(json.dumps({
        "ok": True,
        "processed": res["processed_count"],
        "skipped": res["skipped_count"],
        "out": out
    }))

if __name__ == "__main__":
    main()
