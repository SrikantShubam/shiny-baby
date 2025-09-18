# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-
# """
# CI guardrails for financials v2:
# - numeric_rate_native: fraction of rows with value_num_native not null
# - numeric_rate_inr   : fraction of rows with value_num_inr not null (informational)
# - normalized_period_coverage: fraction of non-BS tables with >=2 distinct normalized periods
# - unit_detection     : fraction of tables with detected/assumed unit (unit_scale present OR parse_flags contains 'assumed_inr')
# - unit_conflict_rate : fraction of tables flagged unit_conflict

# Defensive to missing columns (unit_scale, parse_flags, period_*).
# """

# from __future__ import annotations
# import argparse, sys
# import pandas as pd

# def fail(msg:str):
#     print(f"FAIL: {msg}")
#     sys.exit(1)

# def main():
#     ap = argparse.ArgumentParser()
#     ap.add_argument("parquet_path")
#     ap.add_argument("--min_rows", type=int, default=200)
#     ap.add_argument("--min_numeric_rate", type=float, default=0.85)
#     ap.add_argument("--min_periods_per_table", type=int, default=2)
#     ap.add_argument("--normalized_period_coverage", type=float, default=0.85)
#     ap.add_argument("--min_unit_detection", type=float, default=0.80)
#     ap.add_argument("--max_unit_conflict_rate", type=float, default=0.02)
#     args = ap.parse_args()

#     df = pd.read_parquet(args.parquet_path)

#     # --- numeric rates (native is the KPI; INR is informational) ---
#     if "value_num_native" not in df.columns:
#         # Back-compat: some older ETLs used 'value_num'
#         df["value_num_native"] = df["value_num"] if "value_num" in df.columns else None
#     if "value_num_inr" not in df.columns:
#         df["value_num_inr"] = None

#     n = len(df)
#     num_rate_native = float(df["value_num_native"].notna().mean()) if n else 0.0
#     num_rate_inr    = float(df["value_num_inr"].notna().mean()) if n else 0.0
#     # Use native as the pass/fail gate (per v2 contract)
#     print(f"financials rows={n:,}, numeric_rate_native={num_rate_native:.3f}, numeric_rate_inr={num_rate_inr:.3f}, numeric_rate={num_rate_native:.3f}")
#     if n < args.min_rows: fail(f"rows {n} < min_rows {args.min_rows}")
#     if num_rate_native < args.min_numeric_rate: fail(f"numeric_rate {num_rate_native:.3f} < min_numeric_rate {args.min_numeric_rate:.3f}")

#     # --- normalized period coverage on non-BS only ---
#     if "period_end" not in df.columns:
#         df["period_end"] = None
#     if "period_header" not in df.columns:
#         df["period_header"] = None
#     if "statement_type" not in df.columns:
#         df["statement_type"] = "UNK"
#     if "dossier" not in df.columns:
#         df["dossier"] = "UNK"
#     if "page_number" not in df.columns:
#         df["page_number"] = None
#     if "table_index" not in df.columns:
#         df["table_index"] = None

#     df["table_key"] = df["dossier"].astype(str)+"|"+df["page_number"].astype(str)+"|"+df["table_index"].astype(str)
#     df["norm_period"] = df["period_end"].fillna(df["period_header"])

#     def agg_row(g):
#         stmt = g["statement_type"].iloc[0] if "statement_type" in g else "UNK"
#         # distinct non-null period labels
#         periods = int(g["norm_period"].dropna().astype(str).nunique()) if "norm_period" in g else 0
#         # unit detection: unit_scale present OR parse_flags has assumed_inr
#         unit_detect = False
#         if "unit_scale" in g:
#             unit_detect = bool(g["unit_scale"].notna().any())
#         if not unit_detect and "parse_flags" in g:
#             unit_detect = bool(g["parse_flags"].astype(str).str.contains("assumed_inr", na=False).any())
#         # conflict: parse_flags has unit_conflict
#         conflict = False
#         if "parse_flags" in g:
#             conflict = bool(g["parse_flags"].astype(str).str.contains("unit_conflict", na=False).any())
#         dossier = g["dossier"].iloc[0] if "dossier" in g else "UNK"
#         return pd.Series({
#             "stmt": stmt,
#             "periods": periods,
#             "unit_detect": unit_detect,
#             "conflict": conflict,
#             "dossier": dossier
#         })

#     per_table = df.groupby("table_key", dropna=False).apply(agg_row).reset_index()

#     non_bs = per_table[per_table["stmt"]!="BS"]
#     coverage = float(((non_bs["periods"] >= args.min_periods_per_table).mean())) if len(non_bs) else 1.0
#     print(f"normalized_period_coverage (non-BS): {coverage:.3f}")
#     if coverage < args.normalized_period_coverage:
#         bad = non_bs[non_bs["periods"] < args.min_periods_per_table].head(10)
#         print("Tables with insufficient periods (sample):")
#         for _,r in bad.iterrows():
#             print(f"- {r['dossier']}  {r['periods']} periods")
#         fail(f"normalized_period_coverage {coverage:.3f} < target {args.normalized_period_coverage:.3f}")

#     unit_detect_rate = float(per_table["unit_detect"].mean()) if len(per_table) else 0.0
#     conflict_rate    = float(per_table["conflict"].mean()) if len(per_table) else 0.0
#     print(f"unit_detection={unit_detect_rate:.3f}  unit_conflict_rate={conflict_rate:.3f}")
#     if unit_detect_rate < args.min_unit_detection:
#         fail(f"unit_detection {unit_detect_rate:.3f} < min_unit_detection {args.min_unit_detection:.3f}")
#     if conflict_rate > args.max_unit_conflict_rate:
#         fail(f"unit_conflict_rate {conflict_rate:.3f} > max_unit_conflict_rate {args.max_unit_conflict_rate:.3f}")

#     print("PASS")

# if __name__=="__main__":
#     main()






















































































#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CI guardrails for financials v2
"""
from __future__ import annotations
import argparse, sys
import pandas as pd

def fail(msg:str):
    print(f"FAIL: {msg}")
    sys.exit(1)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("parquet_path")
    ap.add_argument("--min_rows", type=int, default=200)
    ap.add_argument("--min_numeric_rate", type=float, default=0.85)
    ap.add_argument("--min_periods_per_table", type=int, default=2)
    ap.add_argument("--normalized_period_coverage", type=float, default=0.85)
    ap.add_argument("--min_unit_detection", type=float, default=0.80)
    ap.add_argument("--max_unit_conflict_rate", type=float, default=0.02)
    args = ap.parse_args()

    df = pd.read_parquet(args.parquet_path)
    # normalize schema
    for need in ["value_num_inr","value_num_native","statement_type","parse_flags","dossier","page_number","table_index","period_end","period_header","table_title","row_label","unit_scale"]:
        if need not in df.columns:
            df[need] = pd.NA

    n = len(df)
    num_rate = float(df["value_num_native"].notna().mean()) if n else 0.0
    num_rate_inr = float(df["value_num_inr"].notna().mean()) if n else 0.0
    print(f"financials rows={n:,}, numeric_rate_native={num_rate:.3f}, numeric_rate_inr={num_rate_inr:.3f}, numeric_rate={num_rate:.3f}")
    if n < args.min_rows: fail(f"rows {n} < min_rows {args.min_rows}")
    if num_rate < args.min_numeric_rate: fail(f"numeric_rate {num_rate:.3f} < min_numeric_rate {args.min_numeric_rate:.3f}")

    df["table_key"] = df["dossier"].astype(str)+"|"+df["page_number"].astype(str)+"|"+df["table_index"].astype(str)
    df["norm_period"] = df["period_end"].fillna(df["period_header"])

    def agg_row(group: pd.DataFrame):
        stmt = group["statement_type"].iloc[0]
        periods = int(group["norm_period"].dropna().nunique())
        # unit detect: unit_scale present or parse_flags includes 'assumed_inr' or 'unit_hint'
        has_detect = bool(group["unit_scale"].notna().any())
        if not has_detect:
            fl = ";".join([str(x or "") for x in group.get("parse_flags", pd.Series([], dtype=str)).tolist()])
            if ("assumed_inr" in fl) or ("unit_hint" in fl):
                has_detect = True
        conflict = False
        if "parse_flags" in group.columns:
            conflict = any("unit_conflict" in str(x or "") for x in group["parse_flags"])
        dossier = group["dossier"].iloc[0]
        return pd.Series({
            "stmt": stmt, "periods": periods, "unit_detect": has_detect, "conflict": conflict, "dossier": dossier
        })

    per_table = df.groupby("table_key", dropna=False).apply(agg_row).reset_index()

    non_bs = per_table[per_table["stmt"]!="BS"]
    coverage = float(((non_bs["periods"] >= args.min_periods_per_table).mean())) if len(non_bs) else 1.0
    print(f"normalized_period_coverage (non-BS): {coverage:.3f}")
    if coverage < args.normalized_period_coverage:
        bad = non_bs[non_bs["periods"] < args.min_periods_per_table].head(10)
        print("Tables with <min periods (sample):")
        for _,r in bad.iterrows():
            print(f"- {r['dossier']}  {r['table_key']}  periods={r['periods']}")
        fail(f"normalized_period_coverage {coverage:.3f} < target {args.normalized_period_coverage:.3f}")

    unit_detect_rate = float(per_table["unit_detect"].mean()) if len(per_table) else 0.0
    conflict_rate = float(per_table["conflict"].mean()) if len(per_table) else 0.0
    print(f"unit_detection={unit_detect_rate:.3f}  unit_conflict_rate={conflict_rate:.3f}")
    if unit_detect_rate < args.min_unit_detection:
        fail(f"unit_detection {unit_detect_rate:.3f} < min_unit_detection {args.min_unit_detection:.3f}")
    if conflict_rate > args.max_unit_conflict_rate:
        fail(f"unit_conflict_rate {conflict_rate:.3f} > max_unit_conflict_rate {args.max_unit_conflict_rate:.3f}")

    print("PASS")

if __name__=="__main__":
    main()
