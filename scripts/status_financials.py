#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Status dump for financials v2 parquet.
Print high-level metrics and a few diagnostics.
"""
from __future__ import annotations
import sys, pandas as pd

def main():
    if len(sys.argv) < 2:
        print("usage: status_financials.py out/canon/financials.parquet"); return 1
    path = sys.argv[1]
    df = pd.read_parquet(path)
    n = len(df)

    # Column compatibility
    if "value_num_native" not in df.columns and "value_num" in df.columns:
        df["value_num_native"] = df["value_num"]
    if "value_num_inr" not in df.columns:
        df["value_num_inr"] = None

    native_rate = float(df["value_num_native"].notna().mean()) if n else 0.0
    inr_rate    = float(df["value_num_inr"].notna().mean())    if n else 0.0
    print(f"rows={n:,} numeric_rate_native={native_rate:.3f} numeric_rate_inr={inr_rate:.3f}")

    # Bottom-10 by native numeric rate per dossier
    by_dossier = df.groupby("dossier")["value_num_native"].apply(lambda s: float(s.notna().mean()))
    worst = by_dossier.sort_values(ascending=True).head(10)
    print("\nBottom-10 dossier native numeric_rate:")
    for name, rate in worst.items():
        rows = len(df[df["dossier"] == name])
        print(f"- {name:45s} rows={rows:5d}  numeric_rate={rate:.3f}")

    # Period coverage diagnostics
    df["_norm_period"] = df["period_end"].fillna(df.get("period_header"))
    df["table_key"] = df["dossier"].astype(str)+"|"+df["page_number"].astype(str)+"|"+df["table_index"].astype(str)
    per_table = df.groupby("table_key").agg(
        stmt=("statement_type","first"),
        periods=("_norm_period", lambda s: len(set([x for x in s if pd.notna(x)]))),
        dossier=("dossier","first")
    ).reset_index()
    poor = per_table[(per_table["stmt"]!="BS") & (per_table["periods"] < 2)].head(10)
    if len(poor):
        print("\nTables with <2 distinct periods (showing up to 10):")
        for _,r in poor.iterrows():
            print(f"- {r['dossier']:45s}  {r['table_key']}  periods={int(r['periods'])}")
    else:
        print("\nAll tables have ≥2 distinct periods.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
