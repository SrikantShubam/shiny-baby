#!/usr/bin/env python3
import sys, re
from pathlib import Path
import pandas as pd

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")
URL_RE   = re.compile(r"^(https?://)?[A-Za-z0-9.\-]+\.[A-Za-z]{2,}(/.*)?$", re.I)

def main():
    p = Path("out/canon/contacts.parquet")
    if not p.exists():
        print("contacts.parquet missing"); sys.exit(2)
    df = pd.read_parquet(p)
    n = len(df)
    bad_email = df["email"].dropna().map(lambda x: not bool(EMAIL_RE.match(x))).sum() if "email" in df else 0
    bad_url   = df["url"].dropna().map(lambda x: not bool(URL_RE.match(x))).sum() if "url" in df else 0

    # thresholds: at most 5% invalid emails or urls
    fail = False
    if n and (bad_email / max(1, df['email'].notna().sum())) > 0.05:
        print(f"[FAIL] invalid email rate too high: {bad_email} / {df['email'].notna().sum()}"); fail = True
    if n and (bad_url / max(1, df['url'].notna().sum())) > 0.05:
        print(f"[FAIL] invalid url rate too high: {bad_url} / {df['url'].notna().sum()}"); fail = True

    print(f"contacts rows={n}, invalid_email={bad_email}, invalid_url={bad_url}")
    sys.exit(1 if fail else 0)

if __name__ == "__main__":
    main()
