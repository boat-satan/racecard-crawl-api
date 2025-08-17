# -*- coding: utf-8 -*-
"""
integrated/v1 を走査して C(編成・相対)特徴CSVを作る
出力: TENKAI/outputs/features_c.csv
"""
import os, glob, json, csv
from features_c import build_c_features

BASE = "public"
INTEG = os.path.join(BASE, "integrated", "v1")
OUTDIR = os.path.join("TENKAI", "outputs")
OUTCSV = os.path.join(OUTDIR, "features_c.csv")

def _safe_load(p):
    with open(p, encoding="utf-8") as f:
        return json.load(f)

def main():
    os.makedirs(OUTDIR, exist_ok=True)

    rows = []
    for path in sorted(glob.glob(os.path.join(INTEG, "*", "*", "*.json"))):
        try:
            integ = _safe_load(path)
            row = build_c_features(integ)
            rows.append(row)
        except Exception as e:
            print("skip:", path, e)

    if not rows:
        print("no rows.")
        return

    # CSV 書き出し
    cols = list(rows[0].keys())
    with open(OUTCSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print("saved:", OUTCSV, f"({len(rows)} races)")

if __name__ == "__main__":
    main()
