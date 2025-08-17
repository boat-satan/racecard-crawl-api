# -*- coding: utf-8 -*-
"""
integrated/v1 を (date, pid, race) で走査し、
TENKAI C 特徴を 1レース=1CSV (1行) で出力する。

出力先:
  TENKAI/features_c/v1/<date>_<pid>_<race>.csv
"""
import os
import glob
import json
import csv
import argparse

from TENKAI.features_c import build_c_features

BASE   = "public"
INTEG  = os.path.join(BASE, "integrated", "v1")
OUTDIR = os.path.join("TENKAI", "features_c", "v1")

def _safe_load(p):
    with open(p, encoding="utf-8") as f:
        return json.load(f)

def _write_one_row_csv(path: str, row: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    cols = list(row.keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerow(row)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYYMMDD")
    ap.add_argument("--pid",  required=True, help="場コード (pid)")
    ap.add_argument("--race", default="",   help="例: 1R。空なら全R")
    args = ap.parse_args()

    os.makedirs(OUTDIR, exist_ok=True)

    race_pat = f"{args.race}.json" if args.race else "*.json"
    glob_pat = os.path.join(INTEG, args.date, args.pid, race_pat)

    files = sorted(glob.glob(glob_pat))
    if not files:
        print("no targets:", glob_pat)
        return

    for integ_path in files:
        try:
            integ = _safe_load(integ_path)
            row   = build_c_features(integ)

            date = str(row["date"])
            pid  = str(row["pid"])
            race = str(row["race"])
            out_path = os.path.join(OUTDIR, f"{date}_{pid}_{race}.csv")
            _write_one_row_csv(out_path, row)
            print("saved:", out_path)
        except Exception as e:
            print("skip:", integ_path, e)

if __name__ == "__main__":
    main()
