# scripts/TENKAI/build_c_features.py
# -*- coding: utf-8 -*-
"""
integrated/v1 を走査して C(編成・相対)特徴を CSV に出力
- 入力: --date YYYYMMDD, --pid 02 など、--race (例: 1R) は任意
- 走査対象: public/integrated/v1/<date>/<pid>/<race or *.json>
- 出力:  public/TENKAI/features_c/v1/features_c.csv
"""
import os
import glob
import json
import csv
import argparse

# 同ディレクトリの features_c.py を利用
from features_c import build_c_features

BASE = "public"
INTEG = os.path.join(BASE, "integrated", "v1")

# ✅ workflow と揃える（大文字の TENKAI）
OUTDIR = os.path.join(BASE, "TENKAI", "features_c", "v1")
OUTCSV = os.path.join(OUTDIR, "features_c.csv")


def _safe_load(path: str):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="対象日 YYYYMMDD")
    ap.add_argument("--pid",  required=True, help="場コード pid (例: 02)")
    ap.add_argument("--race", default="",   help="レース名 (例: 1R) 空なら全R")
    args = ap.parse_args()

    # 走査パスを組み立て
    race_pat = f"{args.race}.json" if args.race else "*.json"
    glob_pat = os.path.join(INTEG, args.date, args.pid, race_pat)
    paths = sorted(glob.glob(glob_pat))

    if not paths:
        print(f"no targets: {glob_pat}")
        return

    os.makedirs(OUTDIR, exist_ok=True)

    rows = []
    for p in paths:
        try:
            integ = _safe_load(p)
            row = build_c_features(integ)   # features_c.py が dict を返す想定
            if isinstance(row, dict):
                rows.append(row)
            else:
                print("skip (not dict):", p)
        except Exception as e:
            print("skip:", p, e)

    if not rows:
        print("no rows.")
        return

    # 列順は最初の行のキー順に合わせる（安定しない場合はここで並べ替え定義）
    cols = list(rows[0].keys())

    with open(OUTCSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"saved: {OUTCSV} ({len(rows)} rows)")

if __name__ == "__main__":
    main()
