# -*- coding: utf-8 -*-
"""
integrated/v1 を走査して C(編成・相対)特徴をレースごとCSVで出力
出力: public/TENKAI/features_c/v1/<date>/<pid>/<race>.csv
"""
import os, glob, json, csv, sys, argparse

# TENKAI 配下の自作モジュールを import
sys.path.append(os.path.dirname(__file__))
from features_c import build_c_features  # build_c_features(integ_json) -> dict(1行)

BASE   = "public"
INTEG  = os.path.join(BASE, "integrated", "v1")
OUTRT  = os.path.join(BASE, "TENKAI", "features_c", "v1")

def _safe_load(p):
    with open(p, encoding="utf-8") as f:
        return json.load(f)

def _write_one_row_csv(out_path: str, row: dict):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    # 安定順: まず date/pid/race を先頭、その後はキー昇順
    head = ["date", "pid", "race"]
    rest = sorted([k for k in row.keys() if k not in head])
    cols = [c for c in head if c in row] + rest
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerow(row)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYYMMDD")
    ap.add_argument("--pid",  required=True, help="場コード (例: 02)")
    ap.add_argument("--race", default="",  help="例: 1R 空なら全R")
    args = ap.parse_args()

    # 走査対象を絞り込み
    race_pat = f"{args.race}.json" if args.race else "*.json"
    paths = sorted(glob.glob(os.path.join(INTEG, args.date, args.pid, race_pat)))
    if not paths:
        print("no targets:", os.path.join(INTEG, args.date, args.pid, race_pat))
        return

    for p in paths:
        try:
            integ = _safe_load(p)
            row = build_c_features(integ)  # 1レース=1行の辞書
            # 出力パス
            date = str(integ.get("date"))
            pid  = str(integ.get("pid"))
            race = str(integ.get("race"))
            out_path = os.path.join(OUTRT, date, pid, f"{race}.csv")
            _write_one_row_csv(out_path, row)
            print("saved:", out_path)
        except Exception as e:
            print("skip:", p, e)

if __name__ == "__main__":
    main()
