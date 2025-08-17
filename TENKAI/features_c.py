# TENKAI/features_c.py
# 全差し替え版 (2025-08 修正版)
# - Δ列: 平均との差分
# - rank列: 本体値でランク付け (小さい=良い, 同値は最小ランク共有)

import os
import json
import argparse
import pandas as pd
import numpy as np

def load_integrated(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def build_features(integrated):
    date = integrated["date"]
    pid  = integrated["pid"]
    race = integrated["race"]

    lanes = []
    for e in integrated["entries"]:
        lane = e["lane"]
        rc   = e["racecard"]
        ex   = e.get("exhibition", {})
        stats = e.get("stats", {})

        l = dict(
            lane        = lane,
            startCourse = e.get("startCourse"),
            number      = rc["number"],
            class       = rc["classNumber"],
            age         = rc["age"],
            avgST_rc    = rc.get("avgST", 0.0),
            ec_avgST    = ex.get("avgST", 0.0),
            flying      = rc.get("flyingCount", 0),
            late        = rc.get("lateCount", 0),
            ss_starts   = stats.get("ss_starts", 0),
            ss_first    = stats.get("ss_first", 0),
            ss_second   = stats.get("ss_second", 0),
            ss_third    = stats.get("ss_third", 0),
            ms_winRate  = stats.get("ms_winRate", 0.0),
            ms_top2Rate = stats.get("ms_top2Rate", 0.0),
            ms_top3Rate = stats.get("ms_top3Rate", 0.0),
            win_k       = stats.get("win_k", 0),
            lose_k      = stats.get("lose_k", 0),
        )
        lanes.append(l)

    # 平均
    mean_avgST = np.mean([l["avgST_rc"] for l in lanes])
    mean_age   = np.mean([l["age"] for l in lanes])
    mean_class = np.mean([l["class"] for l in lanes])

    # 差分
    for l in lanes:
        l["d_avgST_rc"] = l["avgST_rc"] - mean_avgST
        l["d_age"]      = l["age"]      - mean_age
        l["d_class"]    = l["class"]    - mean_class

    # ランク (小さい=良い)
    df_rank = pd.DataFrame({
        "avgST_rc": [l["avgST_rc"] for l in lanes],
        "age":      [l["age"] for l in lanes],
        "class":    [l["class"] for l in lanes],
    })
    r_avgst = df_rank["avgST_rc"].rank(method="min", ascending=True).astype(int).tolist()
    r_age   = df_rank["age"].rank(method="min", ascending=True).astype(int).tolist()
    r_class = df_rank["class"].rank(method="min", ascending=True).astype(int).tolist()

    for i, l in enumerate(lanes):
        l["rank_avgST"] = r_avgst[i]
        l["rank_age"]   = r_age[i]
        l["rank_class"] = r_class[i]

    # 出力行 (flat化)
    row = dict(date=date, pid=pid, race=race)
    for l in lanes:
        prefix = f"L{l['lane']}"
        for k, v in l.items():
            if k == "lane": continue
            row[f"{prefix}_{k}"] = v

    # 平均も付ける
    row["mean_avgST_rc"] = mean_avgST
    row["mean_age"]      = mean_age
    row["mean_class"]    = mean_class

    return row

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--pid", required=True)
    parser.add_argument("--race", required=True)
    parser.add_argument("--out", default="TENKAI/features_c.csv")
    args = parser.parse_args()

    in_path = f"public/integrated/v1/{args.date}/{args.pid}/{args.race}.json"
    if not os.path.exists(in_path):
        print(f"no targets: {in_path}")
        return

    integrated = load_integrated(in_path)
    row = build_features(integrated)

    # append-or-create
    out_exists = os.path.exists(args.out)
    df = pd.DataFrame([row])
    df.to_csv(args.out, mode="a", header=not out_exists, index=False, encoding="utf-8-sig")

    print(f"wrote → {args.out}")

if __name__ == "__main__":
    main()
