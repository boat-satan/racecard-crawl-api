# -*- coding: utf-8 -*-
"""
C特徴(ワイド) × labels(レーン別) → 学習用テーブル（レーン1行）
入力:
  TENKAI/features_c/v1/{date}/{pid}/{race}.csv  (例: 1R.csv / all.csv)
  TENKAI/labels/v1/{date}/{pid}/{race}.csv      (例: 1R.csv / all.csv)
出力:
  TENKAI/datasets/v1/{date}/{pid}/{race}_train.csv   … 枠番6行
  TENKAI/datasets/v1/{date}/{pid}/all_train.csv      … pid配下まとめ
"""
import os
import re
import argparse
import pandas as pd

FEAT_BASE = os.path.join("TENKAI", "features_c", "v1")
LABL_BASE = os.path.join("TENKAI", "labels",    "v1")
OUT_BASE  = os.path.join("TENKAI", "datasets",  "v1")

LANES = [1,2,3,4,5,6]
PFX_RE = re.compile(r"^L([1-6])_(.+)$")


def _load_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    return pd.read_csv(path, dtype=str).convert_dtypes()


def _to_long_per_lane(df_wide: pd.DataFrame) -> pd.DataFrame:
    """
    L{lane}_xxx を溶かして、(date,pid,race,lane,feature...) にする
    - 共通列(date,pid,race,mean_*)をそのまま持たせる
    - 欠損列は自動で落ちる（存在列のみ展開）
    """
    commons = [c for c in df_wide.columns if not PFX_RE.match(c)]
    # melt相当: 各行ごとに lane=1..6 を作る
    rows = []
    for _, r in df_wide.iterrows():
        base = {c: r[c] for c in commons if c in r.index}
        # lane別のキー収集
        by_lane = {lane: {} for lane in LANES}
        for c in df_wide.columns:
            m = PFX_RE.match(c)
            if not m:
                continue
            lane = int(m.group(1)); key = m.group(2)
            by_lane[lane][key] = r[c]
        for lane in LANES:
            row = dict(base)
            row["lane"] = lane
            # laneの特徴（存在するキーのみ）
            for k, v in by_lane.get(lane, {}).items():
                row[k] = v
            rows.append(row)
    df_long = pd.DataFrame(rows)
    # 数値にできるものは数値化
    for c in df_long.columns:
        if c in ("date","pid","race","decision"):  # これらは文字列
            continue
        df_long[c] = pd.to_numeric(df_long[c], errors="ignore")
    return df_long


def build_training(date: str, pid: str, race: str = ""):
    in_feat_dir = os.path.join(FEAT_BASE, date, pid)
    in_lab_dir  = os.path.join(LABL_BASE, date, pid)
    out_dir     = os.path.join(OUT_BASE,  date, pid)
    os.makedirs(out_dir, exist_ok=True)

    targets = [race] if race else [f"{i}R" for i in range(1,13)]

    all_out = []
    for r in targets:
        feat_path = os.path.join(in_feat_dir, f"{r}.csv")
        lab_path  = os.path.join(in_lab_dir,  f"{r}.csv")
        if not (os.path.exists(feat_path) and os.path.exists(lab_path)):
            print(f"skip (missing): {r}  feat={os.path.exists(feat_path)} label={os.path.exists(lab_path)}")
            continue

        # 読み込み
        df_feat_wide = _load_csv(feat_path)
        df_lab = _load_csv(lab_path)

        # ワイド→レーン1行
        df_feat = _to_long_per_lane(df_feat_wide)

        # キーで内部整合チェック
        key_cols = ["date","pid","race"]
        # 値を文字列正規化（raceは '1R' 形式）
        for c in key_cols:
            df_feat[c] = df_feat[c].astype(str)
            df_lab[c]  = df_lab[c].astype(str)

        # 結合（date,pid,race,lane）
        df_lab["lane"] = pd.to_numeric(df_lab["lane"], errors="coerce")
        df_merged = df_feat.merge(df_lab, on=key_cols+["lane"], how="inner", validate="one_to_one")

        # 列順整備：キー → 主要特徴 → 汎用
        cols_front = ["date","pid","race","lane"]
        # 目的変数
        cols_target = ["rank","win","st","decision"]
        # 前に置きたい代表的C特徴
        prefer_feat = [
            "startCourse","class","age","avgST_rc","ec_avgST",
            "flying","late","ss_starts","ss_first","ss_second","ss_third",
            "ms_winRate","ms_top2Rate","ms_top3Rate","win_k","lose_k",
            "d_avgST_rc","d_age","d_class","rank_avgST","rank_age","rank_class",
            "mean_avgST_rc","mean_age","mean_class"
        ]
        # 実際に存在する列だけ
        exist_pref = [c for c in prefer_feat if c in df_merged.columns]
        # 残りの特徴
        others = [c for c in df_merged.columns if c not in set(cols_front+cols_target+exist_pref)]

        ordered = cols_front + exist_pref + others + cols_target
        df_merged = df_merged.reindex(columns=ordered)

        # 基本的な妥当性出力
        if df_merged["lane"].nunique() != len(LANES):
            print(f"warn: lane count != 6 at {r}  ({df_merged['lane'].nunique()})")
        if not df_merged["rank"].isna().all():
            bad_rank = ~df_merged["rank"].astype("Int64").between(1,6)
            if bad_rank.any():
                print(f"warn: out-of-range rank at {r}")

        # 書き出し
        out_path = os.path.join(out_dir, f"{r}_train.csv")
        df_merged.to_csv(out_path, index=False, encoding="utf-8")
        print(f"wrote {out_path}  (rows={len(df_merged)})")

        all_out.append(df_merged)

    # 集約
    if all_out:
        df_all = pd.concat(all_out, ignore_index=True)
        all_path = os.path.join(out_dir, "all_train.csv")
        df_all.to_csv(all_path, index=False, encoding="utf-8")
        print(f"wrote {all_path}  (rows={len(df_all)})")
    else:
        print("no outputs")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--pid",  required=True)
    ap.add_argument("--race", default="")
    args = ap.parse_args()
    build_training(args.date, args.pid, args.race)


if __name__ == "__main__":
    main()
