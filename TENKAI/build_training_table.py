# -*- coding: utf-8 -*-
"""
C特徴(ワイド) × labels(レーン別) → 学習用テーブル（レーン1行）
入力(どちらでも可):
  TENKAI/features_c/v1/{date}/{pid}/{race}.csv   … 例: 1R.csv
  TENKAI/features_c/v1/{date}/{pid}/all.csv      … まとめ、race列で抽出
  TENKAI/labels/v1/{date}/{pid}/{race}.csv       … 例: 1R.csv
  TENKAI/labels/v1/{date}/{pid}/all.csv          … まとめ、race列で抽出
出力:
  TENKAI/datasets/v1/{date}/{pid}/{race}_train.csv
  TENKAI/datasets/v1/{date}/{pid}/all_train.csv
使い方:
  PYTHONPATH="." python TENKAI/build_training_table.py --date 20250814 --pid 02
  # 単レース:
  PYTHONPATH="." python TENKAI/build_training_table.py --date 20250814 --pid 02 --race 1R
  # “--race”未指定 or "ALL" でそのpidの全レース
"""

import os
import re
import argparse
import pandas as pd

FEAT_BASE = os.path.join("TENKAI", "features_c", "v1")
LABL_BASE = os.path.join("TENKAI", "labels",    "v1")
OUT_BASE  = os.path.join("TENKAI", "datasets",  "v1")

LANES = [1, 2, 3, 4, 5, 6]
PFX_RE = re.compile(r"^L([1-6])_(.+)$")


def _load_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    return pd.read_csv(path, dtype=str).convert_dtypes()


def _to_long_per_lane(df_wide: pd.DataFrame) -> pd.DataFrame:
    """L{lane}_xxx を (date,pid,race,lane,feature...) に変換"""
    commons = [c for c in df_wide.columns if not PFX_RE.match(c)]
    rows = []
    for _, r in df_wide.iterrows():
        base = {c: r[c] for c in commons if c in r.index}
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
            row.update(by_lane.get(lane, {}))
            rows.append(row)
    df_long = pd.DataFrame(rows)
    # 数値化できる列は数値に
    for c in df_long.columns:
        if c in ("date", "pid", "race", "decision"):
            continue
        df_long[c] = pd.to_numeric(df_long[c], errors="ignore")
    return df_long


def _load_feat_for_race(date: str, pid: str, race: str) -> pd.DataFrame:
    """{race}.csv が無ければ all.csv を読み race で抽出"""
    in_dir = os.path.join(FEAT_BASE, date, pid)
    per_path = os.path.join(in_dir, f"{race}.csv")
    if os.path.exists(per_path):
        return _load_csv(per_path)
    all_path = os.path.join(in_dir, "all.csv")
    if not os.path.exists(all_path):
        raise FileNotFoundError(per_path)
    df = _load_csv(all_path)
    return df[df["race"].astype(str) == str(race)].reset_index(drop=True)


def _load_label_for_race(date: str, pid: str, race: str) -> pd.DataFrame:
    """{race}.csv が無ければ all.csv を読み race で抽出"""
    in_dir = os.path.join(LABL_BASE, date, pid)
    per_path = os.path.join(in_dir, f"{race}.csv")
    if os.path.exists(per_path):
        return _load_csv(per_path)
    all_path = os.path.join(in_dir, "all.csv")
    if not os.path.exists(all_path):
        raise FileNotFoundError(per_path)
    df = _load_csv(all_path)
    return df[df["race"].astype(str) == str(race)].reset_index(drop=True)


def _detect_targets(date: str, pid: str, race: str):
    """対象レース一覧を決定（labels > features > 既定1R..12R の順で検出）"""
    if race and race != "ALL":
        return [race]
    # labels の {i}R.csv 群
    lab_dir = os.path.join(LABL_BASE, date, pid)
    if os.path.isdir(lab_dir):
        rs = sorted([f[:-4] for f in os.listdir(lab_dir)
                     if f.endswith("R.csv") and f != "all.csv"],
                    key=lambda x: int(x[:-1]) if x.endswith("R") and x[:-1].isdigit() else 999)
        if rs:
            return rs
        # all.csv から race 列
        all_path = os.path.join(lab_dir, "all.csv")
        if os.path.exists(all_path):
            df = _load_csv(all_path)
            rs = sorted(df["race"].dropna().astype(str).unique(),
                        key=lambda x: int(str(x).rstrip("R")) if str(x).rstrip("R").isdigit() else 999)
            if rs:
                return rs
    # features 側で同様に検出
    feat_dir = os.path.join(FEAT_BASE, date, pid)
    if os.path.isdir(feat_dir):
        rs = sorted([f[:-4] for f in os.listdir(feat_dir)
                     if f.endswith("R.csv") and f != "all.csv"],
                    key=lambda x: int(x[:-1]) if x.endswith("R") and x[:-1].isdigit() else 999)
        if rs:
            return rs
        all_path = os.path.join(feat_dir, "all.csv")
        if os.path.exists(all_path):
            df = _load_csv(all_path)
            rs = sorted(df["race"].dropna().astype(str).unique(),
                        key=lambda x: int(str(x).rstrip("R")) if str(x).rstrip("R").isdigit() else 999)
            if rs:
                return rs
    # 見つからなければ既定
    return [f"{i}R" for i in range(1, 12 + 1)]


def build_training(date: str, pid: str, race: str = ""):
    out_dir = os.path.join(OUT_BASE, date, pid)
    os.makedirs(out_dir, exist_ok=True)

    targets = _detect_targets(date, pid, race)
    all_out = []

    for r in targets:
        try:
            df_feat_wide = _load_feat_for_race(date, pid, r)
            df_lab       = _load_label_for_race(date, pid, r)
        except FileNotFoundError as e:
            print(f"skip (missing): {r} ({e})")
            continue

        if df_feat_wide.empty or df_lab.empty:
            print(f"skip (empty): {r}")
            continue

        # ワイド → レーン1行
        df_feat = _to_long_per_lane(df_feat_wide)

        # キー正規化
        for c in ("date", "pid", "race"):
            df_feat[c] = df_feat[c].astype(str)
            df_lab[c]  = df_lab[c].astype(str)

        df_lab["lane"] = pd.to_numeric(df_lab["lane"], errors="coerce")

        # 結合
        df = df_feat.merge(df_lab, on=["date", "pid", "race", "lane"],
                           how="inner", validate="one_to_one")

        # 列順（キー → 代表C特徴 → 残り → 目的変数）
        cols_front = ["date", "pid", "race", "lane"]
        cols_target = ["rank", "win", "st", "decision"]
        prefer_feat = [
            "startCourse","class","age","avgST_rc","ec_avgST",
            "flying","late","ss_starts","ss_first","ss_second","ss_third",
            "ms_winRate","ms_top2Rate","ms_top3Rate","win_k","lose_k",
            "d_avgST_rc","d_age","d_class","rank_avgST","rank_age","rank_class",
            "mean_avgST_rc","mean_age","mean_class"
        ]
        exist_pref = [c for c in prefer_feat if c in df.columns]
        others = [c for c in df.columns if c not in set(cols_front + exist_pref + cols_target)]
        df = df.reindex(columns=cols_front + exist_pref + others + cols_target)

        out_path = os.path.join(out_dir, f"{r}_train.csv")
        df.to_csv(out_path, index=False, encoding="utf-8")
        print(f"wrote {out_path} (rows={len(df)})")

        all_out.append(df)

    if all_out:
        df_all = pd.concat(all_out, ignore_index=True)
        all_path = os.path.join(out_dir, "all_train.csv")
        df_all.to_csv(all_path, index=False, encoding="utf-8")
        print(f"wrote {all_path} (rows={len(df_all)})")
    else:
        print("no outputs")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--pid",  required=True)
    ap.add_argument("--race", default="")  # "" or "ALL" で全レース
    args = ap.parse_args()
    build_training(args.date, args.pid, args.race)


if __name__ == "__main__":
    main()
