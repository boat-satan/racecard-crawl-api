# -*- coding: utf-8 -*-
"""
integrated/v1 の1開催(=date,pid)から C(編成・相対)特徴を作成
出力: TENKAI/features_c/v1/{date}/{pid}/{race}.csv  （race 未指定なら全Rを個別CSV）
- ボート/モーター要素は使わない
- exhibition が None でも落ちないように全取得は or {} で辞書保証
- 勝ち/負けの決まり手は「抜き」「恵まれ」を除外
"""

import os
import json
import argparse
import pandas as pd
from typing import Dict, Any, List

BASE_INTEG = os.path.join("public", "integrated", "v1")
OUT_BASE   = os.path.join("TENKAI", "features_c", "v1")

ALLOW_K = ("逃げ", "差し", "まくり", "まくり差し")  # 抜き/恵まれは除外


def _safe_load(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _lane_row_prefixed(lane: int, e: Dict[str, Any]) -> Dict[str, Any]:
    """単艇の特徴量（prefix=L{lane}_）を作る。欠損があっても落ちない。"""
    rc = (e.get("racecard") or {})
    st = (e.get("stats") or {})
    ec = (st.get("entryCourse") or {})
    # 決まり手集計
    win_self: Dict[str, Any] = ec.get("winKimariteSelf") or {}
    lose_all: Dict[str, Any] = ec.get("loseKimarite") or {}
    win_k = sum(win_self.get(k, 0) or 0 for k in ALLOW_K)
    lose_k = sum(lose_all.get(k, 0) or 0 for k in ALLOW_K)

    # 自己サマリ
    ss = (ec.get("selfSummary") or {})
    starts = ss.get("starts")
    first  = ss.get("firstCount")
    second = ss.get("secondCount")
    third  = ss.get("thirdCount")

    pref = f"L{lane}_"
    row = {
        pref + "startCourse": e.get("startCourse"),
        pref + "class":       rc.get("classNumber"),
        pref + "age":         rc.get("age"),
        pref + "avgST_rc":    rc.get("avgST"),
        pref + "ec_avgST":    ec.get("avgST"),
        pref + "flying":      rc.get("flyingCount"),
        pref + "late":        rc.get("lateCount"),
        pref + "ss_starts":   starts,
        pref + "ss_first":    first,
        pref + "ss_second":   second,
        pref + "ss_third":    third,
        pref + "ms_winRate":  (ec.get("matrixSelf") or {}).get("winRate"),
        pref + "ms_top2Rate": (ec.get("matrixSelf") or {}).get("top2Rate"),
        pref + "ms_top3Rate": (ec.get("matrixSelf") or {}).get("top3Rate"),
        pref + "win_k":       win_k,
        pref + "lose_k":      lose_k,
        # Δは平均値からの差（後で race 全体平均を出してから埋め直す）
        pref + "d_avgST_rc":  None,
        pref + "d_age":       None,
        pref + "d_class":     None,
        # ランク（小さいほど良い=1位）。後で一括計算して埋める。
        pref + "rank_avgST":  None,
        pref + "rank_age":    None,
        pref + "rank_class":  None,
    }
    return row


def _calc_group_means(row: Dict[str, Any], lanes: List[int]) -> Dict[str, Any]:
    """レース平均（avgST_rc, age, class）の算出"""
    st_vals   = [row.get(f"L{i}_avgST_rc") for i in lanes if row.get(f"L{i}_avgST_rc") is not None]
    age_vals  = [row.get(f"L{i}_age")      for i in lanes if row.get(f"L{i}_age") is not None]
    cls_vals  = [row.get(f"L{i}_class")    for i in lanes if row.get(f"L{i}_class") is not None]

    row["mean_avgST_rc"] = sum(st_vals)/len(st_vals) if st_vals else None
    row["mean_age"]      = sum(age_vals)/len(age_vals) if age_vals else None
    row["mean_class"]    = sum(cls_vals)/len(cls_vals) if cls_vals else None
    return row


def _fill_deltas_and_ranks(row: Dict[str, Any], lanes: List[int]) -> Dict[str, Any]:
    """平均からの差分とランクを埋める（ランクは昇順=小さい値が1位）"""
    mean_st  = row.get("mean_avgST_rc")
    mean_age = row.get("mean_age")
    mean_cls = row.get("mean_class")

    # 差分
    for i in lanes:
        st = row.get(f"L{i}_avgST_rc")
        ag = row.get(f"L{i}_age")
        cl = row.get(f"L{i}_class")
        if st is not None and mean_st is not None:
            row[f"L{i}_d_avgST_rc"] = st - mean_st
        if ag is not None and mean_age is not None:
            row[f"L{i}_d_age"] = ag - mean_age
        if cl is not None and mean_cls is not None:
            row[f"L{i}_d_class"] = cl - mean_cls

    # ランク（None は除外して順位付け）
    def rank_small_is_better(keys: List[str]) -> None:
        vals = [(idx, row.get(k)) for idx, k in enumerate(keys)]
        present = [(idx, v) for idx, v in vals if v is not None]
        present.sort(key=lambda x: x[1])  # 小さい順
        for rank, (idx, _) in enumerate(present, start=1):
            row[keys[idx].replace("avgST_rc", "rank_avgST")
                       .replace("age", "rank_age")
                       .replace("class", "rank_class")] = rank

    # それぞれの指標でキー配列を用意
    rank_small_is_better([f"L{i}_avgST_rc" for i in lanes])
    rank_small_is_better([f"L{i}_age"      for i in lanes])
    rank_small_is_better([f"L{i}_class"    for i in lanes])
    return row


def build_c_features(date: str, pid: str, race: str = ""):
    """
    1開催(date,pid)の C 特徴を CSV 化。
    race を指定すればその R のみ。それ以外は 1R..12R を個別CSVで出力。
    """
    in_dir = os.path.join(BASE_INTEG, str(date), str(pid))
    if not os.path.isdir(in_dir):
        print(f"no dir: {in_dir}")
        return

    targets = [race] if race else [f"{i}R" for i in range(1, 13)]
    out_dir_root = os.path.join(OUT_BASE, str(date), str(pid))
    os.makedirs(out_dir_root, exist_ok=True)

    for r in targets:
        integ_path = os.path.join(in_dir, f"{r}.json")
        if not os.path.exists(integ_path):
            print(f"skip: {integ_path} (not found)")
            continue

        data = _safe_load(integ_path)
        entries = data.get("entries") or []
        # 1..6のレーンで揃える（欠けていればスキップするだけでOK）
        lanes = sorted({int(e.get("lane")) for e in entries if e.get("lane") is not None}) or [1,2,3,4,5,6]

        row: Dict[str, Any] = {"date": str(date), "pid": str(pid), "race": str(r)}
        for e in entries:
            lane = int(e.get("lane"))
            row.update(_lane_row_prefixed(lane, e))

        # 平均と差分・ランク
        row = _calc_group_means(row, lanes)
        row = _fill_deltas_and_ranks(row, lanes)

        # CSV 出力（1レース1行）
        df = pd.DataFrame([row])
        out_path = os.path.join(out_dir_root, f"{r}.csv")
        df.to_csv(out_path, index=False, encoding="utf-8")
        print(f"wrote: {out_path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--pid",  required=True)
    ap.add_argument("--race", default="")
    args = ap.parse_args()
    build_c_features(args.date, args.pid, args.race)


if __name__ == "__main__":
    main()
