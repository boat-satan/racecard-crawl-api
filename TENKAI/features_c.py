# -*- coding: utf-8 -*-
"""
C(編成・相対)特徴を 1レース=1CSV で出力
出力先: TENKAI/features_c/v1/<date>/<pid>/<race>.csv
"""
import os
import json
import pandas as pd
from typing import Any, Dict, List, Optional


def _safe_mean(vals: List[Optional[float]]) -> Optional[float]:
    arr = [v for v in vals if v is not None]
    return (sum(arr) / len(arr)) if arr else None


def _rank_min_ties(vals: List[Optional[float]], asc: bool = True) -> Dict[int, Optional[int]]:
    """同値は同順位（最小順位）。Noneは順位なし(None)"""
    pairs = [(i, v) for i, v in enumerate(vals) if v is not None]
    pairs.sort(key=lambda x: x[1], reverse=not asc)
    ranks: Dict[int, Optional[int]] = {}
    last_val = object()
    last_rank = 0
    for idx, (i, v) in enumerate(pairs, start=1):
        if v != last_val:
            last_rank = idx
            last_val = v
        ranks[i] = last_rank
    # fill None for missing
    for i, v in enumerate(vals):
        if v is None:
            ranks[i] = None
    return ranks


def _sum_dict_values(d: Optional[Dict[str, Any]]) -> int:
    if not isinstance(d, dict):
        return 0
    s = 0
    for _, v in d.items():
        try:
            s += int(v or 0)
        except Exception:
            continue
    return s


def _get(d: Dict[str, Any], path: List[str], default=None):
    cur = d
    try:
        for k in path:
            cur = cur.get(k, {})
        return cur if cur != {} else default
    except Exception:
        return default


def build_c_features(date: str, pid: str, race: str = ""):
    """
    C特徴量を生成し CSV を保存。
    - 偏差 d_* は『各艇 − レース平均』
    - 順位 rank_* は昇順（小さいほど上位）
    - ec_avgST は stats.entryCourse.avgST を採用
    """
    base_dir = f"public/integrated/v1/{date}/{pid}"
    races = [race] if race else [f"{i}R" for i in range(1, 13)]
    outputs: List[Dict[str, Any]] = []

    for r in races:
        path = os.path.join(base_dir, f"{r}.json")
        if not os.path.exists(path):
            print(f"skip {path} (not found)")
            continue

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        entries: List[Dict[str, Any]] = data.get("entries", [])
        if not entries:
            print(f"skip {path} (no entries)")
            continue

        # まずレース内の配列（平均＆順位用）
        avgst_list: List[Optional[float]] = []
        age_list: List[Optional[float]] = []
        class_list: List[Optional[float]] = []

        for e in entries:
            rc = e.get("racecard", {}) or {}
            avgst_list.append(rc.get("avgST"))
            age_list.append(rc.get("age"))
            class_list.append(rc.get("classNumber"))

        mean_avgst = _safe_mean(avgst_list)
        mean_age = _safe_mean(age_list)
        mean_class = _safe_mean(class_list)

        rank_avgst = _rank_min_ties(avgst_list, asc=True)   # 速いSTほど上位
        rank_age = _rank_min_ties(age_list, asc=True)       # 若いほど上位
        rank_class = _rank_min_ties(class_list, asc=True)   # A1=1 が上位

        # 1行に全艇分を詰める
        row: Dict[str, Any] = {"date": date, "pid": pid, "race": r}

        for idx, e in enumerate(entries):
            lane = e.get("lane")
            if lane is None:
                continue
            rc = e.get("racecard", {}) or {}
            ec_stats = _get(e, ["stats", "entryCourse"], {}) or {}

            prefix = f"L{lane}_"

            # selfSummary
            ss = ec_stats.get("selfSummary") or {}
            ss_starts = ss.get("starts")
            ss_first = ss.get("firstCount")
            ss_second = ss.get("secondCount")
            ss_third = ss.get("thirdCount")

            # matrixSelf
            ms = ec_stats.get("matrixSelf") or {}
            ms_win = ms.get("winRate")
            ms_t2 = ms.get("top2Rate")
            ms_t3 = ms.get("top3Rate")

            # 勝ち数: firstCount、負け数: loseKimarite 合計
            lose_k = _sum_dict_values(ec_stats.get("loseKimarite"))

            avgst = rc.get("avgST")
            age = rc.get("age")
            cls = rc.get("classNumber")

            feat = {
                "startCourse": e.get("startCourse"),
                "class": cls,
                "age": age,
                "avgST_rc": avgst,
                "ec_avgST": ec_stats.get("avgST"),  # ← stats.entryCourse 由来
                "flying": rc.get("flyingCount"),
                "late": rc.get("lateCount"),
                "ss_starts": ss_starts,
                "ss_first": ss_first,
                "ss_second": ss_second,
                "ss_third": ss_third,
                "ms_winRate": ms_win,
                "ms_top2Rate": ms_t2,
                "ms_top3Rate": ms_t3,
                "win_k": (ss_first or 0),
                "lose_k": lose_k,
                # 偏差（各艇 − レース平均）
                "d_avgST_rc": (avgst - mean_avgst) if (avgst is not None and mean_avgst is not None) else None,
                "d_age": (age - mean_age) if (age is not None and mean_age is not None) else None,
                "d_class": (cls - mean_class) if (cls is not None and mean_class is not None) else None,
                # レース内順位（1=最上位）
                "rank_avgST": rank_avgst.get(idx),
                "rank_age": rank_age.get(idx),
                "rank_class": rank_class.get(idx),
            }

            for k, v in feat.items():
                row[f"{prefix}{k}"] = v

        # レース平均値を末尾に
        row["mean_avgST_rc"] = mean_avgst
        row["mean_age"] = mean_age
        row["mean_class"] = mean_class

        # 出力（1レース=1CSV）
        outdir = os.path.join("TENKAI", "features_c", "v1", date, pid)
        os.makedirs(outdir, exist_ok=True)
        outfile = os.path.join(outdir, f"{r}.csv")
        pd.DataFrame([row]).to_csv(outfile, index=False, encoding="utf-8")
        print(f"wrote {outfile}")

        outputs.append(row)

    if not outputs:
        print("no outputs")


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--pid", required=True)
    ap.add_argument("--race", default="")
    args = ap.parse_args()

    build_c_features(args.date, args.pid, args.race)
