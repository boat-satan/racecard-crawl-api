# TENKAI/features_c.py
# -*- coding: utf-8 -*-
"""
integrated/v1 から C(編成・相対)特徴を抽出して CSV 出力
出力: TENKAI/features_c/v1/<date>/<pid>/<race or all>.csv
"""

from __future__ import annotations
import os, json, argparse
import pandas as pd
from typing import Any, Dict, List, Optional

BASE = "public/integrated/v1"

def _safe_get(d: Optional[Dict[str, Any]], *keys, default=None):
    cur: Any = d or {}
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def _to_float(x):
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def _rank(values: List[Optional[float]]) -> List[int]:
    """小さい方が良い指標(例: avgST)は昇順＝1が最良。Noneは最下位相当。"""
    pairs = [(i, v if v is not None else float("inf")) for i, v in enumerate(values)]
    pairs.sort(key=lambda t: t[1])
    ranks = [0] * len(values)
    r = 1
    for idx, _ in pairs:
        ranks[idx] = r
        r += 1
    return ranks

KIMARITE_KEYS = ["逃げ","差し","まくり","まくり差し","抜き","恵まれ"]

def build_c_features(date: str, pid: str, race: str = ""):
    base_dir = os.path.join(BASE, date, pid)
    targets = [race] if race else [f"{i}R" for i in range(1, 12 + 1)]

    rows: List[Dict[str, Any]] = []

    for r in targets:
        path = os.path.join(base_dir, f"{r}.json")
        if not os.path.exists(path):
            print(f"skip: {path} (not found)")
            continue

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        entries = data.get("entries", []) or []
        # レース内の相対値用ベクトル
        rc_avgSTs: List[Optional[float]] = []
        ages: List[Optional[float]] = []
        classes: List[Optional[float]] = []
        for e in entries:
            rc = e.get("racecard", {}) or {}
            rc_avgSTs.append(_to_float(rc.get("avgST")))
            ages.append(_to_float(rc.get("age")))
            classes.append(_to_float(rc.get("classNumber")))

        rank_avgST = _rank(rc_avgSTs)
        rank_age   = _rank(ages)      # 若いほうが良い想定で昇順
        rank_class = _rank(classes)   # 低い級のほうが良い想定で昇順

        row: Dict[str, Any] = {"date": date, "pid": pid, "race": r}

        for idx, e in enumerate(entries):
            lane = int(e.get("lane"))
            rc = e.get("racecard", {}) or {}
            # exhibition ではなく stats.entryCourse を ec として使う
            ec = _safe_get(e, "stats", "entryCourse", default={}) or {}
            ss = _safe_get(e, "stats", "entryCourse", "selfSummary", default={}) or {}
            ms = _safe_get(e, "stats", "entryCourse", "matrixSelf", default={}) or {}
            winK = _safe_get(e, "stats", "entryCourse", "winKimariteSelf", default={}) or {}
            loseK = _safe_get(e, "stats", "entryCourse", "loseKimarite", default={}) or {}

            prefix = f"L{lane}_"

            feat = {
                "startCourse": e.get("startCourse"),
                "class": rc.get("classNumber"),
                "age": rc.get("age"),
                "avgST_rc": _to_float(rc.get("avgST")),
                "ec_avgST": _to_float(ec.get("avgST")),
                "flying": rc.get("flyingCount"),
                "late": rc.get("lateCount"),

                "ss_starts": ss.get("starts"),
                "ss_first": ss.get("firstCount"),
                "ss_second": ss.get("secondCount"),
                "ss_third": ss.get("thirdCount"),

                "ms_winRate": _to_float(ms.get("winRate")),
                "ms_top2Rate": _to_float(ms.get("top2Rate")),
                "ms_top3Rate": _to_float(ms.get("top3Rate")),
            }

            # 決まり手別 勝ち/負けカウント（存在しないキーは 0）
            for k in KIMARITE_KEYS:
                feat[f"winK_{k}"]  = int(winK.get(k, 0) or 0)
                feat[f"loseK_{k}"] = int(loseK.get(k, 0) or 0)
            # 合計
            feat["winK_total"]  = sum(feat[f"winK_{k}"]  for k in KIMARITE_KEYS)
            feat["loseK_total"] = sum(feat[f"loseK_{k}"] for k in KIMARITE_KEYS)

            # 差分系（基準は経験的中央値）
            feat["d_avgST_rc"] = (feat["avgST_rc"] if feat["avgST_rc"] is not None else 0.16) - 0.16
            feat["d_age"]      = (feat["age"] if feat["age"] is not None else 40) - 40
            feat["d_class"]    = (feat["class"] if feat["class"] is not None else 3) - 3

            # レース内順位
            feat["rank_avgST"] = rank_avgST[idx]
            feat["rank_age"]   = rank_age[idx]
            feat["rank_class"] = rank_class[idx]

            for k, v in feat.items():
                row[f"{prefix}{k}"] = v

        # レース全体平均
        def _mean(xs: List[Optional[float]]):
            xs = [x for x in xs if x is not None]
            return sum(xs) / len(xs) if xs else None

        row["mean_avgST_rc"] = _mean(rc_avgSTs)
        row["mean_age"]      = _mean(ages)
        row["mean_class"]    = _mean(classes)

        rows.append(row)

    # === 出力（0件でも必ず CSV を作る） ===
    outdir = os.path.join("TENKAI", "features_c", "v1", date, pid)
    os.makedirs(outdir, exist_ok=True)
    outfile = os.path.join(outdir, f"{race or 'all'}.csv")

    if rows:
        df = pd.DataFrame(rows)
        df.to_csv(outfile, index=False, encoding="utf-8")
        print(f"wrote {outfile} ({len(df)} rows)")
    else:
        # ヘッダだけの空CSV（最低限の3列）
        df = pd.DataFrame(columns=["date", "pid", "race"])
        df.to_csv(outfile, index=False, encoding="utf-8")
        print(f"wrote {outfile} (0 rows; no integrated files found under {base_dir})")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--pid", required=True)
    ap.add_argument("--race", default="")
    args = ap.parse_args()
    build_c_features(args.date, args.pid, args.race)
