# -*- coding: utf-8 -*-
"""
results/v1 を読み、レースごとの教師ラベルCSVを作成
- 入力: public/results/v1/{date}/{pid}/{race}.json
- 出力:
  TENKAI/labels/v1/{date}/{pid}/{race}.csv  … レース単位（6行）
  TENKAI/labels/v1/{date}/{pid}/all.csv     … pid配下まとめ（winがある行のみ）
CSV列:
  date,pid,race,lane,rank,st,decision,win
    win = 1(着順=1) else 0（rankが欠損なら None）
"""
from __future__ import annotations

import os
import json
import pandas as pd
import argparse
from typing import List, Dict, Any

DEFAULT_INBASE = os.path.join("public", "results", "v1")
OUTBASE = os.path.join("TENKAI", "labels", "v1")


def _safe(v, default=None):
    return v if v is not None else default


def json_to_rows(res_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    """results JSON → 行リスト（各lane 1行）"""
    date = str(res_obj.get("date"))
    pid = str(res_obj.get("pid"))
    race = str(res_obj.get("race"))

    # map: lane -> rank
    ranks: Dict[int, int | None] = {}
    for x in (res_obj.get("order") or []):
        lane = _safe(x.get("lane"))
        pos = _safe(x.get("pos"))
        if lane is None:
            continue
        try:
            ranks[int(lane)] = int(pos) if pos is not None else None
        except Exception:
            ranks[int(lane)] = None

    # map: lane -> st
    sts: Dict[int, float | None] = {}
    for x in (res_obj.get("start") or []):
        lane = _safe(x.get("lane"))
        st = _safe(x.get("st"))
        if lane is None:
            continue
        try:
            sts[int(lane)] = float(st) if st is not None else None
        except Exception:
            sts[int(lane)] = None

    decision = _safe(res_obj.get("decision"))

    rows: List[Dict[str, Any]] = []
    # 1〜6枠を必ず出す（欠けても空行を置く）
    for lane in range(1, 7):
        rank = ranks.get(lane)
        st = sts.get(lane)
        rows.append({
            "date": date,
            "pid": pid,
            "race": race,
            "lane": lane,
            "rank": rank,
            "st": st,
            "decision": decision,
            "win": 1 if rank == 1 else (0 if rank is not None else None),
        })
    return rows


def _race_names(race: str) -> List[str]:
    """race 引数を 1R..12R のリストに正規化（空 or 'ALL' は全レース）。"""
    if not race:
        return [f"{i}R" for i in range(1, 13)]
    r = race.strip()
    if r.lower() == "all":
        return [f"{i}R" for i in range(1, 13)]
    return [r]


def build_labels(date: str, pid: str, race: str = "", inbase: str = DEFAULT_INBASE):
    """
    指定キーで results JSON → ラベルCSV を出力
    - race 未指定/ALL なら 1R..12R を探索し、存在するJSONのみ処理
    - ブランク結果（全レーン rank/st とも欠損）はスキップ
    - all.csv には win が 0/1 の行のみ蓄積
    """
    targets = _race_names(race)
    outdir = os.path.join(OUTBASE, date, pid)
    os.makedirs(outdir, exist_ok=True)

    all_rows: List[Dict[str, Any]] = []

    for r in targets:
        path = os.path.join(inbase, date, pid, f"{r}.json")
        if not os.path.exists(path):
            print(f"skip (not found): {path}")
            continue

        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)
        except Exception as e:
            print(f"skip (json load error): {path}  {e}")
            continue

        try:
            rows = json_to_rows(obj)
            if not rows:
                print(f"skip (empty json): {path}")
                continue

            # ★ ブランク結果（全レーン rank も st も欠損）は丸ごとスキップ
            if all((row["rank"] is None and row["st"] is None) for row in rows):
                print(f"skip (blank result): {path}")
                continue

            # レース単位 CSV（確認用に6行そのまま保存）
            race_name = rows[0]["race"] or r
            df = pd.DataFrame(rows)
            outfile = os.path.join(outdir, f"{race_name}.csv")
            df.to_csv(outfile, index=False, encoding="utf-8")
            print(f"wrote {outfile}")

            # all.csv には win が 0/1 の行だけ積む
            all_rows.extend([row for row in rows if row["win"] is not None])

        except Exception as e:
            print(f"skip (error): {path}  {e}")

    if all_rows:
        df_all = pd.DataFrame(all_rows)
        outfile_all = os.path.join(outdir, "all.csv")
        df_all.to_csv(outfile_all, index=False, encoding="utf-8")
        print(f"wrote {outfile_all}")
    else:
        print("no outputs")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--pid", required=True)
    ap.add_argument("--race", default="", help="'' or ALL = 1R..12R")
    ap.add_argument("--inbase", default=DEFAULT_INBASE, help="results JSON base dir")
    args = ap.parse_args()
    build_labels(args.date, args.pid, args.race, args.inbase)


if __name__ == "__main__":
    main()
