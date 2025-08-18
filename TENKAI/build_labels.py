# TENKAI/build_labels.py
# -*- coding: utf-8 -*-
"""
results/v1 を読み、レースごとの教師ラベルCSVを作成
- 入力: public/results/v1/{date}/{pid}/{race}.json
- 出力:
  TENKAI/labels/v1/{date}/{pid}/{race}.csv  … レース単位（6行）
  TENKAI/labels/v1/{date}/{pid}/all.csv     … pid配下まとめ（上書き）
CSV列:
  date,pid,race,lane,rank,st,decision,win
    win = 1(着順=1) else 0（順位欠損は None）
"""

import os
import json
import argparse
import pandas as pd
from typing import List, Dict, Any

DEFAULT_INBASE = os.path.join("public", "results", "v1")
OUTBASE = os.path.join("TENKAI", "labels", "v1")


def _safe(v, default=None):
    return v if v is not None else default


def json_to_rows(res_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    """results JSON → 行リスト（各lane 1行, 欠けた枠も None で埋める）"""
    date = str(res_obj.get("date"))
    pid = str(res_obj.get("pid"))
    race = str(res_obj.get("race"))

    # lane -> rank
    ranks: Dict[int, Any] = {}
    for x in (res_obj.get("order") or []):
        lane = _safe(x.get("lane"))
        pos = _safe(x.get("pos"))
        if lane is None:
            continue
        try:
            ranks[int(lane)] = int(pos) if pos is not None else None
        except Exception:
            ranks[int(lane)] = None

    # lane -> st
    sts: Dict[int, Any] = {}
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

    rows = []
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


def _pid_list(pid: str, date: str, inbase: str) -> List[str]:
    """pid='ALL' のときはその日の全場を列挙"""
    if pid and pid.lower() == "all":
        base = os.path.join(inbase, date)
        if not os.path.isdir(base):
            return []
        return sorted([d for d in os.listdir(base)
                       if os.path.isdir(os.path.join(base, d))])
    return [pid]


def _race_list(race: str) -> List[str]:
    """race 未指定 or 'ALL' なら 1R..12R"""
    if race and race.lower() != "all":
        return [race]
    return [f"{i}R" for i in range(1, 13)]


def _is_blank(rows: List[Dict[str, Any]]) -> bool:
    """全行 rank も st も None → ブランクとみなし出力しない"""
    if not rows:
        return True
    for r in rows:
        if r.get("rank") is not None or r.get("st") is not None:
            return False
    return True


def build_labels(date: str, pid: str, race: str = "", inbase: str = DEFAULT_INBASE):
    any_output = False

    for pid_one in _pid_list(pid, date, inbase):
        targets = _race_list(race)
        outdir = os.path.join(OUTBASE, date, pid_one)
        os.makedirs(outdir, exist_ok=True)

        all_rows: List[Dict[str, Any]] = []

        for r in targets:
            path = os.path.join(inbase, date, pid_one, f"{r}.json")
            if not os.path.exists(path):
                print(f"skip (not found): {path}")
                continue

            try:
                with open(path, "r", encoding="utf-8") as f:
                    obj = json.load(f)
                rows = json_to_rows(obj)

                # ブランク（順位・ST全欠損）はスキップ
                if _is_blank(rows):
                    print(f"skip (blank): {path}")
                    continue

                df = pd.DataFrame(rows)
                outfile = os.path.join(outdir, f"{rows[0]['race']}.csv")
                df.to_csv(outfile, index=False, encoding="utf-8")
                print(f"wrote {outfile}")
                all_rows.extend(rows)
                any_output = True
            except Exception as e:
                print(f"skip (error): {path}  {e}")

        # その場で1件以上あれば all.csv を上書き
        if all_rows:
            df_all = pd.DataFrame(all_rows)
            outfile_all = os.path.join(outdir, "all.csv")
            df_all.to_csv(outfile_all, index=False, encoding="utf-8")
            print(f"wrote {outfile_all}")

    if not any_output:
        print("no outputs")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--pid", required=True, help="場コード or ALL")
    ap.add_argument("--race", default="", help="例: 1R / 2R / ... / ALL（空でもALLと同義）")
    ap.add_argument("--inbase", default=DEFAULT_INBASE, help="results JSON base dir")
    args = ap.parse_args()
    build_labels(args.date, args.pid, args.race, args.inbase)


if __name__ == "__main__":
    main()
