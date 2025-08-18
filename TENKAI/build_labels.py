# -*- coding: utf-8 -*-
"""
results/v1 を読み、レースごとの教師ラベルCSVを作成
- 入力: public/results/v1/{date}/{pid}/{race}.json
- 出力:
  TENKAI/labels/v1/{date}/{pid}/{race}.csv  … レース単位（6行）
  TENKAI/labels/v1/{date}/{pid}/all.csv     … pid配下まとめ

使い方:
  pid=01..24  : その場のみ
  pid=ALL     : その日の全場(ディレクトリ検出)を処理
  race 空/ALL : 1R..12R を処理、指定時はそのレースのみ
"""
import os
import json
import argparse
from typing import List, Dict, Any
import pandas as pd

INBASE_DEFAULT = os.path.join("public", "results", "v1")
OUTBASE = os.path.join("TENKAI", "labels", "v1")


def _safe(v, default=None):
    return v if v is not None else default


def json_to_rows(res_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    """results JSON → 行リスト（各lane 1行）"""
    date = str(res_obj.get("date"))
    pid = str(res_obj.get("pid"))
    race = str(res_obj.get("race"))

    # lane -> rank
    ranks: Dict[int, int] = {}
    for x in (res_obj.get("order") or []):
        lane = _safe(x.get("lane"))
        pos = _safe(x.get("pos"))
        if lane is None:
            continue
        ranks[int(lane)] = int(pos) if pos is not None else None

    # lane -> st
    sts: Dict[int, float] = {}
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
            "win": 1 if rank == 1 else 0 if rank is not None else None,
        })
    return rows


def _list_pids(date: str, inbase: str) -> List[str]:
    """date配下にあるpidディレクトリ(二桁)を列挙"""
    root = os.path.join(inbase, date)
    if not os.path.isdir(root):
        return []
    pids = []
    for name in sorted(os.listdir(root)):
        if len(name) == 2 and name.isdigit() and os.path.isdir(os.path.join(root, name)):
            pids.append(name)
    return pids


def _race_names(race: str) -> List[str]:
    if race and race.upper() != "ALL":
        return [race]
    return [f"{i}R" for i in range(1, 13)]


def _process_one_pid(date: str, pid: str, race: str, inbase: str) -> int:
    """単一pidを処理。生成行数を返す"""
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
            rows = json_to_rows(obj)
            if not rows:
                print(f"skip (empty): {path}")
                continue
            df = pd.DataFrame(rows)
            outfile = os.path.join(outdir, f"{rows[0]['race']}.csv")
            df.to_csv(outfile, index=False, encoding="utf-8")
            print(f"wrote {outfile}")
            all_rows.extend(rows)
        except Exception as e:
            print(f"skip (error): {path}  {e}")

    if all_rows:
        df_all = pd.DataFrame(all_rows)
        outfile_all = os.path.join(outdir, "all.csv")
        df_all.to_csv(outfile_all, index=False, encoding="utf-8")
        print(f"wrote {outfile_all}")
        return len(all_rows)
    print(f"no outputs for pid={pid}")
    return 0


def build_labels(date: str, pid: str, race: str = "", inbase: str = INBASE_DEFAULT):
    """pid=01..24 もしくは pid=ALL を受け付ける"""
    if pid.upper() == "ALL":
        pids = _list_pids(date, inbase)
        if not pids:
            print(f"no pid directories under {os.path.join(inbase, date)}")
            return
        print(f"targets pid: {', '.join(pids)}")
        total = 0
        for p in pids:
            total += _process_one_pid(date, p, race, inbase)
        print(f"TOTAL rows: {total}")
    else:
        _process_one_pid(date, pid, race, inbase)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--pid", required=True, help="01..24 または ALL")
    ap.add_argument("--race", default="", help="例: 1R。空/ALLで1R..12R")
    ap.add_argument("--inbase", default=INBASE_DEFAULT)
    args = ap.parse_args()
    build_labels(args.date, args.pid, args.race, args.inbase)


if __name__ == "__main__":
    main()
