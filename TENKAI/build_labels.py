# -*- coding: utf-8 -*-
"""
results/v1 を読み、レースごとの教師ラベルCSVを作成
- 入力: public/results/v1/{date}/{pid}/{race}.json
- 出力:
  TENKAI/labels/v1/{date}/{pid}/{race}.csv  … レース単位（6行）
  TENKAI/labels/v1/{date}/{pid}/all.csv     … pid配下まとめ（append）
CSV列:
  date,pid,race,lane,rank,st,decision,win
    win = 1(着順=1) else 0
"""
import os
import json
import glob
import pandas as pd
import argparse

DEFAULT_INBASE = os.path.join("public", "results", "v1")
OUTBASE = os.path.join("TENKAI", "labels", "v1")


def _safe(v, default=None):
    return v if v is not None else default


def json_to_rows(res_obj: dict):
    """results JSON → 行リスト（各lane 1行）"""
    date = str(res_obj.get("date"))
    pid = str(res_obj.get("pid"))
    race = str(res_obj.get("race"))

    # map: lane -> rank
    ranks = {}
    for x in res_obj.get("order", []) or []:
        lane = _safe(x.get("lane"))
        pos = _safe(x.get("pos"))
        if lane is None:
            continue
        ranks[int(lane)] = int(pos) if pos is not None else None

    # map: lane -> st
    sts = {}
    for x in res_obj.get("start", []) or []:
        lane = _safe(x.get("lane"))
        st = _safe(x.get("st"))
        if lane is None:
            continue
        # STは数値化できなければそのままNone
        try:
            sts[int(lane)] = float(st) if st is not None else None
        except Exception:
            sts[int(lane)] = None

    decision = _safe(res_obj.get("decision"))

    rows = []
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
            "win": 1 if rank == 1 else 0 if rank is not None else None,
        })
    return rows


def build_labels(date: str, pid: str, race: str = "", inbase: str = DEFAULT_INBASE):
    """
    指定キーで results JSON → ラベルCSV を出力
    - race 未指定なら 1R..12R を探索し、存在するJSONのみ処理
    """
    # 入力ファイル列挙
    targets = []
    if race:
        p = os.path.join(inbase, date, pid, f"{race}.json")
        targets = [p]
    else:
        # 1R..12R を順に
        for i in range(1, 13):
            p = os.path.join(inbase, date, pid, f"{i}R.json")
            targets.append(p)

    outdir = os.path.join(OUTBASE, date, pid)
    os.makedirs(outdir, exist_ok=True)

    all_rows = []

    for path in targets:
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
            # レース単位のCSV
            race_name = rows[0]["race"]
            df = pd.DataFrame(rows)
            outfile = os.path.join(outdir, f"{race_name}.csv")
            df.to_csv(outfile, index=False, encoding="utf-8")
            print(f"wrote {outfile}")
            all_rows.extend(rows)
        except Exception as e:
            print(f"skip (error): {path}  {e}")

    if all_rows:
        df_all = pd.DataFrame(all_rows)
        outfile_all = os.path.join(outdir, "all.csv")
        # 既存があれば上書き（常に最新で良い想定）
        df_all.to_csv(outfile_all, index=False, encoding="utf-8")
        print(f"wrote {outfile_all}")
    else:
        print("no outputs")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--pid", required=True)
    ap.add_argument("--race", default="")
    ap.add_argument("--inbase", default=DEFAULT_INBASE, help="results JSON base dir")
    args = ap.parse_args()
    build_labels(args.date, args.pid, args.race, args.inbase)


if __name__ == "__main__":
    main()
