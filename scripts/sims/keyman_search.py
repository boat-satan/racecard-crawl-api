#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, argparse, csv, sys
from typing import Optional, List, Dict, Tuple

def load_json(path: str) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def normalize_race(r: str) -> str:
    r = (r or "").strip().upper()
    return r if (r and r.endswith("R")) else (f"{r}R" if r else "")

def iter_keyman_files(keyman_root: str, dates: set, pids: set, races: set):
    if not os.path.isdir(keyman_root):
        return
    for date in sorted(os.listdir(keyman_root)):
        if dates and date not in dates: continue
        date_dir = os.path.join(keyman_root, date)
        if not os.path.isdir(date_dir): continue
        for pid in sorted(os.listdir(date_dir)):
            if pids and pid not in pids: continue
            pid_dir = os.path.join(date_dir, pid)
            if not os.path.isdir(pid_dir): continue
            for fname in sorted(os.listdir(pid_dir)):
                if not fname.lower().endswith(".json"): continue
                race = normalize_race(os.path.splitext(fname)[0])
                if races and race not in races: continue
                yield date, pid, race, os.path.join(pid_dir, fname)

def extract_kmr_lanes(keyman_json: dict, threshold: float) -> List[int]:
    kmr = ((keyman_json or {}).get("keyman") or {}).get("KEYMAN_RANK") or {}
    out = []
    for k, v in kmr.items():
        try:
            if float(v) >= float(threshold):
                out.append(int(k))
        except Exception:
            pass
    return sorted(out)

def load_result_for_race(results_root: str, date: str, pid: str, race: str) -> Optional[dict]:
    race_norm = normalize_race(race)
    per_path = os.path.join(results_root, date, pid, f"{race_norm}.json")
    d = load_json(per_path)
    if d is not None:
        return d
    dirp = os.path.join(results_root, date, pid)
    if os.path.isdir(dirp):
        for fname in os.listdir(dirp):
            if not fname.lower().endswith(".json"): continue
            path = os.path.join(dirp, fname)
            dj = load_json(path)
            if not isinstance(dj, dict): continue
            container = dj.get("races", dj)
            if isinstance(container, dict):
                if race_norm in container: return container[race_norm]
                for k in list(container.keys()):
                    if normalize_race(str(k)) == race_norm:
                        return container[k]
    return None

def extract_top3_and_trifecta(result_json: dict) -> Tuple[Optional[List[int]], Optional[str], int]:
    """返り値: ([F,S,T] or None, combo_str or None, amount_int)"""
    if not isinstance(result_json, dict):
        return None, None, 0
    # A: order から lanes
    order = result_json.get("order")
    lanes = None
    if isinstance(order, list) and len(order) >= 3:
        def lane_of(x):
            if not isinstance(x, dict): return None
            for key in ("lane", "course", "F", "number"):
                if key in x:
                    try: return int(str(x[key]))
                    except: pass
            return None
        lanes = [lane_of(order[i]) for i in range(3)]
        if not all(isinstance(v, int) for v in lanes):
            lanes = None
    # B: trifecta
    trif = (result_json.get("payouts") or {}).get("trifecta")
    combo, amount = None, 0
    if isinstance(trif, dict):
        combo = trif.get("combo") if isinstance(trif.get("combo"), str) else None
        try:
            amount = int(trif.get("amount") or 0)
        except Exception:
            amount = 0
        if lanes is None and isinstance(combo, str) and "-" in combo:
            parts = combo.split("-")
            if len(parts) >= 3:
                try:
                    lanes = [int(parts[0]), int(parts[1]), int(parts[2])]
                except Exception:
                    pass
    return lanes, combo, amount

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="./public", help="public ディレクトリのルート（results/v1 配下を参照）")
    ap.add_argument("--keyman-dir", default="./scripts/sims/pass1/keyman", help="pass1 の keyman JSON ルート")
    ap.add_argument("--dates", default="", help="YYYYMMDD カンマ区切り")
    ap.add_argument("--pids", default="", help="場コードカンマ区切り")
    ap.add_argument("--races", default="", help="レース名カンマ区切り（例: 1R,12R）")
    ap.add_argument("--threshold", type=float, default=0.7, help="KEYMAN_RANK の閾値（以上を採用）")
    ap.add_argument("--out", default="./scripts/sims/keyman_search_result.csv", help="出力CSVパス")
    args = ap.parse_args()

    results_root = os.path.join(args.base, "results", "v1")
    if not os.path.isdir(results_root):
        print(f"[error] results root not found: {results_root}", file=sys.stderr)
        sys.exit(2)

    dates = set([d.strip() for d in args.dates.split(",") if d.strip()]) if args.dates else set()
    pids = set([p.strip() for p in args.pids.split(",") if p.strip()]) if args.pids else set()
    races = set([normalize_race(r) for r in args.races.split(",") if r.strip()]) if args.races else set()

    rows = []
    total_files = 0
    total_selected_lanes = 0
    total_hits_any = 0

    # per-lane と 着位別の集計
    per_lane_stats: Dict[int, Dict[str, int]] = {}   # {lane: {"appear":A, "hit_top3":H, "pos1":x, "pos2":y, "pos3":z}}
    pos_totals = {"pos1": {"appear":0, "hit":0},
                  "pos2": {"appear":0, "hit":0},
                  "pos3": {"appear":0, "hit":0}}

    for date, pid, race, km_path in iter_keyman_files(args.keyman_dir, dates, pids, races):
        total_files += 1
        km_json = load_json(km_path)
        if not km_json: continue
        lanes_sel = extract_kmr_lanes(km_json, args.threshold)
        if not lanes_sel: continue

        res_json = load_result_for_race(results_root, date, pid, race)
        top3, combo, amount = extract_top3_and_trifecta(res_json) if res_json else (None, None, 0)

        sel_in_pos1 = sel_in_pos2 = sel_in_pos3 = 0
        if top3 and len(top3) >= 3:
            p1, p2, p3 = top3[0], top3[1], top3[2]
            sel_in_pos1 = 1 if p1 in lanes_sel else 0
            sel_in_pos2 = 1 if p2 in lanes_sel else 0
            sel_in_pos3 = 1 if p3 in lanes_sel else 0

            # pos別 totals（=「posXが存在するレースで、選抜の中にそのposXが含まれていたか」）
            pos_totals["pos1"]["appear"] += 1; pos_totals["pos1"]["hit"] += sel_in_pos1
            pos_totals["pos2"]["appear"] += 1; pos_totals["pos2"]["hit"] += sel_in_pos2
            pos_totals["pos3"]["appear"] += 1; pos_totals["pos3"]["hit"] += sel_in_pos3

            # per-lane
            for lane in lanes_sel:
                st = per_lane_stats.setdefault(lane, {"appear":0, "hit_top3":0, "pos1":0, "pos2":0, "pos3":0})
                st["appear"] += 1
                if lane in (p1, p2, p3):
                    st["hit_top3"] += 1
                if lane == p1: st["pos1"] += 1
                if lane == p2: st["pos2"] += 1
                if lane == p3: st["pos3"] += 1

        hit_any = 1 if (sel_in_pos1 or sel_in_pos2 or sel_in_pos3) else 0
        total_selected_lanes += len(lanes_sel)
        total_hits_any += hit_any

        rows.append({
            "date": date,
            "pid": pid,
            "race": race,
            "selected_lanes": ",".join(map(str, lanes_sel)) if lanes_sel else "",
            "pos1_lane": (top3[0] if top3 else ""),
            "pos2_lane": (top3[1] if top3 else ""),
            "pos3_lane": (top3[2] if top3 else ""),
            "sel_in_pos1": sel_in_pos1,
            "sel_in_pos2": sel_in_pos2,
            "sel_in_pos3": sel_in_pos3,
            "hit_any_top3": hit_any,
            "trifecta_combo": (combo or ""),
            "trifecta_amount": amount,
            "keyman_file": km_path
        })

    # --- CSV 出力 ---
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    headers = ["date","pid","race","selected_lanes",
               "pos1_lane","pos2_lane","pos3_lane",
               "sel_in_pos1","sel_in_pos2","sel_in_pos3",
               "hit_any_top3","trifecta_combo","trifecta_amount","keyman_file"]
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    # --- 概要出力 ---
    print("=== keyman_search summary ===")
    print(f"keyman files scanned : {total_files}")
    print(f"threshold            : {args.threshold}")
    print(f"selected lanes (sum) : {total_selected_lanes}")
    print(f"races hit (any top3) : {total_hits_any}")
    if total_files > 0:
        print(f"hit rate (any)       : {total_hits_any/total_files:.3f}")

    # pos 別ヒット率
    print("\n-- per-position selection hit rate (among races parsed) --")
    for k in ("pos1","pos2","pos3"):
        a, h = pos_totals[k]["appear"], pos_totals[k]["hit"]
        rate = (h/a) if a>0 else 0.0
        print(f" {k}: {h}/{a}  ({rate:.3f})")

    # per-lane 成績
    if per_lane_stats:
        print("\n-- per-lane top3 & position hits (among races where lane was selected) --")
        for lane in sorted(per_lane_stats.keys()):
            st = per_lane_stats[lane]
            a, h = st["appear"], st["hit_top3"]
            r = (h/a) if a>0 else 0.0
            print(f" lane {lane}: top3 {h}/{a} ({r:.3f})  | pos1={st['pos1']} pos2={st['pos2']} pos3={st['pos3']}")

    print(f"\n[CSV] {args.out}")

if __name__ == "__main__":
    main()
