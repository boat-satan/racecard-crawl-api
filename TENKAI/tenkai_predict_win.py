# -*- coding: utf-8 -*-
"""
Predict win probability from integrated JSON (no features_c required).

Input:
  public/integrated/v1/{date}/{pid}/{race}.json     (race omitted => 1R..12R)
Model:
  TENKAI/models/v1/{model_date}/ALL/ALL/
  -> fallback: {model_date}/{pid}/ALL/ -> {model_date}/{pid}/{race}/
Output:
  TENKAI/predictions/v1/{date}/{pid}/{race}.json
  TENKAI/predictions/v1/{date}/{pid}/{race}.csv
  (race omitted => also TENKAI/predictions/v1/{date}/{pid}/all.json)
"""

from __future__ import annotations
import os, json, argparse, sys
from typing import Any, Dict, List
import pandas as pd
import numpy as np
import joblib

INTEGRATED_BASE = os.path.join("public", "integrated", "v1")
MODEL_BASE      = os.path.join("TENKAI", "models",      "v1")
OUT_BASE        = os.path.join("TENKAI", "predictions", "v1")

LANES = [1,2,3,4,5,6]

# ---------- utils ----------
def _safe_get(d: Dict[str, Any], *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def _to_float(x):
    try:
        return float(x)
    except Exception:
        return None

def _rank(values: List[float]) -> List[int]:
    pairs = [(i, v if v is not None else float("inf")) for i, v in enumerate(values)]
    pairs.sort(key=lambda t: t[1])
    ranks = [0]*len(values)
    r = 1
    for idx, _ in pairs:
        ranks[idx] = r
        r += 1
    return ranks

def _mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs)/len(xs) if xs else None

# ---------- feature builder from integrated ----------
def build_c_features_from_integrated(obj: Dict[str, Any]) -> pd.DataFrame:
    """integrated JSON -> wide C features row (same layout as features_c.py)"""
    date = str(obj.get("date"))
    pid  = str(obj.get("pid"))
    race = str(obj.get("race"))
    entries = obj.get("entries", []) or []

    rc_avgSTs, ages, classes = [], [], []
    for e in entries:
        rc = e.get("racecard", {}) or {}
        rc_avgSTs.append(_to_float(rc.get("avgST")))
        ages.append(_to_float(rc.get("age")))
        classes.append(_to_float(rc.get("classNumber")))

    rank_avgST = _rank(rc_avgSTs) if entries else []
    rank_age   = _rank(ages)      if entries else []
    rank_class = _rank(classes)   if entries else []

    row: Dict[str, Any] = {"date": date, "pid": pid, "race": race}

    for idx, e in enumerate(entries):
        lane = int(e.get("lane"))
        rc = e.get("racecard", {}) or {}
        ec = _safe_get(e, "stats", "entryCourse", default={}) or {}
        ss = _safe_get(e, "stats", "entryCourse", "selfSummary", default={}) or {}
        ms = _safe_get(e, "stats", "entryCourse", "matrixSelf", default={}) or {}

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
            "win_k": ss.get("firstCount", 0),
            "lose_k": (_safe_get(e, "stats", "entryCourse", "loseKimarite", default={}) or {}).get("まくり", 0),
        }
        feat["d_avgST_rc"] = (feat["avgST_rc"] if feat["avgST_rc"] is not None else 0.16) - 0.16
        feat["d_age"]      = (feat["age"] if feat["age"] is not None else 40) - 40
        feat["d_class"]    = (feat["class"] if feat["class"] is not None else 3) - 3

        feat["rank_avgST"] = rank_avgST[idx]
        feat["rank_age"]   = rank_age[idx]
        feat["rank_class"] = rank_class[idx]

        for k, v in feat.items():
            row[f"{prefix}{k}"] = v

    row["mean_avgST_rc"] = _mean(rc_avgSTs)
    row["mean_age"]      = _mean(ages)
    row["mean_class"]    = _mean(classes)

    return pd.DataFrame([row])

def wide_to_long_per_lane(df_wide: pd.DataFrame) -> pd.DataFrame:
    rows = []
    commons = [c for c in df_wide.columns if not c.startswith("L")]
    for _, r in df_wide.iterrows():
        base = {c: r[c] for c in commons if c in r.index}
        by_lane = {lane:{} for lane in LANES}
        for c in df_wide.columns:
            if not c.startswith("L"): continue
            try:
                lane = int(c[1])
            except Exception:
                continue
            key = c.split("_",1)[1] if "_" in c else None
            if key:
                by_lane[lane][key] = r[c]
        for lane in LANES:
            row = dict(base); row["lane"] = lane
            for k,v in by_lane.get(lane, {}).items(): row[k]=v
            rows.append(row)
    df = pd.DataFrame(rows)
    # numeric cast
    for c in df.columns:
        if c in ("date","pid","race"): continue
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

# ---------- model/feature loader ----------
def load_model_bundle(model_date: str, pid: str, race: str):
    # priority: ALL/ALL -> pid/ALL -> pid/race
    cands = [
        os.path.join(MODEL_BASE, model_date, "ALL", "ALL"),
        os.path.join(MODEL_BASE, model_date, pid, "ALL") if pid else "",
        os.path.join(MODEL_BASE, model_date, pid, race)  if pid and race else "",
    ]
    cands = [p for p in cands if p]
    for d in cands:
        if os.path.exists(os.path.join(d, "model.pkl")) and os.path.exists(os.path.join(d, "features.json")):
            model = joblib.load(os.path.join(d, "model.pkl"))
            with open(os.path.join(d, "features.json"), "r", encoding="utf-8") as f:
                feats = json.load(f)["features"]
            return d, model, feats
    raise FileNotFoundError("model not found under: " + " | ".join(cands))

# ---------- prediction ----------
def predict(date: str, pid: str, race: str, model_date: str):
    model_dir, model, feat_cols = load_model_bundle(model_date, pid, race if race else "ALL")

    # input files
    targets = [race] if race else [f"{i}R" for i in range(1,13)]
    outs = []

    for r in targets:
        in_path = os.path.join(INTEGRATED_BASE, date, pid, f"{r}.json")
        if not os.path.exists(in_path):
            print(f"skip (not found): {in_path}")
            continue
        with open(in_path, "r", encoding="utf-8") as f:
            obj = json.load(f)

        df_wide = build_c_features_from_integrated(obj)
        df_long = wide_to_long_per_lane(df_wide)

        # align features
        X = pd.DataFrame()
        for c in feat_cols:
            if c in df_long.columns:
                X[c] = pd.to_numeric(df_long[c], errors="coerce")
            else:
                # missing feature -> fill 0
                print(f"warn: feature '{c}' missing in integrated; filled 0")
                X[c] = 0.0
        # impute NaN to 0 (学習時は中央値埋めだったが保存していないので0で近似)
        X = X.fillna(0.0)

        proba = model.predict_proba(X)[:,1]
        df_out = df_long[["date","pid","race","lane"]].copy()
        df_out["proba_win"] = proba
        df_out = df_out.sort_values("lane")
        outs.append(df_out)

        # write per-race
        out_dir = os.path.join(OUT_BASE, date, pid)
        os.makedirs(out_dir, exist_ok=True)
        out_json = os.path.join(out_dir, f"{r}.json")
        out_csv  = os.path.join(out_dir, f"{r}.csv")
        records = [
            {"lane": int(int(x.lane)), "proba_win": float(x.proba_win)}
            for x in df_out.itertuples(index=False)
        ]
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump({
                "date": date, "pid": pid, "race": r,
                "model_date": model_date,
                "model_dir": os.path.relpath(model_dir).replace("\\","/"),
                "predictions": records
            }, f, ensure_ascii=False, indent=2)
        df_out.to_csv(out_csv, index=False, encoding="utf-8")
        print(f"wrote {out_json}")

    if outs and not race:
        df_all = pd.concat(outs, ignore_index=True)
        out_dir = os.path.join(OUT_BASE, date, pid)
        with open(os.path.join(out_dir, "all.json"), "w", encoding="utf-8") as f:
            json.dump({
                "date": date, "pid": pid, "race": "ALL",
                "model_date": model_date,
                "predictions": df_all.to_dict(orient="records")
            }, f, ensure_ascii=False, indent=2)
        print(f"wrote {os.path.join(out_dir, 'all.json')}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYYMMDD (予測対象)")
    ap.add_argument("--pid",  required=True, help="場コード (例: 02)")
    ap.add_argument("--race", default="", help="例: 9R（空=1R..12R）")
    ap.add_argument("--model_date", default="", help="学習モデルの日付（空=--date と同じ）")
    args = ap.parse_args()
    model_date = args.model_date or args.date
    predict(args.date, args.pid, args.race, model_date)

if __name__ == "__main__":
    main()
