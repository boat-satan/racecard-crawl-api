# TENKAI/tenkai_predict_win.py
# -*- coding: utf-8 -*-
"""
勝利確率 予測スクリプト（全差し替え版）
- モデルは未指定なら TENKAI/models/v1 の "最新日付" を自動採用
- 予測用特徴は integrated を直接読み出してオンザフライ生成（features_c 不要）
- 出力:
    TENKAI/predictions/v1/<date>/<pid>/<race>.csv   … 枠番6行
    TENKAI/predictions/v1/<date>/<pid>/all.csv     … （race未指定時）全R結合

使い方例:
  # pid単場・全R
  python TENKAI/tenkai_predict_win.py --date 20250814 --pid 02

  # pid単場・レース指定
  python TENKAI/tenkai_predict_win.py --date 20250814 --pid 02 --race 2R

  # （必要なら）モデル日付を固定
  python TENKAI/tenkai_predict_win.py --date 20250814 --pid 02 --model_date 20250810
"""

from __future__ import annotations
import os
import json
import argparse
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
import joblib

# === パス定義 ===
INTEG_BASE = os.path.join("public", "integrated", "v1")
MODEL_BASE = os.path.join("TENKAI", "models", "v1")
OUT_BASE   = os.path.join("TENKAI", "predictions", "v1")

# === ユーティリティ ===
def _is_yyyymmdd(name: str) -> bool:
    return len(name) == 8 and name.isdigit()

def _latest_model_date() -> str:
    if not os.path.isdir(MODEL_BASE):
        raise FileNotFoundError(f"model base not found: {MODEL_BASE}")
    dates = [d for d in os.listdir(MODEL_BASE) if _is_yyyymmdd(d)]
    if not dates:
        raise FileNotFoundError(f"no model dates under: {MODEL_BASE}")
    return sorted(dates)[-1]

def _pick_model_dir(model_date: str, pid: str | None) -> str:
    """
    優先度:
      1) <date>/ALL/ALL
      2) <date>/<pid>/ALL   （pid指定時）
      3) <date> 配下の最初の model.pkl
    """
    cands = [os.path.join(MODEL_BASE, model_date, "ALL", "ALL")]
    if pid:
        cands.append(os.path.join(MODEL_BASE, model_date, pid, "ALL"))
    for d in cands:
        if os.path.exists(os.path.join(d, "model.pkl")):
            return d
    # フォールバック
    for root, _, files in os.walk(os.path.join(MODEL_BASE, model_date)):
        if "model.pkl" in files:
            return root
    raise FileNotFoundError(f"no model.pkl under {os.path.join(MODEL_BASE, model_date)}")

def _safe_get(d: Dict[str, Any], *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def _to_float(x):
    try: return float(x)
    except Exception: return None

def _rank(values: List[float]) -> List[int]:
    # None は最下位扱い（= +inf）
    pairs = [(i, v if v is not None else float("inf")) for i, v in enumerate(values)]
    pairs.sort(key=lambda t: t[1])
    ranks = [0]*len(values)
    r = 1
    for idx, _ in pairs:
        ranks[idx] = r
        r += 1
    return ranks

def _mean(xs: List[float]) -> float | None:
    xs = [x for x in xs if x is not None]
    return float(sum(xs)/len(xs)) if xs else None

# === 特徴抽出（integrated → Cワイド互換 → レーン縦持ち） ===
LANES = [1,2,3,4,5,6]

def _extract_features_from_integrated(date: str, pid: str, race: str) -> pd.DataFrame:
    """
    integrated/v1/<date>/<pid>/<race>.json を読み込み、
    features_c と同じ構造の「ワイド1行」を作ってから、レーン縦持ちに展開して返す
    """
    path = os.path.join(INTEG_BASE, date, pid, f"{race}.json")
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    entries = data.get("entries", []) or []

    # 相対用ベクトル
    rc_avgSTs, ages, classes = [], [], []
    for e in entries:
        rc = e.get("racecard", {}) or {}
        rc_avgSTs.append(_to_float(rc.get("avgST")))
        ages.append(_to_float(rc.get("age")))
        classes.append(_to_float(rc.get("classNumber")))
    r_avgST = _rank(rc_avgSTs)
    r_age   = _rank(ages)
    r_class = _rank(classes)

    # ワイド1行
    wide: Dict[str, Any] = {"date": date, "pid": pid, "race": race}
    for idx, e in enumerate(entries):
        lane = int(e.get("lane"))
        rc = e.get("racecard", {}) or {}
        ec = _safe_get(e, "stats", "entryCourse", default={}) or {}
        ss = _safe_get(e, "stats", "entryCourse", "selfSummary", default={}) or {}
        ms = _safe_get(e, "stats", "entryCourse", "matrixSelf", default={}) or {}

        prefix = f"L{lane}_"
        avgST_rc = _to_float(rc.get("avgST"))
        age      = _to_float(rc.get("age"))
        cls      = _to_float(rc.get("classNumber"))
        feat = {
            "startCourse": e.get("startCourse"),
            "class": cls,
            "age": age,
            "avgST_rc": avgST_rc,
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
            "d_avgST_rc": (avgST_rc if avgST_rc is not None else 0.16) - 0.16,
            "d_age":      (age if age is not None else 40) - 40,
            "d_class":    (cls if cls is not None else 3) - 3,
            "rank_avgST": r_avgST[idx],
            "rank_age":   r_age[idx],
            "rank_class": r_class[idx],
        }
        for k, v in feat.items():
            wide[prefix+k] = v

    # レース平均
    wide["mean_avgST_rc"] = _mean(rc_avgSTs)
    wide["mean_age"]      = _mean(ages)
    wide["mean_class"]    = _mean(classes)

    # ワイド→レーン縦持ち
    df_wide = pd.DataFrame([wide])
    rows = []
    for _, r in df_wide.iterrows():
        base = {c: r[c] for c in ["date","pid","race","mean_avgST_rc","mean_age","mean_class"] if c in r.index}
        by_lane = {lane: {} for lane in LANES}
        for c in df_wide.columns:
            if not c.startswith("L"): continue
            # L{lane}_key
            try:
                lane = int(c[1])
                key = c.split("_", 1)[1]
                by_lane[lane][key] = r[c]
            except Exception:
                pass
        for lane in LANES:
            row = dict(base)
            row["lane"] = lane
            for k, v in by_lane.get(lane, {}).items():
                row[k] = v
            rows.append(row)
    df_long = pd.DataFrame(rows)
    # 数値化（できるもののみ）
    for c in df_long.columns:
        if c in ("date","pid","race"): continue
        df_long[c] = pd.to_numeric(df_long[c], errors="ignore")
    return df_long

# === 推論本体 ===
def _load_model(model_date: str | None, pid: str | None) -> Tuple[object, List[str], str]:
    use_date = model_date or _latest_model_date()
    mdir = _pick_model_dir(use_date, pid)
    model = joblib.load(os.path.join(mdir, "model.pkl"))
    fjson = os.path.join(mdir, "features.json")
    if not os.path.exists(fjson):
        raise FileNotFoundError(f"features.json not found in {mdir}")
    with open(fjson, "r", encoding="utf-8") as f:
        feats = json.load(f).get("features", [])
    if not feats:
        raise ValueError("empty features in features.json")
    return model, feats, mdir

def _predict_one_race(df_feat_long: pd.DataFrame, model, feat_cols: List[str]) -> pd.DataFrame:
    # 説明変数の整形：不足列は追加（NaN）、余剰列は無視
    X = df_feat_long.copy()
    for c in feat_cols:
        if c not in X.columns:
            X[c] = np.nan
    X = X[feat_cols]
    # 数値化
    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors="coerce")
    # 単純な中央値補完（列ごと）
    X = X.copy()
    for c in X.columns:
        col = X[c]
        med = col.median() if col.notna().any() else 0.0
        X[c] = col.fillna(med)
    # 予測
    if hasattr(model, "predict_proba"):
        prob = model.predict_proba(X)[:, 1]
    else:
        # フォールバック（確率がないモデルの場合）
        pred = model.predict(X)
        prob = pred.astype(float)
    out = df_feat_long[["date","pid","race","lane"]].copy()
    out["win_prob"] = prob
    out["pred_win"] = (out["win_prob"] >= 0.5).astype(int)
    # 1着想定の並び（高→低）
    out = out.sort_values(["date","pid","race","win_prob"], ascending=[True, True, True, False]).reset_index(drop=True)
    # レース内順位（予測順位）
    out["pred_rank_in_race"] = out.groupby(["date","pid","race"])["win_prob"].rank(ascending=False, method="first").astype(int)
    return out

def predict(date: str, pid: str, race: str = "", model_date: str | None = None):
    model, feat_cols, model_dir = _load_model(model_date, pid or None)
    print(f"[model] date={model_date or _latest_model_date()} dir={model_dir}")
    out_dir = os.path.join(OUT_BASE, date, pid)
    os.makedirs(out_dir, exist_ok=True)

    targets = [race] if race else [f"{i}R" for i in range(1,13)]
    outs = []
    for r in targets:
        integ_path = os.path.join(INTEG_BASE, date, pid, f"{r}.json")
        if not os.path.exists(integ_path):
            print(f"skip (not found): {integ_path}")
            continue
        try:
            df_feat = _extract_features_from_integrated(date, pid, r)
            df_pred = _predict_one_race(df_feat, model, feat_cols)
            out_path = os.path.join(out_dir, f"{r}.csv")
            df_pred.to_csv(out_path, index=False, encoding="utf-8")
            print(f"wrote {out_path} (rows={len(df_pred)})")
            outs.append(df_pred)
        except Exception as e:
            print(f"skip (error): {integ_path}  {e}")

    if outs:
        df_all = pd.concat(outs, ignore_index=True)
        all_path = os.path.join(out_dir, "all.csv")
        df_all.to_csv(all_path, index=False, encoding="utf-8")
        print(f"wrote {all_path} (rows={len(df_all)})")
    else:
        print("no outputs")

# === CLI ===
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="対象日 YYYYMMDD")
    ap.add_argument("--pid",  required=True, help="場コード（例: 02）")
    ap.add_argument("--race", default="", help="レース（例: 2R）空なら1R..12R全部")
    ap.add_argument("--model_date", default="", help="モデル日付（空=最新を自動採用）")
    args = ap.parse_args()
    predict(args.date, args.pid, args.race, args.model_date or None)

if __name__ == "__main__":
    main()
