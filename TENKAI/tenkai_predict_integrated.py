# -*- coding: utf-8 -*-
"""
統合予測スクリプト（単勝 + 決まり手）
- モデルは未指定なら最新の日付ディレクトリを自動採用
- 特徴量は integrated を直接読み出してオンザフライ生成（features_c不要）
- 出力:
    TENKAI/predictions/v1/<date>/<pid>/<race>.csv   … 6行（枠番）
    TENKAI/predictions/v1/<date>/<pid>/all.csv     … まとめ（race未指定時）

使い方:
  python TENKAI/tenkai_predict.py --date 20250819 --pid 02
  python TENKAI/tenkai_predict.py --date 20250819 --pid 02 --race 1R
  # モデル日付を固定
  python TENKAI/tenkai_predict.py --date 20250819 --pid 02 --tansyo_model_date 20250818 --kimarite_model_date 20250818
"""

from __future__ import annotations
import os
import json
import argparse
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import joblib

# ===== パス =====
INTEG_BASE = os.path.join("public", "integrated", "v1")
OUT_BASE   = os.path.join("TENKAI", "predictions", "v1")
# モデルルート（単勝 / 決まり手）
MODEL_BASE_T = os.path.join("TENKAI", "models_tansyo",   "v1")
MODEL_BASE_K = os.path.join("TENKAI", "models_kimarite", "v1")

LANES = [1, 2, 3, 4, 5, 6]
K_CLASSES_DEFAULT = ["逃げ", "差し", "まくり", "まくり差し", "抜き", "恵まれ"]  # フォールバック

# ===== ユーティリティ =====
def _is_yyyymmdd(s: str) -> bool:
    return len(s) == 8 and s.isdigit()

def _latest_date_under(base: str) -> str:
    if not os.path.isdir(base):
        raise FileNotFoundError(f"base not found: {base}")
    ds = sorted([d for d in os.listdir(base) if _is_yyyymmdd(d)])
    if not ds:
        raise FileNotFoundError(f"no dated dirs under {base}")
    return ds[-1]

def _pick_model_dir(base: str, model_date: str, pid: Optional[str]) -> str:
    """
    優先: <date>/ALL/ALL → <date>/<pid>/ALL → <date>配下で最初に見つかった model.pkl
    """
    cands = [os.path.join(base, model_date, "ALL", "ALL")]
    if pid:
        cands.append(os.path.join(base, model_date, pid, "ALL"))
    for d in cands:
        if os.path.exists(os.path.join(d, "model.pkl")):
            return d
    for root, _, files in os.walk(os.path.join(base, model_date)):
        if "model.pkl" in files:
            return root
    raise FileNotFoundError(f"no model.pkl under {os.path.join(base, model_date)}")

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

def _rank_small_is_better(vals: List[Optional[float]]) -> List[int]:
    pairs = [(i, v if v is not None else float("inf")) for i, v in enumerate(vals)]
    pairs.sort(key=lambda t: t[1])
    rnk = [0] * len(vals)
    r = 1
    for i, _ in pairs:
        rnk[i] = r
        r += 1
    return rnk

def _mean(xs: List[Optional[float]]) -> Optional[float]:
    xs = [x for x in xs if x is not None]
    return float(sum(xs) / len(xs)) if xs else None

# ===== integrated → Cワイド互換 → レーン縦持ち =====
def _extract_features_from_integrated(date: str, pid: str, race: str) -> pd.DataFrame:
    path = os.path.join(INTEG_BASE, date, pid, f"{race}.json")
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    entries = data.get("entries", []) or []
    rc_avgSTs, ages, classes = [], [], []
    for e in entries:
        rc = e.get("racecard", {}) or {}
        rc_avgSTs.append(_to_float(rc.get("avgST")))
        ages.append(_to_float(rc.get("age")))
        classes.append(_to_float(rc.get("classNumber")))
    r_avgST = _rank_small_is_better(rc_avgSTs)
    r_age = _rank_small_is_better(ages)
    r_cls = _rank_small_is_better(classes)

    wide: Dict[str, Any] = {"date": date, "pid": pid, "race": race}
    for idx, e in enumerate(entries):
        lane = int(e.get("lane"))
        rc = e.get("racecard", {}) or {}
        ec = _safe_get(e, "stats", "entryCourse", default={}) or {}
        ss = _safe_get(e, "stats", "entryCourse", "selfSummary", default={}) or {}
        ms = _safe_get(e, "stats", "entryCourse", "matrixSelf", default={}) or {}
        loseK = _safe_get(e, "stats", "entryCourse", "loseKimarite", default={}) or {}

        avgST_rc = _to_float(rc.get("avgST"))
        age = _to_float(rc.get("age"))
        cls = _to_float(rc.get("classNumber"))
        pref = f"L{lane}_"
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
            # 旧互換（単勝の学習で参照している可能性に配慮）
            "win_k": ss.get("firstCount", 0),
            "lose_k": loseK.get("まくり", 0),
            # 差分/ランク
            "d_avgST_rc": (avgST_rc if avgST_rc is not None else 0.16) - 0.16,
            "d_age": (age if age is not None else 40) - 40,
            "d_class": (cls if cls is not None else 3) - 3,
            "rank_avgST": r_avgST[idx],
            "rank_age": r_age[idx],
            "rank_class": r_cls[idx],
        }
        for k, v in feat.items():
            wide[pref + k] = v

    wide["mean_avgST_rc"] = _mean(rc_avgSTs)
    wide["mean_age"] = _mean(ages)
    wide["mean_class"] = _mean(classes)

    # ワイド → 縦
    df_wide = pd.DataFrame([wide])
    rows: List[Dict[str, Any]] = []
    for _, r in df_wide.iterrows():
        base = {c: r[c] for c in ["date", "pid", "race", "mean_avgST_rc", "mean_age", "mean_class"] if c in r.index}
        by_lane = {ln: {} for ln in LANES}
        for c in df_wide.columns:
            if not c.startswith("L"):
                continue
            try:
                lane = int(c[1])
                key = c.split("_", 1)[1]
                by_lane[lane][key] = r[c]
            except Exception:
                pass
        for lane in LANES:
            row = dict(base)
            row["lane"] = lane
            row.update(by_lane.get(lane, {}))
            rows.append(row)
    df = pd.DataFrame(rows)
    for c in df.columns:
        if c in ("date", "pid", "race"):
            continue
        df[c] = pd.to_numeric(df[c], errors="ignore")
    return df

# ===== モデル読み込み =====
def _load_features_list(model_dir: str) -> List[str]:
    fjson = os.path.join(model_dir, "features.json")
    if not os.path.exists(fjson):
        raise FileNotFoundError(f"features.json not found in {model_dir}")
    with open(fjson, "r", encoding="utf-8") as f:
        arr = json.load(f).get("features", [])
    if not arr:
        raise ValueError(f"empty features in {fjson}")
    return arr

def _load_k_classes(model_dir: str) -> List[str]:
    # 優先: classes.json → meta.json['classes'] → 既定
    cj = os.path.join(model_dir, "classes.json")
    if os.path.exists(cj):
        try:
            with open(cj, "r", encoding="utf-8") as f:
                v = json.load(f).get("classes")
                if isinstance(v, list) and v:
                    return v
        except Exception:
            pass
    mj = os.path.join(model_dir, "meta.json")
    if os.path.exists(mj):
        try:
            with open(mj, "r", encoding="utf-8") as f:
                v = json.load(f).get("classes")
                if isinstance(v, list) and v:
                    return v
        except Exception:
            pass
    return K_CLASSES_DEFAULT

def _load_model_pair(t_model_date: Optional[str], k_model_date: Optional[str], pid: Optional[str]):
    # 単勝
    t_date = t_model_date or _latest_date_under(MODEL_BASE_T)
    t_dir = _pick_model_dir(MODEL_BASE_T, t_date, pid)
    t_mod = joblib.load(os.path.join(t_dir, "model.pkl"))
    t_feats = _load_features_list(t_dir)

    # 決まり手（任意: 無ければ None 扱い）
    k_mod = None
    k_feats = None
    k_dir = None
    k_date_used = None
    k_classes = None
    try:
        k_date_used = k_model_date or _latest_date_under(MODEL_BASE_K)
        k_dir = _pick_model_dir(MODEL_BASE_K, k_date_used, pid)
        k_mod = joblib.load(os.path.join(k_dir, "model.pkl"))
        k_feats = _load_features_list(k_dir)
        k_classes = _load_k_classes(k_dir)
    except Exception as e:
        print(f"[warn] kimarite model not available ({e}). Continue with win-only.")

    return {
        "tansyo": {"date": t_date, "dir": t_dir, "model": t_mod, "features": t_feats},
        "kimarite": {
            "date": k_date_used,
            "dir": k_dir,
            "model": k_mod,
            "features": k_feats,
            "classes": k_classes,
        },
    }

# ===== 前処理（学習時の列合わせに整形） =====
def _align_X(df: pd.DataFrame, feat_cols: List[str]) -> pd.DataFrame:
    X = df.copy()
    for c in feat_cols:
        if c not in X.columns:
            X[c] = np.nan
    X = X[feat_cols]
    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors="coerce")
        med = X[c].median() if X[c].notna().any() else 0.0
        X[c] = X[c].fillna(med)
    return X

# ===== 予測 =====
def _predict_tansyo(df_feat: pd.DataFrame, model, feat_cols: List[str]) -> pd.DataFrame:
    X = _align_X(df_feat, feat_cols)
    if hasattr(model, "predict_proba"):
        prob = model.predict_proba(X)[:, 1]
    else:
        prob = model.predict(X).astype(float)
    out = df_feat[["date", "pid", "race", "lane"]].copy()
    out["win_prob"] = prob
    out["pred_win"] = (out["win_prob"] >= 0.5).astype(int)
    out = (
        out.sort_values(["date", "pid", "race", "win_prob"], ascending=[True, True, True, False])
        .reset_index(drop=True)
    )
    out["pred_rank_in_race"] = (
        out.groupby(["date", "pid", "race"])["win_prob"]
        .rank(ascending=False, method="first")
        .astype(int)
    )
    return out

def _proba_df_with_named_columns(prob_mat: np.ndarray, model, classes: List[str]) -> pd.DataFrame:
    """
    モデルの predict_proba 出力を、与えられた `classes`（日本語ラベル）順の DataFrame に整形。
    - model.classes_ が数値(0..K-1)のみの場合は、インデックス順で classes に対応づける。
    - model.classes_ がラベル名の場合はそのまま名寄せ。
    - 欠けた列は 0 で補完。
    """
    model_classes = None
    try:
        mc = getattr(model, "classes_", None)
        if mc is not None:
            model_classes = list(mc)
    except Exception:
        model_classes = None

    if model_classes is None:
        # 順番そのまま classes とみなす
        df = pd.DataFrame(prob_mat, columns=classes[: prob_mat.shape[1]])
    else:
        if all(isinstance(x, (int, np.integer)) for x in model_classes) and len(model_classes) == len(classes):
            # 0..K-1 → 日本語ラベルへ位置対応
            mapped_cols = [classes[int(i)] for i in model_classes]
            df = pd.DataFrame(prob_mat, columns=mapped_cols)
        else:
            # すでに文字ラベル（想定: 日本語）を持っている
            df = pd.DataFrame(prob_mat, columns=[str(c) for c in model_classes])

    # 欠け列を0で補完し、classes順に
    for c in classes:
        if c not in df.columns:
            df[c] = 0.0
    df = df[classes]
    return df.astype(float)

def _predict_kimarite(df_feat: pd.DataFrame, model, feat_cols: List[str], classes: List[str]) -> pd.DataFrame:
    base = df_feat[["date", "pid", "race", "lane"]].copy()
    if model is None or feat_cols is None:
        for c in classes:
            base[f"prob_{c}"] = 0.0
        base["pred_kimarite"] = ""
        base["pred_conf"] = 0.0
        base["uncertainty"] = 1.0
        return base

    X = _align_X(df_feat, feat_cols)
    if hasattr(model, "predict_proba"):
        prob_mat = model.predict_proba(X)
        prob_df = _proba_df_with_named_columns(prob_mat, model, classes)
        for c in classes:
            base[f"prob_{c}"] = prob_df[c].values
    else:
        for c in classes:
            base[f"prob_{c}"] = 0.0

    prob_cols = [f"prob_{c}" for c in classes]
    base["pred_kimarite"] = base[prob_cols].idxmax(axis=1).str.replace("prob_", "", regex=False)
    base["pred_conf"] = base[prob_cols].max(axis=1)
    base["uncertainty"] = 1.0 - base["pred_conf"]
    return base

def _merge_outputs(win_df: pd.DataFrame, kim_df: pd.DataFrame, classes: List[str]) -> pd.DataFrame:
    out = win_df.merge(kim_df, on=["date", "pid", "race", "lane"], how="left", validate="one_to_one")
    for c in classes:
        col = f"prob_{c}"
        if col not in out.columns:
            out[col] = 0.0
    front = ["date", "pid", "race", "lane", "win_prob", "pred_win", "pred_rank_in_race"]
    probs = [f"prob_{c}" for c in classes]
    tail = ["pred_kimarite", "pred_conf", "uncertainty"]
    return out.reindex(columns=front + probs + tail)

def predict(
    date: str,
    pid: str,
    race: str = "",
    tansyo_model_date: str = "",
    kimarite_model_date: str = "",
):
    # モデル読み込み
    pack = _load_model_pair(tansyo_model_date or None, kimarite_model_date or None, pid or None)
    t_mod, t_feats = pack["tansyo"]["model"], pack["tansyo"]["features"]
    k_mod, k_feats = pack["kimarite"]["model"], pack["kimarite"]["features"]
    k_classes = pack["kimarite"]["classes"] or K_CLASSES_DEFAULT

    print(f"[tansyo]   date={pack['tansyo']['date']}   dir={pack['tansyo']['dir']}")
    if k_mod is None:
        print("[kimarite] model=NOT FOUND (skip)")
    else:
        print(f"[kimarite] date={pack['kimarite']['date']} dir={pack['kimarite']['dir']}")

    out_dir = os.path.join(OUT_BASE, date, pid)
    os.makedirs(out_dir, exist_ok=True)

    targets = [race] if race else [f"{i}R" for i in range(1, 13)]
    outs: List[pd.DataFrame] = []

    for r in targets:
        integ_path = os.path.join(INTEG_BASE, date, pid, f"{r}.json")
        if not os.path.exists(integ_path):
            print(f"skip (not found): {integ_path}")
            continue
        try:
            df_feat = _extract_features_from_integrated(date, pid, r)
            win_df = _predict_tansyo(df_feat, t_mod, t_feats)
            kim_df = _predict_kimarite(df_feat, k_mod, k_feats, k_classes)
            merged = _merge_outputs(win_df, kim_df, k_classes)

            out_path = os.path.join(out_dir, f"{r}.csv")
            merged.to_csv(out_path, index=False, encoding="utf-8")
            print(f"wrote {out_path} (rows={len(merged)})")
            outs.append(merged)
        except Exception as e:
            print(f"skip (error): {integ_path}  {e}")

    if outs:
        df_all = pd.concat(outs, ignore_index=True)
        all_path = os.path.join(out_dir, "all.csv")
        df_all.to_csv(all_path, index=False, encoding="utf-8")
        print(f"wrote {all_path} (rows={len(df_all)})")
    else:
        print("no outputs")

# ===== CLI =====
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="対象日 YYYYMMDD")
    ap.add_argument("--pid", required=True, help="場コード（例: 02）")
    ap.add_argument("--race", default="", help="レース（例: 2R）未指定=1R..12R")
    ap.add_argument("--tansyo_model_date", default="", help="単勝モデル日付（空=最新）")
    ap.add_argument("--kimarite_model_date", default="", help="決まり手モデル日付（空=最新）")
    args = ap.parse_args()
    predict(args.date, args.pid, args.race, args.tansyo_model_date, args.kimarite_model_date)

if __name__ == "__main__":
    main()
