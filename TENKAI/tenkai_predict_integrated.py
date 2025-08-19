# -*- coding: utf-8 -*-
"""
統合予測スクリプト（単勝 + 決まり手, 後処理ルール付 / 全差し替え版）
- 既存仕様は維持
- _latest_date_under の return バグ修正
- 例外/ロギングを少し強化（原因特定しやすく）
"""

from __future__ import annotations
import os, json, argparse, sys
from typing import Any, Dict, List, Optional
import numpy as np
import pandas as pd
import joblib

INTEG_BASE    = os.path.join("public", "integrated", "v1")
OUT_BASE      = os.path.join("TENKAI", "predictions", "v1")
MODEL_BASE_T  = os.path.join("TENKAI", "models_tansyo",   "v1")
MODEL_BASE_K  = os.path.join("TENKAI", "models_kimarite", "v1")

LANES = [1,2,3,4,5,6]
K_CLASSES_DEFAULT = ["逃げ","差し","まくり","まくり差し","抜き","恵まれ"]

# ---------------- utils ----------------
def _is_yyyymmdd(s: str) -> bool:
    return isinstance(s, str) and len(s) == 8 and s.isdigit()

def _latest_date_under(base: str) -> str:
    """base 直下の YYYYMMDD を昇順ソートして末尾を返す（存在しなければ例外）"""
    if not os.path.isdir(base):
        raise FileNotFoundError(f"base dir not found: {base}")
    ds = sorted([d for d in os.listdir(base) if _is_yyyymmdd(d)])
    if not ds:
        raise FileNotFoundError(f"no dated dir under: {base}")
    return ds[-1]  # ← ここが元バグ（if 行に巻き込まれて None になっていた）

def _pick_model_dir(base: str, model_date: Optional[str], pid: Optional[str]) -> str:
    """モデル格納ディレクトリを優先順で決定し返す。見つからなければ例外。"""
    if not model_date:
        raise ValueError(f"model_date is None for base={base}")
    cands = [os.path.join(base, model_date, "ALL", "ALL")]
    if pid:
        cands.append(os.path.join(base, model_date, pid, "ALL"))
    for d in cands:
        if os.path.exists(os.path.join(d, "model.pkl")):
            return d
    # fallback: 日付配下を走査
    for r, _, fs in os.walk(os.path.join(base, model_date)):
        if "model.pkl" in fs:
            return r
    raise FileNotFoundError(f"model.pkl not found under {os.path.join(base, model_date)}")

def _safe_get(d: Dict, *keys, default=None):
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

def _rank_small_is_better(vals):
    pairs = [(i, v if v is not None else 1e9) for i, v in enumerate(vals)]
    pairs.sort(key=lambda t: t[1])
    rnk = [0] * len(vals)
    r = 1
    for i, _ in pairs:
        rnk[i] = r
        r += 1
    return rnk

def _mean(xs):
    xs = [x for x in xs if x is not None]
    return (sum(xs) / len(xs)) if xs else None

def _log(*args):
    print("[predict]", *args, file=sys.stderr)

# ---------------- features ----------------
def _extract_features_from_integrated(date: str, pid: str, race: str) -> pd.DataFrame:
    path = os.path.join(INTEG_BASE, date, pid, f"{race}.json")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    entries = data.get("entries", []) or []
    # entries が不足しても（欠艇など）そのまま処理
    rc_avgSTs, ages, classes = [], [], []
    for e in entries:
        rc = e.get("racecard", {}) or {}
        rc_avgSTs.append(_to_float(rc.get("avgST")))
        ages.append(_to_float(rc.get("age")))
        classes.append(_to_float(rc.get("classNumber")))
    r_avgST = _rank_small_is_better(rc_avgSTs)
    r_age   = _rank_small_is_better(ages)
    r_cls   = _rank_small_is_better(classes)

    wide = {"date": date, "pid": pid, "race": race}
    for idx, e in enumerate(entries):
        lane = int(e.get("lane"))
        rc = e.get("racecard", {}) or {}
        ec = _safe_get(e, "stats", "entryCourse", default={}) or {}
        ss = _safe_get(e, "stats", "entryCourse", "selfSummary", default={}) or {}
        ms = _safe_get(e, "stats", "entryCourse", "matrixSelf", default={}) or {}
        loseK = _safe_get(e, "stats", "entryCourse", "loseKimarite", default={}) or {}

        avgST_rc = _to_float(rc.get("avgST"))
        age      = _to_float(rc.get("age"))
        cls      = _to_float(rc.get("classNumber"))

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
            "win_k": ss.get("firstCount", 0),
            "lose_k": loseK.get("まくり", 0),
            "d_avgST_rc": (avgST_rc or 0.16) - 0.16,
            "d_age": (age or 40) - 40,
            "d_class": (cls or 3) - 3,
            "rank_avgST": r_avgST[idx],
            "rank_age": r_age[idx],
            "rank_class": r_cls[idx],
        }
        for k, v in feat.items():
            wide[pref + k] = v

    wide["mean_avgST_rc"] = _mean(rc_avgSTs)
    wide["mean_age"]      = _mean(ages)
    wide["mean_class"]    = _mean(classes)

    df_wide = pd.DataFrame([wide])
    rows: List[Dict[str, Any]] = []

    for _, r in df_wide.iterrows():
        base = {c: r[c] for c in ["date", "pid", "race", "mean_avgST_rc", "mean_age", "mean_class"]}
        by_lane = {ln: {} for ln in LANES}
        for c in df_wide.columns:
            if c.startswith("L") and "_" in c and c[1].isdigit():
                lane = int(c[1])
                key = c.split("_", 1)[1]
                by_lane[lane][key] = r[c]
        for lane in LANES:
            row = dict(base)
            row["lane"] = lane
            row.update(by_lane.get(lane, {}))
            rows.append(row)

    return pd.DataFrame(rows)

# ---------------- model load ----------------
def _load_features_list(model_dir: str) -> List[str]:
    with open(os.path.join(model_dir, "features.json"), "r", encoding="utf-8") as f:
        obj = json.load(f)
    feats = obj.get("features")
    if not isinstance(feats, list):
        raise KeyError(f"features.json malformed at {model_dir}")
    return feats

def _load_k_classes(model_dir: str) -> List[str]:
    cj = os.path.join(model_dir, "classes.json")
    if os.path.exists(cj):
        with open(cj, "r", encoding="utf-8") as f:
            obj = json.load(f)
        classes = obj.get("classes")
        if isinstance(classes, list) and classes:
            return classes
    return K_CLASSES_DEFAULT

def _load_model_pair(t_date: Optional[str], k_date: Optional[str], pid: Optional[str]):
    # tansyo
    t_date_used = t_date or _latest_date_under(MODEL_BASE_T)
    t_dir = _pick_model_dir(MODEL_BASE_T, t_date_used, pid)
    t_mod = joblib.load(os.path.join(t_dir, "model.pkl"))
    t_feats = _load_features_list(t_dir)
    _log(f"tansyo model: date={t_date_used} dir={t_dir} featN={len(t_feats)}")

    # kimarite（任意）
    k_mod = k_feats = k_classes = None
    k_date_used = None
    try:
        k_date_used = k_date or _latest_date_under(MODEL_BASE_K)
        k_dir = _pick_model_dir(MODEL_BASE_K, k_date_used, pid)
        k_mod = joblib.load(os.path.join(k_dir, "model.pkl"))
        k_feats = _load_features_list(k_dir)
        k_classes = _load_k_classes(k_dir)
        _log(f"kimarite model: date={k_date_used} dir={k_dir} featN={len(k_feats)} classes={k_classes}")
    except Exception as e:
        _log(f"kimarite model missing or failed: {e} (fallback to empty probs)")

    return {
        "tansyo":  {"date": t_date_used, "model": t_mod, "features": t_feats},
        "kimarite":{"date": k_date_used, "model": k_mod, "features": k_feats, "classes": k_classes or K_CLASSES_DEFAULT},
    }

# ---------------- predict ----------------
def _align_X(df: pd.DataFrame, feat_cols: List[str]) -> pd.DataFrame:
    X = df.copy()
    for c in feat_cols:
        if c not in X:
            X[c] = np.nan
    X = X[feat_cols].apply(pd.to_numeric, errors="coerce")
    # 全列 NaN の場合に備えて 0 埋めフォールバック
    med = X.median(numeric_only=True)
    X = X.fillna(med)
    if X.isna().any().any():
        X = X.fillna(0.0)
    return X

def _predict_tansyo(df_feat: pd.DataFrame, model, feat_cols: List[str]) -> pd.DataFrame:
    X = _align_X(df_feat, feat_cols)
    prob = model.predict_proba(X)[:, 1]
    out = df_feat[["date", "pid", "race", "lane"]].copy()
    out["win_prob"] = prob
    out["pred_win"] = (prob >= 0.5).astype(int)
    out["pred_rank_in_race"] = (
        out.groupby(["date", "pid", "race"])["win_prob"]
        .rank(ascending=False, method="first")
        .astype(int)
    )
    return out

# --- 決まり手後処理ルール ---
ALLOWED = {
    1: ["逃げ", "抜き", "恵まれ"],
    2: ["差し", "まくり", "抜き", "恵まれ"],
    3: ["差し", "まくり", "まくり差し", "抜き", "恵まれ"],
    4: ["差し", "まくり", "まくり差し", "抜き", "恵まれ"],
    5: ["差し", "まくり", "まくり差し", "抜き", "恵まれ"],
    6: ["差し", "まくり", "まくり差し", "抜き", "恵まれ"],
}

def postprocess_kimarite(df: pd.DataFrame, classes: List[str]) -> pd.DataFrame:
    for i, row in df.iterrows():
        lane = int(row["lane"])
        allowed = ALLOWED.get(lane, classes)
        probs = np.array([row.get(f"prob_{c}", 0.0) for c in classes], dtype=float)
        mask = np.array([c in allowed for c in classes], dtype=bool)
        probs[~mask] = 0.0
        s = probs.sum()
        if s > 0:
            probs /= s
        for j, c in enumerate(classes):
            df.at[i, f"prob_{c}"] = float(probs[j])
        df.at[i, "pred_kimarite"] = classes[int(np.argmax(probs))]
        df.at[i, "pred_conf"] = float(probs.max())
        df.at[i, "uncertainty"] = float(1.0 - probs.max())
    return df

def _predict_kimarite(df_feat: pd.DataFrame, model, feat_cols: Optional[List[str]], classes: List[str]) -> pd.DataFrame:
    base = df_feat[["date", "pid", "race", "lane"]].copy()
    if model is None or not feat_cols:
        for c in classes:
            base[f"prob_{c}"] = 0.0
        base["pred_kimarite"] = ""
        base["pred_conf"] = 0.0
        base["uncertainty"] = 1.0
        return base

    X = _align_X(df_feat, feat_cols)
    prob_mat = model.predict_proba(X)
    # 列名は classes.json の順序を尊重
    prob_df = pd.DataFrame(prob_mat, columns=classes)
    for c in classes:
        base[f"prob_{c}"] = prob_df[c].values
    base["pred_kimarite"] = prob_df.idxmax(axis=1)
    base["pred_conf"]     = prob_df.max(axis=1)
    base["uncertainty"]   = 1.0 - base["pred_conf"]
    return postprocess_kimarite(base, classes)

def _merge_outputs(win_df: pd.DataFrame, kim_df: pd.DataFrame, classes: List[str]) -> pd.DataFrame:
    out = win_df.merge(kim_df, on=["date", "pid", "race", "lane"], how="outer")
    for c in classes:
        col = f"prob_{c}"
        if col not in out:
            out[col] = 0.0
    return out

def predict(date: str, pid: str, race: str = "", tansyo_model_date: str = "", kimarite_model_date: str = ""):
    # モデル読み込み
    pack = _load_model_pair(tansyo_model_date or None, kimarite_model_date or None, pid or None)
    t_mod, t_feats = pack["tansyo"]["model"],   pack["tansyo"]["features"]
    k_mod, k_feats = pack["kimarite"]["model"], pack["kimarite"]["features"]
    k_classes      = pack["kimarite"]["classes"]

    # 対象レース
    targets = [race] if race else [f"{i}R" for i in range(1, 12 + 1)]
    out_dir = os.path.join(OUT_BASE, date, pid)
    os.makedirs(out_dir, exist_ok=True)

    outs: List[pd.DataFrame] = []
    for r in targets:
        integ = os.path.join(INTEG_BASE, date, pid, f"{r}.json")
        if not os.path.exists(integ):
            _log(f"skip (missing): {integ}")
            continue

        _log(f"predicting: date={date} pid={pid} race={r}")
        df_feat = _extract_features_from_integrated(date, pid, r)
        win_df  = _predict_tansyo(df_feat, t_mod, t_feats)
        kim_df  = _predict_kimarite(df_feat, k_mod, k_feats, k_classes)
        merged  = _merge_outputs(win_df, kim_df, k_classes)

        out_csv = os.path.join(out_dir, f"{r}.csv")
        merged.to_csv(out_csv, index=False, encoding="utf-8")
        _log(f"saved: {out_csv} rows={len(merged)}")
        outs.append(merged)

    if outs:
        all_csv = os.path.join(out_dir, "all.csv")
        pd.concat(outs).to_csv(all_csv, index=False, encoding="utf-8")
        _log(f"saved: {all_csv}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--pid",  required=True)
    ap.add_argument("--race", default="")
    ap.add_argument("--tansyo_model_date",   default="")
    ap.add_argument("--kimarite_model_date", default="")
    args = ap.parse_args()
    try:
        predict(args.date, args.pid, args.race, args.tansyo_model_date, args.kimarite_model_date)
    except Exception as e:
        _log(f"FATAL: {e}")
        raise

if __name__ == "__main__":
    main()
