# -*- coding: utf-8 -*-
"""
統合予測スクリプト（単勝 + 決まり手）
- 単勝モデル:    TENKAI/models_tansyo/v1/<model_date>/<pid|ALL>/<race|ALL>/
- 決まり手モデル: TENKAI/models_kimarite/v1/<model_date>/<pid|ALL>/<race|ALL>/
- 予測特徴は integrated をオンザフライで生成（features_c 不要）
- 出力:
    TENKAI/predictions/v1/<date>/<pid>/<race>.csv   … 枠番6行（単勝+決まり手を1枚に統合）
    TENKAI/predictions/v1/<date>/<pid>/all.csv     … 全R縦結合
使い方:
  python TENKAI/tenkai_predict_integrated.py --date 20250814 --pid 02
  python TENKAI/tenkai_predict_integrated.py --date 20250814 --pid 02 --race 2R
  # モデル日付の明示（省略時は各モデル種別ごとに“最新日”を自動採用）
  python TENKAI/tenkai_predict_integrated.py --date 20250814 --pid 02 --tansyo_model_date 20250812 --kimarite_model_date 20250810
"""
from __future__ import annotations
import os, json, argparse
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import joblib

# === パス ===
INTEG_BASE = os.path.join("public", "integrated", "v1")
MODEL_TANSYO_BASE   = os.path.join("TENKAI", "models_tansyo",   "v1")
MODEL_KIMARITE_BASE = os.path.join("TENKAI", "models_kimarite", "v1")
OUT_BASE   = os.path.join("TENKAI", "predictions", "v1")

# === 汎用ユーティリティ ===
def _is_yyyymmdd(s: str) -> bool:
    return len(s) == 8 and s.isdigit()

def _latest_date_under(base: str) -> str:
    if not os.path.isdir(base):
        raise FileNotFoundError(f"model base not found: {base}")
    ds = sorted([d for d in os.listdir(base) if _is_yyyymmdd(d)])
    if not ds:
        raise FileNotFoundError(f"no model dates under: {base}")
    return ds[-1]

def _pick_model_dir(model_root: str, model_date: str, pid: Optional[str]) -> str:
    """
    優先順位:
      1) <date>/ALL/ALL
      2) <date>/<pid>/ALL   （pid指定時）
      3) <date> 配下の最初の model.pkl
    """
    cands = [os.path.join(model_root, model_date, "ALL", "ALL")]
    if pid:
        cands.append(os.path.join(model_root, model_date, pid, "ALL"))
    for d in cands:
        if os.path.exists(os.path.join(d, "model.pkl")):
            return d
    for root, _, files in os.walk(os.path.join(model_root, model_date)):
        if "model.pkl" in files:
            return root
    raise FileNotFoundError(f"no model.pkl under {os.path.join(model_root, model_date)}")

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

def _rank(values: List[Optional[float]]) -> List[int]:
    pairs = [(i, v if v is not None else float("inf")) for i, v in enumerate(values)]
    pairs.sort(key=lambda t: t[1])
    ranks = [0]*len(values)
    r = 1
    for idx, _ in pairs:
        ranks[idx] = r; r += 1
    return ranks

def _mean(xs: List[Optional[float]]) -> Optional[float]:
    xs = [x for x in xs if x is not None]
    return float(sum(xs)/len(xs)) if xs else None

# === features 抽出（integrated → Cワイド互換 → レーン縦持ち） ===
LANES = [1,2,3,4,5,6]

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
    r_avgST = _rank(rc_avgSTs)
    r_age   = _rank(ages)
    r_class = _rank(classes)

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
            # 旧 win_k/lose_k と互換を残す（最低限）
            "win_k": ss.get("firstCount", 0),
            "lose_k": (_safe_get(e, "stats", "entryCourse", "loseKimarite", default={}) or {}).get("まくり", 0),
            # 差分・ランク
            "d_avgST_rc": (avgST_rc if avgST_rc is not None else 0.16) - 0.16,
            "d_age":      (age if age is not None else 40) - 40,
            "d_class":    (cls if cls is not None else 3) - 3,
            "rank_avgST": r_avgST[idx],
            "rank_age":   r_age[idx],
            "rank_class": r_class[idx],
        }
        for k, v in feat.items():
            wide[prefix+k] = v

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
    df = pd.DataFrame(rows)
    for c in df.columns:
        if c in ("date","pid","race"): continue
        df[c] = pd.to_numeric(df[c], errors="ignore")
    return df

# === モデル読み込み ===
def _load_model_with_features(model_root: str, model_date: Optional[str], pid: Optional[str]):
    use_date = model_date or _latest_date_under(model_root)
    mdir = _pick_model_dir(model_root, use_date, pid)
    model = joblib.load(os.path.join(mdir, "model.pkl"))
    fj = os.path.join(mdir, "features.json")
    if not os.path.exists(fj):
        raise FileNotFoundError(f"features.json not found in {mdir}")
    with open(fj, "r", encoding="utf-8") as f:
        fjs = json.load(f)
    feats = fjs.get("features", [])
    # 決まり手モデルは classes も持つ想定（無い場合は None）
    classes = fjs.get("classes", None)
    return model, feats, classes, mdir, use_date

def _align_X(df_long: pd.DataFrame, feat_cols: List[str]) -> pd.DataFrame:
    X = df_long.copy()
    for c in feat_cols:
        if c not in X.columns:
            X[c] = np.nan
    X = X[feat_cols]
    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors="coerce")
        med = X[c].median() if X[c].notna().any() else 0.0
        X[c] = X[c].fillna(med)
    return X

# === 推論 ===
KIMARITE_CANON = ["逃げ","差し","まくり","まくり差し","抜き","恵まれ"]

def _predict_tansyo(df_feat_long: pd.DataFrame, model, feat_cols: List[str]) -> pd.DataFrame:
    X = _align_X(df_feat_long, feat_cols)
    if hasattr(model, "predict_proba"):
        prob = model.predict_proba(X)[:,1]
    else:
        prob = model.predict(X).astype(float)
    out = df_feat_long[["date","pid","race","lane"]].copy()
    out["win_prob"] = prob
    out["pred_win"] = (out["win_prob"] >= 0.5).astype(int)
    out["pred_rank_in_race"] = out.groupby(["date","pid","race"])["win_prob"]\
                                  .rank(ascending=False, method="first").astype(int)
    return out

def _predict_kimarite(df_feat_long: pd.DataFrame, model, feat_cols: List[str], classes_in_model: Optional[List[str]]) -> pd.DataFrame:
    X = _align_X(df_feat_long, feat_cols)
    # プロバ出力を確保
    if hasattr(model, "predict_proba"):
        proba = model.predict_proba(X)
        model_classes = list(getattr(model, "classes_", classes_in_model or []))
        # 列を正規化（KIMARITE_CANON 順に揃え、無いクラスは0埋め）
        prob_map = {cls: proba[:, i] for i, cls in enumerate(model_classes)}
        out = df_feat_long[["date","pid","race","lane"]].copy()
        for cls in KIMARITE_CANON:
            col = f"prob_{cls}"
            out[col] = prob_map.get(cls, np.zeros(len(X)))
        # 予測クラスと信頼度
        probs_stack = out[[f"prob_{c}" for c in KIMARITE_CANON]].to_numpy()
        argmax_idx = probs_stack.argmax(axis=1)
        out["pred_kimarite"] = [KIMARITE_CANON[i] for i in argmax_idx]
        maxprob = probs_stack.max(axis=1)
        out["pred_conf"] = maxprob
        out["uncertainty"] = 1.0 - maxprob
        return out
    else:
        # フォールバック：確率が出せないなら予測のみ・確率はNaN
        pred = model.predict(X)
        out = df_feat_long[["date","pid","race","lane"]].copy()
        for cls in KIMARITE_CANON:
            out[f"prob_{cls}"] = np.nan
        out["pred_kimarite"] = pred
        out["pred_conf"] = np.nan
        out["uncertainty"] = np.nan
        return out

# === メイン ===
def predict(date: str, pid: str, race: str = "", tansyo_model_date: str = "", kimarite_model_date: str = ""):
    # 特徴抽出対象
    targets = [race] if race else [f"{i}R" for i in range(1,13)]
    outs = []

    # モデルロード（それぞれ独立に“最新日”を解決）
    tansyo_model = tansyo_feats = None
    kimarite_model = kimarite_feats = kimarite_classes = None

    # 単勝
    try:
        tansyo_model, tansyo_feats, _classes_unused, tansyo_mdir, t_date = _load_model_with_features(
            MODEL_TANSYO_BASE, tansyo_model_date or None, pid or None
        )
        print(f"[tansyo model] date={t_date} dir={tansyo_mdir}")
    except Exception as e:
        print(f"[tansyo model] not available: {e}")

    # 決まり手
    try:
        kimarite_model, kimarite_feats, kimarite_classes, kimarite_mdir, k_date = _load_model_with_features(
            MODEL_KIMARITE_BASE, kimarite_model_date or None, pid or None
        )
        print(f"[kimarite model] date={k_date} dir={kimarite_mdir} classes={kimarite_classes or 'auto(classes_)'}")
    except Exception as e:
        print(f"[kimarite model] not available: {e}")

    out_dir = os.path.join(OUT_BASE, date, pid)
    os.makedirs(out_dir, exist_ok=True)

    any_written = False
    for r in targets:
        integ_path = os.path.join(INTEG_BASE, date, pid, f"{r}.json")
        if not os.path.exists(integ_path):
            print(f"skip (not found): {integ_path}")
            continue
        try:
            df_feat = _extract_features_from_integrated(date, pid, r)

            # 単勝
            if tansyo_model is not None:
                df_win = _predict_tansyo(df_feat, tansyo_model, tansyo_feats)
            else:
                df_win = df_feat[["date","pid","race","lane"]].copy()
                df_win["win_prob"] = np.nan
                df_win["pred_win"] = np.nan
                df_win["pred_rank_in_race"] = np.nan

            # 決まり手
            if kimarite_model is not None:
                df_km = _predict_kimarite(df_feat, kimarite_model, kimarite_feats, kimarite_classes)
            else:
                df_km = df_feat[["date","pid","race","lane"]].copy()
                for cls in KIMARITE_CANON:
                    df_km[f"prob_{cls}"] = np.nan
                df_km["pred_kimarite"] = ""
                df_km["pred_conf"] = np.nan
                df_km["uncertainty"] = np.nan

            # マージ（完全一致キー）
            df_out = df_win.merge(df_km, on=["date","pid","race","lane"], how="inner", validate="one_to_one")

            out_path = os.path.join(out_dir, f"{r}.csv")
            df_out.to_csv(out_path, index=False, encoding="utf-8")
            print(f"wrote {out_path} (rows={len(df_out)})")
            outs.append(df_out)
            any_written = True
        except Exception as e:
            print(f"skip (error): {integ_path}  {e}")

    if any_written and outs:
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
    ap.add_argument("--race", default="",    help="レース（例: 2R）空なら1R..12R全部")
    ap.add_argument("--tansyo_model_date",   default="", help="単勝モデル日付（空=最新自動）")
    ap.add_argument("--kimarite_model_date", default="", help="決まり手モデル日付（空=最新自動）")
    args = ap.parse_args()
    predict(args.date, args.pid, args.race, args.tansyo_model_date, args.kimarite_model_date)

if __name__ == "__main__":
    main()
