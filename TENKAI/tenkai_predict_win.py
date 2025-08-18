# -*- coding: utf-8 -*-
"""
勝率予測（推論）スクリプト：ALLモデルで任意のpid/raceを推論
入力:
  - 特徴: TENKAI/features_c/v1/<date>/<pid>/<race or all>.csv
  - モデル: TENKAI/models/v1/<model_date>/ALL/ALL/{model.pkl, features.json}
出力:
  - TENKAI/predictions/v1/<date>/<pid>/<race>_pred.csv
  - TENKAI/predictions/v1/<date>/<pid>/all_pred.csv（pid内まとめ、毎回再生成）
使い方例:
  python TENKAI/tenkai_predict_win.py --date 20250814 --model_date LATEST
  python TENKAI/tenkai_predict_win.py --date 20250814 --pid 02 --race 1R --model_date LATEST
"""

from __future__ import annotations
import os, json, argparse, re
from typing import List, Dict
import pandas as pd
import joblib

FEAT_BASE = os.path.join("TENKAI", "features_c", "v1")
PRED_BASE = os.path.join("TENKAI", "predictions", "v1")
MODEL_BASE= os.path.join("TENKAI", "models", "v1")

LANES = [1,2,3,4,5,6]
PFX_RE = re.compile(r"^L([1-6])_(.+)$")

def _latest_model_date_under_all() -> str:
    base = MODEL_BASE
    if not os.path.isdir(base):
        raise FileNotFoundError(f"models dir not found: {base}")
    candidates = []
    for d in os.listdir(base):
        path = os.path.join(base, d, "ALL", "ALL", "model.pkl")
        if os.path.exists(path):
            candidates.append(d)
    if not candidates:
        raise FileNotFoundError("no ALL model found under TENKAI/models/v1/*/ALL/ALL")
    return sorted(candidates)[-1]

def _resolve_model_dir(model_date: str) -> str:
    md = _latest_model_date_under_all() if (not model_date or model_date.upper()=="LATEST") else model_date
    mdir = os.path.join(MODEL_BASE, md, "ALL", "ALL")
    model_pkl = os.path.join(mdir, "model.pkl")
    feats_js  = os.path.join(mdir, "features.json")
    if not (os.path.exists(model_pkl) and os.path.exists(feats_js)):
        raise FileNotFoundError(f"ALL model artifacts missing: {mdir}")
    return mdir

def _load_wide_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    df = pd.read_csv(path, dtype=str).convert_dtypes()
    return df

def _wide_to_long(df_wide: pd.DataFrame) -> pd.DataFrame:
    """L{lane}_xxx → (date,pid,race,lane,feature...)"""
    commons = [c for c in df_wide.columns if not PFX_RE.match(c)]
    rows = []
    for _, r in df_wide.iterrows():
        base = {c: r[c] for c in commons if c in r.index}
        by_lane = {lane:{} for lane in LANES}
        for c in df_wide.columns:
            m = PFX_RE.match(c)
            if not m: continue
            lane = int(m.group(1)); key = m.group(2)
            by_lane[lane][key] = r[c]
        for lane in LANES:
            row = dict(base); row["lane"] = lane
            for k, v in by_lane.get(lane, {}).items():
                row[k] = v
            rows.append(row)
    df = pd.DataFrame(rows)
    for c in df.columns:
        if c in ("date","pid","race","decision"):
            continue
        df[c] = pd.to_numeric(df[c], errors="ignore")
    return df

def _align_features(df_long: pd.DataFrame, feature_list: List[str]) -> pd.DataFrame:
    """features.jsonの列に合わせる：欠け列は0埋め、余分は落とす、順序も揃える"""
    X = df_long.copy()
    for col in feature_list:
        if col not in X.columns:
            X[col] = 0
    X = X[feature_list]
    for c in feature_list:
        X[c] = pd.to_numeric(X[c], errors="coerce").fillna(0)
    return X

def _rank_within_group(df: pd.DataFrame, prob_col: str="win_prob") -> pd.Series:
    """win_prob降順で1位=最も勝つ確率が高い"""
    return df[prob_col].rank(method="min", ascending=False).astype(int)

def _iter_target_files(date: str, pid: str, race: str):
    """推論対象となる特徴CSVのパス群を列挙"""
    if pid:
        base = os.path.join(FEAT_BASE, date, pid)
        if race:
            yield pid, race, os.path.join(base, f"{race}.csv")
        else:
            all_path = os.path.join(base, "all.csv")
            if os.path.exists(all_path):
                yield pid, "all", all_path
            else:
                for i in range(1,13):
                    r = f"{i}R"
                    p = os.path.join(base, f"{r}.csv")
                    if os.path.exists(p):
                        yield pid, r, p
    else:
        date_dir = os.path.join(FEAT_BASE, date)
        if not os.path.isdir(date_dir):
            return
        for pid2 in sorted(os.listdir(date_dir)):
            base = os.path.join(date_dir, pid2)
            if not os.path.isdir(base):
                continue
            all_path = os.path.join(base, "all.csv")
            if os.path.exists(all_path):
                yield pid2, "all", all_path
            else:
                for i in range(1,13):
                    r = f"{i}R"
                    p = os.path.join(base, f"{r}.csv")
                    if os.path.exists(p):
                        yield pid2, r, p

def predict(date: str, pid: str, race: str, model_date: str):
    mdir = _resolve_model_dir(model_date)
    model = joblib.load(os.path.join(mdir, "model.pkl"))
    with open(os.path.join(mdir, "features.json"), "r", encoding="utf-8") as f:
        feature_list = json.load(f)["features"]

    wrote_any = False
    buckets: Dict[str, List[pd.DataFrame]] = {}

    for pid_i, race_i, feat_path in _iter_target_files(date, pid, race):
        df_wide = _load_wide_csv(feat_path)
        df_long = _wide_to_long(df_wide)
        X = _align_features(df_long, feature_list)
        prob = model.predict_proba(X)[:,1]

        out = df_long[["date","pid","race","lane"]].copy()
        out["win_prob"] = prob
        out["rank_pred"] = (
            out.groupby(["date","pid","race"], dropna=False)
               .apply(lambda g: _rank_within_group(g, "win_prob"))
               .reset_index(level=[0,1,2], drop=True)
        ).astype(int)
        out["model_date"]  = os.path.basename(os.path.dirname(os.path.dirname(mdir)))  # <model_date>
        out["model_scope"] = "ALL"

        out_dir = os.path.join(PRED_BASE, date, pid_i)
        os.makedirs(out_dir, exist_ok=True)
        if race_i == "all":
            for r_name, g in out.groupby("race", dropna=False):
                r = str(r_name)
                out_path = os.path.join(out_dir, f"{r}_pred.csv")
                g.to_csv(out_path, index=False, encoding="utf-8")
                print("wrote", out_path, f"(rows={len(g)})")
        else:
            out_path = os.path.join(out_dir, f"{race_i}_pred.csv")
            out.to_csv(out_path, index=False, encoding="utf-8")
            print("wrote", out_path, f"(rows={len(out)})")

        buckets.setdefault(pid_i, []).append(out)
        wrote_any = True

    if wrote_any:
        for pid_i, parts in buckets.items():
            df_all = pd.concat(parts, ignore_index=True)
            all_path = os.path.join(PRED_BASE, date, pid_i, "all_pred.csv")
            df_all.sort_values(["race","lane"], inplace=True)
            df_all.to_csv(all_path, index=False, encoding="utf-8")
            print("wrote", all_path, f"(rows={len(df_all)})")
    else:
        print("no targets to predict.")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date",       required=True, help="対象日 YYYYMMDD")
    ap.add_argument("--pid",        default="",    help="場コード 空=全pid自動")
    ap.add_argument("--race",       default="",    help="レース 空=all.csv優先→1R..12R探索")
    ap.add_argument("--model_date", default="LATEST", help="学習日(ディレクトリ)。LATEST=自動検出")
    args = ap.parse_args()
    predict(args.date, args.pid, args.race, args.model_date)

if __name__ == "__main__":
    main()
