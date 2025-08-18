# -*- coding: utf-8 -*-
"""
学習スクリプト（勝利確率予測）
入力:
  TENKAI/datasets/v1/<date>/<pid>/(<race>_train.csv | all_train.csv)
集約:
  --dates に複数日 (例: 20250801,20250802) もしくは "ALL" を指定可
  --pid 未指定なら全pidを縦結合
  --race 未指定なら all_train.csv を使用
出力:
  TENKAI/models/v1/<tag_date>/<pid|ALL>/<race|ALL>/{model.pkl,features.json,metrics.json,meta.json}
"""

from __future__ import annotations
import os, json, argparse
from typing import List, Tuple
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score, log_loss
import joblib

DATA_BASE  = os.path.join("TENKAI", "datasets", "v1")
MODEL_BASE = os.path.join("TENKAI", "models",   "v1")

KEY_COLS    = ["date","pid","race","lane"]
TARGET_COLS = ["rank","win","st","decision"]  # st/decision は一旦除外

# ------------------------
# IO / 集約
# ------------------------
def _list_all_dates() -> List[str]:
    if not os.path.isdir(DATA_BASE):
        return []
    return sorted([d for d in os.listdir(DATA_BASE) if d.isdigit()])

def _resolve_dates(tag_date: str, dates_arg: str) -> List[str]:
    """
    dates_arg:
      - ""        -> [tag_date]
      - "ALL"     -> DATA_BASE 配下の全日
      - "d1,d2.." -> そのまま
    """
    if not dates_arg:
        return [tag_date]
    if dates_arg.strip().upper() == "ALL":
        ds = _list_all_dates()
        if not ds:
            raise FileNotFoundError(f"No dataset dates found under {DATA_BASE}")
        return ds
    return [d.strip() for d in dates_arg.split(",") if d.strip()]

def _collect_frames(dates: List[str], pid: str, race: str) -> pd.DataFrame:
    """
    dates×pid(or ALL)×race(or all) のCSVを見つけて縦結合
    """
    targets = []
    for d in dates:
        base_d = os.path.join(DATA_BASE, d)
        if not os.path.isdir(base_d):
            continue
        if pid:  # 単場
            pids = [pid]
        else:   # 全場
            pids = sorted([x for x in os.listdir(base_d) if os.path.isdir(os.path.join(base_d, x))])

        for p in pids:
            fname = f"{race}_train.csv" if race else "all_train.csv"
            fpath = os.path.join(base_d, p, fname)
            if os.path.exists(fpath):
                try:
                    df = pd.read_csv(fpath)
                    targets.append(df)
                except Exception:
                    pass

    if not targets:
        raise FileNotFoundError(
            f"no train csv found: dates={dates}, pid={'ALL' if not pid else pid}, "
            f"race={'ALL' if not race else race}"
        )
    return pd.concat(targets, ignore_index=True)

# ------------------------
# 前処理 & 学習
# ------------------------
def _prepare_xy(df: pd.DataFrame):
    if "win" not in df.columns:
        raise ValueError("column 'win' not found in dataset")
    df = df.copy()
    df = df[~df["win"].isna()].reset_index(drop=True)

    drop_set = set(KEY_COLS + TARGET_COLS)
    feat_cols = [c for c in df.columns if c not in drop_set]

    for c in feat_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    keep_cols = []
    for c in feat_cols:
        col = df[c]
        if col.notna().sum() == 0:
            continue
        df[c] = col.fillna(col.median())
        keep_cols.append(c)

    X = df[keep_cols]
    y = df["win"].astype(int)
    return X, y, keep_cols

def _train_eval(X: pd.DataFrame, y: pd.Series):
    metrics = {}
    strat_ok = y.nunique() > 1 and min((y==0).sum(), (y==1).sum()) >= 2

    if strat_ok and len(y) >= 20:
        Xtr, Xte, ytr, yte = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
    else:
        Xtr, ytr = X, y
        Xte = yte = None

    clf = RandomForestClassifier(
        n_estimators=300,
        max_depth=None,
        n_jobs=-1,
        random_state=42,
        class_weight="balanced_subsample",
    )
    clf.fit(Xtr, ytr)

    if Xte is not None:
        yp = clf.predict(Xte)
        yp_prob = clf.predict_proba(Xte)[:,1]
        metrics["accuracy"] = float(accuracy_score(yte, yp))
        try:
            metrics["roc_auc"] = float(roc_auc_score(yte, yp_prob))
        except Exception:
            metrics["roc_auc"] = None
        try:
            metrics["log_loss"] = float(log_loss(yte, yp_prob, labels=[0,1]))
        except Exception:
            metrics["log_loss"] = None
        metrics["n_train"] = int(len(ytr))
        metrics["n_test"]  = int(len(yte))
    else:
        metrics.update({"accuracy": None, "roc_auc": None, "log_loss": None,
                        "n_train": int(len(y)), "n_test": 0})
    return clf, metrics

# ------------------------
# main
# ------------------------
def train_win(dates_arg: str, tag_date: str, pid: str, race: str):
    dates = _resolve_dates(tag_date, dates_arg)
    df = _collect_frames(dates, pid, race)

    X, y, feat_cols = _prepare_xy(df)
    model, metrics = _train_eval(X, y)

    pid_out  = pid if pid else "ALL"
    race_out = race if race else "ALL"

    out_dir = os.path.join(MODEL_BASE, tag_date, pid_out, race_out)
    os.makedirs(out_dir, exist_ok=True)

    joblib.dump(model, os.path.join(out_dir, "model.pkl"))
    with open(os.path.join(out_dir, "features.json"), "w", encoding="utf-8") as f:
        json.dump({"features": feat_cols}, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump({
            "tag_date": tag_date,
            "used_dates": dates,
            "pid": pid_out,
            "race": race_out,
            "source": "TENKAI/datasets/v1",
            "rows": int(len(df))
        }, f, ensure_ascii=False, indent=2)

    print("saved:", out_dir)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date",  required=True, help="モデル保存のタグ日(YYYYMMDD)")
    ap.add_argument("--dates", default="",   help="学習対象日(カンマ区切り or ALL, 空なら --date のみ)")
    ap.add_argument("--pid",   default="",   help="場コード(空=ALL場まとめ)")
    ap.add_argument("--race",  default="",   help="レース(例: 1R, 空=ALL)")
    args = ap.parse_args()
    train_win(args.dates, args.date, args.pid, args.race)

if __name__ == "__main__":
    main()
