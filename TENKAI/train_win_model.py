# -*- coding: utf-8 -*-
"""
学習スクリプト（勝利確率予測） – 複数日まとめ学習対応
入力: TENKAI/datasets/v1/<date>/<pid>/(<race>_train.csv|all_train.csv)
出力: TENKAI/models/v1/<dates_tag>/<pid|ALL>/<race|ALL>/{model.pkl,features.json,metrics.json,meta.json}

使い方:
- 1日・全場:            --date 20250813                     --pid ""  --race ""
- 1日・場別ALL:         --date 20250813 --pid 05            --race ""
- 1日・場別レース:      --date 20250813 --pid 05            --race 9R
- 複数日まとめ(列挙):   --dates 20250801,20250802,20250803   --pid ""  --race ""
- 複数日まとめ(ALL):     --dates ALL                          --pid ""  --race ""
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
TARGET_COLS = ["rank","win","st","decision"]  # これらは説明変数から除外

# ------------ 入出力ヘルパ ------------

def _read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def _list_all_dates() -> List[str]:
    if not os.path.isdir(DATA_BASE):
        return []
    # ディレクトリ名(YYYYMMDD)のみに限定
    return sorted([d for d in os.listdir(DATA_BASE) if os.path.isdir(os.path.join(DATA_BASE, d))])

def _resolve_dates(date: str, dates: str) -> List[str]:
    """
    優先順: --dates があればそれ、なければ --date
    --dates=ALL で DATA_BASE 配下の全日
    """
    if dates:
        if dates.strip().upper() == "ALL":
            ds = _list_all_dates()
            if not ds:
                raise FileNotFoundError(f"No dates found under {DATA_BASE}")
            return ds
        # カンマ区切り
        ds = [d.strip() for d in dates.split(",") if d.strip()]
        if not ds:
            raise ValueError("--dates が空です")
        return sorted(ds)
    if not date:
        raise ValueError("either --date or --dates is required")
    return [date]

def _dates_tag(dates: List[str]) -> str:
    if not dates:
        return "UNKNOWN"
    if len(dates) == 1:
        return dates[0]
    return f"{min(dates)}-{max(dates)}"

def _load_pid_dataset_single(date: str, pid: str, race: str) -> pd.DataFrame:
    base = os.path.join(DATA_BASE, date, pid)
    path = os.path.join(base, f"{race}_train.csv") if race else os.path.join(base, "all_train.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"dataset not found: {path}")
    return _read_csv(path)

def _load_pid_dataset_multi(dates: List[str], pid: str, race: str) -> pd.DataFrame:
    dfs = []
    for d in dates:
        try:
            dfs.append(_load_pid_dataset_single(d, pid, race))
        except FileNotFoundError:
            # その日付にデータが無ければスキップ（続行）
            print(f"skip: missing dataset for date={d}, pid={pid}, race={'ALL' if not race else race}")
    if not dfs:
        raise FileNotFoundError(f"no dataset found for pid={pid}, race={'ALL' if not race else race} in dates={dates}")
    return pd.concat(dfs, ignore_index=True)

def _load_all_pids_dataset_single(date: str) -> pd.DataFrame:
    base = os.path.join(DATA_BASE, date)
    if not os.path.isdir(base):
        raise FileNotFoundError(f"datasets dir not found: {base}")
    dfs = []
    for pid in sorted(os.listdir(base)):
        p = os.path.join(base, pid, "all_train.csv")
        if os.path.exists(p):
            try:
                dfs.append(_read_csv(p))
            except Exception:
                pass
    if not dfs:
        raise FileNotFoundError(f"no all_train.csv found under: {base}")
    return pd.concat(dfs, ignore_index=True)

def _load_all_pids_dataset_multi(dates: List[str]) -> pd.DataFrame:
    dfs = []
    for d in dates:
        try:
            dfs.append(_load_all_pids_dataset_single(d))
        except FileNotFoundError:
            print(f"skip: no pid data on date={d}")
    if not dfs:
        raise FileNotFoundError(f"no all_train.csv found under any of dates={dates}")
    return pd.concat(dfs, ignore_index=True)

# ------------ 前処理＆学習 ------------

def _prepare_xy(df: pd.DataFrame):
    if "win" not in df.columns:
        raise ValueError("column 'win' not found in dataset")
    df = df.copy()
    df = df[~df["win"].isna()].reset_index(drop=True)

    drop_set = set(KEY_COLS + TARGET_COLS)
    feat_cols = [c for c in df.columns if c not in drop_set]

    # 数値化
    for c in feat_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # 欠損は列中央値で補完（全欠損列は捨て）
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
        Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    else:
        Xtr, ytr = X, y
        Xte, yte = None, None

    clf = RandomForestClassifier(
        n_estimators=300,
        max_depth=None,
        n_jobs=-1,
        random_state=42,
        class_weight="balanced_subsample"
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
        metrics.update({
            "accuracy": None, "roc_auc": None, "log_loss": None,
            "n_train": int(len(y)), "n_test": 0
        })
    return clf, metrics

# ------------ メイン ------------

def train_win(dates_arg: str, date: str, pid: str, race: str):
    dates = _resolve_dates(date, dates_arg)
    tag   = _dates_tag(dates)

    # データ読み込み
    if pid:
        df = _load_pid_dataset_multi(dates, pid, race)
        pid_out  = pid
        race_out = race if race else "ALL"
    else:
        df = _load_all_pids_dataset_multi(dates)
        pid_out  = "ALL"
        race_out = "ALL"

    # 学習
    X, y, feat_cols = _prepare_xy(df)
    model, metrics   = _train_eval(X, y)

    # 保存
    out_dir = os.path.join(MODEL_BASE, tag, pid_out, race_out)
    os.makedirs(out_dir, exist_ok=True)

    joblib.dump(model, os.path.join(out_dir, "model.pkl"))
    with open(os.path.join(out_dir, "features.json"), "w", encoding="utf-8") as f:
        json.dump({"features": feat_cols}, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump({
            "dates": dates,
            "dates_tag": tag,
            "pid": pid if pid else "ALL",
            "race": race if race else "ALL",
            "source": "TENKAI/datasets/v1",
            "rows": int(len(df))
        }, f, ensure_ascii=False, indent=2)

    print("saved:", out_dir)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date",  default="", help="単日 YYYYMMDD（--dates未指定時に使用）")
    ap.add_argument("--dates", default="", help="カンマ区切り or 'ALL'（指定があれば優先）")
    ap.add_argument("--pid",   default="", help="場コード (空=ALL場)")
    ap.add_argument("--race",  default="", help="レース (空=ALL)")
    args = ap.parse_args()
    train_win(args.dates, args.date, args.pid, args.race)

if __name__ == "__main__":
    main()
