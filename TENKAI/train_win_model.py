# -*- coding: utf-8 -*-
"""
学習スクリプト（勝利確率予測）
入力: TENKAI/datasets/v1/<date>/<pid>/(<race>_train.csv|all_train.csv)
出力: TENKAI/models/v1/<date>/<pid|ALL>/<race|ALL>/{model.pkl,features.json,metrics.json,meta.json}
- pid="" のとき: <date> 配下の全 pid の all_train.csv を縦結合して学習（全場ALL）
- race="" のとき: all_train.csv を使用（pid 指定時）
"""

from __future__ import annotations
import os, json, argparse
import pandas as pd
from typing import List, Tuple
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score, log_loss
import joblib

DATA_BASE  = os.path.join("TENKAI", "datasets", "v1")
MODEL_BASE = os.path.join("TENKAI", "models",   "v1")

KEY_COLS   = ["date","pid","race","lane"]
TARGET_COLS= ["rank","win","st","decision"]  # st, decision は説明変数にしてもOKだが一旦除外（必要なら include に切替可）

def _read_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    return df

def _load_pid_dataset(date: str, pid: str, race: str) -> pd.DataFrame:
    """pid指定あり → その pid の {race or all}_train.csv を返す"""
    base = os.path.join(DATA_BASE, date, pid)
    path = os.path.join(base, f"{race}_train.csv") if race else os.path.join(base, "all_train.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"dataset not found: {path}")
    return _read_csv(path)

def _load_all_pids_dataset(date: str) -> pd.DataFrame:
    """pid指定なし → <date> 配下の全 pid の all_train.csv を縦結合"""
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

def _prepare_xy(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series, List[str]]:
    """前処理: 目的変数 y=win、特徴量Xの作成（数値化・欠損処理）"""
    # 目的変数
    if "win" not in df.columns:
        raise ValueError("column 'win' not found in dataset")
    df = df.copy()
    # 事故などで win が欠損の行は学習に使えないので落とす
    df = df[~df["win"].isna()].reset_index(drop=True)

    # 説明変数の候補: KEY_COLS と TARGET_COLS を除外した残り
    drop_set = set(KEY_COLS + TARGET_COLS)
    feat_cols = [c for c in df.columns if c not in drop_set]

    # 可能なものを数値化
    for c in feat_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # 欠損は列中央値で埋める（全欠損列は落とす）
    keep_cols = []
    for c in feat_cols:
        col = df[c]
        if col.notna().sum() == 0:
            # 全部NaNなら使えない
            continue
        median = col.median()
        df[c] = col.fillna(median)
        keep_cols.append(c)

    X = df[keep_cols]
    y = df["win"].astype(int)
    return X, y, keep_cols

def _train_eval(X: pd.DataFrame, y: pd.Series) -> Tuple[RandomForestClassifier, dict]:
    """学習と簡易評価（クラスが片寄りすぎで分割不可の時は全量学習）"""
    metrics = {}

    # クラス片方しかない場合はスプリット不可 → 全量で学習してメトリクスはNA
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
        metrics["accuracy"] = None
        metrics["roc_auc"]  = None
        metrics["log_loss"] = None
        metrics["n_train"]  = int(len(y))
        metrics["n_test"]   = 0

    return clf, metrics

def train_win(date: str, pid: str, race: str):
    # データ読み出し
    if pid:
        df = _load_pid_dataset(date, pid, race)
        pid_out  = pid
        race_out = race if race else "ALL"
    else:
        df = _load_all_pids_dataset(date)
        pid_out  = "ALL"
        race_out = "ALL"

    # 前処理
    X, y, feat_cols = _prepare_xy(df)

    # 学習
    model, metrics = _train_eval(X, y)

    # 保存先
    out_dir = os.path.join(MODEL_BASE, date, pid_out, race_out)
    os.makedirs(out_dir, exist_ok=True)

    # アーティファクト保存
    joblib.dump(model, os.path.join(out_dir, "model.pkl"))
    with open(os.path.join(out_dir, "features.json"), "w", encoding="utf-8") as f:
        json.dump({"features": feat_cols}, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump({
            "date": date,
            "pid": pid if pid else "ALL",
            "race": race if race else "ALL",
            "source": "TENKAI/datasets/v1",
            "rows": int(len(df))
        }, f, ensure_ascii=False, indent=2)

    print("saved:", out_dir)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYYMMDD")
    ap.add_argument("--pid",  default="", help="場コード (空=ALL場)")
    ap.add_argument("--race", default="", help="レース (空=ALL)")
    args = ap.parse_args()
    train_win(args.date, args.pid, args.race)

if __name__ == "__main__":
    main()
