# -*- coding: utf-8 -*-
"""
決まり手 学習スクリプト（win=1 行のみ使用）
入力:
  TENKAI/datasets/v1/<date>/<pid>/(<race>_train.csv | all_train.csv)
出力:
  TENKAI/models/v1/<model_date>/<pid|ALL>/<race|ALL>/decision/
    - model.pkl
    - features.json
    - labels.json
    - metrics.json
    - meta.json

引数:
  --dates  … "ALL" または "YYYYMMDD,YYYYMMDD"（学習に使う複数日）
  --pid    … 空=ALL場まとめ / 指定時はその場のデータのみ
  --race   … 空=ALL（pid指定時は all_train.csv を使用）
挙動:
  - 複数日の *_train.csv を縦結合して学習
  - 保存先の <model_date> は dates の中で最大（最新日）
"""

from __future__ import annotations
import os, json, argparse
from typing import List, Tuple

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score, f1_score
import joblib

try:
    from lightgbm import LGBMClassifier
except Exception as e:
    raise RuntimeError("lightgbm がありません。pip install lightgbm を実行してください。") from e

DATA_BASE  = os.path.join("TENKAI", "datasets", "v1")
MODEL_BASE = os.path.join("TENKAI", "models",   "v1")

KEY_COLS    = ["date","pid","race","lane"]
TARGET_COLS = ["rank","win","st","decision"]  # decision=目的変数, win=フィルタに使用


def _read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


def _list_all_dates() -> List[str]:
    if not os.path.isdir(DATA_BASE):
        return []
    # ディレクトリ名が YYYYMMDD 前提
    return sorted([d for d in os.listdir(DATA_BASE) if d.isdigit() and len(d) == 8])


def _parse_dates_arg(dates_arg: str) -> List[str]:
    if not dates_arg or dates_arg.upper() == "ALL":
        return _list_all_dates()
    return [d.strip() for d in dates_arg.split(",") if d.strip()]


def _load_pid_dataset(date: str, pid: str, race: str) -> pd.DataFrame:
    base = os.path.join(DATA_BASE, date, pid)
    path = os.path.join(base, f"{race}_train.csv") if race else os.path.join(base, "all_train.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"dataset not found: {path}")
    return _read_csv(path)


def _load_all_pids_dataset(date: str) -> pd.DataFrame:
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


def _collect_dataset(dates: List[str], pid: str, race: str) -> pd.DataFrame:
    dfs = []
    missing = []
    for d in dates:
        try:
            if pid:
                dfs.append(_load_pid_dataset(d, pid, race))
            else:
                dfs.append(_load_all_pids_dataset(d))
        except Exception:
            missing.append(d)
    if not dfs:
        raise FileNotFoundError(f"対象日のデータが見つかりません: {missing or dates}")
    df = pd.concat(dfs, ignore_index=True)
    return df


def _prepare_xy(df: pd.DataFrame):
    """
    win==1 の行のみ残し、decision を LabelEncode して目的変数に。
    特徴量は KEY/TARGET を除いた数値列（欠損は中央値、全欠損列は除外）。
    """
    if "win" not in df.columns or "decision" not in df.columns:
        raise ValueError("dataset must contain 'win' and 'decision'")

    df = df.copy()
    df = df[(df["win"] == 1) & (~df["decision"].isna())].reset_index(drop=True)
    if len(df) == 0:
        raise ValueError("win=1 かつ decision 有効な行がありません。")

    y_raw = df["decision"].astype(str).values
    le = LabelEncoder()
    y = le.fit_transform(y_raw)
    class_names = list(le.classes_)

    drop_set = set(KEY_COLS + TARGET_COLS)
    feat_cols = [c for c in df.columns if c not in drop_set]

    # 数値化 & 欠損処理
    keep_cols = []
    for c in feat_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
        if df[c].notna().sum() == 0:
            continue
        df[c] = df[c].fillna(df[c].median())
        keep_cols.append(c)

    if not keep_cols:
        raise ValueError("有効な説明変数がありません（全て欠損）。")

    X = df[keep_cols].values
    return X, y, keep_cols, class_names


def _train_eval(X: np.ndarray, y: np.ndarray, class_names: List[str]):
    metrics = {}

    multiclass_ok = (len(np.unique(y)) > 1)
    enough = (len(y) >= max(50, len(class_names) * 10)) and all([(y == k).sum() >= 3 for k in np.unique(y)])

    if multiclass_ok and enough:
        Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    else:
        Xtr, ytr = X, y
        Xte, yte = None, None

    clf = LGBMClassifier(
        objective="multiclass",
        num_class=len(class_names),
        n_estimators=500,
        learning_rate=0.05,
        max_depth=-1,
        subsample=0.9,
        colsample_bytree=0.9,
        random_state=42,
        n_jobs=-1
    )
    clf.fit(Xtr, ytr)

    if Xte is not None:
        yp = clf.predict(Xte)
        metrics["accuracy"] = float(accuracy_score(yte, yp))
        metrics["f1_macro"] = float(f1_score(yte, yp, average="macro"))
        f1_per = f1_score(yte, yp, average=None, labels=np.unique(y))
        metrics["f1_per_class"] = {class_names[k]: float(v) for k, v in zip(np.unique(y), f1_per)}
        metrics["n_train"] = int(len(ytr))
        metrics["n_test"]  = int(len(yte))
    else:
        metrics["accuracy"] = None
        metrics["f1_macro"] = None
        metrics["f1_per_class"] = None
        metrics["n_train"]  = int(len(y))
        metrics["n_test"]   = 0

    return clf, metrics


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default="ALL", help='学習対象日: "ALL" or "YYYYMMDD,YYYYMMDD"')
    ap.add_argument("--pid",  default="", help="場コード (空=ALL場)")
    ap.add_argument("--race", default="", help="レース (空=ALL)")
    args = ap.parse_args()

    dates = _parse_dates_arg(args.dates)
    if not dates:
        raise SystemExit("対象日が見つかりません（datasets/v1 が空かも）")
    model_date = max(dates)

    # データ収集
    df = _collect_dataset(dates, args.pid, args.race)

    # 前処理
    X, y, feat_cols, class_names = _prepare_xy(df)

    # 学習
    model, metrics = _train_eval(X, y, class_names)

    # 保存先
    pid_out  = args.pid if args.pid else "ALL"
    race_out = args.race if args.race else "ALL"
    out_dir = os.path.join(MODEL_BASE, model_date, pid_out, race_out, "decision")
    os.makedirs(out_dir, exist_ok=True)

    # 保存
    joblib.dump(model, os.path.join(out_dir, "model.pkl"))
    with open(os.path.join(out_dir, "features.json"), "w", encoding="utf-8") as f:
        json.dump({"features": feat_cols}, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "labels.json"), "w", encoding="utf-8") as f:
        json.dump({"classes": class_names}, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump({
            "task": "decision_multiclass_on_winners",
            "model_date": model_date,
            "dates_used": dates,
            "pid": pid_out,
            "race": race_out,
            "source": "TENKAI/datasets/v1",
            "rows_used": int(len(y))
        }, f, ensure_ascii=False, indent=2)

    print("saved:", out_dir)


if __name__ == "__main__":
    main()
