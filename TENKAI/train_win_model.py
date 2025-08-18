# TENKAI/train_win_model.py
# -*- coding: utf-8 -*-
"""
学習スクリプト（勝利確率予測 / 複数日対応）
入力: TENKAI/datasets/v1/<date>/<pid>/(<race>_train.csv|all_train.csv)
出力: TENKAI/models/v1/<tag_date>/<pid|ALL>/<race|ALL>/{model.pkl,features.json,metrics.json,meta.json}

使い方例:
  # 特定日・特定場（全R）
  python TENKAI/train_win_model.py --date 20250814 --pid 02

  # 特定日・特定場・特定R
  python TENKAI/train_win_model.py --date 20250814 --pid 02 --race 1R

  # 全場まとめ（その日の全pidの all_train を縦結合）
  python TENKAI/train_win_model.py --date 20250814 --pid ALL

  # 複数日をまとめて学習（全場ALL）
  python TENKAI/train_win_model.py --date 20250817 --dates 20250813,20250814 --pid ALL

  # データベースにある全日付を対象（全場ALL）
  python TENKAI/train_win_model.py --date 20250817 --dates ALL --pid ALL
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
TARGET_COLS = ["rank","win","st","decision"]  # st, decision は学習から除外（必要に応じて含める）

# ---------- ユーティリティ ----------

def _read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def _list_all_dates() -> List[str]:
    """DATA_BASE 直下の日付フォルダ一覧を昇順で返す"""
    if not os.path.isdir(DATA_BASE):
        return []
    ds = [d for d in os.listdir(DATA_BASE) if d.isdigit() and os.path.isdir(os.path.join(DATA_BASE, d))]
    return sorted(ds)

def _resolve_dates(tag_date: str, dates_arg: str) -> List[str]:
    """
    dates_arg:
      ""        -> [tag_date]
      "ALL"     -> DATA_BASE 配下の全日付
      "d1,d2.." -> そのリスト（中に ALL が混じっても全日に展開）
    """
    if not dates_arg:
        return [tag_date]

    arg_up = dates_arg.strip().upper()
    if arg_up == "ALL":
        ds = _list_all_dates()
        if not ds:
            raise FileNotFoundError(f"No dataset dates found under {DATA_BASE}")
        return ds

    parts = [p.strip() for p in dates_arg.split(",") if p.strip()]
    if any(p.upper() == "ALL" for p in parts):
        ds = _list_all_dates()
        if not ds:
            raise FileNotFoundError(f"No dataset dates found under {DATA_BASE}")
        return ds
    return parts

def _collect_frames(dates: List[str], pid: str, race: str) -> pd.DataFrame:
    """
    指定された dates × pid × race から学習CSVを収集して縦結合
    - pid == 'ALL' または '' -> 各 date 配下の全 pid を走査
    - race == 'ALL' または '' -> all_train.csv を使う
    - race 指定あり -> <race>_train.csv を使う
    """
    frames: List[pd.DataFrame] = []
    for d in dates:
        d_dir = os.path.join(DATA_BASE, d)
        if not os.path.isdir(d_dir):
            continue

        target_pids = []
        if (pid or "").upper() == "ALL" or pid == "":
            # 日付配下の全pid
            target_pids = [p for p in os.listdir(d_dir) if os.path.isdir(os.path.join(d_dir, p))]
        else:
            target_pids = [pid]

        for p in sorted(target_pids):
            if (race or "").upper() in ("", "ALL"):
                path = os.path.join(d_dir, p, "all_train.csv")
            else:
                path = os.path.join(d_dir, p, f"{race}_train.csv")
            if os.path.exists(path):
                try:
                    frames.append(_read_csv(path))
                except Exception:
                    pass

    if not frames:
        raise FileNotFoundError(f"no train csv found: dates={dates}, pid={pid or 'ALL'}, race={race or 'ALL'}")
    return pd.concat(frames, ignore_index=True)

def _prepare_xy(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series, List[str]]:
    """ y=win / X=その他の数値列（KEY/TARGET除外）。欠損は列中央値で埋める。"""
    if "win" not in df.columns:
        raise ValueError("column 'win' not found in dataset")

    df = df.copy()
    df = df[~df["win"].isna()].reset_index(drop=True)

    drop_set = set(KEY_COLS + TARGET_COLS)
    feat_cols = [c for c in df.columns if c not in drop_set]

    for c in feat_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    keep_cols: List[str] = []
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
    """データ状況に合わせて学習 + 可能なら簡易評価"""
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
        metrics.update(dict(accuracy=None, roc_auc=None, log_loss=None,
                            n_train=int(len(y)), n_test=0))
    return clf, metrics

# ---------- エントリポイント ----------

def train_win(dates_arg: str, tag_date: str, pid: str, race: str):
    """
    dates_arg: "" / "ALL" / "d1,d2,..."
    tag_date : モデル保存タグ（フォルダ名）
    pid      : "ALL" or 具体
    race     : "ALL" or 具体 (例: "1R")
    """
    dates = _resolve_dates(tag_date, dates_arg)
    pid_norm  = (pid  or "ALL").upper()
    race_norm = (race or "ALL").upper()

    # 学習データ収集
    df = _collect_frames(dates, pid_norm, race_norm if race_norm != "ALL" else "")

    # 前処理
    X, y, feat_cols = _prepare_xy(df)

    # 学習
    model, metrics = _train_eval(X, y)

    # 保存
    out_pid  = pid_norm
    out_race = race_norm
    out_dir = os.path.join(MODEL_BASE, tag_date, out_pid, out_race)
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
            "pid": out_pid,
            "race": out_race,
            "source": "TENKAI/datasets/v1",
            "rows": int(len(df)),
        }, f, ensure_ascii=False, indent=2)

    print("saved:", out_dir)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date",  required=True, help="モデル保存タグ（日付形式を推奨）")
    ap.add_argument("--dates", default="",   help="学習対象日（ALL / カンマ区切り / 空=--date）")
    ap.add_argument("--pid",   default="ALL", help="場コード（ALL=全場）")
    ap.add_argument("--race",  default="ALL", help="レース（ALL=全R, 例: 1R）")
    args = ap.parse_args()
    train_win(args.dates, args.date, args.pid, args.race)

if __name__ == "__main__":
    main()
