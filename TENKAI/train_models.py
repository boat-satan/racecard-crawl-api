# -*- coding: utf-8 -*-
"""
学習スクリプト（単勝=RandomForest / 決まり手=LightGBM、両モード対応）
入力:
  単勝     : TENKAI/datasets/v1/<date>/<pid>/(<race>_train.csv|all_train.csv)
  決まり手 : TENKAI/datasets_kimarite/v1/<date>/<pid>/(<race>_train.csv|all_train.csv)

出力:
  単勝     : TENKAI/models_tansyo/v1/<model_date>/<pid|ALL>/<race|ALL>/{model.pkl,features.json,metrics.json,meta.json}
  決まり手 : TENKAI/models_kimarite/v1/<model_date>/<pid|ALL>/<race|ALL>/{model.pkl,features.json,metrics.json,meta.json}

オプション:
  --task  : tansyo / kimarite / both（既定: both）
  --dates : 学習対象日 'ALL' か 'YYYYMMDD,YYYYMMDD,...'（既定: ALL）
  --date  : モデル保存タグ日（未指定→ --dates の最大 or データセット最新）
  --pid   : 空=ALL場、指定時はその場のみ
  --race  : 空=ALL、指定時は <race>_train.csv を使用
"""

from __future__ import annotations
import os, re, json, argparse
from typing import List, Tuple, Dict, Any

import pandas as pd
import joblib

# 単勝（Binary）
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score, roc_auc_score, log_loss,
    f1_score, classification_report
)

# 決まり手（Multiclass）
try:
    from lightgbm import LGBMClassifier
    _LGBM_AVAILABLE = True
except Exception:
    _LGBM_AVAILABLE = False

# ===== パス設定 =====
DATA_BASE_TANSYO    = os.path.join("TENKAI", "datasets", "v1")
DATA_BASE_KIMARITE  = os.path.join("TENKAI", "datasets_kimarite", "v1")
MODEL_BASE_TANSYO   = os.path.join("TENKAI", "models_tansyo",   "v1")
MODEL_BASE_KIMARITE = os.path.join("TENKAI", "models_kimarite", "v1")

DATE_RE = re.compile(r"^\d{8}$")
KEY_COLS = ["date","pid","race","lane"]
TARGET_COLS_COMMON = ["rank","win","st","decision"]  # 特徴量から除外（片方は目的変数に使用）


# ========= 共通ユーティリティ =========

def _list_dates_under(base: str) -> List[str]:
    if not os.path.isdir(base):
        return []
    return sorted([d for d in os.listdir(base)
                   if DATE_RE.match(d) and os.path.isdir(os.path.join(base, d))])

def _parse_dates_input(dates_arg: str, data_base: str) -> List[str]:
    """
    'ALL' -> data_base 配下の全日付
    'YYYYMMDD,YYYYMMDD' -> その集合
    """
    if not dates_arg or dates_arg == "ALL":
        dates = _list_dates_under(data_base)
        if not dates:
            raise FileNotFoundError(f"no datasets under {data_base}")
        return dates
    items = [x.strip() for x in dates_arg.split(",") if DATE_RE.match(x.strip())]
    if not items:
        raise ValueError(f"invalid --dates: {dates_arg}")
    return sorted(set(items))

def _auto_model_date(dates_arg: str | None, data_base: str) -> str:
    """--date 未指定時の自動決定"""
    if dates_arg and dates_arg != "ALL":
        lst = [x.strip() for x in dates_arg.split(",") if DATE_RE.match(x.strip())]
        if lst:
            return max(lst)
    dates = _list_dates_under(data_base)
    if not dates:
        raise FileNotFoundError(f"no datasets under {data_base} for auto model date")
    return dates[-1]

def _read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def _iter_dataset_paths(base_dir: str, date: str, pid: str, race: str) -> List[str]:
    """
    指定dateでの対象CSVパス群を返す。
    - pid="" -> 全場の同名CSVを集める
    - race="" -> all_train.csv、else <race>_train.csv
    """
    filename = f"{race}_train.csv" if race else "all_train.csv"
    base = os.path.join(base_dir, date)
    if not os.path.isdir(base):
        return []
    paths: List[str] = []
    if pid:
        p = os.path.join(base, pid, filename)
        if os.path.exists(p):
            paths.append(p)
    else:
        for pdir in sorted(os.listdir(base)):
            full = os.path.join(base, pdir, filename)
            if os.path.exists(full):
                paths.append(full)
    return paths

def _collect_frames(base_dir: str, dates: List[str], pid: str, race: str) -> pd.DataFrame:
    """複数日×(ALL場/特定場)のCSVを縦結合"""
    dfs = []
    for d in dates:
        for p in _iter_dataset_paths(base_dir, d, pid, race):
            try:
                dfs.append(_read_csv(p))
            except Exception:
                pass
    if not dfs:
        raise FileNotFoundError(f"no train csv found under {base_dir}: dates={dates}, pid={pid or 'ALL'}, race={race or 'ALL'}")
    return pd.concat(dfs, ignore_index=True)

def _save_bundle(model, features: List[str], metrics: Dict[str, Any],
                 out_dir: str, meta: Dict[str, Any]) -> None:
    os.makedirs(out_dir, exist_ok=True)
    joblib.dump(model, os.path.join(out_dir, "model.pkl"))
    with open(os.path.join(out_dir, "features.json"), "w", encoding="utf-8") as f:
        json.dump({"features": features}, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print("saved:", out_dir)


# ========= 単勝（Binary） =========

def _prepare_xy_tansyo(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series, List[str]]:
    if "win" not in df.columns:
        raise ValueError("column 'win' not found in dataset")
    df = df.copy()
    df = df[~df["win"].isna()].reset_index(drop=True)

    drop_set  = set(KEY_COLS + TARGET_COLS_COMMON)
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

def _train_eval_tansyo(X: pd.DataFrame, y: pd.Series) -> Tuple[Any, Dict[str, Any]]:
    metrics: Dict[str, Any] = {}
    strat_ok = y.nunique() > 1 and min((y==0).sum(), (y==1).sum()) >= 2

    if strat_ok and len(y) >= 20:
        Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    else:
        Xtr, ytr = X, y
        Xte = yte = None

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
        metrics.update({"accuracy": None, "roc_auc": None, "log_loss": None,
                        "n_train": int(len(y)), "n_test": 0})
    return clf, metrics


# ========= 決まり手（Multiclass） =========

def _prepare_xy_kimarite(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series, List[str]]:
    """
    決まり手は「勝った艇のみ」を学習データに使用（win==1 の行）。
    目的変数 y = decision（カテゴリ）。
    """
    if "win" not in df.columns or "decision" not in df.columns:
        raise ValueError("columns 'win' or 'decision' not found in dataset")

    df = df.copy()
    df = df[(df["win"] == 1) & df["decision"].notna()].reset_index(drop=True)
    if df.empty:
        raise ValueError("no rows for kimarite (filter win==1 & decision notna)")

    drop_set  = set(KEY_COLS + TARGET_COLS_COMMON)
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
    y = df["decision"].astype(str)

    return X, y, keep_cols

def _train_eval_kimarite(X: pd.DataFrame, y: pd.Series) -> Tuple[Any, Dict[str, Any]]:
    if not _LGBM_AVAILABLE:
        raise ImportError("lightgbm is not installed. Please install lightgbm for kimarite task.")

    metrics: Dict[str, Any] = {}
    labels = sorted(list(y.unique()))

    strat_ok = (len(labels) > 1)
    if strat_ok and len(y) >= max(50, len(labels) * 5):
        Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    else:
        Xtr, ytr = X, y
        Xte = yte = None

    clf = LGBMClassifier(
        n_estimators=600,
        learning_rate=0.05,
        max_depth=-1,
        num_leaves=63,
        objective="multiclass",
        class_weight="balanced",
        random_state=42,
        n_jobs=-1
    )
    clf.fit(Xtr, ytr)

    # 指標
    if Xte is not None:
        yp = clf.predict(Xte)
        # LightGBM は predict_proba も可（multi）
        metrics["accuracy"] = float(accuracy_score(yte, yp))
        metrics["f1_macro"] = float(f1_score(yte, yp, average="macro"))
        # クラス別F1
        rep = classification_report(yte, yp, output_dict=True, zero_division=0)
        f1_per_class = {lbl: float(rep.get(lbl, {}).get("f1-score", 0.0)) for lbl in labels}
        metrics["f1_per_class"] = f1_per_class
        metrics["n_train"] = int(len(ytr))
        metrics["n_test"]  = int(len(yte))
    else:
        metrics["accuracy"] = None
        metrics["f1_macro"] = None
        metrics["f1_per_class"] = {lbl: None for lbl in labels}
        metrics["n_train"]  = int(len(y))
        metrics["n_test"]   = 0

    return clf, metrics


# ========= タスク実行 =========

def _run_tansyo(dates_arg: str, date_tag: str, pid: str, race: str) -> None:
    dates = _parse_dates_input(dates_arg, DATA_BASE_TANSYO)
    df = _collect_frames(DATA_BASE_TANSYO, dates, pid, race)

    X, y, feats = _prepare_xy_tansyo(df)
    model, metrics = _train_eval_tansyo(X, y)

    pid_out  = pid if pid else "ALL"
    race_out = race if race else "ALL"
    out_dir  = os.path.join(MODEL_BASE_TANSYO, date_tag, pid_out, race_out)

    meta = {
        "task": "tansyo",
        "dates": dates,
        "model_date": date_tag,
        "pid": pid_out,
        "race": race_out,
        "rows": int(len(df)),
        "source": DATA_BASE_TANSYO
    }
    _save_bundle(model, feats, metrics, out_dir, meta)

def _run_kimarite(dates_arg: str, date_tag: str, pid: str, race: str) -> None:
    dates = _parse_dates_input(dates_arg, DATA_BASE_KIMARITE)
    df = _collect_frames(DATA_BASE_KIMARITE, dates, pid, race)

    X, y, feats = _prepare_xy_kimarite(df)
    model, metrics = _train_eval_kimarite(X, y)

    pid_out  = pid if pid else "ALL"
    race_out = race if race else "ALL"
    out_dir  = os.path.join(MODEL_BASE_KIMARITE, date_tag, pid_out, race_out)

    meta = {
        "task": "kimarite",
        "dates": dates,
        "model_date": date_tag,
        "pid": pid_out,
        "race": race_out,
        "rows": int(len(df)),
        "source": DATA_BASE_KIMARITE,
        "classes": sorted(list(y.unique()))
    }
    _save_bundle(model, feats, metrics, out_dir, meta)


# ========= main =========

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--task", choices=["tansyo", "kimarite", "both"], default="both",
                    help="学習タスク（既定: both）")
    ap.add_argument("--dates", default="ALL",
                    help="学習対象日: 'ALL' または カンマ区切り YYYYMMDD")
    ap.add_argument("--date",  default="",
                    help="モデル保存用タグ日付（空=自動: --dates 最大 or データセット最新）")
    ap.add_argument("--pid",   default="", help="場コード（空=ALL場）")
    ap.add_argument("--race",  default="", help="レース（空=ALL）")
    args = ap.parse_args()

    # モデル日付は「単勝」と「決まり手」で同じタグを使うため、単勝側のデータベースで自動決定を優先
    # （両方ALLのときでも差異は通常出ません）
    base_for_auto = DATA_BASE_TANSYO if args.task in ("tansyo", "both") else DATA_BASE_KIMARITE
    date_tag = args.date or _auto_model_date(args.dates, base_for_auto)

    print(f">>> task={args.task}  dates={args.dates}  -> model_date(tag)={date_tag}")
    print(f">>> pid={args.pid or 'ALL'}  race={args.race or 'ALL'}")

    if args.task in ("tansyo", "both"):
        _run_tansyo(args.dates, date_tag, args.pid, args.race)
    if args.task in ("kimarite", "both"):
        _run_kimarite(args.dates, date_tag, args.pid, args.race)


if __name__ == "__main__":
    main()
