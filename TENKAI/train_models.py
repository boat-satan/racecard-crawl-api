# TENKAI/train_models.py
# -*- coding: utf-8 -*-
"""
単勝(勝利確率) & 決まり手モデル 学習スクリプト（ALL対応・学習日自動決定）
入力(共通データセット): TENKAI/datasets/v1/<date>/<pid>/(<race>_train.csv|all_train.csv)
出力:
  単勝:     TENKAI/models_tansyo/v1/<model_date>/<pid|ALL>/<race|ALL>/{model.pkl,features.json,metrics.json,meta.json}
  決まり手: TENKAI/models_kimarite/v1/<model_date>/<pid|ALL>/<race|ALL>/{model.pkl,features.json,metrics.json,meta.json}

引数:
- --tasks  : 'tansyo', 'kimarite', 'both'（既定=both）
- --dates  : 学習対象日。'ALL' または 'YYYYMMDD,YYYYMMDD,...'（既定=ALL）
- --date   : モデル保存タグ日付。未指定なら --dates 最大 or datasets/v1 の最新日
- --pid    : 場コード（空=ALL場）
- --race   : レース（空=ALL）

依存:
  pip install pandas scikit-learn joblib lightgbm
"""

from __future__ import annotations
import os, re, json, argparse
from typing import List, Tuple, Dict, Any, Optional

import pandas as pd
import joblib

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score, log_loss, f1_score, classification_report
from sklearn.ensemble import RandomForestClassifier

try:
    import lightgbm as lgb
except Exception:
    lgb = None  # LightGBM未インストールでも単勝だけは動作

# -------------------------
# パス定義
# -------------------------
DATA_BASE       = os.path.join("TENKAI", "datasets", "v1")
MODEL_BASE_TAN  = os.path.join("TENKAI", "models_tansyo",   "v1")
MODEL_BASE_KIM  = os.path.join("TENKAI", "models_kimarite", "v1")
DATE_RE = re.compile(r"^\d{8}$")

KEY_COLS    = ["date","pid","race","lane"]
TARGET_COLS = ["rank","win","st","decision"]  # 特徴量からは除外

# 決まり手のクラス並び（固定）
KIM_CLASSES = ["逃げ","差し","まくり","まくり差し","抜き","恵まれ"]
KIM_TO_ID   = {k:i for i,k in enumerate(KIM_CLASSES)}

# -------------------------
# ユーティリティ
# -------------------------
def _list_dates_under(base: str) -> List[str]:
    if not os.path.isdir(base):
        return []
    return sorted([d for d in os.listdir(base)
                   if DATE_RE.match(d) and os.path.isdir(os.path.join(base, d))])

def _parse_dates_input(dates_arg: str) -> List[str]:
    if not dates_arg or dates_arg == "ALL":
        dates = _list_dates_under(DATA_BASE)
        if not dates:
            raise FileNotFoundError(f"no datasets under {DATA_BASE}")
        return dates
    items = [x.strip() for x in dates_arg.split(",") if DATE_RE.match(x.strip())]
    if not items:
        raise ValueError(f"invalid --dates: {dates_arg}")
    return sorted(set(items))

def _auto_model_date(dates_arg: Optional[str]) -> str:
    if dates_arg and dates_arg != "ALL":
        lst = [x.strip() for x in dates_arg.split(",") if DATE_RE.match(x.strip())]
        if lst:
            return max(lst)
    dates = _list_dates_under(DATA_BASE)
    if not dates:
        raise FileNotFoundError(f"no datasets under {DATA_BASE} for auto model date")
    return dates[-1]

def _read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def _iter_dataset_paths(date: str, pid: str, race: str) -> List[str]:
    filename = f"{race}_train.csv" if race else "all_train.csv"
    base = os.path.join(DATA_BASE, date)
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

def _collect_frames(dates: List[str], pid: str, race: str) -> pd.DataFrame:
    dfs = []
    for d in dates:
        for p in _iter_dataset_paths(d, pid, race):
            try:
                dfs.append(_read_csv(p))
            except Exception:
                pass
    if not dfs:
        raise FileNotFoundError(f"no train csv found: dates={dates}, pid={pid or 'ALL'}, race={race or 'ALL'}")
    return pd.concat(dfs, ignore_index=True)

# -------------------------
# 前処理
# -------------------------
def _feature_matrix(df: pd.DataFrame, drop_cols: List[str]) -> Tuple[pd.DataFrame, List[str]]:
    df = df.copy()
    # 説明変数候補
    feat_cols = [c for c in df.columns if c not in set(drop_cols)]
    # 数値化
    for c in feat_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    # 欠損補完（列中央値、全欠損は落とす）
    keep: List[str] = []
    for c in feat_cols:
        col = df[c]
        if col.notna().sum() == 0:
            continue
        df[c] = col.fillna(col.median())
        keep.append(c)
    return df[keep], keep

# -------------------------
# 単勝モデル（2値）
# -------------------------
def train_tansyo(df_all: pd.DataFrame) -> Tuple[Any, Dict[str, Any], List[str]]:
    # 目的変数：win（欠損行は除外）
    df = df_all[~df_all["win"].isna()].copy().reset_index(drop=True)
    if df.empty:
        raise ValueError("no rows for tansyo training (all win are NaN)")
    df["win"] = df["win"].astype(int)

    X, feat_cols = _feature_matrix(df, drop_cols=KEY_COLS + TARGET_COLS)
    y = df["win"]

    # データ分割（片寄り考慮）
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

    metrics: Dict[str, Any] = {}
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

    return clf, metrics, feat_cols

# -------------------------
# 決まり手モデル（多クラス）
# -------------------------
def _prepare_kimarite_df(df_all: pd.DataFrame) -> pd.DataFrame:
    """勝ち艇のみ + decision をクラスIDへ"""
    df = df_all.copy()
    df = df[(df["win"] == 1) & (~df["decision"].isna())].copy()
    if df.empty:
        raise ValueError("no rows for kimarite training (need win==1 with decision)")
    # 文字列化
    df["decision"] = df["decision"].astype(str)
    # 許容クラスに限定
    df = df[df["decision"].isin(KIM_CLASSES)].copy()
    if df.empty:
        raise ValueError("no rows for kimarite training after filtering decision classes")
    df["y"] = df["decision"].map(KIM_TO_ID)
    return df.reset_index(drop=True)

def train_kimarite(df_all: pd.DataFrame) -> Tuple[Any, Dict[str, Any], List[str]]:
    if lgb is None:
        raise ImportError("lightgbm is not installed. Please `pip install lightgbm`.")

    df = _prepare_kimarite_df(df_all)

    # 特徴量（decision/その他ターゲット/キーを除外）
    drop_cols = KEY_COLS + TARGET_COLS + ["y"]
    X, feat_cols = _feature_matrix(df, drop_cols=drop_cols)
    y = df["y"].astype(int)

    # 分割
    strat_ok = y.nunique() > 1 and min(y.value_counts().min(), 2) >= 2
    if strat_ok and len(y) >= 60:
        Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    else:
        Xtr, ytr = X, y
        Xte = yte = None

    clf = lgb.LGBMClassifier(
        objective="multiclass",
        num_class=len(KIM_CLASSES),
        n_estimators=500,
        learning_rate=0.05,
        max_depth=-1,
        subsample=0.9,
        colsample_bytree=0.9,
        random_state=42,
        n_jobs=-1
    )
    clf.fit(Xtr, ytr)

    metrics: Dict[str, Any] = {}
    if Xte is not None:
        yp = clf.predict(Xte)
        metrics["accuracy"] = float(accuracy_score(yte, yp))
        # F1（macro）とクラス別F1
        metrics["f1_macro"] = float(f1_score(yte, yp, average="macro"))
        # クラス別F1をラベル名で
        f1_each = f1_score(yte, yp, average=None, labels=list(range(len(KIM_CLASSES))))
        metrics["f1_per_class"] = {KIM_CLASSES[i]: float(f1_each[i]) for i in range(len(KIM_CLASSES))}
        metrics["n_train"] = int(len(ytr))
        metrics["n_test"]  = int(len(yte))
    else:
        metrics.update({
            "accuracy": None,
            "f1_macro": None,
            "f1_per_class": {k: None for k in KIM_CLASSES},
            "n_train": int(len(y)),
            "n_test": 0
        })

    return clf, metrics, feat_cols

# -------------------------
# 保存
# -------------------------
def _save_artifacts(model, metrics: Dict[str, Any], feat_cols: List[str],
                    out_root: str, date_tag: str, pid_out: str, race_out: str,
                    dates_used: List[str], source_tag: str):
    out_dir = os.path.join(out_root, date_tag, pid_out, race_out)
    os.makedirs(out_dir, exist_ok=True)

    joblib.dump(model, os.path.join(out_dir, "model.pkl"))
    with open(os.path.join(out_dir, "features.json"), "w", encoding="utf-8") as f:
        json.dump({"features": feat_cols}, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump({
            "dates": dates_used,
            "model_date": date_tag,
            "pid": pid_out,
            "race": race_out,
            "source": source_tag
        }, f, ensure_ascii=False, indent=2)
    print("saved:", out_dir)

# -------------------------
# メイン
# -------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tasks", choices=["tansyo", "kimarite", "both"], default="both",
                    help="学習タスク（既定: both）")
    ap.add_argument("--dates", default="ALL",
                    help="学習対象日: 'ALL' または カンマ区切り YYYYMMDD")
    ap.add_argument("--date",  default="",
                    help="モデル保存用タグ日付（空=自動: --dates最大 or datasets最新）")
    ap.add_argument("--pid",   default="", help="場コード（空=ALL場）")
    ap.add_argument("--race",  default="", help="レース（空=ALL）")
    args = ap.parse_args()

    date_tag = args.date or _auto_model_date(args.dates)
    dates = _parse_dates_input(args.dates)
    pid_out  = args.pid if args.pid else "ALL"
    race_out = args.race if args.race else "ALL"

    print(f">>> tasks={args.tasks}  dates={dates}  model_date={date_tag}  pid={pid_out}  race={race_out}")

    # 共通データ読み込み（単一読込を両タスクで共有）
    df_all = _collect_frames(dates, args.pid, args.race)

    # 単勝
    if args.tasks in ("tansyo", "both"):
        model_t, metrics_t, feats_t = train_tansyo(df_all)
        _save_artifacts(
            model_t, metrics_t, feats_t,
            MODEL_BASE_TAN, date_tag, pid_out, race_out,
            dates, "TENKAI/datasets/v1"
        )

    # 決まり手
    if args.tasks in ("kimarite", "both"):
        if lgb is None:
            raise ImportError("lightgbm is required for kimarite task. `pip install lightgbm`")
        model_k, metrics_k, feats_k = train_kimarite(df_all)
        _save_artifacts(
            model_k, metrics_k, feats_k,
            MODEL_BASE_KIM, date_tag, pid_out, race_out,
            dates, "TENKAI/datasets/v1"
        )

if __name__ == "__main__":
    main()
