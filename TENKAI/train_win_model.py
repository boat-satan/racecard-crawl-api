# -*- coding: utf-8 -*-
"""
学習スクリプト（勝利確率予測 / ALL対応・学習日自動決定）
入力: TENKAI/datasets/v1/<date>/<pid>/(<race>_train.csv|all_train.csv)
出力: TENKAI/models/v1/<model_date>/<pid|ALL>/<race|ALL>/{model.pkl,features.json,metrics.json,meta.json}

- --dates:  学習に使う対象日。'ALL' または 'YYYYMMDD,YYYYMMDD,...'
- --date :  モデル保存用タグ日付。未指定なら --dates から最大日、
            もしくは datasets/v1 配下の最新日を自動採用。
- --pid  :  空なら全場（ALL）。指定時はその場のみ。
- --race :  空なら all_train.csv、指定時は <race>_train.csv を使用。
"""

from __future__ import annotations
import os, re, json, argparse
from typing import List, Tuple
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score, log_loss
import joblib

DATA_BASE  = os.path.join("TENKAI", "datasets", "v1")
MODEL_BASE = os.path.join("TENKAI", "models",   "v1")
DATE_RE = re.compile(r"^\d{8}$")

KEY_COLS    = ["date","pid","race","lane"]
TARGET_COLS = ["rank","win","st","decision"]  # st/decision は今回は使わない

# -------------------------
# ユーティリティ
# -------------------------

def _list_dates_under(base: str) -> List[str]:
    if not os.path.isdir(base):
        return []
    return sorted([d for d in os.listdir(base)
                   if DATE_RE.match(d) and os.path.isdir(os.path.join(base, d))])

def _parse_dates_input(dates_arg: str) -> List[str]:
    """
    'ALL' -> DATA_BASE配下の全日付
    '20250801,20250802' -> その集合
    空/不正 -> 例外
    """
    if not dates_arg or dates_arg == "ALL":
        dates = _list_dates_under(DATA_BASE)
        if not dates:
            raise FileNotFoundError(f"no datasets under {DATA_BASE}")
        return dates
    items = [x.strip() for x in dates_arg.split(",") if DATE_RE.match(x.strip())]
    if not items:
        raise ValueError(f"invalid --dates: {dates_arg}")
    return sorted(set(items))

def _auto_model_date(dates_arg: str | None) -> str:
    """--date 未指定時の自動決定"""
    if dates_arg and dates_arg != "ALL":
        # 明示列から最大
        lst = [x.strip() for x in dates_arg.split(",") if DATE_RE.match(x.strip())]
        if lst:
            return max(lst)
    # datasets配下の最新
    dates = _list_dates_under(DATA_BASE)
    if not dates:
        raise FileNotFoundError(f"no datasets under {DATA_BASE} for auto model date")
    return dates[-1]

def _read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def _iter_dataset_paths(date: str, pid: str, race: str) -> List[str]:
    """
    指定dateでの対象CSVパス群を返す。
    - pid="" -> 全場の同名CSVを集める
    - race="" -> all_train.csv、else <race>_train.csv
    """
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
    """複数日×(ALL場/特定場)のCSVを縦結合"""
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
# 前処理 & 学習
# -------------------------

def _prepare_xy(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series, List[str]]:
    if "win" not in df.columns:
        raise ValueError("column 'win' not found in dataset")
    df = df.copy()
    df = df[~df["win"].isna()].reset_index(drop=True)

    drop_set  = set(KEY_COLS + TARGET_COLS)
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
    metrics = {}
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

# -------------------------
# メイン
# -------------------------

def train_win(dates_arg: str, date_tag: str, pid: str, race: str):
    dates = _parse_dates_input(dates_arg)
    df = _collect_frames(dates, pid, race)

    X, y, feats = _prepare_xy(df)
    model, metrics = _train_eval(X, y)

    pid_out  = pid if pid else "ALL"
    race_out = race if race else "ALL"
    out_dir  = os.path.join(MODEL_BASE, date_tag, pid_out, race_out)
    os.makedirs(out_dir, exist_ok=True)

    joblib.dump(model, os.path.join(out_dir, "model.pkl"))
    with open(os.path.join(out_dir, "features.json"), "w", encoding="utf-8") as f:
        json.dump({"features": feats}, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump({
            "dates": dates,
            "model_date": date_tag,
            "pid": pid_out,
            "race": race_out,
            "rows": int(len(df)),
            "source": "TENKAI/datasets/v1"
        }, f, ensure_ascii=False, indent=2)
    print("saved:", out_dir)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default="ALL",
                    help="学習対象日: 'ALL' または カンマ区切り YYYYMMDD")
    ap.add_argument("--date",  default="",
                    help="モデル保存用タグ日付（空=自動: --dates最大 or datasets最新）")
    ap.add_argument("--pid",   default="", help="場コード（空=ALL場）")
    ap.add_argument("--race",  default="", help="レース（空=ALL）")
    args = ap.parse_args()

    date_tag = args.date or _auto_model_date(args.dates)
    print(f">>> dates: {args.dates}  -> resolved list")
    print(f">>> model_date(tag): {date_tag}")
    print(f">>> pid: {args.pid or 'ALL'}  race: {args.race or 'ALL'}")

    train_win(args.dates, date_tag, args.pid, args.race)

if __name__ == "__main__":
    main()
