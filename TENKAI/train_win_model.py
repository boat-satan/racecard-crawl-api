# -*- coding: utf-8 -*-
"""
学習用テーブル（build_training_table.pyの出力）から
「1着=win(1/0)」の分類モデルを学習して保存するベースライン。

入力:
  TENKAI/datasets/v1/{date}/{pid}/{race}_train.csv  … race指定時
  TENKAI/datasets/v1/{date}/{pid}/all_train.csv     … race未指定時

出力(モデル/指標/予測):
  TENKAI/models/v1/{date}/{pid}/{race or ALL}/
    - model.joblib               … 学習済みモデル（RandomForest）
    - features.json              … 学習に使った特徴名
    - metrics.json               … CVスコア
    - oof_predictions.csv        … OOF予測 (date,pid,race,lane,win,pred)
    - feature_importance.csv     … 特徴重要度（簡易）
"""
from __future__ import annotations

import os, json, argparse
import numpy as np
import pandas as pd
from typing import List
from sklearn.model_selection import GroupKFold
from sklearn.metrics import roc_auc_score, accuracy_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
import joblib


DATA_BASE = os.path.join("TENKAI", "datasets", "v1")
MODEL_BASE = os.path.join("TENKAI", "models", "v1")


def _load_dataset(date: str, pid: str, race: str) -> pd.DataFrame:
    indir = os.path.join(DATA_BASE, date, pid)
    path = os.path.join(indir, f"{race}_train.csv") if race else os.path.join(indir, "all_train.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    df = pd.read_csv(path)
    # 型を軽く整える
    # 目的変数とキー
    for c in ("lane", "rank", "win"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _pick_features(df: pd.DataFrame) -> List[str]:
    # 予測時に使えない事後情報は除外
    drop_cols = {"date","pid","race","lane","rank","win","st","decision"}
    cols = [c for c in df.columns if c not in drop_cols]
    return cols


def train_win(date: str, pid: str, race: str=""):
    df = _load_dataset(date, pid, race)
    if df["win"].isna().any():
        # 事故などでwinがNaNの行が混ざっていれば学習対象から外す
        df = df[~df["win"].isna()].reset_index(drop=True)

    feature_cols = _pick_features(df)
    X = df[feature_cols].copy()
    y = df["win"].astype(int)
    groups = df["race"].astype(str)  # レース単位で分割（リーク対策）

    # 列型ごとに前処理を定義
    num_cols = [c for c in feature_cols if pd.api.types.is_numeric_dtype(X[c])]
    cat_cols = [c for c in feature_cols if c not in num_cols]

    pre = ColumnTransformer(
        transformers=[
            ("num", SimpleImputer(strategy="median"), num_cols),
            ("cat", Pipeline(steps=[
                ("imp", SimpleImputer(strategy="most_frequent")),
                ("ohe", OneHotEncoder(handle_unknown="ignore"))
            ]), cat_cols),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )

    model = RandomForestClassifier(
        n_estimators=400,
        max_depth=None,
        random_state=42,
        n_jobs=-1,
        class_weight="balanced",   # 勝ちの少数バランスを少し緩和
    )

    pipe = Pipeline(steps=[("pre", pre), ("clf", model)])

    # GroupKFold で OOF 評価
    gkf = GroupKFold(n_splits=min(5, max(2, groups.nunique())))
    oof_pred = np.zeros(len(df))
    aucs, accs = [], []

    for tr_idx, va_idx in gkf.split(X, y, groups):
        X_tr, X_va = X.iloc[tr_idx], X.iloc[va_idx]
        y_tr, y_va = y.iloc[tr_idx], y.iloc[va_idx]

        pipe.fit(X_tr, y_tr)
        proba = pipe.predict_proba(X_va)[:, 1]
        pred = (proba >= 0.5).astype(int)

        # 評価
        try:
            aucs.append(roc_auc_score(y_va, proba))
        except ValueError:
            # 片方しかないfoldなど
            aucs.append(float("nan"))
        accs.append(accuracy_score(y_va, pred))
        oof_pred[va_idx] = proba

    metrics = {
        "cv_auc_mean": float(np.nanmean(aucs)),
        "cv_auc_each": [None if np.isnan(v) else float(v) for v in aucs],
        "cv_acc_mean": float(np.mean(accs)),
        "cv_acc_each": [float(v) for v in accs],
        "n_rows": int(len(df)),
        "n_features": int(len(feature_cols)),
    }

    # 全データで学習して最終モデル保存
    pipe.fit(X, y)

    out_dir = os.path.join(MODEL_BASE, date, pid, race if race else "ALL")
    os.makedirs(out_dir, exist_ok=True)

    joblib.dump(pipe, os.path.join(out_dir, "model.joblib"))
    with open(os.path.join(out_dir, "features.json"), "w", encoding="utf-8") as f:
        json.dump(feature_cols, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    # OOF 予測を書き出し（あとで閾値検討などに使える）
    oof = df[["date","pid","race","lane","win"]].copy()
    oof["pred"] = oof_pred
    oof.to_csv(os.path.join(out_dir, "oof_predictions.csv"), index=False, encoding="utf-8")

    # 簡易の特徴重要度（RFのmean decrease in impurity）
    # OneHot後の列名を取り出す
    try:
        feat_names = pipe.named_steps["pre"].get_feature_names_out()
        importances = pipe.named_steps["clf"].feature_importances_
        imp_df = pd.DataFrame({"feature": feat_names, "importance": importances}).sort_values("importance", ascending=False)
        imp_df.to_csv(os.path.join(out_dir, "feature_importance.csv"), index=False, encoding="utf-8")
    except Exception:
        pass

    print("saved model to", out_dir)
    print("metrics:", metrics)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--pid", required=True)
    ap.add_argument("--race", default="", help="例: 1R。空なら all_train.csv")
    args = ap.parse_args()
    train_win(args.date, args.pid, args.race)


if __name__ == "__main__":
    main()
