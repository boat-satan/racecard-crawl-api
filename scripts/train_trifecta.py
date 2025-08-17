# scripts/train_trifecta.py

import os
import glob
import json
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
import joblib

# === データ読み込み ===
def load_data(data_dir="public/integrated/v1"):
    dfs = []
    for path in glob.glob(os.path.join(data_dir, "*/*/*.json")):
        with open(path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except Exception:
                continue
        # json → DataFrame
        df = pd.json_normalize(data.get("entries", []))
        if len(df) == 0:
            continue
        # レースキー付与
        meta = {
            "date": data.get("date"),
            "pid": data.get("pid"),
            "race": data.get("race"),
        }
        for k, v in meta.items():
            df[k] = v
        dfs.append(df)

    if not dfs:
        raise RuntimeError("データが見つかりませんでした")
    return pd.concat(dfs, ignore_index=True)


# === 特徴量生成 ===
def build_features(df):
    # 数値列だけ残す（stats, exhibitionなど全部使う）
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    # 欠損処理
    df[num_cols] = df[num_cols].fillna(0)

    X = df[num_cols]
    # 目的変数：仮に「的中」列を二値分類とする
    if "result.F" in df.columns and "result.S" in df.columns and "result.T" in df.columns:
        y = (df["lane"] == df["result.F"]) & (
            df["lane"].shift(-1) == df["result.S"]
        ) & (df["lane"].shift(-2) == df["result.T"])
        y = y.astype(int)
    else:
        y = np.zeros(len(df), dtype=int)

    return X, y


# === 学習処理 ===
def train_model(df, outdir="models"):
    X, y = build_features(df)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    clf = RandomForestClassifier(
        n_estimators=200,
        max_depth=12,
        random_state=42,
        n_jobs=-1,
    )
    clf.fit(X_train, y_train)

    score = clf.score(X_test, y_test)
    print(f"Validation accuracy: {score:.4f}")

    os.makedirs(outdir, exist_ok=True)
    joblib.dump(clf, os.path.join(outdir, "trifecta_model.pkl"))


# === 実行エントリ ===
if __name__ == "__main__":
    df = load_data()
    train_model(df)
