# TENKAI/train_models.py
# -*- coding: utf-8 -*-
"""
単勝(勝利確率) + 決まり手 の学習ランナー
入力:  TENKAI/datasets/v1/<date>/<pid>/(<race>_train.csv|all_train.csv)  ← 共通
出力:
  単勝     → TENKAI/models_tansyo/v1/<model_date>/<pid|ALL>/<race|ALL>/*
  決まり手 → TENKAI/models_kimarite/v1/<model_date>/<pid|ALL>/<race|ALL>/*
"""

from __future__ import annotations
import os, re, json, argparse
from typing import List, Tuple
import pandas as pd
import joblib

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score, log_loss, f1_score
from sklearn.preprocessing import LabelEncoder

# LightGBM
try:
    import lightgbm as lgb
except Exception:
    lgb = None

# ========= 共通設定 =========
DATA_BASE = os.path.join("TENKAI", "datasets", "v1")   # ← 共通に一本化
DATE_RE   = re.compile(r"^\d{8}$")

KEY_COLS    = ["date","pid","race","lane"]
TARGET_COLS = ["rank","win","st","decision"]

def _list_dates_under(base: str) -> List[str]:
    if not os.path.isdir(base): return []
    return sorted([d for d in os.listdir(base) if DATE_RE.match(d) and os.path.isdir(os.path.join(base, d))])

def _parse_dates_input(dates_arg: str, data_base: str) -> List[str]:
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
    if dates_arg and dates_arg != "ALL":
        lst = [x.strip() for x in dates_arg.split(",") if DATE_RE.match(x.strip())]
        if lst: return max(lst)
    dates = _list_dates_under(data_base)
    if not dates:
        raise FileNotFoundError(f"no datasets under {data_base} for auto model date")
    return dates[-1]

def _read_csv(p:str)->pd.DataFrame: return pd.read_csv(p)

def _iter_dataset_paths(date:str, pid:str, race:str) -> List[str]:
    fname = f"{race}_train.csv" if race else "all_train.csv"
    base = os.path.join(DATA_BASE, date)
    if not os.path.isdir(base): return []
    paths=[]
    if pid:
        p = os.path.join(base, pid, fname)
        if os.path.exists(p): paths.append(p)
    else:
        for pdir in sorted(os.listdir(base)):
            full = os.path.join(base, pdir, fname)
            if os.path.exists(full): paths.append(full)
    return paths

def _collect_frames(dates: List[str], pid: str, race: str) -> pd.DataFrame:
    dfs=[]
    for d in dates:
        for p in _iter_dataset_paths(d, pid, race):
            try: dfs.append(_read_csv(p))
            except: pass
    if not dfs:
        raise FileNotFoundError(f"no train csv found: dates={dates}, pid={pid or 'ALL'}, race={race or 'ALL'}")
    return pd.concat(dfs, ignore_index=True)

# ========= 単勝（RF） =========
def _prepare_xy_tansyo(df: pd.DataFrame):
    if "win" not in df.columns:
        raise ValueError("column 'win' not found")
    df = df[~df["win"].isna()].copy()
    drop = set(KEY_COLS + TARGET_COLS)
    feats = [c for c in df.columns if c not in drop]
    for c in feats: df[c] = pd.to_numeric(df[c], errors="coerce")
    keep=[]
    for c in feats:
        col=df[c]
        if col.notna().sum()==0: continue
        df[c]=col.fillna(col.median()); keep.append(c)
    X=df[keep]; y=df["win"].astype(int)
    return X,y,keep

def _train_rf(X,y):
    metrics={}
    strat_ok = y.nunique()>1 and min((y==0).sum(), (y==1).sum())>=2
    if strat_ok and len(y)>=20:
        Xtr,Xte,ytr,yte = train_test_split(X,y,test_size=0.2,random_state=42,stratify=y)
    else:
        Xtr,ytr = X,y; Xte=yte=None
    clf = RandomForestClassifier(
        n_estimators=300, max_depth=None, n_jobs=-1, random_state=42,
        class_weight="balanced_subsample"
    )
    clf.fit(Xtr,ytr)
    if Xte is not None:
        yp = clf.predict(Xte); prob = clf.predict_proba(Xte)[:,1]
        metrics["accuracy"]=float(accuracy_score(yte,yp))
        try: metrics["roc_auc"]=float(roc_auc_score(yte,prob))
        except: metrics["roc_auc"]=None
        try: metrics["log_loss"]=float(log_loss(yte,prob,labels=[0,1]))
        except: metrics["log_loss"]=None
        metrics["n_train"]=int(len(ytr)); metrics["n_test"]=int(len(yte))
    else:
        metrics.update({"accuracy":None,"roc_auc":None,"log_loss":None,"n_train":int(len(y)),"n_test":0})
    return clf,metrics

def _run_tansyo(dates_arg:str, date_tag:str, pid:str, race:str):
    dates = _parse_dates_input(dates_arg, DATA_BASE)
    df = _collect_frames(dates, pid, race)
    X,y,feats = _prepare_xy_tansyo(df)
    model,metrics = _train_rf(X,y)
    out_base = os.path.join("TENKAI","models_tansyo","v1",date_tag, pid or "ALL", race or "ALL")
    os.makedirs(out_base, exist_ok=True)
    joblib.dump(model, os.path.join(out_base,"model.pkl"))
    with open(os.path.join(out_base,"features.json"),"w",encoding="utf-8") as f:
        json.dump({"features":feats},f,ensure_ascii=False,indent=2)
    with open(os.path.join(out_base,"metrics.json"),"w",encoding="utf-8") as f:
        json.dump(metrics,f,ensure_ascii=False,indent=2)
    with open(os.path.join(out_base,"meta.json"),"w",encoding="utf-8") as f:
        json.dump({"dates":dates,"model_date":date_tag,"pid":pid or "ALL","race":race or "ALL",
                   "rows":int(len(df)),"source":"TENKAI/datasets/v1"},f,ensure_ascii=False,indent=2)
    print("saved(tansyo):", out_base)

# ========= 決まり手（LightGBM） =========
def _prepare_xy_kimarite(df: pd.DataFrame):
    # 勝った艇だけで教師化（decision が勝者の決まり手）
    df = df.copy()
    df = df[(df["win"]==1) & (~df["decision"].isna())]
    if df.empty:
        raise ValueError("no rows for kimarite (win==1 with decision)")
    drop = set(KEY_COLS + TARGET_COLS)
    feats = [c for c in df.columns if c not in drop]
    for c in feats: df[c] = pd.to_numeric(df[c], errors="coerce")
    keep=[]
    for c in feats:
        col=df[c]
        if col.notna().sum()==0: continue
        df[c]=col.fillna(col.median()); keep.append(c)
    X=df[keep]
    le=LabelEncoder()
    y=le.fit_transform(df["decision"].astype(str))
    classes = list(le.classes_)
    return X,y,keep,classes,le

def _train_lgbm(X,y):
    if lgb is None:
        raise RuntimeError("lightgbm is not installed")
    metrics={}
    if len(y)>=20 and len(set(y))>1:
        Xtr,Xte,ytr,yte = train_test_split(X,y,test_size=0.2,random_state=42,stratify=y)
    else:
        Xtr,ytr = X,y; Xte=yte=None
    params = dict(objective="multiclass", num_class=len(set(y)), learning_rate=0.05,
                  n_estimators=400, subsample=0.8, colsample_bytree=0.8, random_state=42)
    clf = lgb.LGBMClassifier(**params)
    clf.fit(Xtr,ytr)
    if Xte is not None:
        yp = clf.predict(Xte)
        metrics["accuracy"]=float(accuracy_score(yte,yp))
        metrics["f1_macro"]=float(f1_score(yte,yp,average="macro"))
        metrics["n_train"]=int(len(ytr)); metrics["n_test"]=int(len(yte))
    else:
        metrics.update({"accuracy":None,"f1_macro":None,"n_train":int(len(y)),"n_test":0})
    return clf,metrics

def _run_kimarite(dates_arg:str, date_tag:str, pid:str, race:str):
    dates = _parse_dates_input(dates_arg, DATA_BASE)  # ← 共通
    df = _collect_frames(dates, pid, race)
    X,y,feats,classes,le = _prepare_xy_kimarite(df)
    model,metrics = _train_lgbm(X,y)

    out_base = os.path.join("TENKAI","models_kimarite","v1",date_tag, pid or "ALL", race or "ALL")
    os.makedirs(out_base, exist_ok=True)
    joblib.dump({"model":model,"label_encoder":le}, os.path.join(out_base,"model.pkl"))
    with open(os.path.join(out_base,"features.json"),"w",encoding="utf-8") as f:
        json.dump({"features":feats, "classes":classes},f,ensure_ascii=False,indent=2)
    with open(os.path.join(out_base,"metrics.json"),"w",encoding="utf-8") as f:
        json.dump(metrics,f,ensure_ascii=False,indent=2)
    with open(os.path.join(out_base,"meta.json"),"w",encoding="utf-8") as f:
        json.dump({"dates":dates,"model_date":date_tag,"pid":pid or "ALL","race":race or "ALL",
                   "rows":int(len(df)),"source":"TENKAI/datasets/v1"},f,ensure_ascii=False,indent=2)
    print("saved(kimarite):", out_base)

# ========= CLI =========
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default="ALL", help="学習対象日: 'ALL' or YYYYMMDD,YYYYMMDD")
    ap.add_argument("--date",  default="",   help="モデル保存タグ日付（空=自動）")
    ap.add_argument("--pid",   default="",   help="場コード（空=ALL場）")
    ap.add_argument("--race",  default="",   help="レース（空=ALL）")
    ap.add_argument("--mode",  choices=["both","tansyo","kimarite"], default="both")
    args = ap.parse_args()

    date_tag = args.date or _auto_model_date(args.dates, DATA_BASE)
    print(f">>> dates={args.dates} tag={date_tag} pid={args.pid or 'ALL'} race={args.race or 'ALL'} mode={args.mode}")

    if args.mode in ("both","tansyo"):
        _run_tansyo(args.dates, date_tag, args.pid, args.race)
    if args.mode in ("both","kimarite"):
        _run_kimarite(args.dates, date_tag, args.pid, args.race)

if __name__ == "__main__":
    main()
