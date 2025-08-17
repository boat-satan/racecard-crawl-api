# scripts/train_trifecta.py
# 3連単: 統合JSON(integrated) + オッズ(odds) + 結果(results) を直読み
# → 1レース×全組合せの学習データを構築 → LightGBMで学習 → EVバックテスト
# 変更点: stats.entryCourse 以下の実用フィールドを広くフラット展開して特徴量化
# 注意: pip 依存に lightgbm, scikit-learn, pandas, pyarrow が必要

import os, json, glob, re, itertools
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from lightgbm import LGBMClassifier

BASE = "public"
INTEG = os.path.join(BASE, "integrated", "v1")
ODDS  = os.path.join(BASE, "odds",       "v1")
RES   = os.path.join(BASE, "results",    "v1")

# ---------- helpers ----------

def to_float(x):
    if x is None: return None
    s = str(x).strip().replace("kg","")
    s = s.lstrip("F")  # F.02 -> .02
    s = re.sub(r"[^\d\.\-]", "", s)
    try:
        return float(s) if s else None
    except:
        return None

def ex_flag(st_raw):
    return 1 if (st_raw and str(st_raw).strip().startswith("F")) else 0

def safe_load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)

def rate(n, d):
    try:
        n = float(n); d = float(d)
        if d <= 0: return None
        return n / d * 100.0
    except:
        return None

# ---- stats flattener ----
def flatten_entry_course(stats_entry_course: dict) -> dict:
    """stats.entryCourse をできる範囲でフラットに落とす"""
    out = {}
    if not stats_entry_course:
        return out

    # 直下
    out["course"] = stats_entry_course.get("course")
    out["entry_avgST"] = to_float(stats_entry_course.get("avgST"))

    # selfSummary
    ss = stats_entry_course.get("selfSummary") or {}
    starts = ss.get("starts")
    first  = ss.get("firstCount")
    second = ss.get("secondCount")
    third  = ss.get("thirdCount")
    out["self_starts"] = starts
    out["self_first"]  = first
    out["self_second"] = second
    out["self_third"]  = third
    out["self_top1Rate_calc"] = rate(first, starts)
    out["self_top2Rate_calc"] = rate((first or 0)+(second or 0), starts)
    out["self_top3Rate_calc"] = rate((first or 0)+(second or 0)+(third or 0), starts)

    # win/lose 決まり手（動的キー）
    for k, v in (stats_entry_course.get("winKimariteSelf") or {}).items():
        out[f"win_{k}"] = v
    for k, v in (stats_entry_course.get("loseKimarite") or {}).items():
        out[f"lose_{k}"] = v

    # matrixSelf
    ms = stats_entry_course.get("matrixSelf") or {}
    out["matrix_winRate"]  = to_float(ms.get("winRate"))
    out["matrix_top2Rate"] = to_float(ms.get("top2Rate"))
    out["matrix_top3Rate"] = to_float(ms.get("top3Rate"))

    # exTimeRank: rank毎に率を展開
    for item in (stats_entry_course.get("exTimeRank") or []):
        r = item.get("rank")
        if r is None: 
            continue
        out[f"exRank{r}_winRate"]  = to_float(item.get("winRate"))
        out[f"exRank{r}_top2Rate"] = to_float(item.get("top2Rate"))
        out[f"exRank{r}_top3Rate"] = to_float(item.get("top3Rate"))
    return out

def lane_feats(entry):
    """entries[i] → その艇の特徴（racecard/exhibition/stats.entryCourse）"""
    rc = entry.get("racecard", {}) or {}
    ex = entry.get("exhibition", {}) or {}
    ec = (entry.get("stats") or {}).get("entryCourse") or {}

    feat = {
        # racecard
        "num": rc.get("number"),
        "classNumber": rc.get("classNumber"),
        "age": rc.get("age"),
        "weight": to_float(rc.get("weight")),
        "avgST": to_float(rc.get("avgST")),
        "flying": rc.get("flyingCount"),
        "late": rc.get("lateCount"),
        "motorTop2": to_float(rc.get("motorTop2")),
        "motorTop3": to_float(rc.get("motorTop3")),
        "boatTop2":  to_float(rc.get("boatTop2")),
        "boatTop3":  to_float(rc.get("boatTop3")),
        "natTop1":   to_float(rc.get("natTop1")),
        "natTop2":   to_float(rc.get("natTop2")),
        "natTop3":   to_float(rc.get("natTop3")),
        "locTop1":   to_float(rc.get("locTop1")),
        "locTop2":   to_float(rc.get("locTop2")),
        "locTop3":   to_float(rc.get("locTop3")),
        # exhibition
        "tenji": to_float(ex.get("tenjiTime")),
        "exST":  to_float(ex.get("st")),
        "exF":   ex_flag(ex.get("st")),
    }
    feat.update(flatten_entry_course(ec))
    return feat

def build_rows_for_race(integ, odds, result):
    """1レース → オッズに載っている全組み合わせ行を生成"""
    date = integ["date"]; jcd = integ["pid"]; race = integ["race"]

    w = integ.get("weather") or {}
    global_cols = dict(
        date=date, jcd=jcd, race=race,
        weather=w.get("weather"),
        temperature=to_float(w.get("temperature")),
        windSpeed=to_float(w.get("windSpeed")),
        windDir=str(w.get("windDirection")) if w.get("windDirection") is not None else None,
        waterTemperature=to_float(w.get("waterTemperature")),
        waveHeight=to_float(w.get("waveHeight")),
    )

    lane_map = {}
    for e in integ.get("entries", []):
        lane_map[int(e["lane"])] = lane_feats(e)

    payouts = (result or {}).get("payouts") or {}
    tri = payouts.get("trifecta") or {}
    hit_combo = tri.get("combo")  # e.g. "2-5-4"

    trifecta_list = (odds or {}).get("trifecta") or []
    if len(trifecta_list) == 0:
        from itertools import permutations
        combos = ["-".join(map(str, p)) for p in permutations([1,2,3,4,5,6], 3)]
        trifecta_list = [{"combo": c} for c in combos]

    rows = []
    for item in trifecta_list:
        combo = item.get("combo")
        if not combo: 
            continue
        try:
            F, S, T = map(int, [item.get("F"), item.get("S"), item.get("T")] if item.get("F") is not None else combo.split("-"))
        except:
            continue

        def role_prefix(role, lane):
            lf = lane_map.get(lane, {})
            return {f"{role}_{k}": v for k,v in lf.items()}

        row = {}
        row.update(global_cols)
        row.update({"combo": combo, "F": F, "S": S, "T": T})
        row.update(role_prefix("F", F))
        row.update(role_prefix("S", S))
        row.update(role_prefix("T", T))
        row["is_win"] = 1 if (hit_combo and combo == hit_combo) else 0
        row["odds"] = to_float(item.get("odds"))
        row["popularity_rank"] = item.get("popularityRank")
        rows.append(row)

    return rows

def load_dataset(date_glob="*", jcd_glob="*"):
    records = []
    for date_dir in sorted(glob.glob(os.path.join(INTEG, date_glob))):
        date = os.path.basename(date_dir)
        for jcd_dir in sorted(glob.glob(os.path.join(date_dir, jcd_glob))):
            jcd = os.path.basename(jcd_dir)
            for integ_path in sorted(glob.glob(os.path.join(jcd_dir, "*.json"))):
                race_file = os.path.basename(integ_path)
                odds_path = os.path.join(ODDS, date, jcd, race_file)
                res_path  = os.path.join(RES,  date, jcd, race_file)
                if not os.path.exists(odds_path) or not os.path.exists(res_path):
                    continue
                try:
                    integ = safe_load(integ_path)
                    odds  = safe_load(odds_path)
                    result= safe_load(res_path)
                except Exception:
                    continue
                records.extend(build_rows_for_race(integ, odds, result))
    return pd.DataFrame(records)

# ---------- training & backtest ----------

def main():
    df = load_dataset("*", "*")
    if df.empty:
        print("No data found.")
        return

    # 特徴量: 数値カラムのみ採用（object除外）。ターゲット/識別系は除く
    drop_cols = {"combo","is_win","odds","popularity_rank","date","jcd","race","weather","windDir"}
    feature_cols = [c for c in df.columns if c not in drop_cols and df[c].dtype != "object"]

    X_all = df[feature_cols].copy().fillna(0.0).astype(float)
    y_all = df["is_win"].astype(int)

    # レース単位で分割（リーク防止）
    df["race_key"] = df["date"].astype(str) + "-" + df["jcd"].astype(str) + "-" + df["race"]
    races = df["race_key"].unique()
    train_races, test_races = train_test_split(races, test_size=0.2, random_state=42, shuffle=True)
    trn = df["race_key"].isin(train_races)
    tst = df["race_key"].isin(test_races)

    X_tr, y_tr = X_all[trn], y_all[trn]
    X_te, y_te = X_all[tst], y_all[tst]
    df_te = df[tst].copy()

    # 不均衡対応（120点に1点当たり想定）
    clf = LGBMClassifier(
        n_estimators=700,
        learning_rate=0.05,
        max_depth=-1,
        num_leaves=95,
        subsample=0.9,
        colsample_bytree=0.9,
        class_weight={0:1.0, 1:120.0},
        random_state=42
    )
    clf.fit(X_tr, y_tr)

    # 予測 → レース内正規化（合計=1.0）
    df_te["p_raw"] = clf.predict_proba(X_te)[:,1]
    df_te["p_norm"] = df_te.groupby("race_key")["p_raw"].transform(lambda s: s / s.sum().clip(lower=1e-12))

    # EV計算（オッズは学習特徴に未使用）
    df_te["EV"] = df_te["p_norm"] * df_te["odds"]

    # ルール1: EV>1 を均等100円
    unit = 100
    bet1 = df_te[df_te["EV"] > 1.0]
    invest1 = len(bet1) * unit
    return1 = (bet1["is_win"] * bet1["odds"] * unit).sum()
    roi1 = (return1 - invest1) / invest1 if invest1>0 else 0.0
    print(f"EV>1: bets={len(bet1)} invest={invest1:.0f} return={return1:.0f} ROI={roi1:.3f}")

    # ルール2: 各レース EV上位N点買い（例 N=10）
    N = 10
    topN = (df_te.sort_values(["race_key","EV"], ascending=[True,False]).groupby("race_key").head(N))
    investN = len(topN) * unit
    returnN = (topN["is_win"] * topN["odds"] * unit).sum()
    roiN = (returnN - investN) / investN if investN>0 else 0.0
    print(f"Top{N} by EV: bets={len(topN)} invest={investN:.0f} return={returnN:.0f} ROI={roiN:.3f}")

    # 予測保存（任意）
    out_pred = "public/preds/trifecta_test_preds.parquet"
    os.makedirs(os.path.dirname(out_pred), exist_ok=True)
    df_te[["date","jcd","race","combo","odds","is_win","p_norm","EV"]].to_parquet(out_pred, index=False)
    print(f"Saved predictions: {out_pred}")

if __name__ == "__main__":
    main()
