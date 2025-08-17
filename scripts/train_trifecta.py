# scripts/train_trifecta.py
# 統合JSON/結果の直読み（オッズは任意） → 3連単データ展開 → LightGBM学習
# ・stats.entryCourse を広く展開
# ・オッズは学習に使わず、学習後のEV計算のみで利用（存在すれば）
# ・学習済みモデルを models/trifecta_lgbm.pkl に保存
# ・pandas clip は min= を使用

import os, json, glob, re
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from lightgbm import LGBMClassifier
import joblib

BASE = "public"
INTEG = os.path.join(BASE, "integrated", "v1")
ODDS  = os.path.join(BASE, "odds",       "v1")      # ← 任意
RES   = os.path.join(BASE, "results",    "v1")

def to_float(x):
    if x is None: return None
    s = str(x).strip().replace("kg","")
    s = s.lstrip("F")
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

def flatten_entry_course(stats_entry_course: dict) -> dict:
    out = {}
    if not stats_entry_course:
        return out
    out["course"] = stats_entry_course.get("course")
    out["entry_avgST"] = to_float(stats_entry_course.get("avgST"))

    ss = stats_entry_course.get("selfSummary") or {}
    starts = ss.get("starts"); first = ss.get("firstCount"); second = ss.get("secondCount"); third = ss.get("thirdCount")
    out["self_starts"] = starts; out["self_first"] = first; out["self_second"] = second; out["self_third"] = third
    out["self_top1Rate_calc"] = rate(first, starts)
    out["self_top2Rate_calc"] = rate((first or 0)+(second or 0), starts)
    out["self_top3Rate_calc"] = rate((first or 0)+(second or 0)+(third or 0), starts)

    for k, v in (stats_entry_course.get("winKimariteSelf") or {}).items():
        out[f"win_{k}"] = v
    for k, v in (stats_entry_course.get("loseKimarite") or {}).items():
        out[f"lose_{k}"] = v

    ms = stats_entry_course.get("matrixSelf") or {}
    out["matrix_winRate"]  = to_float(ms.get("winRate"))
    out["matrix_top2Rate"] = to_float(ms.get("top2Rate"))
    out["matrix_top3Rate"] = to_float(ms.get("top3Rate"))

    for item in (stats_entry_course.get("exTimeRank") or []):
        r = item.get("rank")
        if r is None: 
            continue
        out[f"exRank{r}_winRate"]  = to_float(item.get("winRate"))
        out[f"exRank{r}_top2Rate"] = to_float(item.get("top2Rate"))
        out[f"exRank{r}_top3Rate"] = to_float(item.get("top3Rate"))
    return out

def lane_feats(entry):
    rc = entry.get("racecard", {}) or {}
    ex = entry.get("exhibition", {}) or {}
    ec = (entry.get("stats") or {}).get("entryCourse") or {}

    feat = {
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
        "tenji": to_float(ex.get("tenjiTime")),
        "exST":  to_float(ex.get("st")),
        "exF":   ex_flag(ex.get("st")),
    }
    feat.update(flatten_entry_course(ec))
    return feat

def build_rows_for_race(integ, odds, result):
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
    hit_combo = tri.get("combo")  # 例 "2-5-4"

    trifecta_list = (odds or {}).get("trifecta") or []
    if len(trifecta_list) == 0:
        # オッズが無い場合は全120通りを自動生成（odds/popularityは欠損のまま）
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
        row["odds"] = to_float(item.get("odds")) if item is not None else None
        row["popularity_rank"] = item.get("popularityRank") if item is not None else None
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
                odds_path = os.path.join(ODDS, date, jcd, race_file)   # ← あれば読む
                res_path  = os.path.join(RES,  date, jcd, race_file)   # ← 必須
                if not os.path.exists(res_path):
                    continue  # 結果が無いと正解ラベルが付かないのでスキップ
                try:
                    integ  = safe_load(integ_path)
                    odds   = safe_load(odds_path) if os.path.exists(odds_path) else None
                    result = safe_load(res_path)
                except Exception:
                    continue
                records.extend(build_rows_for_race(integ, odds, result))
    return pd.DataFrame(records)

def main():
    df = load_dataset("*", "*")
    if df.empty:
        print("No data found.")
        return

    drop_cols = {"combo","is_win","odds","popularity_rank","date","jcd","race","weather","windDir"}
    feature_cols = [c for c in df.columns if c not in drop_cols and df[c].dtype != "object"]

    X_all = df[feature_cols].copy().fillna(0.0).astype(float)
    y_all = df["is_win"].astype(int)

    # レース単位で分割
    df["race_key"] = df["date"].astype(str) + "-" + df["jcd"].astype(str) + "-" + df["race"]
    races = df["race_key"].unique()
    train_races, test_races = train_test_split(races, test_size=0.2, random_state=42, shuffle=True)
    trn = df["race_key"].isin(train_races)
    tst = df["race_key"].isin(test_races)

    X_tr, y_tr = X_all[trn], y_all[trn]
    X_te, y_te = X_all[tst], y_all[tst]
    df_te = df[tst].copy()

    clf = LGBMClassifier(
        n_estimators=700, learning_rate=0.05,
        max_depth=-1, num_leaves=95,
        subsample=0.9, colsample_bytree=0.9,
        class_weight={0:1.0, 1:120.0}, random_state=42
    )
    clf.fit(X_tr, y_tr)

    # モデル保存
    os.makedirs("models", exist_ok=True)
    joblib.dump(clf, "models/trifecta_lgbm.pkl")
    print("Saved model: models/trifecta_lgbm.pkl")

    # 参考: レース内確率正規化（clipはmin=を使用）
    df_te["p_raw"] = clf.predict_proba(X_te)[:,1]
    denom = df_te.groupby("race_key")["p_raw"].transform(lambda s: s.sum().clip(min=1e-12))
    df_te["p_norm"] = df_te["p_raw"] / denom

    # EVはオッズがある行のみ（ない場合はNaNのまま）
    if "odds" in df_te.columns:
        df_te["EV"] = df_te["p_norm"] * df_te["odds"]

    cols_show = ["race_key","combo","is_win","odds","p_norm"]
    if "EV" in df_te.columns:
        cols_show.append("EV")
    print("Eval sample:", df_te[cols_show].head(5).to_string(index=False))

if __name__ == "__main__":
    main()
