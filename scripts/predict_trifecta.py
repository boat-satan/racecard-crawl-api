# -*- coding: utf-8 -*-
"""
推論スクリプト（オッズの有無どちらでもOK）
- 統合JSONと（あれば）オッズJSONを読む
- 3連単120通り（またはオッズ側の候補）を展開
- 学習モデルの raw_score → レース内softmax で確率化
- あればEV（= p * odds）も計算
- 出力CSV: outputs/trifecta_pred_<date>_<jcd>_<race>.csv
"""
import os, json, glob, re
import numpy as np
import pandas as pd
import joblib
from itertools import permutations

BASE   = "public"
INTEG  = os.path.join(BASE, "integrated", "v1")
ODDS   = os.path.join(BASE, "odds",       "v1")
OUTDIR = "outputs"

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

def expand_trifecta_rows(integ, odds):
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

    trifecta_list = (odds or {}).get("trifecta") or []
    if len(trifecta_list) == 0:
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
        row["odds"] = to_float(item.get("odds"))
        row["popularity_rank"] = item.get("popularityRank")
        rows.append(row)
    return pd.DataFrame(rows)

def softmax_by_group(scores: pd.Series, keys: pd.Series) -> pd.Series:
    def _sm(s):
        a = s.values
        m = np.max(a)
        e = np.exp(a - m)
        return pd.Series(e / e.sum(), index=s.index)
    return scores.groupby(keys).apply(_sm)

def predict_one(model_pack, integ_path, odds_path=None, outdir=OUTDIR):
    os.makedirs(outdir, exist_ok=True)
    integ = safe_load(integ_path)
    odds  = safe_load(odds_path) if odds_path and os.path.exists(odds_path) else {}

    df = expand_trifecta_rows(integ, odds)
    if df.empty:
        return None

    feature_cols = model_pack["feature_cols"]
    clf = model_pack["model"]

    # 特徴構築
    drop_cols = {"combo","odds","popularity_rank","date","jcd","race","weather","windDir"}
    # 念のため列を揃える
    for c in feature_cols:
        if c not in df.columns:
            df[c] = 0.0
    X = df[feature_cols].copy().fillna(0.0).astype(float)

    # 予測 → softmax
    df["race_key"] = df["date"].astype(str) + "-" + df["jcd"].astype(str) + "-" + df["race"]
    z = clf.predict(X, raw_score=True)
    df["score"] = z
    df["p"] = softmax_by_group(df["score"], df["race_key"])

    # EV（オッズがある行のみ）
    has_odds = df["odds"].notna()
    df.loc[has_odds, "EV"] = df.loc[has_odds, "p"] * df.loc[has_odds, "odds"]

    # 出力
    date = df.iloc[0]["date"]; jcd = df.iloc[0]["jcd"]; race = df.iloc[0]["race"]
    out = df[["date","jcd","race","combo","F","S","T","odds","p","EV","popularity_rank"]].copy()
    out = out.sort_values("p", ascending=False)
    out_path = os.path.join(outdir, f"trifecta_pred_{date}_{jcd}_{race}.csv")
    out.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path}")
    return out_path

def main():
    model_pack = joblib.load("models/trifecta_lgbm.pkl")
    # 例: 全日全場走査（オッズはあれば読む）
    for integ_path in sorted(glob.glob(os.path.join(INTEG, "*", "*", "*.json"))):
        fname = os.path.basename(integ_path)
        date  = os.path.basename(os.path.dirname(os.path.dirname(integ_path)))
        jcd   = os.path.basename(os.path.dirname(integ_path))
        odds_path = os.path.join(ODDS, date, jcd, fname)
        try:
            predict_one(model_pack, integ_path, odds_path if os.path.exists(odds_path) else None)
        except Exception as e:
            print("skip:", integ_path, e)

if __name__ == "__main__":
    main()
