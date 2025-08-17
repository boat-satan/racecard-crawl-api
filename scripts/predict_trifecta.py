# scripts/predict_trifecta.py
# 使い方例:
#   python scripts/predict_trifecta.py --date 20250815 --jcd 02 --race 10R
#   python scripts/predict_trifecta.py --date 20250815 --jcd 02           # その日の全R
#   python scripts/predict_trifecta.py --date 20250815                    # その日の全場・全R
# 出力: public/preds/{date}/{jcd}/{race}_trifecta_preds.parquet と .csv

import os, re, json, glob, argparse
import numpy as np
import pandas as pd
import joblib

BASE = "public"
INTEG = os.path.join(BASE, "integrated", "v1")
ODDS  = os.path.join(BASE, "odds",       "v1")

MODEL_PATH = "models/trifecta_lgbm.pkl"

# ===== shared helpers (train と同じ前処理) =====
def to_float(x):
    if x is None: return None
    s = str(x).strip().replace("kg","")
    s = s.lstrip("F")  # "F.02" -> ".02"
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

def build_rows_for_race(integ, odds):
    """結果は使わず、予測用に1レース分の行（オッズ掲載の全組み合わせ）を作る"""
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

    # lane -> features
    lane_map = {}
    for e in integ.get("entries", []):
        lane_map[int(e["lane"])] = lane_feats(e)

    trifecta_list = (odds or {}).get("trifecta") or []
    if len(trifecta_list) == 0:
        # オッズがない場合は予測不能（出力なし）にする
        return []

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

    return rows

# ===== prediction =====

def collect_targets(date: str|None, jcd: str|None, race: str|None):
    """予測対象の (integ_path, odds_path) の組を列挙"""
    pairs = []
    date_glob = date if date else "*"
    for ddir in sorted(glob.glob(os.path.join(INTEG, date_glob))):
        d = os.path.basename(ddir)
        jcd_glob = jcd if jcd else "*"
        for jdir in sorted(glob.glob(os.path.join(ddir, jcd_glob))):
            jj = os.path.basename(jdir)
            race_glob = f"{race}.json" if race else "*.json"
            for ipath in sorted(glob.glob(os.path.join(jdir, race_glob))):
                rfile = os.path.basename(ipath)
                opath = os.path.join(ODDS, d, jj, rfile)
                if os.path.exists(opath):
                    pairs.append((ipath, opath))
    return pairs

def predict_for_pair(model, integ_path, odds_path, save=True):
    integ = safe_load(integ_path)
    odds  = safe_load(odds_path)
    rows = build_rows_for_race(integ, odds)
    if not rows:
        return None

    df = pd.DataFrame(rows)

    # 学習時と同じ feature 選択
    drop_cols = {"combo","date","jcd","race","weather","windDir","odds","popularity_rank"}
    feature_cols = [c for c in df.columns if c not in drop_cols and df[c].dtype != "object"]
    X = df[feature_cols].copy().fillna(0.0).astype(float)

    df["p_raw"] = model.predict_proba(X)[:,1]
    # レース内正規化（合計=1.0／ゼロ除算ガード）
    denom = df["p_raw"].sum()
    denom = denom if denom and denom > 0 else 1e-12
    df["p_norm"] = df["p_raw"] / max(denom, 1e-12)
    df["EV"] = df["p_norm"] * df["odds"]

    # 保存
    if save:
        date = df.loc[0, "date"]; jcd = df.loc[0, "jcd"]; race = df.loc[0, "race"]
        outdir = os.path.join("public", "preds", str(date), str(jcd))
        os.makedirs(outdir, exist_ok=True)
        base = f"{race}_trifecta_preds"
        df_out = df.sort_values("EV", ascending=False)
        df_out.to_parquet(os.path.join(outdir, base + ".parquet"), index=False)
        df_out.to_csv    (os.path.join(outdir, base + ".csv"),     index=False, encoding="utf-8")
        print(f"saved: {outdir}/{base}.parquet  and  .csv")

    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="YYYYMMDD（未指定なら全日）")
    ap.add_argument("--jcd",  help="場コード（未指定なら全場）")
    ap.add_argument("--race", help="例: 10R（未指定なら全R）")
    ap.add_argument("--model", default=MODEL_PATH, help="モデルpklのパス")
    args = ap.parse_args()

    if not os.path.exists(args.model):
        raise FileNotFoundError(f"model not found: {args.model}")

    model = joblib.load(args.model)

    pairs = collect_targets(args.date, args.jcd, args.race)
    if not pairs:
        print("No targets matched.")
        return

    for ipath, opath in pairs:
        print(f"predict: {ipath}  +  {opath}")
        predict_for_pair(model, ipath, opath, save=True)

if __name__ == "__main__":
    main()
