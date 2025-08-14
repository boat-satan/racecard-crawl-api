# sims_predict_v1.py
import os, json, argparse
from collections import Counter
import numpy as np
import pandas as pd

# === SimS ver1.0 と同一の中核式 ===
def calc_T1M(ST, R, R_base=100, R_coeff=0.005):
    return R_base + (R - 100) * R_coeff + ST

def pass_prob(Delta_t, Delta_K, theta, a0, b_dt, cK):
    import math
    return 1 / (1 + math.exp(-(a0 + b_dt*(theta - Delta_t) + cK*Delta_K)))

PARAMS = {
    "theta": 0.028,
    "sigma_ST": 0.02,
    "ST_jitter_pct": 0.2,
    "R_jitter": 3.0,
    "kimarite_th": 0.030,
    "a0": 0.0,
    "b_dt": 15.0,
    "cK": 1.2
}
LANES = [1,2,3,4,5,6]

def ensure_out():
    os.makedirs("out", exist_ok=True)

def load_json(p):
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def list_integrated_files(base, dates, pids=None, races=None):
    files = []
    for d in dates:
        root = os.path.join(base, "integrated", "v1", d)
        if not os.path.isdir(root):
            continue
        for pid in sorted(os.listdir(root)):
            if pids and pid not in pids:
                continue
            rroot = os.path.join(root, pid)
            if not os.path.isdir(rroot):
                continue
            for fn in sorted(os.listdir(rroot)):
                if not fn.endswith(".json"):
                    continue
                race = fn[:-5]
                if races and race not in races:
                    continue
                files.append(os.path.join(rroot, fn))
    return files

def parse_ids(fp):
    parts = fp.replace("\\","/").split("/")
    return parts[-3], parts[-2], parts[-1].replace(".json","")  # date,pid,race

def sim_topN(intg, sims=1200, topn=18):
    np.random.seed(0)
    # ver1.0の前提と同じく、ST中央値/助走距離の既定値→データがあれば上書き
    st_med = {l: 0.15 + (l-3)*0.01 for l in LANES}
    r_dist = {l: 100 + (l-4)*5 for l in LANES}

    for e in intg.get("entries", []):
        lane = int(e.get("lane", 0))
        rc = e.get("racecard", {})
        if lane in LANES and "avgST" in rc:
            try:
                st_med[lane] = float(rc["avgST"])
            except Exception:
                pass

    counts = Counter()
    for _ in range(int(sims)):
        ST = {l: np.random.normal(st_med[l], PARAMS["sigma_ST"]*(1+PARAMS["ST_jitter_pct"])) for l in LANES}
        R  = {l: np.clip(r_dist[l] + np.random.uniform(-PARAMS["R_jitter"], PARAMS["R_jitter"]), 80, 130) for l in LANES}
        T1 = {l: calc_T1M(ST[l], R[l]) for l in LANES}

        entry = sorted(T1, key=T1.get)
        exit_rank = entry.copy()
        for i in range(len(entry)-1):
            a, b = exit_rank[i], exit_rank[i+1]
            dt = T1[b] - T1[a]
            if dt < PARAMS["theta"]:
                if np.random.rand() < pass_prob(dt, 0.0, PARAMS["theta"], PARAMS["a0"], PARAMS["b_dt"], PARAMS["cK"]):
                    exit_rank[i], exit_rank[i+1] = b, a

        t = tuple(exit_rank[:3])
        counts[f"{t[0]}-{t[1]}-{t[2]}"] += 1

    total = sum(counts.values()) or 1
    rows = [(k, v/total) for k, v in counts.items()]
    rows.sort(key=lambda x: x[1], reverse=True)
    return [{"ticket": k, "prob": round(p, 6)} for k, p in rows[:topn]]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True)
    ap.add_argument("--dates", required=True)      # "20250810,20250811"
    ap.add_argument("--pids", default="")
    ap.add_argument("--races", default="")
    ap.add_argument("--sims", default="1200")
    ap.add_argument("--topn", default="18")
    args = ap.parse_args()

    dates = [s.strip() for s in args.dates.split(",") if s.strip()]
    pids  = [s.strip() for s in args.pids.split(",") if s.strip()] or None
    races = [s.strip() for s in args.races.split(",") if s.strip()] or None
    sims  = int(args.sims); topn = int(args.topn)
    ensure_out()

    all_rows = []
    for fp in list_integrated_files(args.base, dates, pids, races):
        intg = load_json(fp)
        date, pid, race = parse_ids(fp)
        top = sim_topN(intg, sims=sims, topn=topn)
        with open(f"out/pred_{date}_{pid}_{race}.json","w",encoding="utf-8") as f:
            json.dump({"date":date,"pid":pid,"race":race,"topN":top,"generatedBy":"sims_predict_v1"}, f, ensure_ascii=False, indent=2)
        for i, r in enumerate(top, 1):
            all_rows.append({"date":date,"pid":pid,"race":race,"rank":i,"ticket":r["ticket"],"prob":r["prob"]})

    if all_rows:
        pd.DataFrame(all_rows).to_csv("out/predictions_summary.csv", index=False, encoding="utf-8")
    print("predict done")

if __name__ == "__main__":
    main()
