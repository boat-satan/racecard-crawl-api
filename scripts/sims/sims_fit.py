# -*- coding: utf-8 -*-
"""
sims_fit.py — sims_pure を results に寄せるための簡易パラメータ推定
- 目的関数: 実三連単の -log(sim_prob) を最小化（= 尤度最大化）
- 探索: ランダムサーチ + Nelder-Mead（scipyなし版の手書き 1-2 ステップ）
- 出力: scripts/sims/pass1/fit/{active_params.json, overall.json}
"""
import os, json, math, argparse, random, copy
import numpy as np
import pandas as pd

# sims_pure.py と同ディレクトリに置く想定
from sims_pure import simulate_one, Params, _norm_race

# ------- 既存 I/O ヘルパ（sims_pure と同じ形） -------
def _collect(base, kind, dates:set):
    root_v1=os.path.join(base,kind,"v1"); root=root_v1 if os.path.isdir(root_v1) else os.path.join(base,kind)
    out={}
    date_dirs=list(dates) if dates else [d for d in os.listdir(root) if os.path.isdir(os.path.join(root,d))]
    for d in date_dirs:
        dir_d=os.path.join(root,d)
        if not os.path.isdir(dir_d): continue
        for pid in os.listdir(dir_d):
            dir_pid=os.path.join(dir_d,pid)
            if not os.path.isdir(dir_pid): continue
            for f in os.listdir(dir_pid):
                if f.endswith(".json"):
                    race=f[:-5]; out[(d,pid,race)]=os.path.join(dir_pid,f)
    return out

def _collect_results(base, dates:set):
    root_v1=os.path.join(base,"results","v1"); root=root_v1 if os.path.isdir(root_v1) else os.path.join(base,"results")
    out={}
    date_dirs=list(dates) if dates else [d for d in os.listdir(root) if os.path.isdir(os.path.join(root,d))]
    for d in date_dirs:
        dir_d=os.path.join(root,d)
        if not os.path.isdir(dir_d): continue
        for pid in os.listdir(dir_d):
            dir_pid=os.path.join(dir_d,pid)
            if not os.path.isdir(dir_pid): continue
            per=[f for f in os.listdir(dir_pid) if f.lower().endswith(".json") and f.upper().endswith("R.JSON")]
            if per:
                for f in per:
                    r=f[:-5].upper(); r=r if r.endswith("R") else r+"R"
                    out[(d,pid,r)]=os.path.join(dir_pid,f)
                continue
            # 包含形式
            for f in [f for f in os.listdir(dir_pid) if f.lower().endswith(".json")]:
                p=os.path.join(dir_pid,f)
                try:
                    data=json.load(open(p,"r",encoding="utf-8"))
                    container=data.get("races", data) if isinstance(data,dict) else {}
                    for rk in list(container.keys()):
                        k=str(rk).upper()
                        if k.isdigit(): k+="R"
                        if k.endswith("R"): out[(d,pid,k)]=p+"#"+k
                except: pass
    return out

def _load_result(res_path):
    if "#" in res_path:
        p,r=res_path.split("#",1)
        data=json.load(open(p,"r",encoding="utf-8"))
        cont=data.get("races",data) if isinstance(data,dict) else {}
        d=cont.get(r) or cont.get(r.upper()) or cont.get(r.lower())
        return d if isinstance(d,dict) else {}
    return json.load(open(res_path,"r",encoding="utf-8"))

def _actual_trifecta_combo(res):
    trif=(res or {}).get("payouts",{}).get("trifecta")
    combo=None
    if isinstance(trif,dict):
        combo=trif.get("combo")
    if not combo and isinstance(res,dict):
        order=res.get("order")
        if isinstance(order,list) and len(order)>=3:
            def lane(x): return str(x.get("lane") or x.get("course") or x.get("F") or x.get("number"))
            try:
                f,s,t=lane(order[0]),lane(order[1]),lane(order[2])
                if all([f,s,t]): combo=f"{f}-{s}-{t}"
            except: pass
    return combo  # '1-2-3' 形式 or None

# ------- 目的関数（負の対数尤度 + 正則化） -------
def race_nll(int_json, res_json, sims, params_snapshot):
    # パラメータを一時反映
    for k,v in params_snapshot.items():
        setattr(Params, k, v)
    tri_probs, *_ = simulate_one(int_json, sims=sims)
    hit = _actual_trifecta_combo(res_json)
    if not hit:
        return None  # スキップ
    key = tuple(int(x) for x in hit.split("-"))
    p = tri_probs.get(key, 0.0)
    eps = 1e-12
    return -math.log(p + eps)

def batch_loss(int_idx, res_idx, keys, sims, params_snapshot, max_races=300):
    tot=0.0; n=0
    for (d,pid,r) in keys[:max_races]:
        d_int=json.load(open(int_idx[(d,pid,r)],"r",encoding="utf-8"))
        d_res=_load_result(res_idx[(d,pid,_norm_race(r))])
        nll = race_nll(d_int, d_res, sims, params_snapshot)
        if nll is None: 
            continue
        tot += nll; n += 1
    if n==0:
        return float("inf")
    # 軽い正則化（極端な値の暴走抑制）
    reg = 0.0
    reg += max(0, params_snapshot["p_backoff"]-0.25)*50
    reg += max(0, params_snapshot["beta_wk"]-0.012)*100
    return (tot / n) + reg

# ------- 探索空間 -------
SPACE = {
    "theta":              (0.020, 0.040),
    "b_dt":               (10.0, 20.0),
    "cK":                 (0.8, 1.8),
    "beta_wk":            (0.002, 0.012),
    "session_ST_shift_sd":(0.001, 0.008),
    "p_backoff":          (0.02, 0.20),
    "backoff_ST_shift":   (0.005, 0.025),
    "backoff_A_penalty":  (0.05, 0.25),
}

def sample_params(base=None):
    base = base or {k: getattr(Params,k) for k in SPACE.keys()}
    out = {}
    for k,(lo,hi) in SPACE.items():
        # ログっぽいのは線形でOK（レンジ狭い）
        out[k] = random.uniform(lo, hi)
    return out

def neighbor(p, scale=0.2):
    q={}
    for k,(lo,hi) in SPACE.items():
        span=hi-lo
        val = p[k] + random.uniform(-scale, scale)*span
        q[k] = min(hi, max(lo, val))
    return q

# ------- メイン -------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="./public")
    ap.add_argument("--dates", default="")
    ap.add_argument("--pids",  default="")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--sims",  type=int, default=3000)
    ap.add_argument("--iters", type=int, default=60)
    ap.add_argument("--seed",  type=int, default=2025)
    ap.add_argument("--outdir", default="./scripts/sims/pass1/fit")
    args = ap.parse_args()

    random.seed(args.seed); np.random.seed(args.seed)

    dates=set([d.strip() for d in args.dates.split(",") if d.strip()]) if args.dates else set()
    pids_filter=set([p.strip() for p in args.pids.split(",") if p.strip() and p!="ALL"])

    int_idx=_collect(args.base,"integrated",dates) if dates else _collect(args.base,"integrated", set(os.listdir(os.path.join(args.base,"integrated","v1"))))
    res_idx=_collect_results(args.base,dates)

    keys=sorted(set(int_idx.keys()) & set(res_idx.keys()))
    if pids_filter: keys=[k for k in keys if k[1] in pids_filter]
    if args.limit and args.limit>0: keys=keys[:args.limit]
    if not keys:
        raise SystemExit("no keys (integrated ∩ results)")

    os.makedirs(args.outdir, exist_ok=True)

    # 初期点：現行 Params のクランプ
    current = {k: getattr(Params,k) for k in SPACE.keys()}
    for k,(lo,hi) in SPACE.items():
        current[k] = min(hi, max(lo, current[k]))
    best = current
    best_loss = batch_loss(int_idx, res_idx, keys, args.sims, best)

    hist=[]
    for it in range(args.iters):
        cand = neighbor(best, scale=0.25) if it>10 else sample_params()
        loss = batch_loss(int_idx, res_idx, keys, args.sims, cand)
        hist.append({"iter":it, "loss":loss, **cand})
        if loss < best_loss:
            best_loss = loss; best = cand
            print(f"[improve] iter={it} loss={loss:.4f} -> {json.dumps(best)}")
        else:
            print(f"[try] iter={it} loss={loss:.4f}")

    # ベストを固定して保存
    for k,v in best.items():
        setattr(Params, k, v)
    active={k:getattr(Params,k) for k in dir(Params) if not k.startswith("_") and isinstance(getattr(Params,k),(int,float,bool))}
    json.dump(active, open(os.path.join(args.outdir,"active_params.json"),"w",encoding="utf-8"), ensure_ascii=False, indent=2)

    overall={
        "engine":"SimS pure (fit)",
        "races": len(keys),
        "iters": args.iters,
        "sims_per_race": args.sims,
        "best_loss": best_loss,
        "best_params": best,
        "history_tail": hist[-10:],
    }
    json.dump(overall, open(os.path.join(args.outdir,"overall.json"),"w",encoding="utf-8"), ensure_ascii=False, indent=2)
    print("=== FIT RESULT ===")
    print(json.dumps(overall, ensure_ascii=False, indent=2))

if __name__=="__main__":
    main()
