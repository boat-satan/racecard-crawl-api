# scripts/sims/sims_fit.py
# Fit Params to results by minimizing total NLL over races (coordinate search)
import os, json, math, argparse, copy
import numpy as np

# sims_pure から必要関数を利用
from sims_pure import (
    Params, simulate_one, _collect, _collect_results, _norm_race
)

EPS = 1e-12

# --- 実績トリifecta取り出し ---
def _actual_combo(res_dict):
    trif=(res_dict or {}).get("payouts",{}).get("trifecta")
    if isinstance(trif,dict):
        c=str(trif.get("combo") or "").strip()
        if c: return tuple(int(x) for x in c.replace(" ","").split("-"))
    # フォールバック: order から
    order=res_dict.get("order") if isinstance(res_dict,dict) else None
    if isinstance(order,list) and len(order)>=3:
        def lane(x): 
            return int(x.get("lane") or x.get("course") or x.get("F") or x.get("number"))
        try:
            return (lane(order[0]), lane(order[1]), lane(order[2]))
        except: 
            return None
    return None

# --- 1レースのNLL ---
def race_nll(integrated_json, result_json, sims, params_snapshot:dict):
    # 一時的に Params をスナップショット値へ
    backup={}
    for k,v in params_snapshot.items():
        backup[k]=getattr(Params,k)
        setattr(Params,k,float(v))
    try:
        tri_probs, *_ = simulate_one(integrated_json, sims=sims)
        combo=_actual_combo(result_json)
        if not combo:
            return 0.0  # 実績なしは損失に入れない
        p=tri_probs.get(tuple(combo), 0.0)
        return -math.log(max(p, EPS))
    finally:
        # 元に戻す
        for k,v in backup.items():
            setattr(Params,k,v)

# --- 総NLL ---
def evaluate_total_nll(int_idx, res_idx, keys, sims, params_snapshot):
    total=0.0; cnt=0
    for (date,pid,race) in keys:
        with open(int_idx[(date,pid,race)],"r",encoding="utf-8") as f:
            d_int=json.load(f)
        # 結果JSON（#R キー指定対応）
        rp=res_idx[(date,pid,race)]
        if "#" in rp:
            p,r=rp.split("#",1)
            data=json.load(open(p,"r",encoding="utf-8"))
            races=data.get("races", data) if isinstance(data,dict) else {}
            d_res=races.get(r) or races.get(r.upper()) or races.get(r.lower()) or {}
        else:
            d_res=json.load(open(rp,"r",encoding="utf-8"))

        nll=race_nll(d_int, d_res, sims, params_snapshot)
        total+=nll; cnt+=1
    return total, cnt

# --- チューニングするパラメータ集合とスケール ---
TUNE_LIST = {
    "theta":           1.0,
    "b_dt":            5.0,
    "cK":              0.5,
    "alpha_R":         0.001,
    "alpha_A":         0.002,
    "alpha_Ap":        0.002,
    "beta_sq":         0.002,
    "beta_wk":         0.002,
    "gamma_wall":      0.002,
    "k_turn_err":      0.002,
    "delta_first":     0.10,
    "delta_lineblock": 0.10,
}

def clamp(name, val):
    # ザックリ安全域
    bounds={
        "theta":(0.0,0.08),
        "b_dt":(1.0,40.0),
        "cK":(0.1,3.0),
        "alpha_R":(-0.02,0.02),
        "alpha_A":(-0.05,0.03),
        "alpha_Ap":(-0.05,0.03),
        "beta_sq":(0.0,0.05),
        "beta_wk":(0.0,0.05),
        "gamma_wall":(0.0,0.05),
        "k_turn_err":(0.0,0.05),
        "delta_first":(0.0,1.5),
        "delta_lineblock":(0.0,1.5),
    }
    lo,hi=bounds.get(name,(-1e9,1e9))
    return min(max(val,lo),hi)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--base",default="./public")
    ap.add_argument("--dates",default="")
    ap.add_argument("--pids",default="")
    ap.add_argument("--races",default="")
    ap.add_argument("--sims",type=int,default=2000)
    ap.add_argument("--limit",type=int,default=0)
    ap.add_argument("--iters",type=int,default=40)        # ★ 復活
    ap.add_argument("--lr",type=float,default=0.25)       # ステップ倍率
    ap.add_argument("--outdir",default="./SimS_pure_fitted")
    args=ap.parse_args()

    # keys 抽出
    dates=set([d.strip() for d in args.dates.split(",") if d.strip()])
    pids_filter=set([p.strip() for p in args.pids.split(",") if p.strip() and p.strip()!="ALL"])
    races_filter=set([_norm_race(r) for r in args.races.split(",") if r.strip() and r.strip()!="ALL"])

    int_idx=_collect(args.base,"integrated",dates) if dates else _collect(args.base,"integrated", set(os.listdir(os.path.join(args.base,"integrated","v1"))))
    res_idx=_collect_results(args.base,dates)
    keys=sorted(set(int_idx.keys()) & set(res_idx.keys()))
    if pids_filter: keys=[k for k in keys if k[1] in pids_filter]
    if races_filter: keys=[k for k in keys if _norm_race(k[2]) in races_filter]
    if args.limit and args.limit>0: keys=keys[:args.limit]

    os.makedirs(args.outdir, exist_ok=True)
    log_path=os.path.join(args.outdir,"fit_log.txt")

    # 初期スナップショット
    cur={k:float(getattr(Params,k)) for k in TUNE_LIST.keys()}
    best_val, n = evaluate_total_nll(int_idx, res_idx, keys, args.sims, cur)
    with open(log_path,"w",encoding="utf-8") as w:
        w.write(f"INIT NLL={best_val:.6f} over {n} races\n")

    for it in range(1, args.iters+1):
        improved=False
        for name,scale in TUNE_LIST.items():
            base=cur[name]
            cand=[]
            step=args.lr*scale
            for delta in (+step, -step):
                cur[name]=clamp(name, base+delta)
                val,_=evaluate_total_nll(int_idx, res_idx, keys, args.sims, cur)
                cand.append((val, base+delta))
            # 方向選択
            cand.sort(key=lambda x:x[0])
            best_cand_val, best_param = cand[0]
            if best_cand_val + 1e-9 < best_val:
                best_val=best_cand_val
                cur[name]=best_param
                improved=True
                with open(log_path,"a",encoding="utf-8") as w:
                    w.write(f"[iter {it}] {name}: -> {cur[name]:.6f}  NLL={best_val:.6f}\n")
            else:
                cur[name]=base  # 戻す

        if not improved:
            # 収束気味 → 学習率を落として微調整
            args.lr *= 0.5
            with open(log_path,"a",encoding="utf-8") as w:
                w.write(f"[iter {it}] no improvement, lr -> {args.lr}\n")
            if args.lr < 1e-3:
                with open(log_path,"a",encoding="utf-8") as w:
                    w.write(f"[stop] lr too small\n")
                break

    # 保存（active_params.json 互換）
    active_path=os.path.join(args.outdir,"active_params.json")
    json.dump(cur, open(active_path,"w",encoding="utf-8"), ensure_ascii=False, indent=2)

    # まとめ
    summary={
        "races": len(keys),
        "sims_per_race": args.sims,
        "iters": it,
        "final_nll": best_val,
        "tuned_params": cur,
    }
    json.dump(summary, open(os.path.join(args.outdir,"summary.json"),"w",encoding="utf-8"),
              ensure_ascii=False, indent=2)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"[fit] saved: {active_path}")

if __name__=="__main__":
    main()
