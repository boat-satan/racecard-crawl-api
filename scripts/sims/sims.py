# sims_batch_eval_SimS_v1.py
# SimS ver1.0 — 同時同条件バッチ検証（三連単 TOPN 均等買い）
# 使い方例：
#   python sims_batch_eval_SimS_v1.py --base ./public --dates 20250810,20250811 --sims 1200 --topn 18 --unit 100
# 追加仕様：
#   --predict-only 指定時は ./predict に毎回上書き出力（JSON/CSV）。ROI集計は行わない。

import os, json, math, argparse, shutil
from collections import Counter, defaultdict
import numpy as np
import pandas as pd

# =========================
# SimS ver1.0 パラメータ
# =========================
class Params:
    # T1M 到達式
    b0=100.0; alpha_R=0.005; alpha_A=-0.010; alpha_Ap=-0.012
    # 勝負圏・追い抜き
    theta=0.028; a0=0.0; b_dt=15.0; cK=1.2
    # 決まり手
    tau_k=0.030
    # イベント強度
    beta_sq=0.006; beta_wk=0.004; beta_flow_R=0.002; k_turn_err=0.010
    # 先マイ権/ラインブロック
    delta_first=0.8; delta_lineblock=0.5
    # 安全マージン
    safe_margin_mu=0.005; safe_margin_sigma=0.003; p_safe_margin=0.20
    # ビビり戻し
    p_backoff=0.10; backoff_ST_shift=0.015; backoff_A_penalty=0.15
    # キャビテーション
    p_cav=0.03; cav_A_penalty=0.25
    # セッション揺らぎ
    session_ST_shift_mu=0.0; session_ST_shift_sd=0.004
    session_A_bias_mu=0.0;   session_A_bias_sd=0.10
    # 風（固定的に中立だが式は残す）
    wind_theta_gain=0.002; wind_st_sigma_gain=0.5
    # 壁
    gamma_wall=0.006
    # 引き波
    base_wake=0.20; extra_wake_when_outside=0.25

rng = np.random.default_rng(2025)

# -------- 共通ユーティリティ --------
def sigmoid(x: float) -> float: return 1.0 / (1.0 + math.exp(-x))
def s_base_from_nat(rc: dict) -> float:
    n1=float(rc.get("natTop1",6.0)); n2=float(rc.get("natTop2",50.0)); n3=float(rc.get("natTop3",70.0))
    return 0.5*((n1-6.0)/2.0)+0.3*((n2-50.0)/20.0)+0.2*((n3-70.0)/20.0)

def wind_adjustments(env: dict):
    d=(env.get("wind") or {}).get("dir","cross"); m=float((env.get("wind") or {}).get("mps",0.0))
    sign=1 if d=="tail" else -1 if d=="head" else 0
    return Params.wind_theta_gain*sign*m, 1.0 + Params.wind_st_sigma_gain*(abs(m)/10.0)

def apply_session_bias(ST,A,Ap):
    ST+=rng.normal(Params.session_ST_shift_mu,Params.session_ST_shift_sd)
    g=lambda x: x*(1.0 + rng.normal(Params.session_A_bias_mu,Params.session_A_bias_sd))
    return ST, g(A), g(Ap)

def maybe_backoff(ST,A):
    return (ST+Params.backoff_ST_shift, A*(1.0-Params.backoff_A_penalty)) if rng.random()<Params.p_backoff else (ST,A)

def maybe_cav(A): return A*(1.0-Params.cav_A_penalty) if rng.random()<Params.p_cav else A
def maybe_safe_margin(): return max(0.0, rng.normal(Params.safe_margin_mu,Params.safe_margin_sigma)) if rng.random()<Params.p_safe_margin else 0.0
def flow_bias(env,lane): return 0.0  # 中立
def wake_loss_probability(lane, entry):
    pos=entry.index(lane); base=Params.base_wake+Params.extra_wake_when_outside*((lane-1)/5.0)
    return max(0.0, min(base*(0.3 if pos==0 else 1.0), 0.95))

# -------- 統合データ → エンジン入力 --------
def build_input_from_integrated(d: dict) -> dict:
    lanes=[e["lane"] for e in d["entries"]]; mu={}; S={}; F={}
    for e in d["entries"]:
        lane=e["lane"]; rc=e["racecard"]; ec=(e.get("stats") or {}).get("entryCourse", {})
        vals=[v for v in [rc.get("avgST"), ec.get("avgST")] if isinstance(v,(int,float))]
        m=0.16 if not vals else (float(vals[0]) if len(vals)==1 else 0.5*(float(vals[0])+float(vals[1])))
        if int(rc.get("flyingCount",0))>0: m+=0.010
        mu[lane]=m; S[lane]=s_base_from_nat(rc); F[lane]=int(rc.get("flyingCount",0))
    ST_model={str(l):{"type":"normal","mu":mu[l],"sigma":0.02*(1+0.20*(1 if F[l]>0 else 0)+0.15*max(0.0,-S[l]))} for l in lanes}
    R={str(l):float({1:88,2:92,3:96,4:100,5:104,6:108}.get(l,100.0)) for l in lanes}
    course_bias={1:0.05,2:0.05,3:0.02,4:0.00,5:-0.05,6:-0.06}
    A={l:0.7*S[l]+0.3*((0.16-mu[l])*5.0) for l in lanes}
    Ap={l:0.7*S[l]+0.3*course_bias.get(l,0.0) for l in lanes}
    S1=S.get(1,0.0); squeeze={str(l):(0.0 if l==1 else min(max(0.0,(S1-S[l])*0.20),0.20)) for l in lanes}
    first_right=set(); lineblocks=set()
    if S1>0.30 and mu.get(1,0.16)<=0.17: first_right.add(1)
    S4=S.get(4,0.0)
    if S4>0.10 and mu.get(4,0.16)<=0.17: first_right.add(4)
    if (S1 - S.get(2,0.0))>0.20: lineblocks.add((1,2))
    if (S4 - S1)>0.05:
        sc4=next((e.get("startCourse",4) for e in d["entries"] if e["lane"]==4),4)
        if sc4>=4: lineblocks.add((4,1))
    env={"wind":{"dir":"cross","mps":0.0},"flow":{"dir":"none","rate":0.0}}
    return {"lanes":lanes,"ST_model":ST_model,"R":R,"A":A,"Ap":Ap,"env":env,"squeeze":squeeze,
            "first_right":first_right,"lineblocks":lineblocks}

# -------- 1レース・シミュ --------
def sample_ST(model): return rng.normal(model["mu"], model["sigma"])

def t1m_time(ST,R,A,Ap,sq,env,lane,st_gain):
    ST,A,Ap=apply_session_bias(ST,A,Ap); ST,A=maybe_backoff(ST,A); A=maybe_cav(A)
    return (Params.b0 + Params.alpha_R*(R-100.0) + Params.alpha_A*A + Params.alpha_Ap*Ap
            + Params.beta_sq*sq + flow_bias(env,lane) + ST*st_gain)

def one_pass(entry,T1M,A,Ap,env,lineblocks,first_right):
    exit_order=entry[:]; d_theta,_=wind_adjustments(env); th=Params.theta+d_theta
    for k in range(len(exit_order)-1):
        lead, chase = exit_order[k], exit_order[k+1]
        dt=T1M[chase]-T1M[lead]; dK=(A[chase]+Ap[chase])-(A[lead]+Ap[lead])
        delta=(Params.delta_lineblock if (lead,chase) in lineblocks else 0.0) + (Params.delta_first if lead in first_right else 0.0)
        dt_eff=dt + Params.gamma_wall + Params.k_turn_err*maybe_safe_margin()
        p=sigmoid(Params.a0 + Params.b_dt*(th - dt_eff) + Params.cK*dK + delta)
        if rng.random()<p: exit_order[k],exit_order[k+1]=chase,lead
    return exit_order

def simulate_one(integrated_json: dict, sims: int = 1200):
    inp=build_input_from_integrated(integrated_json); lanes=inp["lanes"]; env=inp["env"]; _,st_gain=wind_adjustments(env)
    trifecta=Counter(); kimarite=Counter()
    for _ in range(sims):
        ST={i: sample_ST(inp["ST_model"][str(i)]) for i in lanes}
        T1M={i: t1m_time(ST[i], inp["R"][str(i)], inp["A"][i], inp["Ap"][i], inp["squeeze"][str(i)], env, i, st_gain) for i in lanes}
        entry=sorted(lanes, key=T1M.get)
        for i in lanes:
            if rng.random() < wake_loss_probability(i, entry): T1M[i]+=Params.beta_wk
        exit_order = one_pass(entry, T1M, inp["A"], inp["Ap"], env, inp["lineblocks"], inp["first_right"])
        lead=exit_order[0]; dt_lead=T1M[exit_order[1]]-T1M[lead]
        kimarite["逃げ" if lead==1 else ("まくり" if dt_lead>=Params.tau_k else "まくり差し")] += 1
        trifecta[tuple(exit_order[:3])] += 1
    tot=float(sims)
    return ({k:v/tot for k,v in trifecta.items()}, {k:v/tot for k,v in kimarite.items()})

# -------- データ収集（v1 フォールバック） --------
def collect_files(base_dir: str, kind: str, dates: set):
    root_v1=os.path.join(base_dir, kind, "v1"); root=root_v1 if os.path.isdir(root_v1) else os.path.join(base_dir, kind)
    date_dirs=list(dates) if dates else [d for d in os.listdir(root) if os.path.isdir(os.path.join(root,d))]
    out={}
    for d in date_dirs:
        dir_d=os.path.join(root,d); 
        if not os.path.isdir(dir_d): continue
        for pid in os.listdir(dir_d):
            dir_pid=os.path.join(dir_d,pid)
            if not os.path.isdir(dir_pid): continue
            for fname in os.listdir(dir_pid):
                if fname.endswith(".json"): out[(d,pid,fname[:-5])]=os.path.join(dir_pid,fname)
    return out

# -------- オッズ/結果 --------
def odds_map(odds_json: dict) -> dict:
    out={}
    for it in odds_json.get("trifecta", []):
        c=it.get("combo") or f'{it.get("F")}-{it.get("S")}-{it.get("T")}'
        if c: out[c]=float(it["odds"])
    return out

def actual_trifecta_combo(result_json: dict):
    trif=(result_json.get("payouts") or {}).get("trifecta")
    if isinstance(trif,dict) and "combo" in trif: return trif["combo"]
    order=result_json.get("order")
    if isinstance(order,list) and len(order)>=3:
        lane_of=lambda x: str(x.get("lane") or x.get("course") or x.get("F") or x.get("number"))
        try:
            f,s,t=lane_of(order[0]),lane_of(order[1]),lane_of(order[2])
            if all([f,s,t]): return f"{f}-{s}-{t}"
        except Exception: return None
    return None

def evaluate_one(int_path:str, odds_path:str, res_path:str, sims:int, topn:int, unit:int):
    with open(int_path,"r",encoding="utf-8") as f: tri_probs,_=simulate_one(json.load(f), sims=sims)
    top=sorted(tri_probs.items(), key=lambda kv: kv[1], reverse=True)[:topn]
    top_keys=['-'.join(map(str,k)) for k,_ in top]
    with open(odds_path,"r",encoding="utf-8") as f: omap=odds_map(json.load(f))
    bets=[c for c in top_keys if c in omap]; stake=unit*len(bets)
    with open(res_path,"r",encoding="utf-8") as f: hit_combo=actual_trifecta_combo(json.load(f))
    payout=int(round(omap.get(hit_combo,0.0)*unit)) if hit_combo in bets else 0
    return stake, payout, (1 if payout>0 else 0), bets, hit_combo

# =========================
# メイン
# =========================
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--base", default="./public")
    ap.add_argument("--dates", default="")
    ap.add_argument("--sims", type=int, default=600)
    ap.add_argument("--topn", type=int, default=18)
    ap.add_argument("--unit", type=int, default=100)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--outdir", default="./SimS_v1.0_eval")
    # predict 固定（./predict 上書き）
    ap.add_argument("--predict-only", action="store_true")
    ap.add_argument("--predout", default="./predict")  # 互換のため受けるが未使用
    ap.add_argument("--pids", default="")
    ap.add_argument("--races", default="")
    args=ap.parse_args()

    dates=set([d.strip() for d in args.dates.split(",") if d.strip()]) if args.dates else set()
    pids_filter=set([p.strip() for p in args.pids.split(",") if p.strip()])
    races_filter=set([r.strip() for r in args.races.split(",") if r.strip()])

    # ---- predict-only ----
    if args.predict_only:
        int_idx=collect_files(args.base,"integrated",dates) if dates else collect_files(args.base,"integrated", set(os.listdir(os.path.join(args.base,"integrated","v1"))))
        keys=sorted(int_idx.keys())
        if pids_filter:  keys=[k for k in keys if k[1] in pids_filter]
        if races_filter: keys=[k for k in keys if k[2] in races_filter]

        pred_dir="./predict"
        if os.path.exists(pred_dir): shutil.rmtree(pred_dir)
        os.makedirs(pred_dir, exist_ok=True)

        rows=[]
        lim=args.limit or len(keys)
        for (date,pid,race) in keys[:lim]:
            with open(int_idx[(date,pid,race)],"r",encoding="utf-8") as f:
                tri_probs,_=simulate_one(json.load(f), sims=args.sims)
            top=sorted(tri_probs.items(), key=lambda kv: kv[1], reverse=True)[:args.topn]
            top_list=[{"ticket":"-".join(map(str,k)), "prob": round(v,6)} for k,v in top]
            with open(os.path.join(pred_dir, f"pred_{date}_{pid}_{race}.json"),"w",encoding="utf-8") as f:
                json.dump({"date":date,"pid":pid,"race":race,"topN":top_list,"engine":"SimS ver1.0"}, f, ensure_ascii=False, indent=2)
            for i,t in enumerate(top_list,1):
                rows.append({"date":date,"pid":pid,"race":race,"rank":i,"ticket":t["ticket"],"prob":t["prob"]})

        if rows: pd.DataFrame(rows).to_csv(os.path.join(pred_dir,"predictions_summary.csv"), index=False, encoding="utf-8")
        print("predict-only done -> ./predict")
        return

    # ---- eval ----
    int_idx = collect_files(args.base,"integrated",dates) if dates else collect_files(args.base,"integrated", set(os.listdir(os.path.join(args.base,"integrated","v1"))))
    odds_idx= collect_files(args.base,"odds",dates)       if dates else collect_files(args.base,"odds",       set(os.listdir(os.path.join(args.base,"odds"))))
    res_idx = collect_files(args.base,"results",dates)    if dates else collect_files(args.base,"results",    set(os.listdir(os.path.join(args.base,"results","v1"))))
    keys=sorted(set(int_idx.keys()) & set(odds_idx.keys()) & set(res_idx.keys()))
    if args.limit>0: keys=keys[:args.limit]

    per_rows=[]; total_stake=total_payout=total_hit=0
    for (date,pid,race) in keys:
        stake,payout,hit,bets,hit_combo = evaluate_one(int_idx[(date,pid,race)],odds_idx[(date,pid,race)],res_idx[(date,pid,race)], sims=args.sims, topn=args.topn, unit=args.unit)
        total_stake+=stake; total_payout+=payout; total_hit+=hit
        per_rows.append({"date":date,"pid":pid,"race":race,"bets":len(bets),"stake":stake,"payout":payout,"hit":hit,"hit_combo":hit_combo})

    df=pd.DataFrame(per_rows)
    overall={"engine":"SimS ver1.0","races":int(len(df)),"bets_total":int(df["bets"].sum()) if len(df)>0 else 0,
             "stake_total":int(total_stake),"payout_total":int(total_payout),
             "hit_rate":float(df["hit"].mean()) if len(df)>0 else 0.0,
             "roi": float((total_payout-total_stake)/total_stake) if total_stake>0 else 0.0,
             "topn":args.topn,"sims_per_race":args.sims,"unit":args.unit}

    by_date=None
    if len(df)>0:
        by_date=(df.groupby("date")
                   .agg(races=("race","count"),stake_total=("stake","sum"),payout_total=("payout","sum"),hit_rate=("hit","mean"))
                   .reset_index())
        by_date["roi"]=(by_date["payout_total"]-by_date["stake_total"])/by_date["stake_total"]

    os.makedirs(args.outdir, exist_ok=True)
    df.to_csv(os.path.join(args.outdir,"per_race_results.csv"), index=False)
    if by_date is not None: by_date.to_csv(os.path.join(args.outdir,"by_date_summary.csv"), index=False)
    with open(os.path.join(args.outdir,"overall.json"),"w",encoding="utf-8") as f: json.dump(overall,f,ensure_ascii=False,indent=2)

    print("=== OVERALL (SimS ver1.0) ===")
    print(json.dumps(overall, ensure_ascii=False, indent=2))
    if by_date is not None:
        print("\n=== BY DATE ===")
        print(by_date.to_string(index=False))

if __name__=="__main__": main()
