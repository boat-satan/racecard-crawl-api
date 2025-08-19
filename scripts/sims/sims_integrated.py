# sims_integrated.py — Pass2: ML×Keyman を反映して SimS 再シム（predict/eval 両対応）
import os, json, math, argparse, shutil, csv
from collections import Counter
import numpy as np
import pandas as pd

try:
    import tomllib
except Exception:
    tomllib = None

# ===== パラメータ/既定係数 =====
class Params:
    # SimS core
    b0=100.0; alpha_R=0.005; alpha_A=-0.010; alpha_Ap=-0.012
    theta=0.0285; a0=0.0; b_dt=15.0; cK=1.2
    tau_k=0.030
    beta_sq=0.006; beta_wk=0.004; k_turn_err=0.010; gamma_wall=0.006
    delta_first=0.70; delta_lineblock=0.5
    safe_margin_mu=0.005; safe_margin_sigma=0.003; p_safe_margin=0.20
    p_backoff=0.10; backoff_ST_shift=0.015; backoff_A_penalty=0.15
    p_cav=0.03; cav_A_penalty=0.25
    session_ST_shift_mu=0.0; session_ST_shift_sd=0.004
    session_A_bias_mu=0.0; session_A_bias_sd=0.10
    wind_theta_gain=0.002; wind_st_sigma_gain=0.5
    base_wake=0.20; extra_wake_when_outside=0.25
    decision_bias_mult=1.0

    # ML融合の既定
    ml_st_gain=0.30            # まくり系に比例して ST μ を前倒し（下げ）
    ml_A_gain=0.20             # A/Ap を攻め/守りでスケール
    ml_consistency=0.30        # 片寄りの緩和
    # セーフティガード
    ml_max_mu_advance=0.003    # ST μ 前倒し上限（秒）
    ml_max_sigma_shrink=0.20   # σ 縮小上限（割合）
    ml_max_A_scale=0.12        # A/Ap の倍率 |±| 上限（割合）

rng = np.random.default_rng(2025)
sigmoid = lambda x: 1/(1+math.exp(-x))

def _load_params_file(path: str) -> dict:
    if not path: return {}
    p = os.path.expanduser(path)
    if not os.path.isfile(p): raise FileNotFoundError(p)
    ext = os.path.splitext(p)[1].lower()
    if ext == ".json":
        return json.load(open(p, "r", encoding="utf-8"))
    if ext == ".toml":
        if tomllib is None: raise RuntimeError("toml は Python 3.11+")
        return tomllib.load(open(p, "rb"))
    raise ValueError(f"Unsupported: {ext}")

def _parse_set(expr: str) -> dict:
    out = {}
    if not expr: return out
    for kv in [p.strip() for p in expr.split(",") if p.strip()]:
        if "=" not in kv: continue
        k, v = kv.split("=", 1); k=k.strip(); v=v.strip()
        try:
            out[k] = (v.lower()=="true") if v.lower() in ("true","false") else (float(v) if any(c in v.lower() for c in ".e") else int(v))
        except: out[k]=v
    return out

def _apply_over(cls, d: dict):
    for k,v in d.items():
        if hasattr(cls,k): setattr(cls,k,v)

# ===== ユーティリティ =====
def _norm_race(r): 
    r=(r or "").strip().upper()
    return r if (not r or r.endswith("R")) else f"{r}R"

def _minmax_norm(d, keys):
    vs=[float(d.get(k,0.0)) for k in keys]; lo=min(vs) if vs else 0.0; hi=max(vs) if vs else 0.0
    den=(hi-lo) or 1.0
    return {k:(float(d.get(k,0.0))-lo)/den for k in keys}

def _bands(bands_str, omin, omax):
    if bands_str:
        out=[]
        for part in bands_str.split(","):
            if "-" not in part: continue
            lo_s,hi_s=[s.strip() for s in part.split("-",1)]
            lo=float(lo_s) if lo_s else float("-inf"); hi=float(hi_s) if hi_s else float("inf")
            if math.isfinite(lo) and math.isfinite(hi) and lo>hi: lo,hi=hi,lo
            out.append((lo,hi))
        return out
    if omin or omax:
        lo=float(omin) if omin>0 else float("-inf"); hi=float(omax) if omax>0 else float("inf")
        if math.isfinite(lo) and math.isfinite(hi) and lo>hi: lo,hi=hi,lo
        return [(lo,hi)]
    return []

def _in_band(odds,bands):
    if not bands: return True
    if odds is None or not math.isfinite(odds): return False
    return any(lo<=odds<=hi for lo,hi in bands)

# ===== ベース入力 =====
def _sbase(rc):
    n1=float(rc.get("natTop1",6.0)); n2=float(rc.get("natTop2",50.0)); n3=float(rc.get("natTop3",70.0))
    return 0.5*((n1-6)/2)+0.3*((n2-50)/20)+0.2*((n3-70)/20)

def _wind(env):
    d=(env.get("wind") or {}).get("dir","cross"); m=float((env.get("wind") or {}).get("mps",0.0))
    sign=1 if d=="tail" else -1 if d=="head" else 0
    return Params.wind_theta_gain*sign*m, 1.0+Params.wind_st_sigma_gain*(abs(m)/10.0)

def _apply_session(ST,A,Ap):
    ST+=rng.normal(Params.session_ST_shift_mu,Params.session_ST_shift_sd)
    g=lambda x: x*(1.0+rng.normal(Params.session_A_bias_mu,Params.session_A_bias_sd))
    return ST,g(A),g(Ap)

def _maybe_backoff(ST,A):
    if rng.random()<Params.p_backoff: return ST+Params.backoff_ST_shift, A*(1-Params.backoff_A_penalty), True
    return ST,A,False

def _maybe_cav(A):
    if rng.random()<Params.p_cav: return A*(1-Params.cav_A_penalty), True
    return A,False

def _maybe_safe():
    if rng.random()<Params.p_safe_margin: 
        return max(0.0, rng.normal(Params.safe_margin_mu, Params.safe_margin_sigma)), True
    return 0.0, False

def _wake_p(lane, entry):
    pos=entry.index(lane)
    base=Params.base_wake+Params.extra_wake_when_outside*((lane-1)/5.0)
    if pos==0: base*=0.3
    return max(0.0, min(base,0.95))

def build_input(d):
    lanes=[e["lane"] for e in d["entries"]]
    mu,S,F={}, {}, {}
    for e in d["entries"]:
        lane=e["lane"]; rc=e["racecard"]; ec=(e.get("stats") or {}).get("entryCourse",{})
        vals=[v for v in [rc.get("avgST"), ec.get("avgST")] if isinstance(v,(int,float))]
        m=0.16 if not vals else float(vals[0]) if len(vals)==1 else 0.5*float(vals[0])+0.5*float(vals[1])
        if int(rc.get("flyingCount",0))>0: m+=0.010
        mu[lane]=m; S[lane]=_sbase(rc); F[lane]=int(rc.get("flyingCount",0))
    ST_model={}
    for lane in lanes:
        sigma=0.02*(1+0.20*(1 if F[lane]>0 else 0)+0.15*max(0.0,-S[lane])); sigma*=1.0+0.1*(lane-1)
        ST_model[str(lane)]={"mu":mu[lane],"sigma":sigma}
    R={str(l):float({1:88,2:92,3:96,4:100,5:104,6:108}.get(l,100.0)) for l in lanes}
    cb={1:0.05,2:0.05,3:0.02,4:0.00,5:-0.05,6:-0.06}
    A,Ap={},{}
    for l in lanes:
        dST=(0.16-mu[l])*5.0
        A[l]=0.7*S[l]+0.3*dST
        Ap[l]=0.7*S[l]+0.3*cb.get(l,0.0)
    S1=S.get(1,0.0)
    squeeze={str(l):(0.0 if l==1 else min(max(0.0,(S1-S[l])*0.20),0.20)) for l in lanes}
    first_right=[]; lineblocks=[]
    if S1>0.30 and mu.get(1,0.16)<=0.17: first_right.append(1)
    if S.get(4,0.0)>0.10 and mu.get(4,0.16)<=0.17: first_right.append(4)
    if (S1 - S.get(2,0.0))>0.20: lineblocks.append((1,2))
    if (S.get(4,0.0)-S1)>0.05:
        sc4=next((e.get("startCourse",4) for e in d["entries"] if e["lane"]==4),4)
        if sc4>=4: lineblocks.append((4,1))
    env={"wind":{"dir":"cross","mps":0.0},"flow":{"dir":"none","rate":0.0}}
    return {"lanes":lanes,"ST_model":ST_model,"R":R,"A":A,"Ap":Ap,"env":env,
            "squeeze":squeeze,"first_right":set(first_right),"lineblocks":set(lineblocks)}

# ===== ML 取り込み・融合 =====
def load_ml_row(path_csv):
    rows=[]
    with open(path_csv, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(r)
    # lane -> dict(prob_*)
    out={}
    for r in rows:
        lane=int(r["lane"])
        out[lane]={
            "p_nige":float(r.get("prob_逃げ",0.0)),
            "p_sashi":float(r.get("prob_差し",0.0)),
            "p_makuri":float(r.get("prob_まくり",0.0)),
            "p_makuri_sashi":float(r.get("prob_まくり差し",0.0)),
            "p_nuki":float(r.get("prob_抜き",0.0)),
            "p_megumare":float(r.get("prob_恵まれ",0.0)),
        }
    return out

def apply_ml_adjustments(inp, ml_probs, st_gain=0.30, A_gain=0.20, consistency=0.30):
    """inp: build_input の戻り値を破壊的に更新。ガード込み。"""
    lanes=inp["lanes"]
    for l in lanes:
        probs=ml_probs.get(l, None)
        if not probs: continue
        p_att = probs["p_makuri"] + probs["p_makuri_sashi"]     # 攻め（外向き）
        p_def = probs["p_sashi"] + probs["p_nuki"] + probs["p_megumare"]  # 守り
        # 片寄り緩和
        p_att *= (1.0 + consistency*(-0.2))
        p_def *= (1.0 + consistency*(+0.1))

        # ST μ 前倒し（最大 0.003 s）
        mu0 = inp["ST_model"][str(l)]["mu"]
        dmu = -min(Params.ml_max_mu_advance, st_gain * 0.010 * p_att)  # 目安 0.0〜0.003
        inp["ST_model"][str(l)]["mu"] = max(0.05, mu0 + dmu)

        # σ縮小（控えめ）
        sigma0 = inp["ST_model"][str(l)]["sigma"]
        shrink = min(Params.ml_max_sigma_shrink, st_gain*0.5*p_att)    # 最大 20%
        inp["ST_model"][str(l)]["sigma"] = max(0.005, sigma0*(1.0 - shrink))

        # A/Ap の攻守スケール（±12%クランプ）
        scale = max(-Params.ml_max_A_scale, min(Params.ml_max_A_scale, A_gain*(p_att - p_def)))
        inp["A"][l]  *= (1.0 + scale)
        inp["Ap"][l] *= (1.0 + scale)

    return inp

# ===== 1レース・シミュ（再利用）=====
def _sample_ST(m): return rng.normal(m["mu"], m["sigma"])

def _t1m(ST,R,A,Ap,sq,env,lane,st_gain):
    ST,A,Ap=_apply_session(ST,A,Ap)
    ST,A,back=_maybe_backoff(ST,A)
    A,cav=_maybe_cav(A)
    t=Params.b0+Params.alpha_R*(R-100.0)+Params.alpha_A*A+Params.alpha_Ap*Ap+Params.beta_sq*sq
    t+=ST*st_gain
    return t, {"backoff":back,"cav":cav}

def _one_pass(entry,T1M,A,Ap,env,lineblocks,first_right,aggr=None):
    aggr=aggr or {}
    exit_order=entry[:]; swaps=[]; blocks=[]; safe_cnt=0
    d_theta,_=_wind(env); theta_eff=Params.theta+d_theta
    for k in range(len(exit_order)-1):
        lead, chase=exit_order[k], exit_order[k+1]
        dt=T1M[chase]-T1M[lead]
        dK=(A[chase]+Ap[chase])-(A[lead]+Ap[lead])
        delta=(Params.delta_lineblock if (lead,chase) in lineblocks else 0.0)
        if str(lead) in aggr: delta+=0.10*float(aggr[str(lead)])
        if lead in first_right: delta+=Params.delta_first
        terr,used=_maybe_safe(); 
        if used: safe_cnt+=1
        logit=Params.a0+Params.b_dt*(theta_eff-(dt+Params.gamma_wall+Params.k_turn_err*terr))+Params.cK*dK+delta
        if str(chase) in aggr: logit+=0.45*float(aggr[str(chase)])
        logit*= (Params.decision_bias_mult or 1.0)
        if rng.random()<sigmoid(logit):
            swaps.append((chase,lead)); exit_order[k],exit_order[k+1]=chase,lead
        else:
            if delta>0: blocks.append((lead,chase))
    return exit_order, swaps, blocks, safe_cnt

def simulate_one(inp, sims=600, boost_map=None, aggr_map=None):
    # keyman boost/aggr
    if boost_map:
        for l in list(inp["A"].keys()):
            b=float(boost_map.get(str(l),0.0))
            if b: inp["A"][l]*=(1.0+b); inp["Ap"][l]*=(1.0+b)
    aggr_map=aggr_map or {}
    if aggr_map:
        for k in list(inp["ST_model"].keys()):
            a=float(aggr_map.get(str(k),0.0))
            if a>0:
                inp["ST_model"][k]["mu"]=max(0.05, inp["ST_model"][k]["mu"]-0.006*a)
                inp["ST_model"][k]["sigma"]=max(0.005, inp["ST_model"][k]["sigma"]*(1-0.35*a))

    lanes=inp["lanes"]; env=inp["env"]; _,st_gain=_wind(env)
    trif=Counter(); ex2=Counter(); thd=Counter()
    for _ in range(sims):
        ST={i:_sample_ST(inp["ST_model"][str(i)]) for i in lanes}
        T1M={}
        for i in lanes:
            t,_fl=_t1m(ST[i], inp["R"][str(i)], inp["A"][i], inp["Ap"][i], inp["squeeze"][str(i)], env, i, st_gain)
            T1M[i]=t
        entry=sorted(lanes, key=lambda x:T1M[x])
        for i in lanes:
            p=_wake_p(i, entry)
            if aggr_map:
                p = p*(1-0.60*float(aggr_map.get(str(i),0.0))) if str(i) in aggr_map else p*(1+0.05*max(aggr_map.values(), default=0.0))
            if rng.random()<min(0.95,max(0.0,p)):
                T1M[i]+=Params.beta_wk
        exit_order, _, _, _ = _one_pass(entry, T1M, inp["A"], inp["Ap"], env, inp["lineblocks"], inp["first_right"], aggr_map)
        trif[tuple(exit_order[:3])]+=1; ex2[(exit_order[0],exit_order[1])]+=1; thd[exit_order[2]]+=1

    total=sims
    tri_probs={k:v/total for k,v in trif.items()}
    ex_probs={k:v/total for k,v in ex2.items()}
    th_probs={k:v/total for k,v in thd.items()}
    return tri_probs, ex_probs, th_probs

# ===== 生成/フィルタ/評価 =====
def generate_tickets(tri, ex2, th3, topn=18, strategy="trifecta_topN", k=2, m=4,
                     exclude_first1=False, only_first1=False):
    if strategy=="exacta_topK_third_topM":
        top2=sorted(ex2.items(), key=lambda kv: kv[1], reverse=True)[:k]
        top3=[t for t,_ in sorted(th3.items(), key=lambda kv: kv[1], reverse=True)[:m]]
        seen=set(); out=[]
        for (f,s), p2 in top2:
            for t in top3:
                if t!=f and t!=s:
                    key=(f,s,t)
                    if key in seen: continue
                    seen.add(key)
                    out.append((key, p2*th3.get(t,0.0)))
        out=[(k_,p_) for (k_,p_) in out if ((not only_first1) or k_[0]==1) and ((not exclude_first1) or k_[0]!=1)]
        return sorted(out, key=lambda kv: kv[1], reverse=True)
    top=sorted(tri.items(), key=lambda kv: kv[1], reverse=True)[:topn]
    return [(k_,p_) for (k_,p_) in top if ((not only_first1) or k_[0]==1) and ((not exclude_first1) or k_[0]!=1)]

def load_odds(odds_base,date,pid,race):
    try:
        race=race if race.upper().endswith("R") else f"{race}R"
        path=os.path.join(odds_base,date,pid,f"{race}.json")
        if not os.path.isfile(path): return {}
        trif=(json.load(open(path,"r",encoding="utf-8")).get("trifecta")) or []
        out={}
        for row in trif:
            combo=str(row.get("combo") or "").strip()
            if not combo:
                F,S,T=row.get("F"),row.get("S"),row.get("T")
                if all(isinstance(v,(int,float)) for v in [F,S,T]): combo=f"{int(F)}-{int(S)}-{int(T)}"
            if not combo: continue
            odds=row.get("odds")
            if isinstance(odds,(int,float)) and math.isfinite(odds): out[combo]={"odds":float(odds)}
        return out
    except: return {}

def _actual_trifecta_and_amount(res_json: dict):
    trif=(res_json or {}).get("payouts",{}).get("trifecta")
    combo=None; amt=0
    if isinstance(trif,dict):
        combo=trif.get("combo"); amt=int(trif.get("amount") or 0)
    if not combo and isinstance(res_json,dict):
        order=res_json.get("order")
        if isinstance(order,list) and len(order)>=3:
            def lane(x): return str(x.get("lane") or x.get("course") or x.get("F") or x.get("number"))
            try:
                f,s,t=lane(order[0]),lane(order[1]),lane(order[2])
                if all([f,s,t]): combo=f"{f}-{s}-{t}"
            except: pass
    return combo, amt

def load_results(base,date,pid,race):
    # results/v1/<date>/<pid>/<race>.json  or  results/v1/<date>/<pid>/summary.json#<race>
    root_v1=os.path.join(base,"results","v1"); root=root_v1 if os.path.isdir(root_v1) else os.path.join(base,"results")
    dirp=os.path.join(root,date,pid)
    # per-race
    cand=os.path.join(dirp, f"{race if race.endswith('R') else race+'R'}.json")
    if os.path.isfile(cand): return json.load(open(cand,"r",encoding="utf-8"))
    # summary
    for f in os.listdir(dirp):
        if not f.lower().endswith(".json"): continue
        data=json.load(open(os.path.join(dirp,f),"r",encoding="utf-8"))
        cont=data.get("races", data) if isinstance(data,dict) else {}
        rk=_norm_race(race)
        if rk in cont: return cont[rk]
    return {}

# ===== キーマン読み込み =====
def load_keyman(pass1_dir, date, pid, race):
    p=os.path.join(pass1_dir,"keyman",date,pid,f"{race}.json")
    if not os.path.isfile(p): return {}
    d=json.load(open(p,"r",encoding="utf-8"))
    return (d.get("keyman") or {})

def keyman_maps(pass1_dir,date,pid,race,thr=0.70, boost=0.15, aggr=0.25):
    km=load_keyman(pass1_dir,date,pid,race)
    kmr=(km.get("KEYMAN_RANK") or {})
    lanes=[k for k,v in kmr.items() if isinstance(v,(int,float)) and float(v)>=thr]
    boost_map={str(int(l)):float(boost) for l in lanes}
    aggr_map ={str(int(l)):float(aggr)  for l in lanes}
    return boost_map, aggr_map, km

# ===== メインパス =====
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--base",default="./public")
    ap.add_argument("--dates",default="")
    ap.add_argument("--pids",default="")
    ap.add_argument("--races",default="")
    ap.add_argument("--sims",type=int,default=600)
    ap.add_argument("--topn",type=int,default=18)
    ap.add_argument("--k",type=int,default=2)
    ap.add_argument("--m",type=int,default=4)
    ap.add_argument("--exclude-first1",action="store_true")
    ap.add_argument("--only-first1",action="store_true")
    ap.add_argument("--limit",type=int,default=0)
    ap.add_argument("--outdir",default="./SimS_v1.0_eval")
    ap.add_argument("--predict-only",action="store_true")
    # ML
    ap.add_argument("--ml-root",default="./ml/predictions")
    ap.add_argument("--ml-st-gain",type=float,default=Params.ml_st_gain)
    ap.add_argument("--ml-A-gain",type=float,default=Params.ml_A_gain)
    ap.add_argument("--ml-consistency",type=float,default=Params.ml_consistency)
    # keyman
    ap.add_argument("--keyman-threshold",type=float,default=0.70)
    ap.add_argument("--keyman-boost",type=float,default=0.15)
    ap.add_argument("--keyman-aggr",type=float,default=0.25)
    # odds/results
    ap.add_argument("--odds-base",default="./public/odds/v1")
    ap.add_argument("--min-ev",type=float,default=0.0)
    ap.add_argument("--require-odds",action="store_true")
    ap.add_argument("--odds-bands",default="")
    ap.add_argument("--odds-min",type=float,default=0.0)
    ap.add_argument("--odds-max",type=float,default=0.0)
    # params override
    ap.add_argument("--params",default="")
    ap.add_argument("--set",default="")
    # log
    ap.add_argument("--log-level",default="info", choices=["warn","info","debug"])
    args=ap.parse_args()

    # Param override
    if args.params: _apply_over(Params, _load_params_file(args.params))
    over=_parse_set(getattr(args,"set","")); 
    if over: _apply_over(Params, over)

    root_out=os.path.abspath(args.outdir)
    pass1_dir=os.path.join(root_out, "pass1")
    pass2_dir=os.path.join(root_out, "pass2")
    os.makedirs(pass2_dir, exist_ok=True)

    # active params snapshot（監査）
    try:
        active={k:getattr(Params,k) for k in dir(Params) if not k.startswith("_") and isinstance(getattr(Params,k),(int,float,bool))}
        json.dump(active, open(os.path.join(pass2_dir,"active_params.json"),"w",encoding="utf-8"), ensure_ascii=False, indent=2)
    except: pass

    # キー集合
    root_v1=os.path.join(args.base,"integrated","v1"); root_integrated = root_v1 if os.path.isdir(root_v1) else os.path.join(args.base,"integrated")
    dates=set([d.strip() for d in args.dates.split(",") if d.strip()]) if args.dates else set()
    pids_filter=set([p.strip() for p in args.pids.split(",") if p.strip()])
    races_filter=set([_norm_race(r) for r in args.races.split(",") if r.strip()])

    int_idx={}
    for d in (list(dates) if dates else [x for x in os.listdir(root_integrated) if os.path.isdir(os.path.join(root_integrated,x))]):
        dir_d=os.path.join(root_integrated,d)
        for pid in os.listdir(dir_d):
            dir_p=os.path.join(dir_d,pid)
            if not os.path.isdir(dir_p): continue
            for f in os.listdir(dir_p):
                if f.endswith(".json"):
                    race=f[:-5]
                    int_idx[(d,pid,race)]=os.path.join(dir_p,f)

    keys=sorted(int_idx.keys())
    if pids_filter: keys=[k for k in keys if k[1] in pids_filter]
    if races_filter: keys=[k for k in keys if _norm_race(k[2]) in races_filter]
    if args.limit and args.limit>0: keys=keys[:args.limit]

    bands=_bands(args.odds_bands, args.odds_min, args.odds_max)
    pred_dir=os.path.join(pass2_dir,"predict")
    if os.path.exists(pred_dir): shutil.rmtree(pred_dir)
    os.makedirs(pred_dir, exist_ok=True)
    rows_summary=[]

    for (date,pid,race) in keys:
        # 入力
        d_int=json.load(open(int_idx[(date,pid,race)],"r",encoding="utf-8"))
        inp=build_input(d_int)

        # ML 読み込み（無ければ素通し）
        ml_csv=os.path.join(args.ml_root, date, pid, f"{_norm_race(race)}.csv")
        if not os.path.isfile(ml_csv):
            # 代替： race が "8R" などの場合、"8R" で保存していると仮定
            ml_csv=os.path.join(args.ml_root, date, pid, f"{race if race.endswith('R') else race+'R'}.csv")
        ml_probs={}
        if os.path.isfile(ml_csv):
            try:
                ml_probs=load_ml_row(ml_csv)
            except Exception as e:
                if args.log_level!="warn":
                    print(f"[warn] ML CSV read failed {ml_csv}: {e}")
        else:
            if args.log_level=="debug":
                print(f"[debug] ML CSV not found -> skip ML for {date}/{pid}/{race}")

        # ML 調整
        if ml_probs:
            inp = apply_ml_adjustments(inp, ml_probs, st_gain=args.ml_st_gain, A_gain=args.ml_A_gain, consistency=args.ml_consistency)

        # keyman ブースト/アグレ
        boost_map, aggr_map, kmjson = keyman_maps(pass1_dir,date,pid,race, args.keyman_threshold, args.keyman_boost, args.keyman_aggr)

        # 監査ダンプ
        audit_dir=os.path.join(pass2_dir,"audit",date,pid); os.makedirs(audit_dir, exist_ok=True)
        json.dump({
            "date":date,"pid":pid,"race":race,
            "ml_csv_exists": bool(ml_probs),
            "ml_coeffs": {"st_gain":args.ml_st_gain,"A_gain":args.ml_A_gain,"consistency":args.ml_consistency},
            "keyman": {"threshold":args.keyman_threshold,"boost":args.keyman_boost,"aggr":args.keyman_aggr},
            "boost_map": boost_map, "aggr_map": aggr_map
        }, open(os.path.join(audit_dir,f"{race}.json"),"w",encoding="utf-8"), ensure_ascii=False, indent=2)

        # 再シム
        tri, ex2, th3 = simulate_one(inp, sims=args.sims, boost_map=boost_map, aggr_map=aggr_map)
        tickets = generate_tickets(tri, ex2, th3, topn=args.topn, strategy="trifecta_topN",
                                   k=args.k, m=args.m, exclude_first1=args.exclude_first1, only_first1=args.only_first1)

        # オッズ/EV フィルタ（predict でも要求されれば適用）
        odds_map={}
        if (args.min_ev>0) or args.require_odds or bands:
            odds_map=load_odds(args.odds_base, date, pid, race)
        kept=[]
        for (key,prob) in tickets:
            combo="-".join(map(str,key)); rec=odds_map.get(combo); odds=rec["odds"] if rec else None
            if bands and (odds is None or not _in_band(odds,bands)): continue
            if (not bands) and args.require_odds and odds is None: continue
            if args.min_ev>0 and odds is not None and prob*odds<args.min_ev: continue
            kept.append((key,prob,odds))
        tickets=kept

        # 出力（predict JSON/CSV）
        out_list=[]
        for (key,p,odds) in tickets:
            ev = (p*odds) if (odds is not None) else None
            out_list.append({"ticket":"-".join(map(str,key)),"score":round(p,6),"odds":(None if odds is None else float(odds)),"ev":(None if ev is None else round(ev,6))})
        json.dump({"date":date,"pid":pid,"race":race,"buylist":out_list,
                   "engine":"SimS integrated (E1+ML)","sims_per_race":int(args.sims),
                   "ml_coeffs":{"st_gain":args.ml_st_gain,"A_gain":args.ml_A_gain,"consistency":args.ml_consistency},
                   "keyman":{"threshold":args.keyman_threshold,"boost":args.keyman_boost,"aggr":args.keyman_aggr},
                   "exclude_first1":bool(args.exclude_first1),"only_first1":bool(args.only_first1),
                   "min_ev":float(args.min_ev),"require_odds":bool(args.require_odds),
                   "odds_bands":args.odds_bands or "","odds_min":float(args.odds_min),"odds_max":float(args.odds_max)},
                  open(os.path.join(pred_dir,f"pred_{date}_{pid}_{race}.json"),"w",encoding="utf-8"),
                  ensure_ascii=False, indent=2)

        for i,t in enumerate(out_list,1):
            rows_summary.append({"date":date,"pid":pid,"race":race,"rank":i,"ticket":t["ticket"],"score":t["score"],"odds":t["odds"],"ev":t["ev"]})

        # eval のみ：的中/ROI
        if not args.predict_only:
            d_res = load_results(args.base, date, pid, race)
            hit_combo, pay = _actual_trifecta_and_amount(d_res)
            bets=[t["ticket"] for t in out_list]
            hit = 1 if hit_combo in bets else 0
            # 1点100円固定（unit はワークフローで外から渡す前提なら追加してOK）
            # ここでは JSON 側は predict 中心なので集計はワークフローで揃える想定
            if args.log_level=="debug":
                print(f"[debug] {date}/{pid}/{race}: bets={len(bets)}, hit={hit}, hit_combo={hit_combo}, payout={pay}")

    # summary CSV
    pd.DataFrame(rows_summary).to_csv(os.path.join(pred_dir,"predictions_summary.csv"), index=False, encoding="utf-8")
    print(f"[predict/pass2] {len(keys)} races -> {pred_dir}")
    if not args.predict_only:
        print("[eval] 集計はワークフロー側で per_race/overall にまとめる設計（必要なら追加実装可）")

if __name__=="__main__":
    main()
