# simsv2.py — SimS ver1.0 (keyman-free) + ML補正対応 最小変更版
# 変更点:
# - キーマン関連の引数/処理/保存を全削除
# - --ml-root から B の CSV を読み込み、ST/A/Ap/R を軽微に補正
# - 既存の入出力(予測JSON/CSV, overall.json)と乱数の位置は極力維持

import os, json, math, argparse, shutil, csv
from collections import Counter
import numpy as np
import pandas as pd

# ===== パラメータ上書きユーティリティ =====
try:
    import tomllib
except Exception:
    tomllib = None

def _load_params_file(path: str) -> dict:
    if not path: return {}
    p = os.path.expanduser(path)
    if not os.path.isfile(p): raise FileNotFoundError(p)
    ext = os.path.splitext(p)[1].lower()
    if ext == ".json":
        return json.load(open(p, "r", encoding="utf-8"))
    if ext == ".toml":
        if tomllib is None: raise RuntimeError("tomlはPython3.11+が必要")
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

# ===== SimS ver1.0 Params =====
class Params:
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

# ML補正の上限（安全弁）
ML_MAX_MU_ADVANCE = 0.003     # ST μ 前倒し最大秒
ML_MAX_SIGMA_SHRINK = 0.20    # σ縮小の最大割合(20%)
ML_MAX_A_SCALE = 0.12         # A/Ap倍率の |±| 上限(12%)
ML_MAX_R_SHIFT = 2.0          # Rの微調整(±)

rng = np.random.default_rng(2025)

# ===== 共通小物 =====
def sigmoid(x): return 1/(1+math.exp(-x))

def _minmax_norm(d, keys):
    vs=[float(d.get(k,0.0)) for k in keys]; lo=min(vs) if vs else 0.0; hi=max(vs) if vs else 0.0
    den=(hi-lo) or 1.0
    return {k:(float(d.get(k,0.0))-lo)/den for k in keys}

def _norm_race(r): 
    r=(r or "").strip().upper()
    return r if (not r or r.endswith("R")) else f"{r}R"

def _parse_bool(v, default):
    if v is None: return default
    s=str(v).strip().lower()
    return True if s in ("1","true","yes","y","on") else False if s in ("0","false","no","n","off") else default

# ===== 変換・環境 =====
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

# ===== ML補正読込 & 適用 =====
def _load_ml_csv(ml_root:str, date:str, pid:str, race:str):
    """Bが吐く CSV を読み込み lane→dict を返す。無い場合は空。"""
    if not ml_root: return {}
    race_norm = race if race.upper().endswith("R") else f"{race}R"
    p=os.path.join(ml_root, date, pid, f"{race_norm}.csv")
    if not os.path.isfile(p): 
        # 互換
        alt=os.path.join(ml_root, date, pid, f"{race}.csv")
        if not os.path.isfile(alt): return {}
        p=alt
    out={}
    with open(p, newline="", encoding="utf-8") as f:
        rd=csv.DictReader(f)
        for r in rd:
            try:
                l=int(r.get("lane") or r.get("L") or 0)
                if l<=0: continue
                out[l]={
                    "win_prob": float(r.get("win_prob",0) or 0),
                    "p_makuri": float(r.get("prob_まくり",0) or 0),
                    "p_makuri_sashi": float(r.get("prob_まくり差し",0) or 0),
                    "p_sashi": float(r.get("prob_差し",0) or 0),
                    "p_nige": float(r.get("prob_逃げ",0) or 0),
                    "p_nuki": float(r.get("prob_抜き",0) or 0),
                    "p_megumare": float(r.get("prob_恵まれ",0) or 0),
                }
            except: pass
    return out

def _apply_ml_adjustments(inp, ml_map, st_gain=0.30, A_gain=0.20, consistency=0.30, win_gain=0.50):
    """inp を破壊的に更新。ガード付きで ST/A/Ap/R を微調整。"""
    if not ml_map: return inp
    lanes=inp["lanes"]
    for l in lanes:
        probs=ml_map.get(l); 
        if not probs: continue
        p_att = (probs.get("p_makuri",0.0)+probs.get("p_makuri_sashi",0.0))
        p_def = (probs.get("p_sashi",0.0)+probs.get("p_nuki",0.0)+probs.get("p_megumare",0.0))
        # 片寄り緩和（ほんの少し）
        p_att*= (1.0 - 0.2*consistency)
        p_def*= (1.0 + 0.1*consistency)

        # ST μ 前倒し（上限 0.003s）
        mu0=inp["ST_model"][str(l)]["mu"]
        dmu= -min(ML_MAX_MU_ADVANCE, st_gain*0.010*p_att)
        inp["ST_model"][str(l)]["mu"]=max(0.05, mu0 + dmu)

        # σ縮小（最大 20%）
        sg0=inp["ST_model"][str(l)]["sigma"]
        shrink=min(ML_MAX_SIGMA_SHRINK, st_gain*0.5*p_att)
        inp["ST_model"][str(l)]["sigma"]=max(0.005, sg0*(1.0 - shrink))

        # A/Ap スケール（±12%）
        scale = max(-ML_MAX_A_SCALE, min(ML_MAX_A_SCALE, A_gain*(p_att - p_def)))
        inp["A"][l]*=(1.0+scale); inp["Ap"][l]*=(1.0+scale)

        # R 微調整（win_prob に比例、±ML_MAX_R_SHIFT）
        winp=float(probs.get("win_prob",0.0))
        dR = max(-ML_MAX_R_SHIFT, min(ML_MAX_R_SHIFT, win_gain*2.0*(winp-1.0/6.0)))
        inp["R"][str(l)] = float(inp["R"][str(l)]) + dR
    return inp

# ===== 1レース・シミュ（乱数順不変）=====
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

def simulate_one(integrated_json, sims=600, ml_map=None, ml_st_gain=0.30, ml_A_gain=0.20, ml_consistency=0.30, ml_win_gain=0.50):
    inp=build_input(integrated_json)
    # ML 補正（キーマンは一切なし）
    if ml_map:
        _apply_ml_adjustments(inp, ml_map, st_gain=ml_st_gain, A_gain=ml_A_gain, consistency=ml_consistency, win_gain=ml_win_gain)

    lanes=inp["lanes"]; env=inp["env"]; _,st_gain=_wind(env)
    trif=Counter(); kim=Counter(); ex2=Counter(); thd=Counter()
    H1=Counter(); H2=Counter(); H3=Counter()
    wake=Counter(); back=Counter(); cav=Counter()
    swp=Counter(); blk=Counter(); safe_total=0
    posd={i:0 for i in lanes}

    for _ in range(sims):
        ST={i:_sample_ST(inp["ST_model"][str(i)]) for i in lanes}
        T1M={}
        for i in lanes:
            t,fl=_t1m(ST[i], inp["R"][str(i)], inp["A"][i], inp["Ap"][i], inp["squeeze"][str(i)], env, i, st_gain)
            T1M[i]=t
            if fl["backoff"]: back[i]+=1
            if fl["cav"]: cav[i]+=1
        entry=sorted(lanes, key=lambda x:T1M[x])

        for i in lanes:
            p=_wake_p(i, entry)
            if rng.random()<min(0.95,max(0.0,p)):
                wake[i]+=1; T1M[i]+=Params.beta_wk

        exit_order, swaps, blocks, safe_cnt = _one_pass(entry, T1M, inp["A"], inp["Ap"], env, inp["lineblocks"], inp["first_right"], None)
        safe_total+=safe_cnt

        lead=exit_order[0]; dt_lead=T1M[exit_order[1]]-T1M[lead]
        kim["逃げ" if lead==1 else ("まくり" if dt_lead>=Params.tau_k else "まくり差し")]+=1

        trif[tuple(exit_order[:3])]+=1; ex2[(exit_order[0],exit_order[1])]+=1; thd[exit_order[2]]+=1
        H1[exit_order[0]]+=1; H2[exit_order[1]]+=1; H3[exit_order[2]]+=1
        for c,l in swaps: swp[(c,l)]+=1
        for l,c in blocks: blk[(l,c)]+=1
        ent_pos={b:i for i,b in enumerate(entry)}; ex_pos={b:i for i,b in enumerate(exit_order)}
        for i in lanes: posd[i]+= (ent_pos[i]-ex_pos[i])

    total=sims
    tri_probs={k:v/total for k,v in trif.items()}
    kim_probs={k:v/total for k,v in kim.items()}
    ex_probs={k:v/total for k,v in ex2.items()}
    th_probs={k:v/total for k,v in thd.items()}
    return tri_probs, kim_probs, ex_probs, th_probs

# ===== ファイル収集/読込 =====
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
            for f in [f for f in os.listdir(dir_pid) if f.lower().endswith(".json")]:
                p=os.path.join(dir_pid,f)
                try:
                    data=json.load(open(p,"r",encoding="utf-8"))
                    container=data.get("races", data) if isinstance(data,dict) else {}
                    for rk in list(container.keys()):
                        k=str(rk).upper(); 
                        if k.isdigit(): k+= "R"
                        if k.endswith("R"): out[(d,pid,k)]=p+"#"+k
                except: pass
    return out

def _load_result(res_path):
    if "#" in res_path:
        p,r=res_path.split("#",1); data=json.load(open(p,"r",encoding="utf-8")); cont=data.get("races",data) if isinstance(data,dict) else {}
        d=cont.get(r) or cont.get(r.upper()) or cont.get(r.lower()); return d if isinstance(d,dict) else {}
    return json.load(open(res_path,"r",encoding="utf-8"))

def _load_odds(odds_base,date,pid,race):
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

# ===== オッズ帯 =====
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

# ===== 生成/フィルタ =====
def generate_tickets(strategy, tri, ex2, th3, topn=18, k=2, m=4, exclude_first1=False, only_first1=False):
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

# ===== 評価 =====
def _actual_trifecta_and_amount(res):
    trif=(res or {}).get("payouts",{}).get("trifecta")
    combo=None; amt=0
    if isinstance(trif,dict):
        combo=trif.get("combo"); amt=int(trif.get("amount") or 0)
    if not combo and isinstance(res,dict):
        order=res.get("order")
        if isinstance(order,list) and len(order)>=3:
            def lane(x): return str(x.get("lane") or x.get("course") or x.get("F") or x.get("number"))
            try:
                f,s,t=lane(order[0]),lane(order[1]),lane(order[2])
                if all([f,s,t]): combo=f"{f}-{s}-{t}"
            except: pass
    return combo, amt

def evaluate_one(int_path,res_path,sims,unit,strategy,topn,k,m,exclude_first1=False,only_first1=False,
                 odds_base=None,min_ev=0.0,require_odds=False,odds_bands=None,outdir="./SimS_v1.0_eval",
                 ml_root="", ml_st_gain=0.30, ml_A_gain=0.20, ml_consistency=0.30, ml_win_gain=0.50):
    d_int=json.load(open(int_path,"r",encoding="utf-8"))

    # ML 読込み
    date=pid=race=None
    try:
        p=os.path.normpath(int_path).split(os.sep); race=os.path.splitext(p[-1])[0]; pid=p[-2]; date=p[-3]
    except: pass
    ml_map=_load_ml_csv(ml_root, date, pid, race) if ml_root else {}

    tri,kim,ex2,th3=simulate_one(d_int,sims=sims, ml_map=ml_map,
                                 ml_st_gain=ml_st_gain, ml_A_gain=ml_A_gain,
                                 ml_consistency=ml_consistency, ml_win_gain=ml_win_gain)
    tickets=generate_tickets(strategy,tri,ex2,th3,topn,k,m,exclude_first1,only_first1)

    odds_map={}
    if (min_ev>0) or require_odds or odds_bands:
        if date and pid and race: odds_map=_load_odds(odds_base,date,pid,race)
    kept=[]
    bands=odds_bands or []
    for (key,prob) in tickets:
        combo="-".join(map(str,key)); rec=odds_map.get(combo); odds=rec["odds"] if rec else None
        if bands and (odds is None or not _in_band(odds,bands)): continue
        if (not bands) and require_odds and odds is None: continue
        if min_ev>0 and odds is not None and prob*odds<min_ev: continue
        kept.append((key,prob))
    tickets=kept

    bets=['-'.join(map(str,k)) for k,_ in tickets]; stake=unit*len(bets)
    d_res=_load_result(res_path) if res_path else {}
    hit_combo, pay=_actual_trifecta_and_amount(d_res)
    payout=pay if hit_combo in bets else 0

    return {"stake":stake,"payout":payout,"hit":1 if payout>0 else 0, "bets":bets,
            "hit_combo":hit_combo,"tri_probs":tri,"kim_probs":kim}

# ===== メイン =====
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--base",default="./public"); ap.add_argument("--dates",default="")
    ap.add_argument("--sims",type=int,default=600); ap.add_argument("--topn",type=int,default=18)
    ap.add_argument("--unit",type=int,default=100); ap.add_argument("--limit",type=int,default=0)
    ap.add_argument("--outdir",default="./SimS_v1.0_eval")
    ap.add_argument("--predict-only",action="store_true")
    ap.add_argument("--pids",default=""); ap.add_argument("--races",default="")
    ap.add_argument("--strategy",default="trifecta_topN",choices=["trifecta_topN","exacta_topK_third_topM"])
    ap.add_argument("--k",type=int,default=2); ap.add_argument("--m",type=int,default=4)
    ap.add_argument("--exclude-first1",action="store_true"); ap.add_argument("--only-first1",action="store_true")
    ap.add_argument("--odds-base",default="./public/odds/v1")
    ap.add_argument("--min-ev",type=float,default=0.0); ap.add_argument("--require-odds",action="store_true")
    ap.add_argument("--odds-bands",default=""); ap.add_argument("--odds-min",type=float,default=0.0); ap.add_argument("--odds-max",type=float,default=0.0)
    ap.add_argument("--params",default=""); ap.add_argument("--set",default="")
    # ML補正
    ap.add_argument("--ml-root",default="")                         # 例: TENKAI/predictions/v1
    ap.add_argument("--ml-st-gain",type=float,default=0.30)
    ap.add_argument("--ml-A-gain",type=float,default=0.20)
    ap.add_argument("--ml-consistency",type=float,default=0.30)
    ap.add_argument("--ml-win-gain",type=float,default=0.50)
    args=ap.parse_args()

    if args.exclude_first1 and args.only_first1: raise SystemExit("--exclude-first1 と --only_first1 は同時指定不可")

    # Param 上書き
    try:
        if args.params: _apply_over(Params, _load_params_file(args.params))
        over=_parse_set(getattr(args,"set","")); 
        if over: _apply_over(Params, over)
    except Exception as e:
        raise SystemExit(f"[params] override failed: {e}")

    root_out=os.path.abspath(args.outdir); pass1_dir=os.path.join(root_out,"pass1")
    os.makedirs(pass1_dir, exist_ok=True)
    try:
        active={k:getattr(Params,k) for k in dir(Params) if not k.startswith("_") and isinstance(getattr(Params,k),(int,float,bool))}
        json.dump(active, open(os.path.join(pass1_dir,"active_params.json"),"w",encoding="utf-8"), ensure_ascii=False, indent=2)
    except: pass

    bands=_bands(args.odds_bands, args.odds_min, args.odds_max)
    dates=set([d.strip() for d in args.dates.split(",") if d.strip()]) if args.dates else set()
    pids_filter=set([p.strip() for p in args.pids.split(",") if p.strip()])
    races_filter=set([_norm_race(r) for r in args.races.split(",") if r.strip()])

    # --- インデックス ---
    int_idx=_collect(args.base,"integrated",dates) if dates else _collect(args.base,"integrated", set(os.listdir(os.path.join(args.base,"integrated","v1"))))

    # predict-only なら results を見ない
    if args.predict_only:
        keys = sorted(int_idx.keys())
    else:
        res_idx=_collect_results(args.base,dates)
        keys=sorted(set(int_idx.keys()) & set(res_idx.keys()))

    if pids_filter: keys=[k for k in keys if k[1] in pids_filter]
    if races_filter: keys=[k for k in keys if _norm_race(k[2]) in races_filter]
    if args.limit and args.limit>0: keys=keys[:args.limit]

    # predict-only（オッズ未使用でOK）
    if args.predict_only:
        pred_dir=os.path.join(pass1_dir,"predict")
        if os.path.exists(pred_dir): shutil.rmtree(pred_dir)
        os.makedirs(pred_dir, exist_ok=True)
        rows=[]; lim=args.limit or len(keys)
        for (date,pid,race) in keys[:lim]:
            d_int=json.load(open(int_idx[(date,pid,race)],"r",encoding="utf-8"))
            # ML 読込み
            ml_map=_load_ml_csv(args.ml_root, date, pid, race) if args.ml_root else {}
            tri,kim,ex2,th3=simulate_one(d_int,sims=args.sims, ml_map=ml_map,
                                         ml_st_gain=args.ml_st_gain, ml_A_gain=args.ml_A_gain,
                                         ml_consistency=args.ml_consistency, ml_win_gain=args.ml_win_gain)
            tickets=generate_tickets(args.strategy,tri,ex2,th3,args.topn,args.k,args.m,args.exclude_first1,args.only_first1)
            out_list=[{"ticket":"-".join(map(str,k)),"score":round(p,6),"odds":None,"ev":None} for (k,p) in tickets]
            json.dump({"date":date,"pid":pid,"race":race,"buylist":out_list,
                       "engine":"SimS ver1.0 (E1, ML-adjust)","exclude_first1":bool(args.exclude_first1),
                       "only_first1":bool(args.only_first1),"min_ev":float(args.min_ev),
                       "require_odds":bool(args.require_odds),"odds_bands":args.odds_bands or "",
                       "odds_min":float(args.odds_min),"odds_max":float(args.odds_max),
                       "ml_root": args.ml_root,
                       "ml_coeffs":{"st_gain":args.ml_st_gain,"A_gain":args.ml_A_gain,"consistency":args.ml_consistency,"win_gain":args.ml_win_gain}},
                      open(os.path.join(pred_dir,f"pred_{date}_{pid}_{race}.json"),"w",encoding="utf-8"),
                      ensure_ascii=False, indent=2)
            for i,t in enumerate(out_list,1):
                rows.append({"date":date,"pid":pid,"race":race,"rank":i,"ticket":t["ticket"],"score":t["score"],"odds":t["odds"],"ev":t["ev"]})
        pd.DataFrame(rows).to_csv(os.path.join(pred_dir,"predictions_summary.csv"), index=False, encoding="utf-8")
        print(f"[predict] {len(keys[:lim])} races -> {pred_dir}")
        return

    # 以降は eval（results 必須）
    res_idx=_collect_results(args.base,dates)
    print(f"[eval] races: {len(keys)}")
    per=[]; stake_sum=0; pay_sum=0
    for (date,pid,race) in keys:
        ev=evaluate_one(int_idx[(date,pid,race)], res_idx[(date,pid,race)], args.sims, args.unit,
                        args.strategy, args.topn, args.k, args.m, args.exclude_first1, args.only_first1,
                        args.odds_base, args.min_ev, args.require_odds, bands, pass1_dir,
                        args.ml_root, args.ml_st_gain, args.ml_A_gain, args.ml_consistency, args.ml_win_gain)
        stake_sum+=ev["stake"]; pay_sum+=ev["payout"]
        per.append({"date":date,"pid":pid,"race":race,"bets":len(ev["bets"]),"stake":ev["stake"],
                    "payout":ev["payout"],"hit":ev["hit"],"hit_combo":ev["hit_combo"]})
    df=pd.DataFrame(per)
    overall={"engine":"SimS ver1.0 (E1, ML-adjust)","races":int(len(df)),"bets_total":int(df["bets"].sum()) if len(df)>0 else 0,
             "stake_total":int(stake_sum),"payout_total":int(pay_sum),"hit_rate":float(df["hit"].mean()) if len(df)>0 else 0.0,
             "roi": float((pay_sum-stake_sum)/stake_sum) if stake_sum>0 else 0.0, "strategy":args.strategy,
             "topn":args.topn,"k":args.k,"m":args.m,"sims_per_race":args.sims,"unit":args.unit,
             "exclude_first1":bool(args.exclude_first1),"only_first1":bool(args.only_first1),
             "min_ev":float(args.min_ev),"require_odds":bool(args.require_odds),
             "odds_bands":args.odds_bands or "","odds_min":float(args.odds_min),"odds_max":float(args.odds_max),
             "ml_root": args.ml_root,
             "ml_coeffs":{"st_gain":args.ml_st_gain,"A_gain":args.ml_A_gain,"consistency":args.ml_consistency,"win_gain":args.ml_win_gain}}
    df.to_csv(os.path.join(pass1_dir,"per_race_results.csv"), index=False)
    json.dump(overall, open(os.path.join(pass1_dir,"overall.json"),"w",encoding="utf-8"), ensure_ascii=False, indent=2)
    print("=== OVERALL ==="); print(json.dumps(overall, ensure_ascii=False, indent=2))

if __name__=="__main__":
    main()
