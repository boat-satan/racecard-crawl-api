# SimS_integrated.py — SimS × ML ハイブリッド（predict/eval両対応, pass2相当）
# - predict-only: results/odds なしで ./predict にTOPN出力
# - eval       : results/odds 読込で ROI 集計（outdir配下）
# - 入力:
#   * integrated: public/integrated/v1/<date>/<pid>/<race>.json
#   * pass1 keyman: <pass1-dir>/keyman/<date>/<pid>/<race>.json（任意）
#   * ML CSV: <ml-root>/<date>/<pid>/<race>.csv（任意）
#
# MLの使いどころ:
#  - ST(μ,σ) 前倒し/分散調整, A/Ap 乗算, aggr_map（攻め度）, first_right/lineblock 微調整,
#    スワップロジット加点, 決まり手のsoft prior（決定は既存ルールを基本に上書き最小限）
#
# 使い方例:
#   # predict
#   python SimS_integrated.py --base ./public --dates 20250819 --sims 1200 --topn 18 --predict-only \
#     --pass1-dir ./SimS_v1.0_eval/pass1 --ml-root ./ml_outputs
#
#   # eval
#   python SimS_integrated.py --base ./public --dates 20250819 --sims 1200 --topn 18 \
#     --outdir ./out --pass1-dir ./SimS_v1.0_eval/pass1 --ml-root ./ml_outputs \
#     --odds-base ./public/odds/v1 --min-ev 0.0

import os, csv, json, math, argparse, shutil
from collections import Counter, defaultdict
import numpy as np
import pandas as pd

# ========= 基本パラメータ =========
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

rng = np.random.default_rng(2025)

# ========= 小物 =========
def sigmoid(x): return 1/(1+math.exp(-x))
def _norm_race(r): 
    r=(r or "").strip().upper()
    return r if (not r or r.endswith("R")) else f"{r}R"
def _parse_bool(v, default):
    if v is None: return default
    s=str(v).strip().lower()
    return True if s in ("1","true","yes","y","on") else False if s in ("0","false","no","n","off") else default
def _in_band(odds,bands):
    if not bands: return True
    if odds is None or not math.isfinite(odds): return False
    return any(lo<=odds<=hi for lo,hi in bands)

# ========= 入力構築 =========
def _sbase(rc):
    n1=float(rc.get("natTop1",6.0)); n2=float(rc.get("natTop2",50.0)); n3=float(rc.get("natTop3",70.0))
    return 0.5*((n1-6)/2)+0.3*((n2-50)/20)+0.2*((n3-70)/20)

def _wind(env):
    d=(env.get("wind") or {}).get("dir","cross"); m=float((env.get("wind") or {}).get("mps",0.0))
    sign=1 if d=="tail" else -1 if d=="head" else 0
    return Params.wind_theta_gain*sign*m, 1.0+Params.wind_st_sigma_gain*(abs(m)/10.0)

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

# ========= ML 読み込み & 補正マップ作成 =========
def load_ml_csv(ml_root, date, pid, race):
    """1レース1CSV: lane, win_prob, prob_*, pred_kimarite, pred_conf, uncertainty"""
    path=os.path.join(ml_root, date, pid, f"{race}.csv")
    if not os.path.isfile(path): return {}
    rows={}
    with open(path, newline="", encoding="utf-8") as f:
        r=csv.DictReader(f)
        for row in r:
            try:
                lane=int(row["lane"])
            except: continue
            rows[lane]=row
    return rows

def _get_float(d, k, default=0.0):
    try: return float(d.get(k, default))
    except: return default

def build_ml_adjusters(ml_rows, keyman_rank=None, gains=None):
    """
    returns:
      st_mu_shift[lane], st_sigma_mult[lane], A_mult[lane], Ap_mult[lane],
      aggr_map[lane], first_right_extra:set, lineblock_extra:set, swap_bias[(lead,chase)], kim_prior[lane]->dict
    """
    keyman_rank = keyman_rank or {}
    g = {
        "st_mu_in":0.010, "st_mu_out":0.008, "st_sig_tight":0.20, "st_sig_loose":0.10,
        "A_M":0.20, "A_S":0.10, "A_X":0.05, "Ap_M":0.25, "Ap_S":0.10, "Ap_X":0.10,
        "line_12":0.20, "line_41":0.15, "wake_cut":0.60, "swap_M":0.8, "swap_S":0.4, "swap_N":0.3,
        "prior_N":0.5, "prior_M":0.6, "prior_S":0.5,
        "key_boost_A":0.15, "key_mu_shift":0.006, "key_sig_tight":0.35
    }
    if gains: g.update(gains)

    st_mu_shift=defaultdict(float); st_sigma_mult=defaultdict(lambda:1.0)
    A_mult=defaultdict(lambda:1.0); Ap_mult=defaultdict(lambda:1.0)
    aggr_map=defaultdict(float)
    first_right_extra=set(); lineblock_extra=set()
    swap_bias_lead=defaultdict(float); swap_bias_chase=defaultdict(float)
    kim_prior=defaultdict(lambda: {"逃げ":0.0,"差し":0.0,"まくり":0.0,"まくり差し":0.0})

    for lane,row in ml_rows.items():
        M = _get_float(row,"prob_まくり",0)+_get_float(row,"prob_まくり差し",0)
        S = _get_float(row,"prob_差し",0)
        N = _get_float(row,"prob_逃げ",0) if lane==1 else 0.0
        X = _get_float(row,"prob_抜き",0)+_get_float(row,"prob_恵まれ",0)
        C = max(0.0, min(1.0, _get_float(row,"pred_conf",1.0)*(1.0-_get_float(row,"uncertainty",0.0))))
        K = float(keyman_rank.get(str(lane), 0.0))

        # ST
        st_mu_shift[lane] += -(g["st_mu_in"] * N * C) if lane==1 else -(g["st_mu_out"] * M * C)
        st_sigma_mult[lane] *= (1.0 - g["st_sig_tight"]*M*C + g["st_sig_loose"]*X*C)

        # A / Ap
        A_mult[lane]  *= (1.0 + g["A_M"]*M*C + g["A_S"]*S*C + g["A_X"]*X*C)
        Ap_mult[lane] *= (1.0 + g["Ap_M"]*M*C + g["Ap_S"]*S*C - g["Ap_X"]*X*C)

        # 攻め度
        aggr_map[lane] = max(0.0, min(1.0, M*C))

        # 先マイ/ラインブロック（控えめに）
        if lane==1 and N*C > 0.5: first_right_extra.add(1)
        if lane in (3,4) and M*C > 0.5: first_right_extra.add(lane)
        if N*C > 0.6: lineblock_extra.add((1,2))
        if lane==4 and M*C > 0.5: lineblock_extra.add((4,1))

        # スワップロジット加点は one_pass 側で aggr_map を使用（+追記バイアス）
        swap_bias_chase[lane] += g["swap_M"]*M*C + g["swap_S"]*S*C
        if lane==1: swap_bias_lead[lane] += g["swap_N"]*N*C

        # 決まり手prior（勝者にだけ効く想定）
        kim_prior[lane]["逃げ"]      += g["prior_N"]*N*C
        kim_prior[lane]["まくり"]    += g["prior_M"]*_get_float(row,"prob_まくり",0)*C
        kim_prior[lane]["まくり差し"]+= 0.5*g["prior_M"]*_get_float(row,"prob_まくり差し",0)*C
        kim_prior[lane]["差し"]      += g["prior_S"]*S*C

        # キーマン追加ブースト（乗算/前倒しは小さめ。pass2想定）
        if K>0:
            A_mult[lane]  *= (1.0 + g["key_boost_A"]*K*C)
            Ap_mult[lane] *= (1.0 + g["key_boost_A"]*K*C)
            st_mu_shift[lane] += -g["key_mu_shift"]*K*C
            st_sigma_mult[lane] *= (1.0 - g["key_sig_tight"]*K*C)

    return (st_mu_shift, st_sigma_mult, A_mult, Ap_mult,
            aggr_map, first_right_extra, lineblock_extra,
            swap_bias_lead, swap_bias_chase, kim_prior)

# ========= 1レース・シミュ =========
def _sample_ST(m): return rng.normal(m["mu"], m["sigma"])

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

def _t1m(ST,R,A,Ap,sq,env,st_gain):
    ST,A,Ap=_apply_session(ST,A,Ap)
    ST,A,back=_maybe_backoff(ST,A)
    A,cav=_maybe_cav(A)
    t=Params.b0+Params.alpha_R*(R-100.0)+Params.alpha_A*A+Params.alpha_Ap*Ap+Params.beta_sq*sq
    t+=ST*st_gain
    return t, {"backoff":back,"cav":cav}

def one_pass(entry,T1M,A,Ap,env,lineblocks,first_right,aggr_map=None,
             swap_bias_lead=None, swap_bias_chase=None):
    aggr_map=aggr_map or {}
    swap_bias_lead=swap_bias_lead or {}
    swap_bias_chase=swap_bias_chase or {}
    exit_order=entry[:]; swaps=[]; blocks=[]; safe_cnt=0
    d_theta,_=_wind(env); theta_eff=Params.theta+d_theta
    for k in range(len(exit_order)-1):
        lead, chase=exit_order[k], exit_order[k+1]
        dt=T1M[chase]-T1M[lead]
        dK=(A[chase]+Ap[chase])-(A[lead]+Ap[lead])
        delta=(Params.delta_lineblock if (lead,chase) in lineblocks else 0.0)
        if str(lead) in aggr_map: delta+=0.10*float(aggr_map[str(lead)])
        if lead in first_right: delta+=Params.delta_first
        terr,used=_maybe_safe()
        if used: safe_cnt+=1
        logit=Params.a0+Params.b_dt*(theta_eff-(dt+Params.gamma_wall+Params.k_turn_err*terr))+Params.cK*dK+delta
        if str(chase) in aggr_map: logit+=0.45*float(aggr_map[str(chase)])
        logit+= float(swap_bias_chase.get(chase,0.0)) + float(swap_bias_lead.get(lead,0.0))
        logit*= (Params.decision_bias_mult or 1.0)
        if rng.random()<sigmoid(logit):
            swaps.append((chase,lead)); exit_order[k],exit_order[k+1]=chase,lead
        else:
            if delta>0: blocks.append((lead,chase))
    return exit_order, swaps, blocks, safe_cnt

def simulate_one(integrated_json, sims=600,
                 ml_adj=None):
    inp=build_input(integrated_json)

    # ---- ML補正適用（静的部分）----
    if ml_adj:
        (st_mu_shift, st_sigma_mult, A_mult, Ap_mult,
         aggr_map, first_right_extra, lineblock_extra,
         swap_bias_lead, swap_bias_chase, kim_prior) = ml_adj
        # ST
        for k,st in list(inp["ST_model"].items()):
            l=int(k)
            st["mu"]=max(0.05, st["mu"] + float(st_mu_shift.get(l,0.0)))
            st["sigma"]=max(0.005, st["sigma"] * float(st_sigma_mult.get(l,1.0)))
        # A/Ap
        for l in list(inp["A"].keys()):
            inp["A"][l]  *= float(A_mult.get(l,1.0))
            inp["Ap"][l] *= float(A_mult.get(l,1.0))  # A系
            inp["Ap"][l] *= float(Ap_mult.get(l,1.0))/max(1e-9,float(A_mult.get(l,1.0)))  # Ap差分
        # 先マイ/ライン追加
        inp["first_right"] = set(inp["first_right"]) | set(first_right_extra)
        inp["lineblocks"]  = set(inp["lineblocks"])  | set(lineblock_extra)
    else:
        aggr_map={}; swap_bias_lead={}; swap_bias_chase={}; kim_prior=defaultdict(lambda:{"逃げ":0,"差し":0,"まくり":0,"まくり差し":0})

    lanes=inp["lanes"]; env=inp["env"]; _,st_gain=_wind(env)

    trif=Counter(); kim=Counter(); ex2=Counter(); thd=Counter()
    wake=Counter(); back=Counter(); cav=Counter()
    H1=Counter(); H2=Counter(); H3=Counter()
    swp=Counter(); blk=Counter()
    for _ in range(sims):
        ST={i:_sample_ST(inp["ST_model"][str(i)]) for i in lanes}
        T1M={}
        for i in lanes:
            t,fl=_t1m(ST[i], inp["R"][str(i)], inp["A"][i], inp["Ap"][i], inp["squeeze"][str(i)], env, st_gain)
            T1M[i]=t
            if fl["backoff"]: back[i]+=1
            if fl["cav"]: cav[i]+=1
        entry=sorted(lanes, key=lambda x:T1M[x])

        # wake: aggr で切り裂き
        for i in lanes:
            p=_wake_p(i, entry)
            if aggr_map:
                if str(i) in aggr_map:
                    p *= max(0.0, 1.0 - 0.60*float(aggr_map[str(i)]))
                else:
                    p *= (1.0 + 0.05*max([float(v) for v in aggr_map.values()]+[0.0]))
            if rng.random()<min(0.95,max(0.0,p)):
                wake[i]+=1; T1M[i]+=Params.beta_wk

        exit_order, swaps, blocks, _safe = one_pass(
            entry, T1M, inp["A"], inp["Ap"], env,
            inp["lineblocks"], inp["first_right"],
            aggr_map=aggr_map,
            swap_bias_lead=swap_bias_lead, swap_bias_chase=swap_bias_chase
        )

        lead=exit_order[0]
        dt_lead=T1M[exit_order[1]]-T1M[lead]
        base = "逃げ" if lead==1 else ("まくり" if dt_lead>=Params.tau_k else "まくり差し")
        # soft prior
        prior = kim_prior.get(lead, {})
        scored = {lab:(1.0 if lab==base else 0.0)+float(prior.get(lab,0.0)) for lab in ["逃げ","まくり","まくり差し","差し"]}
        decided = max(scored.items(), key=lambda kv: kv[1])[0]
        kim[decided]+=1

        trif[tuple(exit_order[:3])]+=1; ex2[(exit_order[0],exit_order[1])]+=1; thd[exit_order[2]]+=1
        H1[exit_order[0]]+=1; H2[exit_order[1]]+=1; H3[exit_order[2]]+=1
        for c,l in swaps: swp[(c,l)]+=1
        for l,c in blocks: blk[(l,c)]+=1

    total=sims
    tri_probs={k:v/total for k,v in trif.items()}
    kim_probs={k:v/total for k,v in kim.items()}
    ex_probs={k:v/total for k,v in ex2.items()}
    th_probs={k:v/total for k,v in thd.items()}

    keyman={
        "trials":int(total),
        "H1":{str(i):H1[i]/total for i in lanes},
        "H2":{str(i):H2[i]/total for i in lanes},
        "H3":{str(i):H3[i]/total for i in lanes},
        "SWAP":{f"{c}>{l}":int(v) for (c,l),v in swp.items()},
        "BLOCK":{f"{l}|{c}":int(v) for (l,c),v in blk.items()},
        "WAKE":{str(i):wake[i]/total for i in lanes},
    }
    return tri_probs, kim_probs, ex_probs, th_probs, keyman

# ========= ファイル収集/読込 =========
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
                    out[(d,pid,r)]=os.path.join(dir_pid,f); 
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

# ========= 買い目生成 =========
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

# ========= KEYMAN 読み込み =========
def load_keyman_rank(pass1_dir, date, pid, race):
    path=os.path.join(pass1_dir,"keyman",date,pid,f"{race}.json")
    if not os.path.isfile(path): return {}
    try:
        d=json.load(open(path,"r",encoding="utf-8"))
        return (d.get("keyman") or {}).get("KEYMAN_RANK", {}) or {}
    except: return {}

# ========= 3連単判定 =========
def actual_trifecta_and_amount(res):
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

# ========= メイン =========
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--base",default="./public")
    ap.add_argument("--dates",default="")
    ap.add_argument("--pids",default="")
    ap.add_argument("--races",default="")
    ap.add_argument("--sims",type=int,default=1200)
    ap.add_argument("--topn",type=int,default=18)
    ap.add_argument("--unit",type=int,default=100)
    ap.add_argument("--limit",type=int,default=0)
    ap.add_argument("--predict-only",action="store_true")

    ap.add_argument("--odds-base",default="./public/odds/v1")
    ap.add_argument("--min-ev",type=float,default=0.0)
    ap.add_argument("--require-odds",action="store_true")
    ap.add_argument("--odds-bands",default="")
    ap.add_argument("--odds-min",type=float,default=0.0)
    ap.add_argument("--odds-max",type=float,default=0.0)

    ap.add_argument("--strategy",default="trifecta_topN",choices=["trifecta_topN","exacta_topK_third_topM"])
    ap.add_argument("--k",type=int,default=2); ap.add_argument("--m",type=int,default=4)
    ap.add_argument("--exclude-first1",action="store_true"); ap.add_argument("--only-first1",action="store_true")

    ap.add_argument("--outdir",default="./SimS_integrated_out")
    ap.add_argument("--pass1-dir",default="./SimS_v1.0_eval/pass1")
    ap.add_argument("--ml-root",default="./ml_outputs")
    args=ap.parse_args()

    # 収集
    dates=set([d.strip() for d in args.dates.split(",") if d.strip()]) if args.dates else set()
    pids_filter=set([p.strip() for p in args.pids.split(",") if p.strip()])
    races_filter=set([_norm_race(r) for r in args.races.split(",") if r.strip()])
    int_idx=_collect(args.base,"integrated",dates) if dates else _collect(args.base,"integrated", set(os.listdir(os.path.join(args.base,"integrated","v1"))))

    # predict-only は results 不要
    if args.predict_only:
        keys=sorted(int_idx.keys())
    else:
        res_idx=_collect_results(args.base,dates)
        keys=sorted(set(int_idx.keys()) & set(res_idx.keys()))

    if pids_filter: keys=[k for k in keys if k[1] in pids_filter]
    if races_filter: keys=[k for k in keys if _norm_race(k[2]) in races_filter]
    if args.limit and args.limit>0: keys=keys[:args.limit]

    # オッズ帯
    bands=[]
    if args.odds_bands:
        for part in args.odds_bands.split(","):
            if "-" not in part: continue
            lo_s,hi_s=[s.strip() for s in part.split("-",1)]
            lo=float(lo_s) if lo_s else float("-inf"); hi=float(hi_s) if hi_s else float("inf")
            if math.isfinite(lo) and math.isfinite(hi) and lo>hi: lo,hi=hi,lo
            bands.append((lo,hi))
    elif args.odds_min or args.odds_max:
        lo=float(args.odds_min) if args.odds_min>0 else float("-inf")
        hi=float(args.odds_max) if args.odds_max>0 else float("inf")
        if math.isfinite(lo) and math.isfinite(hi) and lo>hi: lo,hi=hi,lo
        bands=[(lo,hi)]

    # 出力ディレクトリ
    root_out=os.path.abspath(args.outdir)
    os.makedirs(root_out, exist_ok=True)

    # ==== predict-only: ./predict に出す（互換のため）====
    if args.predict_only:
        pred_dir="./predict"
        if os.path.exists(pred_dir): shutil.rmtree(pred_dir)
        os.makedirs(pred_dir, exist_ok=True)
        rows=[]

        for (date,pid,race) in keys:
            d_int=json.load(open(int_idx[(date,pid,race)],"r",encoding="utf-8"))

            # pass1 keyman & ML 読込 → adjust 作成
            km_rank = load_keyman_rank(args.pass1_dir, date, pid, race)
            ml_rows = load_ml_csv(args.ml_root, date, pid, race)
            ml_adj=None
            if ml_rows:
                ml_adj = build_ml_adjusters(ml_rows, keyman_rank=km_rank)

            tri,kim,ex2,th3,_ = simulate_one(d_int, sims=args.sims, ml_adj=ml_adj)

            tickets=generate_tickets(args.strategy, tri, ex2, th3, args.topn, args.k, args.m,
                                     args.exclude_first1, args.only_first1)

            # オッズ不要・EVなしでそのまま書き出し
            out_list=[{"ticket":"-".join(map(str,k)),"score":round(p,6),"odds":None,"ev":None} for (k,p) in tickets]

            json.dump({"date":date,"pid":pid,"race":race,"buylist":out_list,
                       "engine":"SimS integrated (pass2, ML)"},
                      open(os.path.join(pred_dir,f"pred_{date}_{pid}_{race}.json"),"w",encoding="utf-8"),
                      ensure_ascii=False, indent=2)

            for i,t in enumerate(out_list,1):
                rows.append({"date":date,"pid":pid,"race":race,"rank":i,
                             "ticket":t["ticket"],"score":t["score"],"odds":t["odds"],"ev":t["ev"]})

        pd.DataFrame(rows).to_csv(os.path.join(pred_dir,"predictions_summary.csv"), index=False, encoding="utf-8")
        print(f"[predict/integrated] races={len(keys)} -> {pred_dir}")
        return

    # ==== eval ====
    per=[]; stake_sum=0; pay_sum=0
    for (date,pid,race) in keys:
        d_int=json.load(open(int_idx[(date,pid,race)],"r",encoding="utf-8"))
        res=d_res=_load_result(res_idx[(date,pid,race)])

        km_rank = load_keyman_rank(args.pass1_dir, date, pid, race)
        ml_rows = load_ml_csv(args.ml_root, date, pid, race)
        ml_adj=None
        if ml_rows:
            ml_adj = build_ml_adjusters(ml_rows, keyman_rank=km_rank)

        tri,kim,ex2,th3,_ = simulate_one(d_int, sims=args.sims, ml_adj=ml_adj)
        tickets=generate_tickets(args.strategy, tri, ex2, th3, args.topn, args.k, args.m,
                                 args.exclude_first1, args.only_first1)

        odds_map=_load_odds(args.odds_base, date, pid, race)
        kept=[]
        for (key,prob) in tickets:
            combo="-".join(map(str,key)); rec=odds_map.get(combo); odds=rec["odds"] if rec else None
            if bands and (odds is None or not _in_band(odds,bands)): continue
            if (not bands) and args.require_odds and odds is None: continue
            if args.min_ev>0 and odds is not None and prob*odds<args.min_ev: continue
            kept.append((key,prob,odds))
        tickets=kept

        bets=['-'.join(map(str,k)) for k,_,_ in tickets]; stake=args.unit*len(bets)
        hit_combo, amt = actual_trifecta_and_amount(res)
        payout = amt if hit_combo in bets else 0
        stake_sum+=stake; pay_sum+=payout

        per.append({"date":date,"pid":pid,"race":race,
                    "bets":len(bets),"stake":stake,"payout":payout,
                    "hit":1 if payout>0 else 0,"hit_combo":hit_combo})

    df=pd.DataFrame(per)
    os.makedirs(root_out, exist_ok=True)
    df.to_csv(os.path.join(root_out,"per_race_results.csv"), index=False)
    overall={"engine":"SimS integrated (pass2, ML)","races":int(len(df)),
             "bets_total":int(df["bets"].sum()) if len(df)>0 else 0,
             "stake_total":int(stake_sum),"payout_total":int(pay_sum),
             "hit_rate":float(df["hit"].mean()) if len(df)>0 else 0.0,
             "roi": float((pay_sum-stake_sum)/stake_sum) if stake_sum>0 else 0.0}
    json.dump(overall, open(os.path.join(root_out,"overall.json"),"w",encoding="utf-8"),
              ensure_ascii=False, indent=2)
    print("=== OVERALL (integrated) ===")
    print(json.dumps(overall, ensure_ascii=False, indent=2))

if __name__=="__main__":
    main()
