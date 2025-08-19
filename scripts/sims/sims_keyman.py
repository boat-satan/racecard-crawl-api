#!/usr/bin/env python3
# sims_keyman.py — SimS Pass1 抽出版（キーマン算出専用）
# - 結果/オッズ/買い目ロジックは一切なし
# - 出力: <outdir>/keyman/<date>/<pid>/<race>.json に Keyman 指標（KEYMAN_RANK含む）
# - 依存: numpy, pandas(不要), tomllib(任意: --params=*.toml の時のみ)
# - 乱数: --seed で制御（デフォルト 2025）

import os, json, math, argparse
from collections import Counter
import numpy as np

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
        with open(p, "r", encoding="utf-8") as f: return json.load(f)
    if ext == ".toml":
        if tomllib is None: raise RuntimeError("tomlはPython3.11+が必要")
        with open(p, "rb") as f: return tomllib.load(f)
    raise ValueError(f"Unsupported params file: {ext}")

def _parse_set(expr: str) -> dict:
    out = {}
    if not expr: return out
    for kv in [p.strip() for p in expr.split(",") if p.strip()]:
        if "=" not in kv: continue
        k, v = kv.split("=", 1); k=k.strip(); v=v.strip()
        try:
            if v.lower() in ("true","false"): out[k] = (v.lower()=="true")
            else: out[k] = float(v) if any(c in v.lower() for c in ".e") else int(v)
        except: out[k]=v
    return out

def _apply_over(cls, d: dict):
    for k,v in d.items():
        if hasattr(cls,k): setattr(cls,k,v)

# ===== SimS ver1.0 Params（本体と同値） =====
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

# ===== 小物 =====
def sigmoid(x): return 1.0/(1.0+math.exp(-x))

def _minmax_norm(d, keys):
    vs=[float(d.get(k,0.0)) for k in keys]
    lo=min(vs) if vs else 0.0; hi=max(vs) if vs else 0.0
    den=(hi-lo) or 1.0
    return {k:(float(d.get(k,0.0))-lo)/den for k in keys}

def _sbase(rc):
    n1=float(rc.get("natTop1",6.0)); n2=float(rc.get("natTop2",50.0)); n3=float(rc.get("natTop3",70.0))
    return 0.5*((n1-6)/2)+0.3*((n2-50)/20)+0.2*((n3-70)/20)

def _wind(env):
    d=(env.get("wind") or {}).get("dir","cross"); m=float((env.get("wind") or {}).get("mps",0.0))
    sign=1 if d=="tail" else -1 if d=="head" else 0
    return Params.wind_theta_gain*sign*m, 1.0+Params.wind_st_sigma_gain*(abs(m)/10.0)

# ===== 入力構築 =====
def build_input(d, rng):
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
        sigma=0.02*(1+0.20*(1 if F[lane]>0 else 0)+0.15*max(0.0,-S[lane]))
        sigma*=1.0+0.1*(lane-1)
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

# ===== 1レース・シミュ（キーマン指標の算出まで） =====
def _apply_session(rng, ST,A,Ap):
    ST+=rng.normal(Params.session_ST_shift_mu,Params.session_ST_shift_sd)
    g=lambda x: x*(1.0+rng.normal(Params.session_A_bias_mu,Params.session_A_bias_sd))
    return ST,g(A),g(Ap)

def _maybe_backoff(rng, ST,A):
    if rng.random()<Params.p_backoff: return ST+Params.backoff_ST_shift, A*(1-Params.backoff_A_penalty), True
    return ST,A,False

def _maybe_cav(rng, A):
    if rng.random()<Params.p_cav: return A*(1-Params.cav_A_penalty), True
    return A,False

def _maybe_safe(rng):
    if rng.random()<Params.p_safe_margin:
        return max(0.0, rng.normal(Params.safe_margin_mu, Params.safe_margin_sigma)), True
    return 0.0, False

def _wake_p(lane, entry):
    pos=entry.index(lane)
    base=Params.base_wake+Params.extra_wake_when_outside*((lane-1)/5.0)
    if pos==0: base*=0.3
    return max(0.0, min(base,0.95))

def simulate_one_keyman(integrated_json, sims=600, seed=2025):
    rng = np.random.default_rng(int(seed))
    inp=build_input(integrated_json, rng)
    lanes=inp["lanes"]; env=inp["env"]; _,st_gain=_wind(env)

    trif=Counter();  # 参考（使わないが副作用同一に保つため）
    kim=Counter()
    ex2=Counter(); thd=Counter()
    H1=Counter(); H2=Counter(); H3=Counter()
    wake=Counter(); back=Counter(); cav=Counter()
    swp=Counter(); blk=Counter(); posd={i:0 for i in lanes}
    safe_total=0

    for _ in range(sims):
        ST={i: rng.normal(inp["ST_model"][str(i)]["mu"], inp["ST_model"][str(i)]["sigma"]) for i in lanes}

        T1M={}
        for i in lanes:
            st, A, Ap = ST[i], inp["A"][i], inp["Ap"][i]
            st,A,Ap = _apply_session(rng, st,A,Ap)
            st,A,was_back = _maybe_backoff(rng, st,A)
            A,was_cav = _maybe_cav(rng, A)
            if was_back: back[i]+=1
            if was_cav:  cav[i]+=1
            t=Params.b0+Params.alpha_R*(inp["R"][str(i)]-100.0)+Params.alpha_A*A+Params.alpha_Ap*Ap+Params.beta_sq*inp["squeeze"][str(i)]
            t+=st*st_gain
            T1M[i]=t

        entry=sorted(lanes, key=lambda x:T1M[x])

        for i in lanes:
            p=_wake_p(i, entry)
            if rng.random()<min(0.95,max(0.0,p)):
                wake[i]+=1; T1M[i]+=Params.beta_wk

        # 1回転の追い越し判定
        exit_order=entry[:]; swaps=[]; blocks=[]
        d_theta,_=_wind(env); theta_eff=Params.theta+d_theta
        safe_cnt=0
        for k in range(len(exit_order)-1):
            lead, chase=exit_order[k], exit_order[k+1]
            dt=T1M[chase]-T1M[lead]
            dK=(inp["A"][chase]+inp["Ap"][chase])-(inp["A"][lead]+inp["Ap"][lead])
            delta=(Params.delta_lineblock if (lead, chase) in inp["lineblocks"] else 0.0)
            if lead in inp["first_right"]: delta+=Params.delta_first
            terr,used=_maybe_safe(rng)
            if used: safe_cnt+=1
            logit=Params.a0+Params.b_dt*(theta_eff-(dt+Params.gamma_wall+Params.k_turn_err*terr))+Params.cK*dK+delta
            logit*= (Params.decision_bias_mult or 1.0)
            if rng.random()<sigmoid(logit):
                swaps.append((chase,lead)); exit_order[k],exit_order[k+1]=chase,lead
            else:
                if delta>0: blocks.append((lead,chase))
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
    keyman={
        "trials":int(total),
        "H1":{str(i):H1[i]/total for i in lanes},
        "H2":{str(i):H2[i]/total for i in lanes},
        "H3":{str(i):H3[i]/total for i in lanes},
        "SWAP":{f"{c}>{l}":int(v) for (c,l),v in swp.items()},
        "BLOCK":{f"{l}|{c}":int(v) for (l,c),v in blk.items()},
        "WAKE":{str(i):wake[i]/total for i in lanes},
        "BACKOFF":{str(i):back[i]/total for i in lanes},
        "CAV":{str(i):cav[i]/total for i in lanes},
        "POS_DELTA_AVG":{str(i):posd[i]/total for i in lanes},
        "SAFE_MARGIN_EVENTS_PER_TRIAL": safe_total/(total*max(1,(len(lanes)-1)))
    }

    # KEYMAN_RANK（現行と同じ式）
    try:
        lanes_s=sorted((keyman.get("WAKE") or {}).keys(), key=lambda x:int(x))
        wake_n=_minmax_norm(keyman.get("WAKE",{}), lanes_s)
        pos_raw={k:float(keyman.get("POS_DELTA_AVG",{}).get(k,0.0)) for k in lanes_s}
        pos_n=_minmax_norm({k:(v if v>0 else 0.0) for k,v in pos_raw.items()}, lanes_s)
        swaps=keyman.get("SWAP",{}) or {}; total_sw=sum(int(v) for v in swaps.values()) or 1
        out_cnt={k:0 for k in lanes_s}
        for pair,cnt in swaps.items():
            try:
                ch,ld=pair.split(">"); 
                if int(ch)>int(ld): out_cnt[str(int(ch))]=out_cnt.get(str(int(ch)),0)+int(cnt)
            except: pass
        swap_n=_minmax_norm({k:out_cnt.get(k,0)/total_sw for k in lanes_s}, lanes_s)
        back_n=_minmax_norm(keyman.get("BACKOFF",{}), lanes_s)
        cav_n=_minmax_norm(keyman.get("CAV",{}), lanes_s)
        raw={k:0.35*wake_n[k]+0.25*pos_n[k]+0.25*swap_n[k]+0.10*back_n[k]+0.05*cav_n[k] for k in lanes_s}
        keyman["KEYMAN_RANK"]=_minmax_norm(raw, lanes_s)
    except: pass

    return keyman

# ===== ファイル収集 =====
def _collect_integrated(base, dates:set):
    root_v1=os.path.join(base,"integrated","v1"); root=root_v1 if os.path.isdir(root_v1) else os.path.join(base,"integrated")
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
                    race=f[:-5]
                    out[(d,pid,race)]=os.path.join(dir_pid,f)
    return out

def _norm_race(r): 
    r=(r or "").strip().upper()
    return r if (not r or r.endswith("R")) else f"{r}R"

# ===== メイン =====
def main():
    ap=argparse.ArgumentParser(description="SimS Pass1 (keyman only)")
    ap.add_argument("--base", default="./public")
    ap.add_argument("--dates", default="")
    ap.add_argument("--pids", default="")
    ap.add_argument("--races", default="")
    ap.add_argument("--sims", type=int, default=1200)
    ap.add_argument("--seed", type=int, default=2025)
    ap.add_argument("--outdir", default="./SimS_keyman_pass1")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--params", default="")
    ap.add_argument("--set", default="")
    args=ap.parse_args()

    # Param 上書き（本体と同等の仕組み）
    try:
        if args.params: _apply_over(Params, _load_params_file(args.params))
        over=_parse_set(getattr(args,"set",""))
        if over: _apply_over(Params, over)
    except Exception as e:
        raise SystemExit(f"[params] override failed: {e}")

    # 出力ルート
    out_root=os.path.abspath(args.outdir)
    os.makedirs(out_root, exist_ok=True)
    # 監査用パラメータ出力
    try:
        active={k:getattr(Params,k) for k in dir(Params) if not k.startswith("_") and isinstance(getattr(Params,k),(int,float,bool))}
        with open(os.path.join(out_root,"active_params.json"),"w",encoding="utf-8") as f:
            json.dump(active, f, ensure_ascii=False, indent=2)
    except: pass

    dates=set([d.strip() for d in args.dates.split(",") if d.strip()]) if args.dates else set()
    pids_filter=set([p.strip() for p in args.pids.split(",") if p.strip()])
    races_filter=set([_norm_race(r) for r in args.races.split(",") if r.strip()])

    int_idx=_collect_integrated(args.base, dates if dates else set(os.listdir(os.path.join(args.base,"integrated","v1"))))
    keys=sorted(int_idx.keys())
    if pids_filter:  keys=[k for k in keys if k[1] in pids_filter]
    if races_filter: keys=[k for k in keys if _norm_race(k[2]) in races_filter]
    if args.limit and args.limit>0: keys=keys[:args.limit]

    print(f"[keyman/pass1] targets: {len(keys)}")
    saved=0
    for (date,pid,race) in keys:
        try:
            with open(int_idx[(date,pid,race)],"r",encoding="utf-8") as f:
                d_int=json.load(f)
            keyman = simulate_one_keyman(d_int, sims=args.sims, seed=args.seed)

            dirp=os.path.join(out_root,"keyman",date,pid)
            os.makedirs(dirp, exist_ok=True)
            payload={"date":date,"pid":pid,"race":race,
                     "engine":"SimS ver1.0 (Pass1-only)",
                     "sims_per_race": int(args.sims),
                     "keyman": keyman}
            with open(os.path.join(dirp,f"{race}.json"),"w",encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            saved+=1
        except Exception as e:
            print(f"[warn] skip {date}/{pid}/{race}: {e}")

    print(f"[done] keyman files saved: {saved} -> {os.path.join(out_root,'keyman')}")


if __name__=="__main__":
    main()
