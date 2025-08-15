# sims_batch_eval_SimS_v1.py
# SimS ver1.0 — 同時同条件バッチ検証（三連単 TOPN 均等買い／可変買い目ロジック対応）
#  - 統合データ/結果を読み込み（払い戻しは results から参照）
#  - SimS ver1.0 で各レースを N試行シミュ
#  - 買い目を生成（デフォルトは三連単 TOP18）し、的中率・ROIを算出
#  - さらに「的中群の傾向レポート」を出力（決まり手・1着コース・オッズ帯・出目・確率順位帯）

import os, json, math, argparse, csv, shutil, itertools
from collections import Counter, defaultdict
import numpy as np
import pandas as pd

# =========================
# SimS ver1.0 パラメータ（調整済みを反映）
# =========================
class Params:
    # T1M 到達式（線形近似）
    b0=100.0
    alpha_R=0.005          # 助走距離係数
    alpha_A=-0.010         # 伸びA
    alpha_Ap=-0.012        # 出足Ap（入口直前～旋回寄与を簡易で混ぜる）

    # 勝負圏・追い抜き
    theta=0.0285
    a0=0.0
    b_dt=15.0
    cK=1.2

    # 固定旋回コスト
    leadOuterDrift_1=0.034
    outerBaseCost=0.022
    tau_k=0.030

    # イベント強度
    beta_sq=0.006
    beta_wk=0.004
    k_turn_err=0.010
    gamma_wall=0.006

    # 先マイ権/ラインブロック
    delta_first=0.70
    delta_lineblock=0.5

    # 安全マージン
    safe_margin_mu=0.005
    safe_margin_sigma=0.003
    p_safe_margin=0.20

    # ビビり戻し
    p_backoff=0.10
    backoff_ST_shift=0.015
    backoff_A_penalty=0.15

    # キャビ
    p_cav=0.03
    cav_A_penalty=0.25

    # セッション揺らぎ
    session_ST_shift_mu=0.0
    session_ST_shift_sd=0.004
    session_A_bias_mu=0.0
    session_A_bias_sd=0.10

    # 風（簡易）
    wind_theta_gain=0.002
    wind_st_sigma_gain=0.5

    # 引き波発生率
    base_wake=0.20
    extra_wake_when_outside=0.25

    # 決まり手バイアス倍率
    decision_bias_mult=1.0

# 固定シード
rng = np.random.default_rng(2025)

# ========== ユーティリティ ==========
def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))

def s_base_from_nat(rc: dict) -> float:
    n1 = float(rc.get("natTop1", 6.0))
    n2 = float(rc.get("natTop2", 50.0))
    n3 = float(rc.get("natTop3", 70.0))
    return (0.5 * ((n1 - 6.0) / 2.0)
          + 0.3 * ((n2 - 50.0) / 20.0)
          + 0.2 * ((n3 - 70.0) / 20.0))

def wind_adjustments(env: dict):
    d = (env.get("wind") or {}).get("dir", "cross")
    m = float((env.get("wind") or {}).get("mps", 0.0))
    sign = 1 if d=="tail" else -1 if d=="head" else 0
    d_theta = Params.wind_theta_gain * sign * m
    st_sigma_gain = 1.0 + Params.wind_st_sigma_gain * (abs(m)/10.0)
    return d_theta, st_sigma_gain

def apply_session_bias(ST, A, Ap):
    ST += rng.normal(Params.session_ST_shift_mu, Params.session_ST_shift_sd)
    A  *= (1.0 + rng.normal(Params.session_A_bias_mu, Params.session_A_bias_sd))
    Ap *= (1.0 + rng.normal(Params.session_A_bias_mu, Params.session_A_bias_sd))
    return ST, A, Ap

def maybe_backoff(ST, A):
    if rng.random() < Params.p_backoff:
        return ST + Params.backoff_ST_shift, A * (1.0 - Params.backoff_A_penalty)
    return ST, A

def maybe_cav(A):
    if rng.random() < Params.p_cav:
        return A * (1.0 - Params.cav_A_penalty)
    return A

def maybe_safe_margin():
    if rng.random() < Params.p_safe_margin:
        return max(0.0, rng.normal(Params.safe_margin_mu, Params.safe_margin_sigma))
    return 0.0

def flow_bias(env, lane):
    return 0.0

def wake_loss_probability(lane, entry_order):
    pos = entry_order.index(lane)
    base = Params.base_wake + Params.extra_wake_when_outside * ((lane - 1) / 5.0)
    if pos == 0:
        base *= 0.3
    return max(0.0, min(base, 0.95))

# ========== 入力変換 ==========
def build_input_from_integrated(d: dict) -> dict:
    lanes = [e["lane"] for e in d["entries"]]

    # ST mu
    mu = {}; S = {}; F = {}
    for e in d["entries"]:
        lane = e["lane"]
        rc = e["racecard"]
        ec = (e.get("stats") or {}).get("entryCourse", {})
        rc_st = rc.get("avgST", None)
        ec_st = ec.get("avgST", None)
        vals = [v for v in [rc_st, ec_st] if isinstance(v, (int, float))]
        if not vals:
            m = 0.16
        elif len(vals) == 1:
            m = float(vals[0])
        else:
            m = 0.5 * float(vals[0]) + 0.5 * float(vals[1])
        if int(rc.get("flyingCount", 0)) > 0:
            m += 0.010
        mu[lane] = m
        S[lane]  = s_base_from_nat(rc)
        F[lane]  = int(rc.get("flyingCount", 0))

    # ST 分布（外枠ほど効きを強く）
    ST_model = {}
    for lane in lanes:
        sigma = 0.02 * (1 + 0.20 * (1 if F[lane] > 0 else 0) + 0.15 * max(0.0, -S[lane]))
        lane_gain = 1.0 + 0.1 * (lane - 1)   # 1.0→1.5
        sigma *= lane_gain
        ST_model[str(lane)] = {"type": "normal", "mu": mu[lane], "sigma": sigma}

    # 助走距離
    R = {str(l): float({1: 88, 2: 92, 3: 96, 4: 100, 5: 104, 6: 108}.get(l, 100.0)) for l in lanes}

    # A / Ap
    course_bias = {1: 0.05, 2: 0.05, 3: 0.02, 4: 0.00, 5: -0.05, 6: -0.06}
    A  = {}
    Ap = {}
    for l in lanes:
        deltaST = (0.16 - mu[l]) * 5.0
        A[l]  = 0.7 * S[l] + 0.3 * deltaST
        Ap[l] = 0.7 * S[l] + 0.3 * course_bias.get(l, 0.0)

    # squeeze
    S1 = S.get(1, 0.0)
    squeeze = {}
    for l in lanes:
        val = 0.0 if l == 1 else max(0.0, (S1 - S[l]) * 0.20)
        squeeze[str(l)] = min(val, 0.20)

    # 先マイ権 / ラインブロック
    first_right = []
    lineblocks  = []
    if S1 > 0.30 and mu.get(1, 0.16) <= 0.17:
        first_right.append(1)
    S4 = S.get(4, 0.0)
    if S4 > 0.10 and mu.get(4, 0.16) <= 0.17:
        first_right.append(4)
    S2 = S.get(2, 0.0)
    if (S1 - S2) > 0.20:
        lineblocks.append((1, 2))
    if (S4 - S1) > 0.05:
        sc4 = next((e.get("startCourse", 4) for e in d["entries"] if e["lane"] == 4), 4)
        if sc4 >= 4:
            lineblocks.append((4, 1))

    env = {"wind": {"dir": "cross", "mps": 0.0}, "flow": {"dir": "none", "rate": 0.0}}
    return {
        "lanes": lanes, "ST_model": ST_model, "R": R, "A": A, "Ap": Ap, "env": env,
        "squeeze": squeeze, "first_right": set(first_right), "lineblocks": set(lineblocks)
    }

# ========== 1レース・シミュ ==========
def sample_ST(model):
    return rng.normal(model["mu"], model["sigma"])

def t1m_time(ST, R, A, Ap, sq, env, lane, st_gain):
    ST, A, Ap = apply_session_bias(ST, A, Ap)
    ST, A     = maybe_backoff(ST, A)
    A         = maybe_cav(A)
    t = (Params.b0
         + Params.alpha_R * (R - 100.0)
         + Params.alpha_A * A
         + Params.alpha_Ap * Ap
         + Params.beta_sq * sq
         + flow_bias(env, lane))
    t += ST * st_gain
    return t

def decision_bias_term(lead, chase, lane, kimarite_hint=None):
    base = 1.0
    if Params.decision_bias_mult != 1.0:
        base *= Params.decision_bias_mult
    return base

def one_pass(entry, T1M, A, Ap, env, lineblocks, first_right):
    exit_order = entry[:]
    d_theta, _ = wind_adjustments(env)
    theta_eff = Params.theta + d_theta
    for k in range(len(exit_order) - 1):
        lead, chase = exit_order[k], exit_order[k+1]
        dt = T1M[chase] - T1M[lead]
        dK = (A[chase] + Ap[chase]) - (A[lead] + Ap[lead])
        delta = (Params.delta_lineblock if (lead, chase) in lineblocks else 0.0)
        if lead in first_right:
            delta += Params.delta_first
        turn_err = maybe_safe_margin()
        dt_eff = dt + Params.gamma_wall + Params.k_turn_err * turn_err
        logit = Params.a0 + Params.b_dt * (theta_eff - dt_eff) + Params.cK * dK + delta
        logit *= decision_bias_term(lead, chase, chase)
        p = sigmoid(logit)
        if rng.random() < p:
            exit_order[k], exit_order[k+1] = chase, lead
    return exit_order

def simulate_one(integrated_json: dict, sims: int = 600):
    inp = build_input_from_integrated(integrated_json)
    lanes = inp["lanes"]; env = inp["env"]
    _, st_gain = wind_adjustments(env)

    trifecta = Counter()
    kimarite = Counter()
    pair_counts = Counter()   # 2連単
    third_counts = Counter()  # 3着

    for _ in range(sims):
        ST = {i: sample_ST(inp["ST_model"][str(i)]) for i in lanes}
        T1M = {
            i: t1m_time(ST[i], inp["R"][str(i)], inp["A"][i], inp["Ap"][i],
                        inp["squeeze"][str(i)], env, i, st_gain)
            for i in lanes
        }
        entry = sorted(lanes, key=lambda x: T1M[x])
        for i in lanes:
            if rng.random() < wake_loss_probability(i, entry):
                T1M[i] += Params.beta_wk
        exit_order = one_pass(entry, T1M, inp["A"], inp["Ap"], env, inp["lineblocks"], inp["first_right"])

        lead = exit_order[0]
        dt_lead = T1M[exit_order[1]] - T1M[lead]
        kim = "逃げ" if lead == 1 else ("まくり" if dt_lead >= Params.tau_k else "まくり差し")
        kimarite[kim] += 1

        trifecta[tuple(exit_order[:3])] += 1
        pair_counts[(exit_order[0], exit_order[1])] += 1
        third_counts[exit_order[2]] += 1

    total = sims
    tri_probs = {k: v/total for k, v in trifecta.items()}
    kim_probs = {k: v/total for k, v in kimarite.items()}
    exacta_probs = {k: v/total for k, v in pair_counts.items()}
    third_probs  = {k: v/total for k, v in third_counts.items()}
    return tri_probs, kim_probs, exacta_probs, third_probs

# ========== データ収集 ==========
def collect_files(base_dir: str, kind: str, dates: set):
    # まず .../<kind>/v1 を探し、無ければ .../<kind> を使う
    root_v1 = os.path.join(base_dir, kind, "v1")
    root    = root_v1 if os.path.isdir(root_v1) else os.path.join(base_dir, kind)
    out = []
    date_dirs = list(dates) if dates else [
        d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))
    ]
    for d in date_dirs:
        dir_d = os.path.join(root, d)
        if not os.path.isdir(dir_d):
            continue
        for pid in os.listdir(dir_d):
            dir_pid = os.path.join(dir_d, pid)
            if not os.path.isdir(dir_pid):
                continue
            for fname in os.listdir(dir_pid):
                if fname.endswith(".json"):
                    race = fname[:-5]
                    out.append(((d, pid, race), os.path.join(dir_pid, fname)))
    return dict(out)

# results 用（レース別 or まとめJSON の両対応）
def collect_results_files(base_dir: str, dates: set):
    root_v1 = os.path.join(base_dir, "results", "v1")
    root    = root_v1 if os.path.isdir(root_v1) else os.path.join(base_dir, "results")
    out = {}
    date_dirs = list(dates) if dates else [
        d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))
    ]
    for d in date_dirs:
        dir_d = os.path.join(root, d)
        if not os.path.isdir(dir_d):
            continue
        for pid in os.listdir(dir_d):
            dir_pid = os.path.join(dir_d, pid)
            if not os.path.isdir(dir_pid):
                continue

            # ① レース別ファイル（1R.json 等）
            per_race = [f for f in os.listdir(dir_pid) if f.lower().endswith(".json") and f.upper().endswith("R.JSON")]
            if per_race:
                for fname in per_race:
                    race = fname[:-5].upper()
                    if not race.endswith("R"):
                        race += "R"
                    out[(d, pid, race)] = os.path.join(dir_pid, fname)
                continue

            # ② まとめファイル（中に 1R,2R,...）
            json_files = [f for f in os.listdir(dir_pid) if f.lower().endswith(".json")]
            for fname in json_files:
                path = os.path.join(dir_pid, fname)
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    races_obj = data.get("races") if isinstance(data, dict) else None
                    container = races_obj if isinstance(races_obj, dict) else (data if isinstance(data, dict) else {})
                    for race_key in list(container.keys()):
                        rk = str(race_key).upper()
                        if rk.isdigit(): rk += "R"
                        if not rk.endswith("R"):  # 想定外キーはスキップ
                            continue
                        out[(d, pid, rk)] = path + "#" + rk  # 後で取り出すための印
                except Exception:
                    pass
    return out

# ハッシュ付きパスに対応した results ローダ
def load_result_for_race(res_path: str):
    if "#" in res_path:
        path, race = res_path.split("#", 1)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        races_obj = data.get("races", data) if isinstance(data, dict) else {}
        d = races_obj.get(race) or races_obj.get(race.upper()) or races_obj.get(race.lower())
        return d if isinstance(d, dict) else None
    else:
        with open(res_path, "r", encoding="utf-8") as f:
            return json.load(f)

# ========== 結果のパース ==========
def actual_trifecta_combo_and_amount(result_json: dict):
    trif = (result_json or {}).get("payouts", {}).get("trifecta") if isinstance(result_json, dict) else None
    combo = None; amount = 0
    if isinstance(trif, dict):
        combo = trif.get("combo")
        amount = int(trif.get("amount") or 0)
    if not combo and isinstance(result_json, dict):
        order = result_json.get("order")
        if isinstance(order, list) and len(order) >= 3:
            def lane_of(x):
                return str(x.get("lane") or x.get("course") or x.get("F") or x.get("number"))
            try:
                f = lane_of(order[0]); s = lane_of(order[1]); t = lane_of(order[2])
                if all([f,s,t]): combo = f"{f}-{s}-{t}"
            except Exception:
                pass
    return combo, amount

# ========== 買い目生成 ==========
def generate_tickets(strategy, tri_probs, exacta_probs, third_probs, topn=18, k=2, m=4):
    tickets = []
    if strategy == "exacta_topK_third_topM":
        top2 = sorted(exacta_probs.items(), key=lambda kv: kv[1], reverse=True)[:k]
        top3 = [t for t,_ in sorted(third_probs.items(), key=lambda kv: kv[1], reverse=True)[:m]]
        seen = set()
        for (f,s), _ in top2:
            for t in top3:
                if t!=f and t!=s:
                    key = (f,s,t)
                    if key not in seen:
                        seen.add(key)
                        tickets.append((key, exacta_probs.get((f,s),0.0) * third_probs.get(t,0.0)))
        tickets = sorted(tickets, key=lambda kv: kv[1], reverse=True)
    else:
        top = sorted(tri_probs.items(), key=lambda kv: kv[1], reverse=True)[:topn]
        tickets = [(k, p) for k,p in top]
    return tickets

# ========== 評価（1レース） ==========
def evaluate_one(int_path: str, res_path: str, sims: int, unit: int,
                 strategy: str, topn: int, k: int, m: int,
                 exclude_first1: bool=False):
    with open(int_path, "r", encoding="utf-8") as f:
        d_int = json.load(f)
    tri_probs, kim_probs, exacta_probs, third_probs = simulate_one(d_int, sims=sims)

    tickets = generate_tickets(strategy, tri_probs, exacta_probs, third_probs, topn=topn, k=k, m=m)
    # ★ 1着=1号艇の除外オプション
    if exclude_first1:
        tickets = [(key,score) for (key,score) in tickets if int(key[0]) != 1]

    bets = ['-'.join(map(str, key)) for key,_ in tickets]
    stake = unit * len(bets)

    d_res = load_result_for_race(res_path) or {}
    hit_combo, payout_amount = actual_trifecta_combo_and_amount(d_res)
    payout = payout_amount if hit_combo in bets else 0

    rank_map = { '-'.join(map(str,k)): i+1 for i,(k,_) in enumerate(sorted(tri_probs.items(), key=lambda kv: kv[1], reverse=True)) }
    rank_hit = rank_map.get(hit_combo, None)

    return {
        "stake": stake, "payout": payout, "hit": 1 if payout>0 else 0,
        "bets": bets, "hit_combo": hit_combo, "tri_probs": tri_probs,
        "rank_hit": rank_hit, "kim_probs": kim_probs
    }

# ========== メイン ==========
def _norm_race(r: str) -> str:
    r = (r or "").strip().upper()
    if not r: return ""
    return r if r.endswith("R") else f"{r}R"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="./public", help="public ディレクトリのパス")
    ap.add_argument("--dates", default="", help="カンマ区切り（日付）例: 20250810,20250811")
    ap.add_argument("--sims", type=int, default=600, help="1レースあたりの試行回数")
    ap.add_argument("--topn", type=int, default=18, help="（trifecta_topN用）買い目TOPN")
    ap.add_argument("--unit", type=int, default=100, help="1点あたりの賭け金（円）")
    ap.add_argument("--limit", type=int, default=0, help="先頭からNレースだけ評価（0なら全件）")
    ap.add_argument("--outdir", default="./SimS_v1.0_eval", help="(eval)出力先")
    ap.add_argument("--predict-only", action="store_true", help="TOPN確率のみ出力")
    ap.add_argument("--pids", default="", help="場コードフィルタ（カンマ区切り）")
    ap.add_argument("--races", default="", help="レース名フィルタ（例 1R,2R もしくは 1,2）")
    ap.add_argument("--strategy", default="trifecta_topN", choices=["trifecta_topN","exacta_topK_third_topM"],
                    help="買い目生成ロジック")
    ap.add_argument("--k", type=int, default=2, help="exacta_topK_third_topM: 2連単TOPK")
    ap.add_argument("--m", type=int, default=4, help="exacta_topK_third_topM: 3着TOPM")
    # ★ 追加: 1着=1号艇の買い目を除外
    ap.add_argument("--exclude-first1", action="store_true",
                    help="買い目生成後に 1着=1号艇 の組み合わせを除外する")
    args = ap.parse_args()

    dates = set([d.strip() for d in args.dates.split(",") if d.strip()]) if args.dates else set()
    pids_filter  = set([p.strip() for p in args.pids.split(",") if p.strip()])
    races_filter = set([_norm_race(r) for r in args.races.split(",") if r.strip()])

    # ファイルインデックス
    int_idx  = collect_files(args.base, "integrated", dates) if dates else \
               collect_files(args.base, "integrated", set(os.listdir(os.path.join(args.base, "integrated", "v1"))))
    res_idx  = collect_results_files(args.base, dates)  # 柔軟版

    # 共通キー
    keys_all = set(int_idx.keys()) & set(res_idx.keys())
    keys = sorted(keys_all)
    if pids_filter:
        keys = [k for k in keys if k[1] in pids_filter]
    if races_filter:
        keys = [k for k in keys if k[2] in races_filter]
    if args.limit and args.limit > 0:
        keys = keys[:args.limit]

    os.makedirs(args.outdir, exist_ok=True)

    # 予測のみ
    if args.predict_only:
        pred_dir = os.path.join(args.outdir, "predict")
        if os.path.exists(pred_dir): shutil.rmtree(pred_dir)
        os.makedirs(pred_dir, exist_ok=True)

        rows = []
        limit_n = args.limit or len(keys)
        for (date, pid, race) in keys[:limit_n]:
            with open(int_idx[(date,pid,race)], "r", encoding="utf-8") as f:
                d_int = json.load(f)
            tri_probs, kim_probs, exacta_probs, third_probs = simulate_one(d_int, sims=args.sims)
            if args.strategy == "exacta_topK_third_topM":
                tickets = generate_tickets(args.strategy, tri_probs, exacta_probs, third_probs, k=args.k, m=args.m)
            else:
                tickets = generate_tickets("trifecta_topN", tri_probs, exacta_probs, third_probs, topn=args.topn)
            # ★ 予測のみ側にも除外適用
            if args.exclude_first1:
                tickets = [(key,score) for (key,score) in tickets if int(key[0]) != 1]

            top_list = [{"ticket": "-".join(map(str, k)), "score": round(v, 6)} for k, v in tickets]

            with open(os.path.join(pred_dir, f"pred_{date}_{pid}_{race}.json"), "w", encoding="utf-8") as f:
                json.dump({"date":date,"pid":pid,"race":race,"buylist":top_list,
                           "engine":"SimS ver1.0 (E1)","exclude_first1":bool(args.exclude_first1)},
                          f, ensure_ascii=False, indent=2)

            for i, t in enumerate(top_list, 1):
                rows.append({"date":date,"pid":pid,"race":race,"rank":i,"ticket":t["ticket"],"score":t["score"]})

        pd.DataFrame(rows).to_csv(os.path.join(pred_dir, "predictions_summary.csv"),
                                  index=False, encoding="utf-8")
        print(f"[predict] candidates: {len(keys)}  -> {pred_dir}")
        return

    # ---- eval ----
    print(f"[eval] races to evaluate: {len(keys)}")
    per_rows = []
    total_stake = 0
    total_payout = 0
    total_hit = 0

    hit_detail = []
    bucket_odds = [(1,10),(10,20),(20,50),(50,100),(100,1000),(1000,1_000_000)]

    for (date, pid, race) in keys:
        ev = evaluate_one(
            int_idx[(date,pid,race)],
            res_idx[(date,pid,race)],
            sims=args.sims, unit=args.unit,
            strategy=args.strategy, topn=args.topn, k=args.k, m=args.m,
            exclude_first1=args.exclude_first1
        )
        total_stake += ev["stake"]
        total_payout += ev["payout"]
        total_hit += ev["hit"]

        per_rows.append({
            "date": date, "pid": pid, "race": race,
            "bets": len(ev["bets"]), "stake": ev["stake"], "payout": ev["payout"],
            "hit": ev["hit"], "hit_combo": ev["hit_combo"]
        })

        if ev["hit"]:
            k_lab = None
            if ev["kim_probs"]:
                k_lab = sorted(ev["kim_probs"].items(), key=lambda kv: kv[1], reverse=True)[0][0]
            payout100 = ev["payout"] / args.unit if args.unit>0 else 0
            bz = None
            for lo,hi in bucket_odds:
                if lo <= payout100 < hi:
                    bz = f"{lo}-{hi}"
                    break
            hit_detail.append({
                "date":date,"pid":pid,"race":race,
                "hit_combo":ev["hit_combo"],
                "rank_hit":ev["rank_hit"],
                "payout":ev["payout"], "odds_approx": payout100,
                "kim_est":k_lab
            })

    df = pd.DataFrame(per_rows)
    overall = {
        "engine": "SimS ver1.0 (E1)",
        "races": int(len(df)),
        "bets_total": int(df["bets"].sum()) if len(df)>0 else 0,
        "stake_total": int(total_stake),
        "payout_total": int(total_payout),
        "hit_rate": float(df["hit"].mean()) if len(df)>0 else 0.0,
        "roi": float((total_payout - total_stake)/total_stake) if total_stake>0 else 0.0,
        "strategy": args.strategy,
        "topn": args.topn, "k": args.k, "m": args.m,
        "sims_per_race": args.sims, "unit": args.unit,
        "exclude_first1": bool(args.exclude_first1)
    }

    # 保存
    df.to_csv(os.path.join(args.outdir, "per_race_results.csv"), index=False)
    with open(os.path.join(args.outdir, "overall.json"), "w", encoding="utf-8") as f:
        json.dump(overall, f, ensure_ascii=False, indent=2)

    # ---- 的中群レポート ----
    rep_path = os.path.join(args.outdir, "hit_report.json")
    if len([r for r in per_rows if r["hit"]]) > 0:
        hit_df = pd.DataFrame([h for h in 
            [{"date":d["date"],"pid":d["pid"],"race":d["race"],"hit_combo":d["hit_combo"],
              "rank_hit":None,"payout":d["payout"],"odds_approx":(d["payout"]/overall["unit"] if overall["unit"]>0 else 0),
              "kim_est":None} for d in per_rows if d["hit"]]] )

        by_kim = pd.DataFrame(columns=["kim_est","hits"])

        def first_lane(c):
            try:
                return int(str(c).split("-")[0])
            except:
                return None
        hit_df["first_lane"] = hit_df["hit_combo"].map(first_lane)
        by_first = (hit_df.groupby("first_lane").size().reset_index(name="hits")
                    .sort_values("first_lane"))

        def band(x):
            try:
                o = float(x)
            except:
                return "unknown"
            if o < 10: return "01-09"
            if o < 20: return "10-19"
            if o < 50: return "20-49"
            if o < 100: return "50-99"
            if o < 1000: return "100-999"
            return "1000+"
        hit_df["odds_band"] = hit_df["odds_approx"].map(band)
        by_band = (hit_df.groupby("odds_band").size().reset_index(name="hits")
                   .sort_values("odds_band"))

        by_combo = (hit_df.groupby("hit_combo").size().reset_index(name="hits")
                    .sort_values("hits", ascending=False).head(10))

        by_rank = pd.DataFrame([{"rank_bucket":"NA","hits":len(hit_df)}])

        report = {
            "summary": overall,
            "by_kimarite_est": by_kim.to_dict(orient="records"),
            "by_first_lane": by_first.to_dict(orient="records"),
            "by_odds_band": by_band.to_dict(orient="records"),
            "by_hit_combo_top10": by_combo.to_dict(orient="records"),
            "by_rank_bucket": by_rank.to_dict(orient="records")
        }
        with open(rep_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
    else:
        with open(rep_path, "w", encoding="utf-8") as f:
            json.dump({"summary": overall, "note": "no hits found"}, f, ensure_ascii=False, indent=2)

    print("=== OVERALL (SimS ver1.0 E1) ===")
    print(json.dumps(overall, ensure_ascii=False, indent=2))
    print(f"\n[files] {args.outdir}/per_race_results.csv, {args.outdir}/overall.json, {rep_path}")

if __name__ == "__main__":
    main()
