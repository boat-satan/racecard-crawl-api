# sims_batch_eval_SimS_v1.py
# SimS ver1.0 — 同時同条件バッチ検証（三連単 TOPN 均等買い）
#  - 統合データ/オッズ/リザルトを読み込み
#  - SimS ver1.0 で各レースを N試行シミュ
#  - TOPN買いで的中率・ROIを算出
#  - CSV/JSON に保存
# 使い方例：
#   python sims_batch_eval_SimS_v1.py --base ./public --dates 20250810,20250811,20250812 --sims 1200 --topn 18 --unit 100

import os, json, math, argparse, csv
from collections import defaultdict, Counter
import numpy as np
import pandas as pd

# =========================
# SimS ver1.0 パラメータ
# =========================
class Params:
    # T1M 到達式
    b0=100.0
    alpha_R=0.005          # 助走距離係数
    alpha_A=-0.010         # 伸びA
    alpha_Ap=-0.012        # 出足Ap（入口直前～旋回寄与を簡易で混ぜる）

    # 勝負圏・追い抜き
    theta=0.028
    a0=0.0
    b_dt=15.0
    cK=1.2

    # 決まり手
    tau_k=0.030

    # イベント強度
    beta_sq=0.006          # 並走圧（squeeze）→到達遅延
    beta_wk=0.004          # 引き波微損
    beta_flow_R=0.002      # 流れ（今回は未使用/中立）
    k_turn_err=0.010

    # 先マイ権/ラインブロック
    delta_first=0.8
    delta_lineblock=0.5

    # 安全マージン
    safe_margin_mu=0.005
    safe_margin_sigma=0.003
    p_safe_margin=0.20

    # ビビり戻し（ST直前に戻す）
    p_backoff=0.10
    backoff_ST_shift=0.015
    backoff_A_penalty=0.15

    # キャビテーション
    p_cav=0.03
    cav_A_penalty=0.25

    # セッション（気温/水温などのランダム揺らぎ）
    session_ST_shift_mu=0.0
    session_ST_shift_sd=0.004
    session_A_bias_mu=0.0
    session_A_bias_sd=0.10

    # 風（簡易）：今回は env=中立で固定
    wind_theta_gain=0.002
    wind_st_sigma_gain=0.5

    # 壁
    gamma_wall=0.006

    # 引き波発生率の基準
    base_wake=0.20
    extra_wake_when_outside=0.25

# 乱数
rng = np.random.default_rng(2025)

# ========== ユーティリティ ==========
def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))

def s_base_from_nat(rc: dict) -> float:
    """全国勝率・2連率・3連率から素点 S_base を作る（級別は不使用）"""
    n1 = float(rc.get("natTop1", 6.0))
    n2 = float(rc.get("natTop2", 50.0))
    n3 = float(rc.get("natTop3", 70.0))
    return (0.5 * ((n1 - 6.0) / 2.0)
          + 0.3 * ((n2 - 50.0) / 20.0)
          + 0.2 * ((n3 - 70.0) / 20.0))

def wind_adjustments(env: dict):
    """風補正（今回は env=中立想定だが式は残す）"""
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
    # 今回は env を中立とし、流れ補正は無効化
    return 0.0

def wake_loss_probability(lane, entry_order):
    pos = entry_order.index(lane)
    base = Params.base_wake + Params.extra_wake_when_outside * ((lane - 1) / 5.0)
    if pos == 0:
        base *= 0.3
    return max(0.0, min(base, 0.95))

# ========== 入力変換（統合データ → SimS ver1.0 入力） ==========
def build_input_from_integrated(d: dict) -> dict:
    lanes = [e["lane"] for e in d["entries"]]

    # ST mu
    mu = {}
    S  = {}
    F  = {}
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

    # ST 分布（正規）
    ST_model = {}
    for lane in lanes:
        sigma = 0.02 * (1 + 0.20 * (1 if F[lane] > 0 else 0) + 0.15 * max(0.0, -S[lane]))
        ST_model[str(lane)] = {"type": "normal", "mu": mu[lane], "sigma": sigma}

    # 助走距離（スロー基準仮）
    R = {str(l): float({1: 88, 2: 92, 3: 96, 4: 100, 5: 104, 6: 108}.get(l, 100.0)) for l in lanes}

    # A / Ap
    course_bias = {1: 0.05, 2: 0.05, 3: 0.02, 4: 0.00, 5: -0.05, 6: -0.06}
    A  = {}
    Ap = {}
    for l in lanes:
        deltaST = (0.16 - mu[l]) * 5.0
        A[l]  = 0.7 * S[l] + 0.3 * deltaST
        Ap[l] = 0.7 * S[l] + 0.3 * course_bias.get(l, 0.0)

    # squeeze（1の壁強→外に負担）
    S1 = S.get(1, 0.0)
    squeeze = {}
    for l in lanes:
        val = 0.0 if l == 1 else max(0.0, (S1 - S[l]) * 0.20)
        squeeze[str(l)] = min(val, 0.20)

    # 先マイ権 / ラインブロック（初期自動）
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
        p = sigmoid(Params.a0 + Params.b_dt * (theta_eff - dt_eff) + Params.cK * dK + delta)
        if rng.random() < p:
            exit_order[k], exit_order[k+1] = chase, lead
    return exit_order

def simulate_one(integrated_json: dict, sims: int = 1200):
    inp = build_input_from_integrated(integrated_json)
    lanes = inp["lanes"]; env = inp["env"]
    _, st_gain = wind_adjustments(env)

    trifecta = Counter()
    kimarite = Counter()
    for _ in range(sims):
        ST = {i: sample_ST(inp["ST_model"][str(i)]) for i in lanes}
        T1M = {
            i: t1m_time(ST[i], inp["R"][str(i)], inp["A"][i], inp["Ap"][i],
                        inp["squeeze"][str(i)], env, i, st_gain)
            for i in lanes
        }
        entry = sorted(lanes, key=lambda x: T1M[x])
        # 引き波微損
        for i in lanes:
            if rng.random() < wake_loss_probability(i, entry):
                T1M[i] += Params.beta_wk
        exit_order = one_pass(entry, T1M, inp["A"], inp["Ap"], env, inp["lineblocks"], inp["first_right"])
        # 決まり手
        lead = exit_order[0]
        dt_lead = T1M[exit_order[1]] - T1M[lead]
        kim = "逃げ" if lead == 1 else ("まくり" if dt_lead >= Params.tau_k else "まくり差し")
        kimarite[kim] += 1
        trifecta[tuple(exit_order[:3])] += 1

    # 確率化
    total = sims
    tri_probs = {k: v/total for k, v in trifecta.items()}
    kim_probs = {k: v/total for k, v in kimarite.items()}
    return tri_probs, kim_probs

# ========== データ収集 ==========
def collect_files(base_dir: str, kind: str, dates: set):
    root = os.path.join(base_dir, kind, "v1")
    out = []
    for d in dates:
        dir_d = os.path.join(root, d)
        if not os.path.isdir(dir_d): 
            continue
        for pid in os.listdir(dir_d):
            dir_pid = os.path.join(dir_d, pid)
            if not os.path.isdir(dir_pid): 
                continue
            for fname in os.listdir(dir_pid):
                if fname.endswith(".json"):
                    race = fname[:-5]  # drop .json
                    out.append(((d, pid, race), os.path.join(dir_pid, fname)))
    return dict(out)

# ========== オッズ/結果のパース ==========
def odds_map(odds_json: dict) -> dict:
    """三連単オッズを { '1-2-3': decimal_odds, ... } で返す"""
    out = {}
    for item in odds_json.get("trifecta", []):
        combo = item.get("combo") or f'{item.get("F")}-{item.get("S")}-{item.get("T")}'
        if combo:
            out[combo] = float(item["odds"])
    return out

def actual_trifecta_combo(result_json: dict):
    """確定三連単の '1-2-3' 形式を返す（フォールバック含む）"""
    trif = (result_json.get("payouts") or {}).get("trifecta")
    if isinstance(trif, dict) and "combo" in trif:
        return trif["combo"]
    # フォールバック：order 先頭3つ
    order = result_json.get("order")
    if isinstance(order, list) and len(order) >= 3:
        def lane_of(x):
            # lane / course / number のいずれか
            return str(x.get("lane") or x.get("course") or x.get("F") or x.get("number"))
        try:
            f = lane_of(order[0]); s = lane_of(order[1]); t = lane_of(order[2])
            if all([f,s,t]): return f"{f}-{s}-{t}"
        except Exception:
            return None
    return None

# ========== 評価（1レース） ==========
def evaluate_one(int_path: str, odds_path: str, res_path: str, sims: int, topn: int, unit: int):
    with open(int_path, "r") as f:
        d_int = json.load(f)
    tri_probs, _ = simulate_one(d_int, sims=sims)

    # TOPN
    top = sorted(tri_probs.items(), key=lambda kv: kv[1], reverse=True)[:topn]
    top_keys = ['-'.join(map(str, k)) for k, _ in top]

    # オッズ
    with open(odds_path, "r") as f:
        d_odds = json.load(f)
    omap = odds_map(d_odds)
    bets = [c for c in top_keys if c in omap]
    stake = unit * len(bets)

    # 結果
    with open(res_path, "r") as f:
        d_res = json.load(f)
    hit_combo = actual_trifecta_combo(d_res)
    payout = int(round(omap.get(hit_combo, 0.0) * unit)) if hit_combo in bets else 0

    return stake, payout, (1 if payout > 0 else 0), bets, hit_combo

# ========== メイン ==========
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="./public", help="public ディレクトリのパス")
    ap.add_argument("--dates", default="", help="カンマ区切りの日付（例: 20250810,20250811,20250812）")
    ap.add_argument("--sims", type=int, default=600, help="1レースあたりの試行回数")
    ap.add_argument("--topn", type=int, default=18, help="買い目TOPN")
    ap.add_argument("--unit", type=int, default=100, help="1点あたりの賭け金（円）")
    ap.add_argument("--limit", type=int, default=0, help="先頭からNレースだけ評価（0なら全件）")
    ap.add_argument("--outdir", default="./SimS_v1.0_eval", help="出力先ディレクトリ")
    args = ap.parse_args()

    dates = set([d.strip() for d in args.dates.split(",") if d.strip()]) if args.dates else set()

    # ファイル収集
    int_idx  = collect_files(args.base, "integrated", dates) if dates else collect_files(args.base, "integrated", set(os.listdir(os.path.join(args.base, "integrated", "v1"))))
    odds_idx = collect_files(args.base, "odds",       dates) if dates else collect_files(args.base, "odds",       set(os.listdir(os.path.join(args.base, "odds", "v1"))))
    res_idx  = collect_files(args.base, "results",    dates) if dates else collect_files(args.base, "results",    set(os.listdir(os.path.join(args.base, "results", "v1"))))

    # 共通キー
    keys = sorted(set(int_idx.keys()) & set(odds_idx.keys()) & set(res_idx.keys()))
    if args.limit and args.limit > 0:
        keys = keys[:args.limit]

    per_rows = []
    total_stake = 0
    total_payout = 0
    total_hit = 0

    for (date, pid, race) in keys:
        stake, payout, hit, bets, hit_combo = evaluate_one(
            int_idx[(date,pid,race)],
            odds_idx[(date,pid,race)],
            res_idx[(date,pid,race)],
            sims=args.sims, topn=args.topn, unit=args.unit
        )
        total_stake += stake
        total_payout += payout
        total_hit += hit

        per_rows.append({
            "date": date, "pid": pid, "race": race,
            "bets": len(bets), "stake": stake, "payout": payout,
            "hit": hit, "hit_combo": hit_combo
        })

    # 集計
    df = pd.DataFrame(per_rows)
    overall = {
        "engine": "SimS ver1.0",
        "races": int(len(df)),
        "bets_total": int(df["bets"].sum()) if len(df)>0 else 0,
        "stake_total": int(total_stake),
        "payout_total": int(total_payout),
        "hit_rate": float(df["hit"].mean()) if len(df)>0 else 0.0,
        "roi": float((total_payout - total_stake)/total_stake) if total_stake>0 else 0.0,
        "topn": args.topn, "sims_per_race": args.sims, "unit": args.unit
    }
    by_date = None
    if len(df) > 0:
        by_date = (df.groupby("date")
                     .agg(races=("race","count"),
                          stake_total=("stake","sum"),
                          payout_total=("payout","sum"),
                          hit_rate=("hit","mean"))
                     .reset_index())
        by_date["roi"] = (by_date["payout_total"] - by_date["stake_total"]) / by_date["stake_total"]

    # 保存
    os.makedirs(args.outdir, exist_ok=True)
    per_csv = os.path.join(args.outdir, "per_race_results.csv")
    df.to_csv(per_csv, index=False)

    if by_date is not None:
        by_date_csv = os.path.join(args.outdir, "by_date_summary.csv")
        by_date.to_csv(by_date_csv, index=False)

    with open(os.path.join(args.outdir, "overall.json"), "w", encoding="utf-8") as f:
        json.dump(overall, f, ensure_ascii=False, indent=2)

    # 画面出力
    print("=== OVERALL (SimS ver1.0) ===")
    print(json.dumps(overall, ensure_ascii=False, indent=2))
    if by_date is not None:
        print("\n=== BY DATE ===")
        print(by_date.to_string(index=False))

if __name__ == "__main__":
    main()