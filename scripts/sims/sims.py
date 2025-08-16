# sims_batch_eval_SimS_v1.py
# SimS ver1.0 — 同時同条件バッチ検証 + キーマン出力 + キーマン外加点ランク
# - 統合データ/結果を読み込み（払い戻しは results から参照）
# - SimS ver1.0 で各レースを N試行シミュ
# - 買い目を生成（デフォルト: 三連単 TOPN）→ 的中率・ROI算出
# - 任意: 1着=1号艇の除外/限定、EVフィルタ、オッズバンドフィルタ対応
# - 簡易ヒットレポート出力
# - 外部パラメータ上書き (--params/--set)
# - キーマン指標を <outdir>/keyman/<date>/<pid>/<race>.json に保存（KEYMAN_RANK 追加）

import os, json, math, argparse, shutil
from collections import Counter, defaultdict
import numpy as np
import pandas as pd

# ======== パラメータ上書きユーティリティ ========
try:
    import tomllib  # py311+
except Exception:
    tomllib = None

def load_param_file(path: str) -> dict:
    if not path:
        return {}
    p = os.path.expanduser(path)
    if not os.path.isfile(p):
        raise FileNotFoundError(p)
    ext = os.path.splitext(p)[1].lower()
    if ext == ".json":
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    if ext == ".toml":
        if tomllib is None:
            raise RuntimeError("tomlファイルを読むには Python 3.11 以上が必要です")
        with open(p, "rb") as f:
            return tomllib.load(f)
    raise ValueError(f"Unsupported params file extension: {ext} (use .json or .toml)")

def parse_set_overrides(expr: str) -> dict:
    out = {}
    if not expr:
        return out
    for kv in [p.strip() for p in expr.split(",") if p.strip()]:
        if "=" not in kv:
            continue
        k, v = kv.split("=", 1)
        k = k.strip()
        v = v.strip()
        try:
            if v.lower() in ("true","false"):
                out[k] = (v.lower() == "true")
            else:
                out[k] = float(v) if (("." in v) or ("e" in v.lower())) else int(v)
        except Exception:
            out[k] = v
    return out

def apply_overrides_to_class(cls, over: dict):
    for k, v in over.items():
        if hasattr(cls, k):
            setattr(cls, k, v)

# =========================
# SimS ver1.0 パラメータ（調整済み）
# =========================
class Params:
    b0=100.0
    alpha_R=0.005
    alpha_A=-0.010
    alpha_Ap=-0.012
    theta=0.0285
    a0=0.0
    b_dt=15.0
    cK=1.2
    leadOuterDrift_1=0.034
    outerBaseCost=0.022
    tau_k=0.030
    beta_sq=0.006
    beta_wk=0.004
    k_turn_err=0.010
    gamma_wall=0.006
    delta_first=0.70
    delta_lineblock=0.5
    safe_margin_mu=0.005
    safe_margin_sigma=0.003
    p_safe_margin=0.20
    p_backoff=0.10
    backoff_ST_shift=0.015
    backoff_A_penalty=0.15
    p_cav=0.03
    cav_A_penalty=0.25
    session_ST_shift_mu=0.0
    session_ST_shift_sd=0.004
    session_A_bias_mu=0.0
    session_A_bias_sd=0.10
    wind_theta_gain=0.002
    wind_st_sigma_gain=0.5
    base_wake=0.20
    extra_wake_when_outside=0.25
    decision_bias_mult=1.0

# 乱数固定
rng = np.random.default_rng(2025)

# ---------- ユーティリティ ----------
def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))

def s_base_from_nat(rc: dict) -> float:
    n1 = float(rc.get("natTop1", 6.0)); n2 = float(rc.get("natTop2", 50.0)); n3 = float(rc.get("natTop3", 70.0))
    return (0.5 * ((n1 - 6.0) / 2.0) + 0.3 * ((n2 - 50.0) / 20.0) + 0.2 * ((n3 - 70.0) / 20.0))

def wind_adjustments(env: dict):
    d = (env.get("wind") or {}).get("dir", "cross"); m = float((env.get("wind") or {}).get("mps", 0.0))
    sign = 1 if d=="tail" else -1 if d=="head" else 0
    d_theta = Params.wind_theta_gain * sign * m
    st_sigma_gain = 1.0 + Params.wind_st_sigma_gain * (abs(m)/10.0)
    return d_theta, st_sigma_gain

def apply_session_bias(ST, A, Ap):
    ST += rng.normal(Params.session_ST_shift_mu, Params.session_ST_shift_sd)
    A  *= (1.0 + rng.normal(Params.session_A_bias_mu, Params.session_A_bias_sd))
    Ap *= (1.0 + rng.normal(Params.session_A_bias_mu, Params.session_A_bias_sd))
    return ST, A, Ap

# （フラグ返却）
def maybe_backoff(ST, A):
    if rng.random() < Params.p_backoff:
        return ST + Params.backoff_ST_shift, A * (1.0 - Params.backoff_A_penalty), True
    return ST, A, False

def maybe_cav(A):
    if rng.random() < Params.p_cav:
        return A * (1.0 - Params.cav_A_penalty), True
    return A, False

def maybe_safe_margin():
    if rng.random() < Params.p_safe_margin:
        return max(0.0, rng.normal(Params.safe_margin_mu, Params.safe_margin_sigma)), True
    return 0.0, False

def flow_bias(env, lane):  # 今回は無効化
    return 0.0

def wake_loss_probability(lane, entry_order):
    pos = entry_order.index(lane)
    base = Params.base_wake + Params.extra_wake_when_outside * ((lane - 1) / 5.0)
    if pos == 0: base *= 0.3
    return max(0.0, min(base, 0.95))

# ---------- キーマンスコア（外加点版） ----------
def compute_keyman_rank(keyman: dict,
                        outside_beta: float = 0.6,
                        outside_gamma: float = 1.0,
                        w: dict | None = None) -> dict:
    """
    laneごとのキーマンスコア K_i を 0〜1 に正規化して返す（max=1.0）。
    BaseScore := w1*H1 + w2*H2 + w3*H3 + w4*pos_plus + w5*swap_gain
                 - (wr1*BACKOFF + wr2*CAV + wr3*WAKE)
    OutsideBonus := outside_beta * ((lane-1)/5)^outside_gamma * max(BaseScore, 0)
    K_raw := BaseScore + OutsideBonus
    """
    if not keyman:
        return {}

    if w is None:
        w = dict(h1=1.0, h2=0.6, h3=0.3,
                 pos=0.6, swap=0.4,
                 risk_backoff=0.5, risk_cav=0.4, risk_wake=0.7)

    H1 = keyman.get("H1", {})
    H2 = keyman.get("H2", {})
    H3 = keyman.get("H3", {})
    POS = keyman.get("POS_DELTA_AVG", {})
    BACK = keyman.get("BACKOFF", {})
    CAV  = keyman.get("CAV", {})
    WAKE = keyman.get("WAKE", {})
    SWAP = keyman.get("SWAP", {})

    # swap_gain を各艇別に集計（chase>lead のchase側に加点）
    swap_gain = defaultdict(float)
    for pair, cnt in SWAP.items():
        # 形式 "c>l"
        if ">" in pair:
            c, l = pair.split(">", 1)
            try:
                c_i = int(c)
                swap_gain[c_i] += float(cnt)
            except:
                pass
    # 規模感を揃えるため総試行回数でスケール（なければ1で割る）
    trials = max(1, int(keyman.get("trials") or 1))
    for i in list(swap_gain.keys()):
        swap_gain[i] = swap_gain[i] / trials

    lanes = sorted({*map(int, [*H1.keys(), *H2.keys(), *H3.keys(), *POS.keys(),
                               *BACK.keys(), *CAV.keys(), *WAKE.keys(),
                               *map(lambda x: int(x.split(">")[0]) if ">" in x else 0, SWAP.keys())])})
    scores = {}
    for i in lanes:
        h1 = float(H1.get(str(i), 0.0))
        h2 = float(H2.get(str(i), 0.0))
        h3 = float(H3.get(str(i), 0.0))
        pos_plus = max(0.0, float(POS.get(str(i), 0.0)))  # マイナスは切り捨て
        sw = float(swap_gain.get(i, 0.0))
        r_back = float(BACK.get(str(i), 0.0))
        r_cav  = float(CAV.get(str(i), 0.0))
        r_wake = float(WAKE.get(str(i), 0.0))

        base = (w["h1"]*h1 + w["h2"]*h2 + w["h3"]*h3 +
                w["pos"]*pos_plus + w["swap"]*sw -
                (w["risk_backoff"]*r_back + w["risk_cav"]*r_cav + w["risk_wake"]*r_wake))

        outside_factor = ((i - 1) / 5.0) ** outside_gamma  # 1→0, 6→1
        bonus = outside_beta * outside_factor * max(0.0, base)
        k_raw = base + bonus
        scores[i] = max(0.0, k_raw)

    # 0〜1へ正規化（max=1）
    mx = max(scores.values()) if scores else 1.0
    if mx <= 0:
        return {str(i): 0.0 for i in lanes}
    return {str(i): (scores[i]/mx) for i in lanes}

# ---------- 入力変換 ----------
def build_input_from_integrated(d: dict) -> dict:
    lanes = [e["lane"] for e in d["entries"]]
    mu = {}; S = {}; F = {}
    for e in d["entries"]:
        lane = e["lane"]; rc = e["racecard"]; ec = (e.get("stats") or {}).get("entryCourse", {})
        rc_st = rc.get("avgST", None); ec_st = ec.get("avgST", None)
        vals = [v for v in [rc_st, ec_st] if isinstance(v, (int, float))]
        if not vals: m = 0.16
        elif len(vals) == 1: m = float(vals[0])
        else: m = 0.5 * float(vals[0]) + 0.5 * float(vals[1])
        if int(rc.get("flyingCount", 0)) > 0: m += 0.010
        mu[lane] = m; S[lane] = s_base_from_nat(rc); F[lane] = int(rc.get("flyingCount", 0))

    ST_model = {}
    for lane in lanes:
        sigma = 0.02 * (1 + 0.20 * (1 if F[lane] > 0 else 0) + 0.15 * max(0.0, -S[lane]))
        sigma *= (1.0 + 0.1 * (lane - 1))  # 外ほど大きく
        ST_model[str(lane)] = {"type": "normal", "mu": mu[lane], "sigma": sigma}

    R = {str(l): float({1: 88, 2: 92, 3: 96, 4: 100, 5: 104, 6: 108}.get(l, 100.0)) for l in lanes}

    course_bias = {1: 0.05, 2: 0.05, 3: 0.02, 4: 0.00, 5: -0.05, 6: -0.06}
    A  = {}; Ap = {}
    for l in lanes:
        deltaST = (0.16 - mu[l]) * 5.0
        A[l]  = 0.7 * S[l] + 0.3 * deltaST
        Ap[l] = 0.7 * S[l] + 0.3 * course_bias.get(l, 0.0)

    S1 = S.get(1, 0.0)
    squeeze = {str(l): (0.0 if l==1 else min(max(0.0, (S1 - S[l]) * 0.20), 0.20)) for l in lanes}

    first_right = []; lineblocks = []
    if S1 > 0.30 and mu.get(1, 0.16) <= 0.17: first_right.append(1)
    S4 = S.get(4, 0.0)
    if S4 > 0.10 and mu.get(4, 0.16) <= 0.17: first_right.append(4)
    S2 = S.get(2, 0.0)
    if (S1 - S2) > 0.20: lineblocks.append((1, 2))
    if (S4 - S1) > 0.05:
        sc4 = next((e.get("startCourse", 4) for e in d["entries"] if e["lane"] == 4), 4)
        if sc4 >= 4: lineblocks.append((4, 1))

    env = {"wind": {"dir": "cross", "mps": 0.0}, "flow": {"dir": "none", "rate": 0.0}}
    return {
        "lanes": lanes, "ST_model": ST_model, "R": R, "A": A, "Ap": Ap, "env": env,
        "squeeze": squeeze, "first_right": set(first_right), "lineblocks": set(lineblocks)
    }

# ---------- 1レース・シミュ ----------
def sample_ST(model): return rng.normal(model["mu"], model["sigma"])

# （フラグも返す）
def t1m_time(ST, R, A, Ap, sq, env, lane, st_gain):
    ST, A, Ap = apply_session_bias(ST, A, Ap)
    ST, A, did_backoff = maybe_backoff(ST, A)
    A, did_cav = maybe_cav(A)
    t = (Params.b0 + Params.alpha_R*(R-100.0) + Params.alpha_A*A + Params.alpha_Ap*Ap
         + Params.beta_sq*sq + flow_bias(env, lane))
    t += ST * st_gain
    return t, {"backoff": did_backoff, "cav": did_cav}

def decision_bias_term(lead, chase, lane, kimarite_hint=None):
    base = 1.0
    if Params.decision_bias_mult != 1.0: base *= Params.decision_bias_mult
    return base

# 入れ替わり/ブロックの記録を返す
def one_pass(entry, T1M, A, Ap, env, lineblocks, first_right):
    exit_order = entry[:]
    swaps = []   # list of (chase, lead)
    blocks = []  # list of (lead, chase) when delta>0 and no swap
    safe_used_count = 0
    d_theta, _ = wind_adjustments(env); theta_eff = Params.theta + d_theta
    for k in range(len(exit_order) - 1):
        lead, chase = exit_order[k], exit_order[k+1]
        dt = T1M[chase] - T1M[lead]
        dK = (A[chase] + Ap[chase]) - (A[lead] + Ap[lead])
        delta = (Params.delta_lineblock if (lead, chase) in lineblocks else 0.0)
        if lead in first_right: delta += Params.delta_first
        turn_err, used = maybe_safe_margin()
        if used: safe_used_count += 1
        dt_eff = dt + Params.gamma_wall + Params.k_turn_err * turn_err
        logit = Params.a0 + Params.b_dt*(theta_eff - dt_eff) + Params.cK*dK + delta
        logit *= decision_bias_term(lead, chase, chase)
        will_swap = (rng.random() < sigmoid(logit))
        if will_swap:
            swaps.append((chase, lead))
            exit_order[k], exit_order[k+1] = chase, lead
        else:
            if delta > 0.0:
                blocks.append((lead, chase))
    return exit_order, swaps, blocks, safe_used_count

def simulate_one(integrated_json: dict, sims: int = 600):
    inp = build_input_from_integrated(integrated_json)
    lanes = inp["lanes"]; env = inp["env"]; _, st_gain = wind_adjustments(env)

    # 既存集計
    trifecta = Counter(); kimarite = Counter()
    pair_counts = Counter(); third_counts = Counter()

    # キーマン用集計
    first_counts = Counter(); second_counts = Counter(); third_only = Counter()
    wake_hits = Counter(); backoff_hits = Counter(); cav_hits = Counter()
    swap_pair = Counter(); block_pair = Counter()
    pos_delta_sum = {i: 0 for i in lanes}
    safe_used_total = 0

    for _ in range(sims):
        ST = {i: sample_ST(inp["ST_model"][str(i)]) for i in lanes}

        T1M = {}
        for i in lanes:
            t, fl = t1m_time(ST[i], inp["R"][str(i)], inp["A"][i], inp["Ap"][i],
                             inp["squeeze"][str(i)], env, i, st_gain)
            T1M[i] = t
            if fl["backoff"]: backoff_hits[i] += 1
            if fl["cav"]:     cav_hits[i] += 1

        entry = sorted(lanes, key=lambda x: T1M[x])

        # ウェイク
        for i in lanes:
            if rng.random() < wake_loss_probability(i, entry):
                wake_hits[i] += 1
                T1M[i] += Params.beta_wk

        exit_order, swaps, blocks, safe_used_cnt = one_pass(
            entry, T1M, inp["A"], inp["Ap"], env, inp["lineblocks"], inp["first_right"]
        )
        safe_used_total += safe_used_cnt

        # 既存集計
        lead = exit_order[0]
        dt_lead = T1M[exit_order[1]] - T1M[lead]
        kim = "逃げ" if lead == 1 else ("まくり" if dt_lead >= Params.tau_k else "まくり差し")
        kimarite[kim] += 1

        trifecta[tuple(exit_order[:3])] += 1
        pair_counts[(exit_order[0], exit_order[1])] += 1
        third_counts[exit_order[2]] += 1

        # 着順分布
        if len(exit_order) >= 3:
            first_counts[exit_order[0]]  += 1
            second_counts[exit_order[1]] += 1
            third_only[exit_order[2]]    += 1

        # swap / block
        for c,l in swaps:  swap_pair[(c,l)] += 1
        for l,c in blocks: block_pair[(l,c)] += 1

        # ポジション変化
        entry_pos = {b:i for i,b in enumerate(entry)}
        exit_pos  = {b:i for i,b in enumerate(exit_order)}
        for i in lanes:
            pos_delta_sum[i] += (entry_pos[i] - exit_pos[i])  # +:前進

    total = sims
    tri_probs = {k: v/total for k, v in trifecta.items()}
    kim_probs = {k: v/total for k, v in kimarite.items()}
    exacta_probs = {k: v/total for k, v in pair_counts.items()}
    third_probs  = {k: v/total for k, v in third_counts.items()}

    # キーマン集計を整形
    keyman = {
        "trials": int(total),
        "H1": {str(i): first_counts[i]/total for i in lanes},
        "H2": {str(i): second_counts[i]/total for i in lanes},
        "H3": {str(i): third_only[i]/total for i in lanes},
        "SWAP": {f"{c}>{l}": int(cnt) for (c,l),cnt in swap_pair.items()},
        "BLOCK": {f"{l}|{c}": int(cnt) for (l,c),cnt in block_pair.items()},
        "WAKE": {str(i): wake_hits[i]/total for i in lanes},
        "BACKOFF": {str(i): backoff_hits[i]/total for i in lanes},
        "CAV": {str(i): cav_hits[i]/total for i in lanes},
        "POS_DELTA_AVG": {str(i): pos_delta_sum[i]/total for i in lanes},
        "SAFE_MARGIN_EVENTS_PER_TRIAL": safe_used_total/(total*max(1,(len(lanes)-1)))
    }
    return tri_probs, kim_probs, exacta_probs, third_probs, keyman

# ---------- データ収集 ----------
def collect_files(base_dir: str, kind: str, dates: set):
    root_v1 = os.path.join(base_dir, kind, "v1")
    root    = root_v1 if os.path.isdir(root_v1) else os.path.join(base_dir, kind)
    out = []
    date_dirs = list(dates) if dates else [d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))]
    for d in date_dirs:
        dir_d = os.path.join(root, d)
        if not os.path.isdir(dir_d): continue
        for pid in os.listdir(dir_d):
            dir_pid = os.path.join(dir_d, pid)
            if not os.path.isdir(dir_pid): continue
            for fname in os.listdir(dir_pid):
                if fname.endswith(".json"):
                    race = fname[:-5]
                    out.append(((d, pid, race), os.path.join(dir_pid, fname)))
    return dict(out)

def collect_results_files(base_dir: str, dates: set):
    root_v1 = os.path.join(base_dir, "results", "v1")
    root    = root_v1 if os.path.isdir(root_v1) else os.path.join(base_dir, "results")
    out = {}
    date_dirs = list(dates) if dates else [d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))]
    for d in date_dirs:
        dir_d = os.path.join(root, d)
        if not os.path.isdir(dir_d): continue
        for pid in os.listdir(dir_d):
            dir_pid = os.path.join(dir_d, pid)
            if not os.path.isdir(dir_pid): continue
            # ① レース別
            per_race = [f for f in os.listdir(dir_pid) if f.lower().endswith(".json") and f.upper().endswith("R.JSON")]
            if per_race:
                for fname in per_race:
                    race = fname[:-5].upper()
                    if not race.endswith("R"): race += "R"
                    out[(d, pid, race)] = os.path.join(dir_pid, fname)
                continue
            # ② まとめファイル
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
                        if not rk.endswith("R"): continue
                        out[(d, pid, rk)] = path + "#" + rk
                except Exception:
                    pass
    return out

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

# ---------- オッズ読込 ----------
def load_trifecta_odds(odds_base: str, date: str, pid: str, race: str):
    try:
        race_norm = race.upper() if race.upper().endswith("R") else f"{race}R"
        path = os.path.join(odds_base, date, pid, f"{race_norm}.json")
        if not os.path.isfile(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        trif = d.get("trifecta") or []
        out = {}
        for row in trif:
            combo = str(row.get("combo") or "").strip()
            if not combo:
                F = row.get("F"); S = row.get("S"); T = row.get("T")
                if all(isinstance(v, (int,float)) for v in [F,S,T]):
                    combo = f"{int(F)}-{int(S)}-{int(T)}"
            if not combo:
                continue
            odds = row.get("odds")
            rank = row.get("popularityRank")
            if isinstance(odds, (int,float)) and math.isfinite(odds):
                out[combo] = {"odds": float(odds), "rank": int(rank) if isinstance(rank,(int,float)) else None}
        return out
    except Exception:
        return {}

# ---------- オッズバンドユーティリティ ----------
def parse_odds_bands(bands_str: str, odds_min: float, odds_max: float):
    bands = []
    if bands_str:
        for part in bands_str.split(","):
            part = part.strip()
            if not part: continue
            if "-" not in part:
                continue
            lo_s, hi_s = part.split("-", 1)
            lo = float(lo_s) if lo_s.strip() else float("-inf")
            hi = float(hi_s) if hi_s.strip() else float("inf")
            if math.isfinite(lo) and math.isfinite(hi) and lo > hi:
                lo, hi = hi, lo
            bands.append((lo, hi))
    else:
        lo = float(odds_min) if odds_min and odds_min>0 else float("-inf")
        hi = float(odds_max) if odds_max and odds_max>0 else float("inf")
        if (math.isfinite(lo) or math.isfinite(hi)) and (lo != float("-inf") or hi != float("inf")):
            if math.isfinite(lo) and math.isfinite(hi) and lo > hi:
                lo, hi = hi, lo
            bands.append((lo, hi))
    return bands

def odds_in_any_band(odds: float, bands: list[tuple[float,float]]) -> bool:
    if not bands:
        return True
    if odds is None or not math.isfinite(odds):
        return False
    for lo, hi in bands:
        if odds >= lo and odds <= hi:
            return True
    return False

# ---------- 買い目生成 ----------
def generate_tickets(strategy, tri_probs, exacta_probs, third_probs,
                     topn=18, k=2, m=4, exclude_first1=False, only_first1=False):
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
                        score = exacta_probs.get((f,s),0.0) * third_probs.get(t,0.0)
                        tickets.append((key, score))
        tickets = [(k_,p_) for (k_,p_) in tickets
                   if ((not only_first1) or k_[0]==1) and ((not exclude_first1) or k_[0]!=1)]
        tickets.sort(key=lambda kv: kv[1], reverse=True)
    else:
        top = sorted(tri_probs.items(), key=lambda kv: kv[1], reverse=True)[:topn]
        top = [(k_,p_) for (k_,p_) in top
               if ((not only_first1) or k_[0]==1) and ((not exclude_first1) or k_[0]!=1)]
        tickets = [(k_, p_) for k_,p_ in top]
    return tickets

# ---------- キーマンJSON保存 ----------
def save_keyman(outdir: str, date: str, pid: str, race: str, keyman: dict, meta: dict):
    try:
        dirp = os.path.join(outdir, "keyman", date, pid)
        os.makedirs(dirp, exist_ok=True)
        payload = {"date": date, "pid": pid, "race": race}
        payload.update(meta or {})
        payload["keyman"] = keyman
        with open(os.path.join(dirp, f"{race}.json"), "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[warn] keyman save failed for {date}/{pid}/{race}: {e}")

# ---------- 評価（1レース） ----------
def evaluate_one(int_path: str, res_path: str, sims: int, unit: int,
                 strategy: str, topn: int, k: int, m: int,
                 exclude_first1: bool=False, only_first1: bool=False,
                 odds_base: str=None, min_ev: float=0.0, require_odds: bool=False,
                 odds_bands: list[tuple[float,float]] = None,
                 outdir: str = "./SimS_v1.0_eval"):
    # 予測
    with open(int_path, "r", encoding="utf-8") as f:
        d_int = json.load(f)
    tri_probs, kim_probs, exacta_probs, third_probs, keyman = simulate_one(d_int, sims=sims)

    # 生成 & 1着1除外/限定
    tickets = generate_tickets(strategy, tri_probs, exacta_probs, third_probs,
                               topn=topn, k=k, m=m,
                               exclude_first1=exclude_first1, only_first1=only_first1)

    # オッズ（EV/帯 用）
    date = pid = race = None
    odds_map = {}
    try:
        p = os.path.normpath(int_path).split(os.sep)
        race = os.path.splitext(p[-1])[0]; pid = p[-2]; date = p[-3]
    except Exception:
        pass
    if odds_base and date and pid and race:
        odds_map = load_trifecta_odds(odds_base, date, pid, race)

    # フィルタ（オッズバンド → EV）
    bands = odds_bands or []
    kept = []
    for (key, prob) in tickets:
        combo = "-".join(map(str, key))
        rec = odds_map.get(combo)
        odds = rec["odds"] if rec else None

        if bands:
            if odds is None:
                continue
            if not odds_in_any_band(odds, bands):
                continue
        elif require_odds and odds is None:
            continue

        if min_ev and min_ev > 0:
            if odds is None:
                if require_odds:
                    continue
            else:
                ev = prob * odds
                if ev < min_ev:
                    continue

        kept.append((key, prob))

    tickets = kept

    bets = ['-'.join(map(str, key)) for key,_ in tickets]
    stake = unit * len(bets)

    # 的中・払戻
    d_res = load_result_for_race(res_path) or {}
    hit_combo, payout_amount = actual_trifecta_combo_and_amount(d_res)
    payout = payout_amount if hit_combo in bets else 0

    # 参考: 予測順位でのヒット位置
    rank_map = { '-'.join(map(str,k)): i+1
                 for i,(k,_) in enumerate(sorted(tri_probs.items(), key=lambda kv: kv[1], reverse=True)) }
    rank_hit = rank_map.get(hit_combo, None)

    # NEW: キーマン保存（外加点ランクも含めて保存）
    if all([date, pid, race]):
        keyman_rank = compute_keyman_rank(keyman, outside_beta=0.6, outside_gamma=1.0)
        keyman_out = dict(keyman)
        keyman_out["KEYMAN_RANK"] = keyman_rank  # 0〜1規格化
        meta = {
            "engine": "SimS ver1.0 (E1)",
            "sims_per_race": int(sims),
            "strategy": strategy,
            "topn": int(topn), "k": int(k), "m": int(m),
            "exclude_first1": bool(exclude_first1),
            "only_first1": bool(only_first1),
            "min_ev": float(min_ev),
            "require_odds": bool(require_odds),
            "odds_bands": "",
        }
        save_keyman(outdir, date, pid, race, keyman_out, meta)

    return {
        "stake": stake, "payout": payout, "hit": 1 if payout>0 else 0,
        "bets": bets, "hit_combo": hit_combo, "tri_probs": tri_probs,
        "rank_hit": rank_hit, "kim_probs": kim_probs
    }

# ---------- メイン ----------
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
    ap.add_argument("--races", default="", help="レース名フィルタ（例 1R,2R or 1,2）")
    ap.add_argument("--strategy", default="trifecta_topN",
                    choices=["trifecta_topN","exacta_topK_third_topM"], help="買い目ロジック")
    ap.add_argument("--k", type=int, default=2, help="exacta_topK_third_topM: 2連単TOPK")
    ap.add_argument("--m", type=int, default=4, help="exacta_topK_third_topM: 3着TOPM")
    ap.add_argument("--exclude-first1", action="store_true", help="1着=1号艇を除外")
    ap.add_argument("--only_first1", action="store_true", help="1着=1号艇のみ購入")

    # オッズ/EV フィルタ
    ap.add_argument("--odds-base", default="./public/odds/v1", help="オッズJSONのルート")
    ap.add_argument("--min-ev", type=float, default=0.0, help="このEV以上のみ購入 (EV=p*odds)")
    ap.add_argument("--require-odds", action="store_true", help="オッズが無い買い目は除外")

    # オッズバンド指定（複数帯 or 単一レンジ）
    ap.add_argument("--odds-bands", default="",
                    help='オッズ帯のホワイトリスト。例: "01-09,10-19,20-49", "50-", "-20"')
    ap.add_argument("--odds-min", type=float, default=0.0, help="単一レンジの下限（--odds-bands が優先）")
    ap.add_argument("--odds-max", type=float, default=0.0, help="単一レンジの上限（--odds-bands が優先）")

    # 外部パラメータ上書き
    ap.add_argument("--params", default="", help="パラメータ上書きファイル(.json/.toml)")
    ap.add_argument("--set", default="", help="個別キー上書き。例: b_dt=17,cK=1.05,base_wake=0.15")

    args = ap.parse_args()

    if args.exclude_first1 and args.only_first1:
        raise SystemExit("--exclude-first1 と --only_first1 は同時指定できません")

    # Params 上書き
    try:
        if args.params:
            file_over = load_param_file(args.params)
            apply_overrides_to_class(Params, file_over)
        cli_over = parse_set_overrides(args.__dict__.get("set",""))
        if cli_over:
            apply_overrides_to_class(Params, cli_over)
    except Exception as e:
        raise SystemExit(f"[params] override failed: {e}")

    # 現在値を書き出し（監査）
    try:
        os.makedirs(args.outdir, exist_ok=True)
        active = {k:getattr(Params,k) for k in dir(Params)
                  if not k.startswith("_") and isinstance(getattr(Params,k),(int,float,bool))}
        with open(os.path.join(args.outdir, "active_params.json"), "w", encoding="utf-8") as f:
            json.dump(active, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    # オッズバンドを解釈
    bands = parse_odds_bands(args.odds_bands, args.odds_min, args.odds_max)

    dates = set([d.strip() for d in args.dates.split(",") if d.strip()]) if args.dates else set()
    pids_filter  = set([p.strip() for p in args.pids.split(",") if p.strip()])
    races_filter = set([_norm_race(r) for r in args.races.split(",") if r.strip()])

    int_idx = collect_files(args.base, "integrated", dates) if dates else \
              collect_files(args.base, "integrated",
                            set(os.listdir(os.path.join(args.base, "integrated", "v1"))))
    res_idx = collect_results_files(args.base, dates)

    keys_all = set(int_idx.keys()) & set(res_idx.keys())
    keys = sorted(keys_all)
    if pids_filter:  keys = [k for k in keys if k[1] in pids_filter]
    if races_filter: keys = [k for k in keys if k[2] in races_filter]
    if args.limit and args.limit > 0: keys = keys[:args.limit]

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
            tri_probs, kim_probs, exacta_probs, third_probs, keyman = simulate_one(d_int, sims=args.sims)

            # 生成
            tickets = generate_tickets(args.strategy, tri_probs, exacta_probs, third_probs,
                                       topn=args.topn, k=args.k, m=args.m,
                                       exclude_first1=args.exclude_first1,
                                       only_first1=args.only_first1)

            # オッズ読み込み
            odds_map = {}
            bands = parse_odds_bands(args.odds_bands, args.odds_min, args.odds_max)
            if (args.min_ev > 0) or args.require_odds or bands:
                odds_map = load_trifecta_odds(args.odds_base, date, pid, race)

            out_list = []
            for (key, p) in tickets:
                combo = "-".join(map(str, key))
                rec = odds_map.get(combo) if odds_map else None
                odds = rec["odds"] if rec else None
                ev   = (p * odds) if (odds is not None) else None

                # 帯 → EV → require_odds
                if bands:
                    if odds is None or not odds_in_any_band(odds, bands):
                        continue
                elif args.require_odds and odds is None:
                    continue

                if args.min_ev > 0:
                    if odds is None:
                        if args.require_odds:
                            continue
                    else:
                        if ev < args.min_ev:
                            continue

                out_list.append({"ticket": combo, "score": round(p,6),
                                 "odds": (None if odds is None else float(odds)),
                                 "ev": (None if ev is None else round(ev,6))})

            with open(os.path.join(pred_dir, f"pred_{date}_{pid}_{race}.json"), "w", encoding="utf-8") as f:
                json.dump({"date":date,"pid":pid,"race":race,"buylist":out_list,
                           "engine":"SimS ver1.0 (E1)",
                           "exclude_first1":bool(args.exclude_first1),
                           "only_first1":bool(args.only_first1),
                           "min_ev": float(args.min_ev),
                           "require_odds": bool(args.require_odds),
                           "odds_bands": args.odds_bands or "",
                           "odds_min": float(args.odds_min),
                           "odds_max": float(args.odds_max)},
                          f, ensure_ascii=False, indent=2)

            # キーマンも保存（外加点ランク込み）
            keyman_rank = compute_keyman_rank(keyman, outside_beta=0.6, outside_gamma=1.0)
            keyman_out = dict(keyman)
            keyman_out["KEYMAN_RANK"] = keyman_rank
            save_keyman(args.outdir, date, pid, race, keyman_out, {
                "engine": "SimS ver1.0 (E1)",
                "sims_per_race": int(args.sims),
                "strategy": args.strategy,
                "topn": int(args.topn), "k": int(args.k), "m": int(args.m),
                "exclude_first1": bool(args.exclude_first1),
                "only_first1": bool(args.only_first1),
                "min_ev": float(args.min_ev),
                "require_odds": bool(args.require_odds),
                "odds_bands": args.odds_bands or "",
            })

            for i, t in enumerate(out_list, 1):
                rows.append({"date":date,"pid":pid,"race":race,"rank":i,
                             "ticket":t["ticket"],"score":t["score"],
                             "odds":t["odds"],"ev":t["ev"]})

        pd.DataFrame(rows).to_csv(os.path.join(pred_dir, "predictions_summary.csv"),
                                  index=False, encoding="utf-8")
        print(f"[predict] candidates: {len(keys)}  -> {pred_dir}")
        return

    # ---- eval ----
    print(f"[eval] races to evaluate: {len(keys)}")
    per_rows = []; total_stake = 0; total_payout = 0

    for (date, pid, race) in keys:
        ev = evaluate_one(int_idx[(date,pid,race)], res_idx[(date,pid,race)],
                          sims=args.sims, unit=args.unit, strategy=args.strategy,
                          topn=args.topn, k=args.k, m=args.m,
                          exclude_first1=args.exclude_first1, only_first1=args.only_first1,
                          odds_base=args.odds_base, min_ev=args.min_ev, require_odds=args.require_odds,
                          odds_bands=bands, outdir=args.outdir)
        total_stake += ev["stake"]; total_payout += ev["payout"]
        per_rows.append({"date": date, "pid": pid, "race": race,
                         "bets": len(ev["bets"]), "stake": ev["stake"],
                         "payout": ev["payout"], "hit": ev["hit"],
                         "hit_combo": ev["hit_combo"]})

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
        "exclude_first1": bool(args.exclude_first1),
        "only_first1": bool(args.only_first1),
        "min_ev": float(args.min_ev),
        "require_odds": bool(args.require_odds),
        "odds_bands": args.odds_bands or "",
        "odds_min": float(args.odds_min),
        "odds_max": float(args.odds_max),
    }

    df.to_csv(os.path.join(args.outdir, "per_race_results.csv"), index=False)
    with open(os.path.join(args.outdir, "overall.json"), "w", encoding="utf-8") as f:
        json.dump(overall, f, ensure_ascii=False, indent=2)

    # 簡易ヒットレポート
    rep_path = os.path.join(args.outdir, "hit_report.json")
    if (len(df) > 0) and (df["hit"]==1).any():
        hit_df = df[df["hit"]==1].copy()
        def first_lane(c):
            try: return int(str(c).split("-")[0])
            except: return None
        hit_df["first_lane"] = hit_df["hit_combo"].map(first_lane)

        def band_by_odds(payout, unit):
            try:
                o = float(payout)/unit if unit>0 else 0.0
            except: return "unknown"
            if o < 10: return "01-09"
            if o < 20: return "10-19"
            if o < 50: return "20-49"
            if o < 100: return "50-99"
            if o < 1000: return "100-999"
            return "1000+"

        hit_df["odds_band"] = hit_df.apply(lambda r: band_by_odds(r["payout"], args.unit), axis=1)
        by_first = (hit_df.groupby("first_lane").size().reset_index(name="hits")
                    .sort_values("first_lane"))
        by_band = (hit_df.groupby("odds_band").size().reset_index(name="hits")
                   .sort_values("odds_band"))
        by_combo = (hit_df.groupby("hit_combo").size().reset_index(name="hits")
                    .sort_values("hits", ascending=False).head(10))
        by_rank  = pd.DataFrame([{"rank_bucket":"NA","hits":len(hit_df)}])

        report = {
            "summary": overall,
            "by_kimarite_est": [],
            "by_first_lane": by_first.to_dict(orient="records"),
            "by_odds_band": by_band.to_dict(orient="records"),
            "by_hit_combo_top10": by_combo.to_dict(orient="records"),
            "by_rank_bucket": by_rank.to_dict(orient="records")
        }
    else:
        report = {"summary": overall, "note": "no hits found"}

    with open(rep_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("=== OVERALL (SimS ver1.0 E1) ===")
    print(json.dumps(overall, ensure_ascii=False, indent=2))
    print(f"\n[files] {args.outdir}/per_race_results.csv, {args.outdir}/overall.json, {rep_path}")

# ---------- trifecta 決定の抽出 ----------
def actual_trifecta_combo_and_amount(result_json: dict):
    trif = (result_json or {}).get("payouts", {}).get("trifecta") if isinstance(result_json, dict) else None
    combo = None; amount = 0
    if isinstance(trif, dict):
        combo = trif.get("combo"); amount = int(trif.get("amount") or 0)
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

if __name__ == "__main__":
    main()
