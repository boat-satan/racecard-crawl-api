# -*- coding: utf-8 -*-
"""
sims_fit.py — resultsに寄せたパラメータ評価/軽いフィット
- 重要: simulate_one には integrated.json を必ず渡す（resultsは正解取得のみ）
- NLL = -log(p(的中三連単)) を最小化対象に集計
"""

import os, json, math, argparse, copy, sys, random
from typing import Dict, Tuple, Set

# 同ディレクトリの sims_pure をインポートできるように
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if THIS_DIR not in sys.path:
    sys.path.insert(0, THIS_DIR)

from sims_pure import simulate_one, Params as SimParams  # type: ignore

# ---------- util ----------
def _norm_race(r: str) -> str:
    r = (r or "").strip().upper()
    return r if (not r or r.endswith("R")) else f"{r}R"

def _collect(base: str, kind: str, dates: Set[str]):
    root_v1 = os.path.join(base, kind, "v1")
    root    = root_v1 if os.path.isdir(root_v1) else os.path.join(base, kind)
    out = {}
    if dates:
        date_dirs = list(dates)
    else:
        date_dirs = [d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))]
    for d in date_dirs:
        dir_d = os.path.join(root, d)
        if not os.path.isdir(dir_d): continue
        for pid in os.listdir(dir_d):
            dir_pid = os.path.join(dir_d, pid)
            if not os.path.isdir(dir_pid): continue
            for f in os.listdir(dir_pid):
                if f.lower().endswith(".json"):
                    race = _norm_race(os.path.splitext(f)[0])
                    out[(d, pid, race)] = os.path.join(dir_pid, f)
    return out

def _collect_results(base: str, dates: Set[str]):
    """results 側はファイル形状がばらつくので sims_pure と同じ堅牢収集を簡略化して実装"""
    root_v1 = os.path.join(base, "results", "v1")
    root    = root_v1 if os.path.isdir(root_v1) else os.path.join(base, "results")
    out = {}
    if dates:
        date_dirs = list(dates)
    else:
        date_dirs = [d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))]
    for d in date_dirs:
        dir_d = os.path.join(root, d)
        if not os.path.isdir(dir_d): continue
        for pid in os.listdir(dir_d):
            dir_pid = os.path.join(dir_d, pid)
            if not os.path.isdir(dir_pid): continue
            for f in os.listdir(dir_pid):
                if f.lower().endswith(".json"):
                    race = _norm_race(os.path.splitext(f)[0])
                    out[(d, pid, race)] = os.path.join(dir_pid, f)
    return out

def _load_json(p: str):
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def _actual_trifecta_combo_from_result(d: Dict) -> str | None:
    """results.json から ‘F-S-T’ 形式の三連単を抽出"""
    # 標準形
    tri = (d or {}).get("payouts", {}).get("trifecta")
    if isinstance(tri, dict):
        combo = tri.get("combo")
        if combo:
            return str(combo)
    # fallback: order から構築
    order = (d or {}).get("order")
    if isinstance(order, list) and len(order) >= 3:
        def lane(x): 
            return str(x.get("lane") or x.get("course") or x.get("F") or x.get("number"))
        try:
            f, s, t = lane(order[0]), lane(order[1]), lane(order[2])
            if f and s and t:
                return f"{f}-{s}-{t}"
        except:
            pass
    return None

# ---------- core ----------
def race_nll(integrated_json: Dict, results_json: Dict, sims: int) -> float:
    """1レースの -log p(的中三連単) を返す"""
    tri_probs, *_ = simulate_one(integrated_json, sims=sims)
    combo = _actual_trifecta_combo_from_result(results_json)
    if not combo:
        return 0.0  # 正解不明なら寄与ゼロ（無視）
    # tri_probs のキーは (F,S,T) タプル
    try:
        fs = tuple(int(x) for x in combo.split("-"))
        p  = float(tri_probs.get(fs, 0.0))
    except:
        p = 0.0
    eps = 1e-12
    return -math.log(max(p, eps))

def evaluate_total_nll(base: str, keys, sims: int) -> Tuple[float, int]:
    """全レース合計 NLL と有効レース数"""
    total = 0.0
    used  = 0
    for (date, pid, race, int_p, res_p) in keys:
        d_int = _load_json(int_p)
        d_res = _load_json(res_p)
        nll = race_nll(d_int, d_res, sims)
        if nll > 0.0:
            total += nll
            used  += 1
    return total, used

def snapshot_params() -> Dict[str, float]:
    """最適化対象のサブセットだけ抜き出す（必要なら増やしてOK）"""
    cand = ["theta","alpha_A","alpha_Ap","beta_wk","beta_sq","cK","b_dt","gamma_wall","k_turn_err"]
    out = {}
    for k in cand:
        v = getattr(SimParams, k, None)
        if isinstance(v, (int, float)):
            out[k] = float(v)
    return out

def apply_params(d: Dict[str, float]):
    for k, v in d.items():
        if hasattr(SimParams, k):
            setattr(SimParams, k, float(v))

def coord_descent_step(p: Dict[str, float], base_nll: float, keys, sims: int, step: float = 0.02) -> Tuple[Dict[str, float], float]:
    """超簡易：各パラメータを ±step で試す座標降下1周"""
    best_p = copy.deepcopy(p)
    best_nll = base_nll
    for name, val in list(p.items()):
        for sign in (+1.0, -1.0):
            trial = copy.deepcopy(best_p)
            trial[name] = val * (1.0 + sign*step)
            apply_params(trial)
            nll, used = evaluate_total_nll(args.base, keys, sims)
            if used > 0 and nll < best_nll:
                best_nll = nll
                best_p = trial
    # 元に戻す
    apply_params(best_p)
    return best_p, best_nll

# ---------- cli ----------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="./public")
    ap.add_argument("--dates", default="")
    ap.add_argument("--pids",  default="")
    ap.add_argument("--sims", type=int, default=2000)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--iters", type=int, default=0, help=">0 なら座標降下の反復回数")
    ap.add_argument("--outdir", default="SimS_pure_fitted")
    return ap.parse_args()

args = parse_args()

def main():
    # インデックス作成（integrated ∩ results）
    dates = set([d.strip() for d in args.dates.split(",") if d.strip()]) if args.dates else set()
    pids_filter = set([p.strip() for p in args.pids.split(",") if p.strip() and p.strip().upper()!="ALL"])

    int_idx = _collect(args.base, "integrated", dates)
    res_idx = _collect_results(args.base, dates)
    keys_all = sorted(set(int_idx.keys()) & set(res_idx.keys()))
    if pids_filter:
        keys_all = [k for k in keys_all if k[1] in pids_filter]
    if args.limit and args.limit > 0:
        keys_all = keys_all[:args.limit]

    # (date,pid,race,int_path,res_path) に解決
    keys = [(d,p,r,int_idx[(d,p,r)],res_idx[(d,p,r)]) for (d,p,r) in keys_all]

    os.makedirs(args.outdir, exist_ok=True)
    # 評価のみ or 事後に座標降下
    base_params = snapshot_params()
    apply_params(base_params)

    base_nll, used = evaluate_total_nll(args.base, keys, args.sims)
    report = {
        "races": used,
        "sims_per_race": args.sims,
        "initial_params": base_params,
        "initial_total_nll": base_nll,
    }

    best_params = copy.deepcopy(base_params)
    best_nll = base_nll

    for it in range(max(0, args.iters)):
        best_params, best_nll = coord_descent_step(best_params, best_nll, keys, args.sims, step=0.02)

    report["fitted_params"] = best_params
    report["fitted_total_nll"] = best_nll
    report["improved"] = float(best_nll - base_nll)

    # 保存
    with open(os.path.join(args.outdir, "fit_report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # 使えるように active_params も出力
    with open(os.path.join(args.outdir, "active_params.json"), "w", encoding="utf-8") as f:
        json.dump(best_params, f, ensure_ascii=False, indent=2)

    print(json.dumps(report, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
