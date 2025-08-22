# sims_fit.py — Fit Params of sims_pure to results (NLL minimization)
# - integrated を build_input() で変換してから simulate_one() に渡す（KeyError: 'lanes' 回避）
# - dates 複数(カンマ区切り) / PID=ALL / races フィルタ対応
# - 単純な段階的座標探索(座標降下 + ステップ縮小)
# - 出力: scripts/sims/pass1/fit/{fitted_params.json, fit_summary.json}

import os, json, math, argparse, copy, random
import numpy as np

# --- 既存の関数を sims_pure から借用 ---
from sims_pure import (
    Params, build_input, simulate_one, _collect, _collect_results,
    _load_result, _norm_race
)

# 結果JSONから実三連単コンボを取得（sims_pure と同型式）
def _actual_trifecta_combo(res: dict):
    tri = (res or {}).get("payouts", {}).get("trifecta")
    if isinstance(tri, dict) and tri.get("combo"):
        return str(tri["combo"]).strip()
    order = (res or {}).get("order")
    if isinstance(order, list) and len(order) >= 3:
        def lane(x):
            return str(x.get("lane") or x.get("course") or x.get("F") or x.get("number"))
        try:
            f, s, t = lane(order[0]), lane(order[1]), lane(order[2])
            if all([f, s, t]): return f"{f}-{s}-{t}"
        except: pass
    return None

# ----------------- NLL -----------------
_EPS = 1e-12
def race_nll(int_json: dict, res_json: dict, sims: int) -> float:
    """一件のレースに対する -log P(実現三連単)"""
    # build_input を必ず噛ませる
    try:
        tri_probs, *_ = simulate_one(build_input(int_json), sims=sims)
    except Exception as e:
        # データ異常は大きな罰
        return 50.0

    hit_combo = _actual_trifecta_combo(res_json)
    if not hit_combo:
        # 的中コンボ取れない場合はスキップ扱い（寄与ゼロ）
        return 0.0

    # tri_probs の key は (F,S,T) タプル
    prob = 0.0
    try:
        F, S, T = [int(x) for x in hit_combo.replace(" ", "").split("-")]
        prob = float(tri_probs.get((F, S, T), 0.0))
    except:
        prob = 0.0
    prob = max(prob, _EPS)
    return -math.log(prob)

def evaluate_total_nll(int_paths, res_paths, sims: int) -> float:
    total = 0.0
    for ip, rp in zip(int_paths, res_paths):
        try:
            d_int = json.load(open(ip, "r", encoding="utf-8"))
            d_res = _load_result(rp)
            total += race_nll(d_int, d_res, sims)
        except:
            total += 50.0
    return total

# ----------------- 係数の座標探索 -----------------
# 調整対象（必要に応じて増やせます）
TARGET_FIELDS = [
    ("alpha_R",   0.001,  0.0002),
    ("alpha_A",  -0.002,  0.0002),
    ("alpha_Ap", -0.002,  0.0002),
    ("theta",     0.003,  0.0004),
    ("b_dt",      2.0,    0.3),
    ("cK",        0.20,   0.03),
]

def snapshot_params():
    return {k: getattr(Params, k) for k in dir(Params)
            if not k.startswith("_") and isinstance(getattr(Params, k), (int, float, bool))}

def restore_params(snap: dict):
    for k, v in snap.items():
        if hasattr(Params, k):
            setattr(Params, k, v)

def coordinate_descent(int_paths, res_paths, sims: int, iters: int):
    # 初期スコア
    best_snap = snapshot_params()
    best_nll  = evaluate_total_nll(int_paths, res_paths, sims)

    cur_step = {k: s for k, s, _ in TARGET_FIELDS}
    min_step = {k: m for k, _, m in TARGET_FIELDS}

    for it in range(1, iters + 1):
        improved = False
        for k, _, _ in TARGET_FIELDS:
            base = getattr(Params, k)
            step = cur_step[k]

            tried = []
            for delta in (+step, -step):
                setattr(Params, k, base + delta)
                nll = evaluate_total_nll(int_paths, res_paths, sims)
                tried.append((nll, base + delta))

            # どちらか良ければ採用
            tried.sort(key=lambda x: x[0])
            if tried[0][0] + 1e-9 < best_nll:
                best_nll = tried[0][0]
                best_snap = snapshot_params()
                setattr(Params, k, tried[0][1])
                improved = True
            else:
                # 戻す
                setattr(Params, k, base)

        # 改善が無ければステップ縮小
        if not improved:
            for k in cur_step:
                cur_step[k] *= 0.5

        # すべてのステップが閾値未満なら終了
        if all(cur_step[k] <= min_step[k] for k in cur_step):
            break

    # ベストに復元
    restore_params(best_snap)
    return best_snap, best_nll

# ----------------- 収集ヘルパ -----------------
def collect_keys(base, dates_set, pids_filter, races_filter, limit):
    int_idx = _collect(base, "integrated", dates_set) if dates_set else \
              _collect(base, "integrated", set(os.listdir(os.path.join(base, "integrated", "v1"))))
    res_idx = _collect_results(base, dates_set)
    keys = sorted(set(int_idx.keys()) & set(res_idx.keys()))
    if pids_filter:
        keys = [k for k in keys if (k[1] in pids_filter)]
    if races_filter:
        keys = [k for k in keys if _norm_race(k[2]) in races_filter]
    if limit and limit > 0:
        keys = keys[:limit]
    int_paths = [int_idx[k] for k in keys]
    res_paths = [res_idx[k] for k in keys]
    return keys, int_paths, res_paths

# ----------------- CLI -----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="./public")
    ap.add_argument("--dates", default="")            # 例: 20250809,20250810
    ap.add_argument("--pids", default="")             # 例: 02,05  / "ALL" で全場
    ap.add_argument("--races", default="")            # 例: 1R,2R   空で全R
    ap.add_argument("--sims", type=int, default=1500)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--iters", type=int, default=40)  # 座標降下イテレーション
    ap.add_argument("--outdir", default="scripts/sims/pass1/fit")
    args = ap.parse_args()

    dates_set = set([d.strip() for d in args.dates.split(",") if d.strip()]) if args.dates else set()

    if args.pids.strip().upper() == "ALL":
        # base/results/v1/{date}/ 以下のディレクトリを全場として採用
        pids_filter = set()
    else:
        pids_filter = set([p.strip() for p in args.pids.split(",") if p.strip()])

    races_filter = set([_norm_race(r) for r in args.races.split(",") if r.strip()])

    # 収集
    keys, int_paths, res_paths = collect_keys(args.base, dates_set, pids_filter, races_filter, args.limit)
    if not int_paths:
        print("[fit] no races found.")
        return

    os.makedirs(args.outdir, exist_ok=True)

    # フィット実行
    print(f"[fit] races={len(int_paths)} sims={args.sims} iters={args.iters}")
    before_snap = snapshot_params()
    before_nll  = evaluate_total_nll(int_paths, res_paths, args.sims)

    best_snap, best_nll = coordinate_descent(int_paths, res_paths, args.sims, args.iters)

    # 出力
    fitted_path = os.path.join(args.outdir, "fitted_params.json")
    summary_path = os.path.join(args.outdir, "fit_summary.json")
    json.dump(best_snap, open(fitted_path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    json.dump({
        "races": len(int_paths),
        "sims": args.sims,
        "iters": args.iters,
        "nll_before": before_nll,
        "nll_after": best_nll,
        "improved": float(before_nll - best_nll),
        "used_fields": [k for k,_,_ in TARGET_FIELDS]
    }, open(summary_path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

    print(f"[fit] saved: {fitted_path}")
    print(f"[fit] summary: {summary_path}")

if __name__ == "__main__":
    main()
