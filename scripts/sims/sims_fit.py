# scripts/sims/sims_fit.py
# Fit helper: results(実績)に対する NLL を集計。lanes欠落のレースは安全にスキップ。

import os, sys, json, math, argparse
from typing import Dict, Tuple, List

# sims_pure の simulate_one をインポートできるように同ディレクトリをパス追加
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if THIS_DIR not in sys.path:
    sys.path.append(THIS_DIR)

from sims_pure import _norm_race  # 型合わせ用（フィルタに使用）
from sims_pure import simulate_one  # 既存のシミュレータをそのまま利用

# ---------- 収集系（sims_pureと同等の最小セット） ----------
def _collect(base: str, kind: str, dates:set) -> Dict[Tuple[str,str,str], str]:
    root_v1 = os.path.join(base, kind, "v1")
    root = root_v1 if os.path.isdir(root_v1) else os.path.join(base, kind)
    out = {}
    date_dirs = list(dates) if dates else [d for d in os.listdir(root) if os.path.isdir(os.path.join(root,d))]
    for d in date_dirs:
        dir_d = os.path.join(root, d)
        if not os.path.isdir(dir_d): 
            continue
        for pid in os.listdir(dir_d):
            dir_pid = os.path.join(dir_d, pid)
            if not os.path.isdir(dir_pid): 
                continue
            for f in os.listdir(dir_pid):
                if f.endswith(".json"):
                    race = f[:-5]
                    out[(d, pid, race)] = os.path.join(dir_pid, f)
    return out

def _collect_results(base: str, dates:set) -> Dict[Tuple[str,str,str], str]:
    root_v1 = os.path.join(base,"results","v1")
    root = root_v1 if os.path.isdir(root_v1) else os.path.join(base,"results")
    out={}
    date_dirs=list(dates) if dates else [d for d in os.listdir(root) if os.path.isdir(os.path.join(root,d))]
    for d in date_dirs:
        dir_d=os.path.join(root,d)
        if not os.path.isdir(dir_d): 
            continue
        for pid in os.listdir(dir_d):
            dir_pid=os.path.join(dir_d,pid)
            if not os.path.isdir(dir_pid): 
                continue
            per=[f for f in os.listdir(dir_pid) if f.lower().endswith(".json") and f.upper().endswith("R.JSON")]
            if per:
                for f in per:
                    r=f[:-5].upper()
                    r=r if r.endswith("R") else r+"R"
                    out[(d,pid,r)] = os.path.join(dir_pid,f)
                continue
            # 複合 JSON 形式への対応
            for f in [f for f in os.listdir(dir_pid) if f.lower().endswith(".json")]:
                p=os.path.join(dir_pid,f)
                try:
                    data=json.load(open(p,"r",encoding="utf-8"))
                    container=data.get("races",data) if isinstance(data,dict) else {}
                    for rk in list(container.keys()):
                        k=str(rk).upper()
                        if k.isdigit(): k += "R"
                        if k.endswith("R"):
                            out[(d,pid,k)] = p + "#" + k
                except:
                    pass
    return out

def _load_result(res_path: str) -> dict:
    if "#" in res_path:
        p,r = res_path.split("#",1)
        data=json.load(open(p,"r",encoding="utf-8"))
        cont=data.get("races",data) if isinstance(data,dict) else {}
        d=cont.get(r) or cont.get(r.upper()) or cont.get(r.lower())
        return d if isinstance(d,dict) else {}
    return json.load(open(res_path,"r",encoding="utf-8"))

def _load_json(path: str) -> dict:
    try:
        return json.load(open(path,"r",encoding="utf-8"))
    except Exception:
        return {}

# 実着順 → 3連単コンボ ＆ 払戻
def _actual_trifecta_and_amount(res):
    trif=(res or {}).get("payouts",{}).get("trifecta")
    combo=None; amt=0
    if isinstance(trif,dict):
        combo=trif.get("combo")
        try:
            amt=int(trif.get("amount") or 0)
        except:
            amt=0
    if not combo and isinstance(res,dict):
        order=res.get("order")
        if isinstance(order,list) and len(order)>=3:
            def lane(x): return str(x.get("lane") or x.get("course") or x.get("F") or x.get("number"))
            try:
                f,s,t=lane(order[0]), lane(order[1]), lane(order[2])
                if all([f,s,t]): combo=f"{f}-{s}-{t}"
            except:
                pass
    return combo, amt

# ---------- NLL ----------
def race_nll(integrated_json: dict, result_json: dict, sims: int = 2000) -> float:
    """
    integrated_json を sims_pure.simulate_one に渡し、実際の3連単コンボの
    予測確率 p を取り、-log(max(p,eps)) を返す。
    lanes が無ければ None を返す（呼び出し側でスキップ）。
    """
    # build_input 後に sims_pure 側で 'lanes' を使うため、entries が無い/空なら不可
    if not isinstance(integrated_json, dict):
        return None
    if not isinstance(integrated_json.get("entries"), list) or len(integrated_json["entries"]) == 0:
        return None

    tri_probs, *_ = simulate_one(integrated_json, sims=sims)
    actual_combo, _ = _actual_trifecta_and_amount(result_json)
    if not actual_combo:
        return None
    p = float(tri_probs.get(tuple(int(x) for x in actual_combo.split("-")), 0.0))
    eps = 1e-12
    return -math.log(max(p, eps))

# ---------- メイン ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="./public")
    ap.add_argument("--dates", default="")
    ap.add_argument("--pids",  default="")
    ap.add_argument("--races", default="")
    ap.add_argument("--sims", type=int, default=2000)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    # インデックス収集
    dates = set([d.strip() for d in args.dates.split(",") if d.strip()])
    int_idx = _collect(args.base, "integrated", dates) if dates else _collect(args.base, "integrated", set(os.listdir(os.path.join(args.base,"integrated","v1"))))
    res_idx = _collect_results(args.base, dates)

    # 交差
    keys_all = sorted(set(int_idx.keys()) & set(res_idx.keys()))

    # フィルタ
    pids_filter  = set([p.strip() for p in args.pids.split(",") if p.strip()])
    races_filter = set([_norm_race(r) for r in args.races.split(",") if r.strip()])
    if pids_filter:
        keys_all = [k for k in keys_all if k[1] in pids_filter]
    if races_filter:
        keys_all = [k for k in keys_all if _norm_race(k[2]) in races_filter]
    if args.limit and args.limit > 0:
        keys_all = keys_all[:args.limit]

    # lanes 無いレースを除外しつつ NLL を集計
    total_nll = 0.0
    used = 0
    skipped = 0

    for (d,p,r) in keys_all:
        int_p = int_idx[(d,p,r)]
        res_p = res_idx[(d,p,r)]
        d_int = _load_json(int_p)
        if not isinstance(d_int.get("entries"), list) or len(d_int["entries"]) == 0:
            skipped += 1
            print(f"[skip] no lanes/entries: {int_p}", file=sys.stderr)
            continue
        d_res = _load_result(res_p)
        nll = race_nll(d_int, d_res, sims=args.sims)
        if nll is None:
            skipped += 1
            print(f"[skip] invalid race data: int={int_p} res={res_p}", file=sys.stderr)
            continue
        total_nll += nll
        used += 1

    print(json.dumps({
        "races_total": len(keys_all),
        "races_used": used,
        "races_skipped": skipped,
        "sims_per_race": args.sims,
        "total_nll": total_nll,
        "avg_nll": (total_nll/used) if used>0 else None
    }, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
