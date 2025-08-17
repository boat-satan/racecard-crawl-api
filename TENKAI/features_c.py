# -*- coding: utf-8 -*-
"""
TENKAI: C(編成・相対)特徴のみを抽出
- 入力: integrated/v1 の1レース dict
- 出力: 1レース=1行の特徴 dict
※ ボート/モーターは使わない
"""
import math
import json
import numpy as np

# ── 小道具 ────────────────────────────────────────────────────────────────
def _to_float(x):
    try:
        return float(x) if x is not None else np.nan
    except:
        return np.nan

def _ratio(n, d):
    n = _to_float(n); d = _to_float(d)
    if d is None or math.isnan(d) or d <= 0: return 0.0
    n = 0.0 if n is None or math.isnan(n) else n
    return float(n)/float(d)

def _get_entry_by_lane(integ, lane):
    for e in integ.get("entries", []):
        if int(e.get("lane")) == lane:
            return e
    return {}

def _ec(e):
    return ((e.get("stats") or {}).get("entryCourse")) or {}

def _ss(e):
    return (_ec(e).get("selfSummary")) or {}

def _winK(e):
    # 決まり手（抜き・恵まれは除外）
    wk = (_ec(e).get("winKimariteSelf")) or {}
    return {
        "逃げ": int(wk.get("逃げ", 0) or 0),
        "差し": int(wk.get("差し", 0) or 0),
        "まくり": int(wk.get("まくり", 0) or 0),
        "まくり差し": int(wk.get("まくり差し", 0) or 0),
    }

def _loseK(e):
    lk = (_ec(e).get("loseKimarite")) or {}
    # ここも抜き・恵まれは度外視
    return {
        "逃げ": int(lk.get("逃げ", 0) or 0),
        "差し": int(lk.get("差し", 0) or 0),
        "まくり": int(lk.get("まくり", 0) or 0),
        "まくり差し": int(lk.get("まくり差し", 0) or 0),
    }

def _rc(e):
    return e.get("racecard") or {}

def _val_pref_rc_then_ec_avgST(e):
    # STはレースカードavgSTを優先、無ければコース別avgST
    v = _to_float(_rc(e).get("avgST"))
    if v is None or math.isnan(v):
        v = _to_float(_ec(e).get("avgST"))
    return v

# ── 主処理 ────────────────────────────────────────────────────────────────
def build_c_features(integ: dict) -> dict:
    """1レースのC特徴量を返す（dict）"""
    # レース基本
    date = str(integ.get("date") or "")
    pid  = str(integ.get("pid") or "")
    race = str(integ.get("race") or "")

    # 6艇分を配列化
    lanes = range(1, 7)
    ST = []
    cls = []
    age = []
    late = []
    fly = []
    natTop3 = []
    locTop3 = []

    # 決まり手系
    starts = []
    win_1_escape = []   # 1枠の逃げなど個別で使うので lane=1等は別算出もする
    win_2_sashi  = []
    win_3_makuri = []

    # for outer pressure, etc.
    win_makuri_outer = []
    win_makurizashi_outer = []

    # lose for lane=1
    lose1_sashi = lose1_makuri = lose1_makurizashi = 0
    starts1 = 0

    for L in lanes:
        e = _get_entry_by_lane(integ, L)
        rc = _rc(e)
        ec = _ec(e)
        ss = _ss(e)
        wk = _winK(e)
        lk = _loseK(e)

        ST.append(_val_pref_rc_then_ec_avgST(e))
        cls.append(_to_float(rc.get("classNumber")))
        age.append(_to_float(rc.get("age")))
        late.append(_to_float(rc.get("lateCount")))
        fly.append(_to_float(rc.get("flyingCount")))
        natTop3.append(_to_float(rc.get("natTop3")))
        locTop3.append(_to_float(rc.get("locTop3")))
        starts.append(_to_float(ss.get("starts")))

        if L == 1:
            win_1_escape.append(wk["逃げ"])
            lose1_sashi = _to_float((_loseK(e)).get("差し"))
            lose1_makuri = _to_float((_loseK(e)).get("まくり"))
            lose1_makurizashi = _to_float((_loseK(e)).get("まくり差し"))
            starts1 = _to_float(ss.get("starts"))
        if L == 2:
            win_2_sashi.append(wk["差し"])
        if L == 3:
            win_3_makuri.append(wk["まくり"])
        if L in (4,5,6):
            win_makuri_outer.append(wk["まくり"])
            win_makurizashi_outer.append(wk["まくり差し"])

    # numpy 化（NaN安全）
    ST = np.array(ST, dtype=float)
    cls = np.array(cls, dtype=float)
    age = np.array(age, dtype=float)
    late = np.array(late, dtype=float)
    fly = np.array(fly, dtype=float)
    natTop3 = np.array(natTop3, dtype=float)
    locTop3 = np.array(locTop3, dtype=float)

    # 基本統計
    ST_min = np.nanmin(ST) if np.isfinite(ST).any() else np.nan
    ST_max = np.nanmax(ST) if np.isfinite(ST).any() else np.nan
    ST_range = ST_max - ST_min if np.isfinite(ST_min) and np.isfinite(ST_max) else np.nan
    ST_spread = np.nanstd(ST) if np.isfinite(ST).any() else np.nan

    # 内外グループ
    inner_idx = np.array([0,1,2])  # lanes 1-3
    outer_idx = np.array([3,4,5])  # lanes 4-6

    def _mean_safe(arr, idx):
        v = arr[idx]
        return float(np.nanmean(v)) if np.isfinite(v).any() else np.nan

    ST_inner = _mean_safe(ST, inner_idx)
    ST_outer = _mean_safe(ST, outer_idx)
    # 平均STは小さいほど速いので「内有利を＋」にしたいなら outer - inner
    inner_ST_adv = (ST_outer - ST_inner) if (np.isfinite(ST_inner) and np.isfinite(ST_outer)) else np.nan

    cls_inner = _mean_safe(cls, inner_idx)
    cls_outer = _mean_safe(cls, outer_idx)
    class_inner_minus_outer = cls_inner - cls_outer if (np.isfinite(cls_inner) and np.isfinite(cls_outer)) else np.nan

    proA_inner = _mean_safe(natTop3, inner_idx)
    proA_outer = _mean_safe(natTop3, outer_idx)
    proA_gap_inner_outer = proA_inner - proA_outer if (np.isfinite(proA_inner) and np.isfinite(proA_outer)) else np.nan

    loc_inner = _mean_safe(locTop3, inner_idx)
    loc_outer = _mean_safe(locTop3, outer_idx)
    local_bias_inner = loc_inner - loc_outer if (np.isfinite(loc_inner) and np.isfinite(loc_outer)) else np.nan

    # 率系（分母 starts）
    in_win_trait = _ratio(sum(win_1_escape or [0]), starts1)
    two_sashi_trait = _ratio(sum(win_2_sashi or [0]), starts[1] if len(starts)>1 else 0)
    three_makuri_trait = _ratio(sum(win_3_makuri or [0]), starts[2] if len(starts)>2 else 0)

    outer_makuri_pressure = _ratio(sum(win_makuri_outer)+sum(win_makurizashi_outer), np.nansum(starts[3:6]) if len(starts)>=6 else 0)

    in_vulnerability = _ratio(
        (lose1_sashi or 0) + (lose1_makuri or 0) + (lose1_makurizashi or 0),
        starts1
    )

    # 攻め要員・リスク
    attackers_exist_outer = int(np.nansum((ST[3:6] <= 0.14).astype(int))) if len(ST)>=6 else 0
    late_risk_inner = int(np.nansum(late[0:3])) if len(late)>=3 else 0
    flying_risk_outer = int(np.nansum(fly[3:6])) if len(fly)>=6 else 0

    # スタイル相性
    style_match_2on1 = two_sashi_trait - in_win_trait
    style_match_3on1 = three_makuri_trait - in_win_trait

    # 荒れ/総合シグナル（係数は最初は1.0固定）
    outer_takeover_signal = outer_makuri_pressure + (ST_spread if np.isfinite(ST_spread) else 0.0)

    # 上位集中度
    nat_sorted = np.sort(natTop3[~np.isnan(natTop3)])[::-1]
    if len(nat_sorted) >= 3:
        top2_mean = float(np.mean(nat_sorted[:2]))
        rest_mean = float(np.mean(nat_sorted[2:])) if len(nat_sorted[2:]) > 0 else 0.0
        top3_concentration = top2_mean - rest_mean
    else:
        top3_concentration = 0.0

    # 年齢の若手優位（下位=若手）
    age_sorted = np.sort(age[~np.isnan(age)])
    if len(age_sorted) >= 4:
        youth_vs_experience = float(np.mean(age_sorted[:2]) - np.mean(age_sorted[-2:]))  # 若手優位ならマイナスに出やすい
    else:
        youth_vs_experience = 0.0

    # 代表的ペアのST差（3起点/4起点）
    pair_ST_delta_13 = float(ST[2]-ST[0]) if len(ST)>=3 and np.isfinite(ST[2]) and np.isfinite(ST[0]) else np.nan
    pair_ST_delta_14 = float(ST[3]-ST[0]) if len(ST)>=4 and np.isfinite(ST[3]) and np.isfinite(ST[0]) else np.nan

    # カオス指数（暫定）
    class_std = float(np.nanstd(cls)) if np.isfinite(cls).any() else 0.0
    chaos_index = (ST_spread if np.isfinite(ST_spread) else 0.0) + class_std + (in_vulnerability or 0.0) - (proA_gap_inner_outer if (proA_gap_inner_outer is not None) else 0.0)

    # lane別 natTop3 ランク（1=最強）
    nat_valid = natTop3.copy()
    order = np.argsort(-np.nan_to_num(nat_valid, nan=-1e9))
    ranks = np.empty_like(order)
    ranks[order] = np.arange(1, len(order)+1)

    out = {
        "date": date, "pid": pid, "race": race,

        "c_ST_min": ST_min, "c_ST_max": ST_max, "c_ST_range": ST_range, "c_ST_spread": ST_spread,
        "c_inner_ST_adv": inner_ST_adv,  # (+)で内が速い（outer - inner）

        "c_class_inner_minus_outer": class_inner_minus_outer,
        "c_proA_gap_inner_outer": proA_gap_inner_outer,
        "c_local_bias_inner": local_bias_inner,

        "c_in_win_trait": in_win_trait,
        "c_two_sashi_trait": two_sashi_trait,
        "c_three_makuri_trait": three_makuri_trait,
        "c_outer_makuri_pressure": outer_makuri_pressure,
        "c_in_vulnerability": in_vulnerability,

        "c_attackers_exist_outer": attackers_exist_outer,
        "c_late_risk_inner": late_risk_inner,
        "c_flying_risk_outer": flying_risk_outer,

        "c_style_match_2on1": style_match_2on1,
        "c_style_match_3on1": style_match_3on1,
        "c_outer_takeover_signal": outer_takeover_signal,

        "c_top3_concentration": top3_concentration,
        "c_youth_vs_experience": youth_vs_experience,
        "c_pair_ST_delta_13": pair_ST_delta_13,
        "c_pair_ST_delta_14": pair_ST_delta_14,
        "c_chaos_index": chaos_index,
    }

    # lane別 natTop3 ランク
    for i, r in enumerate(ranks, start=1):
        out[f"c_natTop3_rank_L{i}"] = int(r) if np.isfinite(r) else 999

    return out
