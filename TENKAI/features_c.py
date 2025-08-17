# -*- coding: utf-8 -*-
"""
TENKAI C-features builder
- integrated/v1 の 1レース分 dict を受け取り、編成・相対特徴の1行 dict を返す
- ボート/モーターの素性は含めない（依頼どおり）
- 決まり手カウントは「抜き」「恵まれ」を除外
"""

from typing import Dict, Any, List
import math

EXCLUDE_KIMARITE = {"抜き", "恵まれ"}

def _nan_to_none(x):
    # pandas未使用の軽い正規化
    if x is None:
        return None
    try:
        if isinstance(x, float) and math.isnan(x):
            return None
    except Exception:
        pass
    return x

def _get(d: Dict, *keys, default=None):
    cur = d
    for k in keys:
        if cur is None:
            return default
        cur = cur.get(k)
    return default if cur is None else cur

def _num(x):
    try:
        return float(x)
    except Exception:
        return None

def _lane_stat(entry: Dict) -> Dict[str, Any]:
    rc = entry.get("racecard", {}) or {}
    ec = (_get(entry, "stats", "entryCourse") or {}) if entry.get("stats") else {}

    # 決まり手（自艇勝ち/負け）から「抜き」「恵まれ」を除外して合計
    win_k = {k: int(v) for k, v in (ec.get("winKimariteSelf") or {}).items()
             if k not in EXCLUDE_KIMARITE}
    lose_k = {k: int(v) for k, v in (ec.get("loseKimarite") or {}).items()
              if k not in EXCLUDE_KIMARITE}
    win_sum  = sum(win_k.values())
    lose_sum = sum(lose_k.values())

    ss = ec.get("selfSummary") or {}
    ms = ec.get("matrixSelf") or {}

    out = dict(
        startCourse=_get(entry, "startCourse"),
        classNumber=_get(rc, "classNumber"),
        age=_get(rc, "age"),
        avgST_rc=_num(_get(rc, "avgST")),
        flying=_get(rc, "flyingCount"),
        late=_get(rc, "lateCount"),
        ec_avgST=_num(_get(ec, "avgST")),
        ss_starts=_get(ss, "starts"),
        ss_first=_get(ss, "firstCount"),
        ss_second=_get(ss, "secondCount"),
        ss_third=_get(ss, "thirdCount"),
        ms_winRate=_num(_get(ms, "winRate")),
        ms_top2Rate=_num(_get(ms, "top2Rate")),
        ms_top3Rate=_num(_get(ms, "top3Rate")),
        win_k_count=win_sum,
        lose_k_count=lose_sum,
    )
    return out

def _mean(values: List[float]) -> float:
    xs = [float(v) for v in values if v is not None]
    return sum(xs) / len(xs) if xs else 0.0

def _rank(values: List[float], asc=True) -> List[int]:
    # Noneは末尾に
    arr = [(i, v) for i, v in enumerate(values)]
    arr.sort(key=lambda t: (t[1] is None, t[1]), reverse=not asc)
    rank = [0]*len(values)
    for r, (i, _) in enumerate(arr, start=1):
        rank[i] = r
    return rank

def build_c_features(integ: Dict[str, Any]) -> Dict[str, Any]:
    # 基本メタ
    meta = dict(
        date=str(integ.get("date")),
        pid=str(integ.get("pid")),
        race=str(integ.get("race")),
    )

    # レーンごとの素性収集
    lanes = {int(e["lane"]): _lane_stat(e) for e in integ.get("entries", [])}

    # 相対量のために全レーン配列化
    avgst_rc_list = [lanes.get(i, {}).get("avgST_rc") for i in range(1, 7)]
    age_list      = [lanes.get(i, {}).get("age")      for i in range(1, 7)]
    cls_list      = [lanes.get(i, {}).get("classNumber") for i in range(1, 7)]

    m_avgst = _mean(avgst_rc_list)
    m_age   = _mean(age_list)
    m_cls   = _mean(cls_list)

    # ランク（avgSTは小さいほど速い → asc=True）
    r_avgst = _rank(avgst_rc_list, asc=True)
    r_age   = _rank(age_list, asc=False)     # 年齢は大きいほどランク上位にしないので降順=Falseにするならasc=False
    r_cls   = _rank(cls_list, asc=False)     # 級は数字小さいほど上位ではないため、相対序数として降順ランク

    # 出力1行にフラット化
    row = {}
    row.update(meta)

    for i in range(1, 7):
        li = lanes.get(i, {})
        p = f"L{i}"
        row[f"{p}_startCourse"] = _nan_to_none(li.get("startCourse"))
        row[f"{p}_class"]       = _nan_to_none(li.get("classNumber"))
        row[f"{p}_age"]         = _nan_to_none(li.get("age"))
        row[f"{p}_avgST_rc"]    = _nan_to_none(li.get("avgST_rc"))
        row[f"{p}_ec_avgST"]    = _nan_to_none(li.get("ec_avgST"))
        row[f"{p}_flying"]      = _nan_to_none(li.get("flying"))
        row[f"{p}_late"]        = _nan_to_none(li.get("late"))
        row[f"{p}_ss_starts"]   = _nan_to_none(li.get("ss_starts"))
        row[f"{p}_ss_first"]    = _nan_to_none(li.get("ss_first"))
        row[f"{p}_ss_second"]   = _nan_to_none(li.get("ss_second"))
        row[f"{p}_ss_third"]    = _nan_to_none(li.get("ss_third"))
        row[f"{p}_ms_winRate"]  = _nan_to_none(li.get("ms_winRate"))
        row[f"{p}_ms_top2Rate"] = _nan_to_none(li.get("ms_top2Rate"))
        row[f"{p}_ms_top3Rate"] = _nan_to_none(li.get("ms_top3Rate"))
        row[f"{p}_win_k"]       = _nan_to_none(li.get("win_k_count"))
        row[f"{p}_lose_k"]      = _nan_to_none(li.get("lose_k_count"))

        # 相対・ランク
        row[f"{p}_d_avgST_rc"]  = _nan_to_none((li.get("avgST_rc") or 0) - m_avgst)
        row[f"{p}_d_age"]       = _nan_to_none((li.get("age") or 0) - m_age)
        row[f"{p}_d_class"]     = _nan_to_none((li.get("classNumber") or 0) - m_cls)
        row[f"{p}_rank_avgST"]  = r_avgst[i-1]
        row[f"{p}_rank_age"]    = r_age[i-1]
        row[f"{p}_rank_class"]  = r_cls[i-1]

    # レース全体の集約（平均）
    row["mean_avgST_rc"] = m_avgst
    row["mean_age"]      = m_age
    row["mean_class"]    = m_cls

    return row
