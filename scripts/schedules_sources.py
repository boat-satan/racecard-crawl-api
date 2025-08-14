# -*- coding: utf-8 -*-
import re, json
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple, Any

JST = timezone(timedelta(hours=9))

PID2PLACE = {
    "01":"桐生","02":"戸田","03":"江戸川","04":"平和島","05":"多摩川","06":"浜名湖","07":"蒲郡",
    "08":"常滑","09":"津","10":"三国","11":"びわこ","12":"住之江","13":"尼崎","14":"鳴門","15":"丸亀",
    "16":"児島","17":"宮島","18":"徳山","19":"下関","20":"若松","21":"芦屋","22":"福岡","23":"唐津","24":"大村"
}

def norm_race_label(name: str) -> str:
    """ '1R' / '01R' / '1r.json' などを '1R' に正規化 """
    m = re.search(r"(\d+)\s*[Rr]", name)
    if m:
        return f"{int(m.group(1))}R"
    # うまく取れなければそのまま（上位で弾く）
    return name.upper()

def off_fallback(cutoff_hm: str, minutes: int = 3) -> str:
    dt = datetime.strptime(cutoff_hm, "%H:%M").replace(tzinfo=JST)
    return (dt + timedelta(minutes=minutes)).strftime("%H:%M")

def _pick_hm_str(s: Any) -> Optional[str]:
    """
    候補文字列/数値から HH:MM を抽出（'10:45', '10：45', '10時45分', '1045' も許容）
    """
    if s is None:
        return None
    if isinstance(s, (int, float)):
        s = str(s)
    if not isinstance(s, str):
        return None
    s = s.strip()

    # パターン1: 10:45 / 9:03 / 10：45（全角コロン対応）
    s2 = s.replace("：", ":")
    m = re.search(r"\b(\d{1,2}):(\d{2})\b", s2)
    if m:
        hh, mm = int(m.group(1)), int(m.group(2))
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return f"{hh:02d}:{mm:02d}"

    # パターン2: 10時45分
    m = re.search(r"(\d{1,2})\s*時\s*(\d{1,2})\s*分", s)
    if m:
        hh, mm = int(m.group(1)), int(m.group(2))
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return f"{hh:02d}:{mm:02d}"

    # パターン3: 1045（4桁）
    m = re.fullmatch(r"(\d{3,4})", s)
    if m and len(s) in (3,4):
        hh = int(s[:-2])
        mm = int(s[-2:])
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return f"{hh:02d}:{mm:02d}"
    return None

def _dig(obj: Any, keys_like: List[str]) -> Optional[Any]:
    """
    JSONを深掘りして、それっぽいキー名（部分一致・小文字化）を優先探索。
    """
    target_keys = [k.lower() for k in keys_like]
    def walk(x: Any) -> Optional[Any]:
        if isinstance(x, dict):
            # 直接キー一致（ゆるく部分一致）
            for k, v in x.items():
                kl = k.lower()
                if any(t in kl for t in target_keys):
                    return v
            # ネスト探索
            for v in x.values():
                r = walk(v)
                if r is not None:
                    return r
        elif isinstance(x, list):
            for v in x:
                r = walk(v)
                if r is not None:
                    return r
        return None
    return walk(obj)

def extract_cutoff_off_series(obj: Dict) -> Tuple[Optional[str], Optional[str], str]:
    """
    v2の各レースJSONから
    - 締切（cutoff_hm）
    - 発走（off_hm）
    - 開催名（series）
    を推定抽出。キー名の揺れに強め。
    """
    # 締切候補キー
    cutoff_candidates = [
        "cutoff","cutoff_hm","betCloseTime","closeTime","closingTime","limitTime","deadline","betsClose",
        "締切","投票締切","締切予定","締切時刻","締切時間"
    ]
    # 発走候補キー
    off_candidates = [
        "off","off_hm","startTime","postTime","raceStart","発走","発走予定","発走時刻","発走時間"
    ]
    # 開催名
    series_candidates = [
        "series","meet_name","meetingName","開催名","シリーズ","タイトル"
    ]

    cutoff_raw = _dig(obj, cutoff_candidates)
    off_raw    = _dig(obj, off_candidates)
    series_raw = _dig(obj, series_candidates)

    cutoff_hm = _pick_hm_str(cutoff_raw)
    off_hm    = _pick_hm_str(off_raw)
    series    = ""
    if isinstance(series_raw, str):
        series = series_raw.strip()

    return cutoff_hm, off_hm, series

def read_programs_cutoffs(base_dir: Path, date: str) -> List[Dict]:
    """
    public/programs/v2/{date}/{pid}/{race}.json から締切などを収集
    """
    rows: List[Dict] = []
    root = base_dir / "public" / "programs" / "v2" / date
    if not root.exists():
        return rows

    for pid_dir in sorted([p for p in root.iterdir() if p.is_dir()]):
        pid = pid_dir.name  # '01' など
        for f in sorted(pid_dir.glob("*.json")):
            race_label = norm_race_label(f.stem)
            if not re.match(r"^\d{1,2}R$", race_label):
                continue
            try:
                obj = json.loads(f.read_text(encoding="utf-8"))
            except Exception:
                continue

            cutoff_hm, off_hm, series = extract_cutoff_off_series(obj)
            if not cutoff_hm and not off_hm:
                # どちらも無い場合はスキップ
                continue
            if cutoff_hm and not off_hm:
                off_hm = off_fallback(cutoff_hm)
            if not cutoff_hm and off_hm:
                # 逆補完（発走−3分を締切に）
                dt = datetime.strptime(off_hm, "%H:%M").replace(tzinfo=JST)
                cutoff_hm = (dt - timedelta(minutes=3)).strftime("%H:%M")

            rows.append({
                "date": date,
                "pid": pid,
                "place_name": PID2PLACE.get(pid, ""),
                "race": race_label,
                "cutoff_hm": cutoff_hm,
                "off_hm": off_hm,
                "series": series,
                "notes": ""  # ここに安定板や遅延フラグを後で足してもOK
            })
    # pid昇順→レース番号昇順
    return sorted(rows, key=lambda r: (r["pid"], int(r["race"].replace("R",""))))

def merge_sources(primary: List[Dict], fallback: List[Dict]) -> List[Dict]:
    """
    互換用：今回は primary=programs(v2)、fallback=（必要なら）公式スクレイプ等。
    """
    key = lambda r: (r["date"], r["pid"], r["race"])
    out: Dict[Tuple[str,str,str], Dict] = {}
    for r in fallback:
        out[key(r)] = r
    for r in primary:
        out[key(r)] = {**out.get(key(r), {}), **r}
    return sorted(out.values(), key=lambda r: (r["pid"], int(r["race"].replace("R",""))))
