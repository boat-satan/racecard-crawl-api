# scripts/build_discord_message.py
# predict/predictions_summary.csv から TOPN をコンパクト表記に圧縮し
# 「場 レース」→改行→各買い目(1行ずつ)→空行 を出力して
# predict/discord_message.txt を生成

import os, csv
from collections import defaultdict

CSV_PATH = "./predict/predictions_summary.csv"
OUT_PATH = "./predict/discord_message.txt"

topn  = int(os.environ.get("TOPN", "18"))
dates = os.environ.get("DATES", "")
pids  = os.environ.get("PIDS", "")
races = os.environ.get("RACES", "")
sims  = os.environ.get("SIMS", "")

# PID -> 場名（01〜24）
VENUE_NAMES = {
    "01":"桐生","02":"戸田","03":"江戸川","04":"平和島","05":"多摩川","06":"浜名湖",
    "07":"蒲郡","08":"常滑","09":"津","10":"三国","11":"びわこ","12":"住之江",
    "13":"尼崎","14":"鳴門","15":"丸亀","16":"児島","17":"宮島","18":"徳山",
    "19":"下関","20":"若松","21":"芦屋","22":"福岡","23":"唐津","24":"大村"
}

def _read_rows(path):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows

def _group_topn(rows, topn):
    by_race = defaultdict(list)
    for r in rows:
        try:
            if int(r.get("rank", 9999)) <= topn:
                by_race[(r["date"], r["pid"], r["race"])].append(r["ticket"])
        except Exception:
            pass
    return by_race

def _compact_tickets(tickets):
    # 入力: ["a-b-c", ...]
    triples = []
    for t in tickets:
        p = t.split("-")
        if len(p) == 3:
            triples.append((p[0], p[1], p[2]))
        else:
            triples.append(tuple((p + [""] * (3 - len(p)))[:3]))

    used = set()
    out  = []

    # (1) F/S 可換: a=b-XYZ
    fs_to_cs = defaultdict(set)
    for a,b,c in triples:
        fs_to_cs[(a,b)].add(c)
    paired = set()
    for (a,b), cs in list(fs_to_cs.items()):
        pair = (b,a)
        if a == b or (a,b) in paired:
            continue
        if pair in fs_to_cs and fs_to_cs[pair] == cs and len(cs) > 0:
            A, B = sorted([int(a), int(b)])
            tails = "".join(sorted(cs, key=lambda x: int(x)))
            out.append(f"{A}={B}-{tails}")
            for c in cs:
                used.add((a,b,c)); used.add((b,a,c))
            paired.add((a,b)); paired.add((b,a))

    # (2) S/T 可換: a-b=c
    remaining = [x for x in triples if x not in used]
    exist = set(remaining)
    seen = set()
    for a,s,t in remaining:
        if (a,s,t) in used:
            continue
        if (a,t,s) in exist and (a,t,s) not in used and (a,s,t) not in seen and (a,t,s) not in seen:
            s1, s2 = sorted([int(s), int(t)])
            out.append(f"{int(a)}-{s1}={s2}")
            used.add((a,s,t)); used.add((a,t,s))
            seen.add((a,s,t)); seen.add((a,t,s))

    # (3) 3着束: a-b-XYZ
    remaining2 = [x for x in triples if x not in used]
    by_ab = defaultdict(set)
    passthrough = []
    for a,b,c in remaining2:
        if a and b and c:
            by_ab[(int(a), int(b))].add(int(c))
        else:
            passthrough.append("-".join([a,b,c]).strip("-"))
    for (a,b), cs in by_ab.items():
        if len(cs) >= 2:
            tails = "".join(str(x) for x in sorted(cs))
            out.append(f"{a}-{b}-{tails}")
            for c in cs:
                used.add((str(a), str(b), str(c)))

    # (4) 2着束: a-BC-c（新規）
    remaining3 = [x for x in triples if x not in used]
    by_ac = defaultdict(set)
    passthrough2 = []
    for a,b,c in remaining3:
        if a and b and c:
            by_ac[(int(a), int(c))].add(int(b))
        else:
            passthrough2.append("-".join([a,b,c]).strip("-"))
    for (a,c), bs in by_ac.items():
        if len(bs) >= 2:
            mids = "".join(str(x) for x in sorted(bs))
            out.append(f"{a}-{mids}-{c}")
            for b in bs:
                used.add((str(a), str(b), str(c)))

    # (5) 残りは素通し（順序維持で重複除去）
    remaining4 = [ "-".join(t).strip("-") for t in triples if t not in used ]
    out.extend(passthrough)
    out.extend(passthrough2)
    out.extend(remaining4)

    # 順序維持の重複排除
    return list(dict.fromkeys(out))

def _sort_compacted(bets):
    """a=b-XYZ / a-b=c / a-b-XYZ / a-BC-c を整列（a昇順→b(最小)昇順→型→後尾）"""
    def _min_digits(s):
        try:
            return min(int(ch) for ch in s if ch.isdigit())
        except Exception:
            return 999

    def key(s):
        try:
            parts = s.split("-")

            # 2パーツ: "a=b-XYZ" or "a-b=c"
            if len(parts) == 2:
                if "=" in parts[0]:  # a=b-XYZ
                    a1, b1 = parts[0].split("=", 1)
                    return (min(int(a1), int(b1)), max(int(a1), int(b1)), 0, parts[1])
                if "=" in parts[1]:  # a-b=c
                    a = int(parts[0]); s2, t2 = parts[1].split("=", 1)
                    s2i, t2i = int(s2), int(t2)
                    return (a, min(s2i, t2i), 1, f"{max(s2i, t2i)}")

            # 3パーツ以上（a-b-XYZ / a-BC-c）
            a = int(parts[0])
            mid = parts[1] if len(parts) >= 2 else "999"
            mid_key = _min_digits(mid)
            tail = "-".join(parts[2:]) if len(parts) >= 3 else ""
            return (a, mid_key, 2, tail or "")
        except Exception:
            return (999, 999, 9, s)

    return sorted(bets, key=key)

def main():
    if (not os.path.exists(CSV_PATH)) or os.path.getsize(CSV_PATH) == 0:
        os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
        with open(OUT_PATH, "w", encoding="utf-8") as f:
            f.write("No predictions.\n")
        return

    rows = _read_rows(CSV_PATH)
    by_race = _group_topn(rows, topn)

    lines = []
    for (d, pid, race), tickets in sorted(by_race.items()):
        pid2 = str(pid).zfill(2)
        venue = VENUE_NAMES.get(pid2, pid2)
        lines.append(f"{venue} {race}")
        compacted = _compact_tickets(tickets)
        for bet in _sort_compacted(compacted):
            lines.append(bet)
        lines.append("")  # 空行

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).rstrip() + "\n")

if __name__ == "__main__":
    main()