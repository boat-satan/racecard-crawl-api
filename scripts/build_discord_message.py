# scripts/build_discord_message.py
# predict/predictions_summary.csv から TOPN をコンパクト表記に圧縮し
# 「場 レース」→ 改行 → 各買い目を1行ずつ → 空行 を出力して
# predict/discord_message.txt を生成
#
# 圧縮ルール:
#  (A) 1着=2着： 1-3-2456 と 3-1-2456 → 1=3-2456
#  (B) 2着=3着： 2-3-1 と 2-1-3     → 2-1=3
#  (C) 3着束    ： 1-2-3,4,5        → 1-2-345
#
# 並び順:
#  1着→2着の数値昇順。a=b-… は (min(a,b), max(a,b))、a-b=c は (a, min(b,c)) をキーに。

import os, csv
from collections import defaultdict

CSV_PATH = "./predict/predictions_summary.csv"
OUT_PATH = "./predict/discord_message.txt"

topn  = int(os.environ.get("TOPN", "18"))
dates = os.environ.get("DATES", "")
pids  = os.environ.get("PIDS", "")
races = os.environ.get("RACES", "")
sims  = os.environ.get("SIMS", "")

VENUE_NAMES = {
    # "06": "蒲郡",
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
    triples = []
    for t in tickets:
        p = t.split("-")
        if len(p) == 3:
            triples.append((p[0], p[1], p[2]))
        else:
            triples.append(tuple((p + [""] * (3 - len(p)))[:3]))

    used = set()
    out  = []

    # (A) F/S 可換
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

    # (B) S/T 可換
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

    # (C) 3着束
    remaining2 = [x for x in triples if x not in used]
    by_ab = defaultdict(set)
    passthrough = []
    for a,b,c in remaining2:
        if a and b and c:
            by_ab[(int(a), int(b))].add(int(c))
        else:
            passthrough.append("-".join([a,b,c]).strip("-"))
    for (a,b), cs in by_ab.items():
        tails = "".join(str(x) for x in sorted(cs))
        out.append(f"{a}-{b}-{tails}")

    out.extend(passthrough)
    return list(dict.fromkeys(out))

def _sort_compacted(bets):
    """1着→2着 昇順。a=b-XYZ / a-b=c / a-b-XYZ の3形を正しく判定してキー化。"""
    def key(s):
        try:
            # まず2つ目以降の区切りを確認
            parts = s.split("-")
            if len(parts) == 2:
                # パターン: a-b=c  （例: "2-1=3"）
                a = int(parts[0])
                if "=" in parts[1]:
                    s2, t2 = parts[1].split("=", 1)
                    s2i, t2i = int(s2), int(t2)
                    return (a, min(s2i, t2i), 1, f"{max(s2i,t2i)}")
            elif len(parts) >= 2 and "=" in parts[0]:
                # パターン: a=b-XYZ （例: "1=3-2456"）
                a1, b1 = map(int, parts[0].split("=", 1))
                return (min(a1,b1), max(a1,b1), 0, "-".join(parts[1:]))
            elif len(parts) >= 3:
                # パターン: a-b-XYZ  （例: "1-2-345"）
                a, b = int(parts[0]), int(parts[1])
                return (a, b, 2, "-".join(parts[2:]))

            # どれにも当たらない場合は末尾寄りだが安定化
            return (999, 999, 9, s)
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
        venue = VENUE_NAMES.get(pid, pid)
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