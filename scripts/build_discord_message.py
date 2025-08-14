# scripts/build_discord_message.py
# predict/predictions_summary.csv から TOPN をコンパクト表記に圧縮し
# 「場 レース」→ 改行 → 各買い目を1行ずつ → 空行 … の形で
# predict/discord_message.txt を生成
#
# 圧縮ルールの優先順位:
#  (A) 1着と2着の可換:  1-3-2456 と 3-1-2456 → 1=3-2456
#  (B) 2着と3着の可換:  2-3-1 と 2-1-3   → 2-1=3
#  (C) 3着の束圧縮:      1-2-3,1-2-4,1-2-5 → 1-2-345
#
# 整列ルール:
#  1着→2着の数値昇順でソート（= を含む場合は min/max を使って正規化して比較）
#  例: 1=3-2456 は 1着=1, 2着=3 として並べ替えキーを作る

import os, csv
from collections import defaultdict

CSV_PATH = "./predict/predictions_summary.csv"
OUT_PATH = "./predict/discord_message.txt"

topn  = int(os.environ.get("TOPN", "18"))
dates = os.environ.get("DATES", "")
pids  = os.environ.get("PIDS", "")
races = os.environ.get("RACES", "")
sims  = os.environ.get("SIMS", "")

# （任意）場コード→場名。未知は pid をそのまま表示
VENUE_NAMES = {
    # "02": "戸田",
    # "06": "蒲郡",
    # 必要なら追加
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
                key = (r["date"], r["pid"], r["race"])
                by_race[key].append(r["ticket"])
        except Exception:
            pass
    return by_race

def _compact_tickets(tickets):
    """
    入力: ["a-b-c", ...]
    出力: ルール(A)(B)(C)を適用後の文字列リスト（= と 3着結合 345 表記）
    """
    # 正規化して三つ組へ
    triples = []
    for t in tickets:
        p = t.split("-")
        if len(p) == 3:
            triples.append((p[0], p[1], p[2]))
        else:
            # 想定外はスキップしないで、空埋め（後でそのまま吐く）
            triples.append(tuple((p + [""] * (3 - len(p)))[:3]))

    used = set()
    out  = []

    # ---- (A) F/S 可換： (a,b,*) と (b,a,*) の c集合が同じ
    fs_to_cs = defaultdict(set)
    for a,b,c in triples:
        fs_to_cs[(a,b)].add(c)

    paired = set()
    for (a,b), cs in list(fs_to_cs.items()):
        pair = (b,a)
        if a == b or (a,b) in paired:  # a==b は対象外、重複回避
            continue
        if pair in fs_to_cs and fs_to_cs[pair] == cs and len(cs) > 0:
            A, B = sorted([int(a), int(b)])  # 数値化して並べる
            tails = "".join(sorted(cs, key=lambda x: int(x)))
            out.append(f"{A}={B}-{tails}")
            for c in cs:
                used.add((a,b,c)); used.add((b,a,c))
            paired.add((a,b)); paired.add((b,a))

    # ---- (B) S/T 可換： (a,s,t) と (a,t,s)
    remaining = [x for x in triples if x not in used]
    exist = set(remaining)
    st_seen = set()
    for a,s,t in remaining:
        if (a,s,t) in used: 
            continue
        if (a,t,s) in exist and (a,t,s) not in used and (a,s,t) not in st_seen and (a,t,s) not in st_seen:
            s1, s2 = sorted([int(s), int(t)])
            out.append(f"{int(a)}-{s1}={s2}")
            used.add((a,s,t)); used.add((a,t,s))
            st_seen.add((a,s,t)); st_seen.add((a,t,s))

    # ---- (C) 3着束圧縮： (a,b,*) を a-b-XYZ に
    remaining2 = [x for x in triples if x not in used]
    by_ab = defaultdict(set)
    passthrough = []
    for a,b,c in remaining2:
        if a and b and c:
            by_ab[(int(a), int(b))].add(int(c))
        else:
            # 形式外は素通し
            passthrough.append("-".join([a,b,c]).strip("-"))

    for (a,b), cs in by_ab.items():
        tails = "".join(str(x) for x in sorted(cs))
        out.append(f"{a}-{b}-{tails}")

    out.extend(passthrough)
    # 重複除去（保持順）
    return list(dict.fromkeys(out))

def _sort_compacted(bets):
    """
    1着→2着の数値昇順でソート。
    形式:
      a=b-XYZ  -> key=(min(a,b), max(a,b), 0, XYZ文字列)
      a-b=c    -> key=(a, min(b,c), 1, 残り文字)
      a-b-XYZ  -> key=(a, b, 2, XYZ文字列)
    第3キーでタイプの安定順を持たせ、最後に表記で安定化。
    """
    def key(b):
        try:
            if "=" in b and "-" in b:
                # a=b-...  か  a-b=c
                left, tail = b.split("-", 1)
                if "=" in left:
                    # a=b-XYZ
                    a, b2 = map(int, left.split("="))
                    return (min(a,b2), max(a,b2), 0, tail)
                else:
                    # a-b=c
                    a, rest = left.split("-", 1)[0], left.split("-", 1)[1]
                    s, t = map(int, rest.split("="))
                    return (int(a), min(s,t), 1, tail)
            elif "-" in b:
                a, s, tail = b.split("-", 2)
                return (int(a), int(s), 2, tail)
            return (999, 999, 9, b)
        except Exception:
            return (999, 999, 9, b)
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
    # ヘッダなど不要：場 レース → 改行 → 各買い目（1行ごと）
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