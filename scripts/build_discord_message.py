# scripts/build_discord_message.py
# predict/predictions_summary.csv から TOPN を圧縮し（1-2-3/4/5 → 1-2-345）
# 「場 レース（改行）買い目」の2行構成で predict/discord_message.txt を生成

import os, csv
from collections import defaultdict

CSV_PATH = "./predict/predictions_summary.csv"
OUT_PATH = "./predict/discord_message.txt"

topn  = int(os.environ.get("TOPN", "18"))
dates = os.environ.get("DATES", "")
pids  = os.environ.get("PIDS", "")
races = os.environ.get("RACES", "")
sims  = os.environ.get("SIMS", "")

# 場コード → 場名（必要に応じて追記）
VENUE_NAMES = {
    "01": "桐生", "02": "戸田", "03": "江戸川", "04": "平和島", "05": "多摩川",
    "06": "浜名湖", "07": "蒲郡", "08": "常滑", "09": "津", "10": "三国",
    "11": "びわこ", "12": "住之江", "13": "尼崎", "14": "鳴門", "15": "丸亀",
    "16": "児島", "17": "宮島", "18": "徳山", "19": "下関", "20": "若松",
    "21": "芦屋", "22": "福岡", "23": "唐津", "24": "大村",
}

if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write("No predictions.\n")
    raise SystemExit(0)

rows = []
with open(CSV_PATH, newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        rows.append(r)

by_race = defaultdict(list)
for r in rows:
    try:
        if int(r.get("rank", 9999)) <= topn:
            by_race[(r["date"], r["pid"], r["race"])].append(r["ticket"])
    except Exception:
        pass

def compact_tickets_no_slash(tickets):
    """
    同一 (F,S) で T をまとめて結合（例: 1-2-3/4/5 → 1-2-345）
    """
    buckets = defaultdict(list)
    keep = []
    for t in tickets:
        parts = t.split("-")
        if len(parts) == 3:
            buckets[(parts[0], parts[1])].append(parts[2])
        else:
            keep.append(t)

    out = []
    for (a, b), tails in buckets.items():
        tails = sorted(set(tails))  # 重複除去＋昇順
        if len(tails) == 1:
            out.append(f"{a}-{b}-{tails[0]}")
        else:
            out.append(f"{a}-{b}-" + "".join(tails))
    out.extend(keep)
    return out

lines = []
for (d, p, r), tickets in sorted(by_race.items()):
    venue = VENUE_NAMES.get(p, p)  # 未登録コードはそのまま
    # 1行目：場 レース（raceが'10R'形式ならそのまま、数字だけの場合は末尾にRを付与）
    race_label = r if (isinstance(r, str) and r.endswith("R")) else f"{r}R"
    lines.append(f"{venue} {race_label}")
    # 2行目：買い目（スペース区切り）
    lines.append(" ".join(compact_tickets_no_slash(tickets)))
    # 空行で区切り
    lines.append("")

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
with open(OUT_PATH, "w", encoding="utf-8") as f:
    f.write("\n".join(lines).rstrip() + "\n")
