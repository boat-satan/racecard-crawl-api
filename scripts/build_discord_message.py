# scripts/build_discord_message.py
# predict/predictions_summary.csv から TOPN を 1-2-3/5 形式で圧縮し
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

def compact_tickets(tickets):
    buckets = defaultdict(list); keep = []
    for t in tickets:
        parts = t.split("-")
        if len(parts) == 3:
            buckets[(parts[0], parts[1])].append(parts[2])
        else:
            keep.append(t)
    out = []
    for (a,b), tails in buckets.items():
        tails = sorted(set(tails))
        out.append(f"{a}-{b}-" + (tails[0] if len(tails)==1 else "/".join(tails)))
    out.extend(keep)
    return out

lines = [f"**SimS v1 Predict**  dates={dates}  pid='{pids}'  races='{races}'  sims={sims}  topN={topn}"]
for (d,p,r), tickets in sorted(by_race.items()):
    lines.append(f"[{d} {p} {r}]  " + "、".join(compact_tickets(tickets)))

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
with open(OUT_PATH, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))