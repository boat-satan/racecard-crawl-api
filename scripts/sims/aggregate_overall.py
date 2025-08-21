# scripts/sims/aggregate_overall.py

import json
import os
import glob

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def main(outdir, date, mode, unit, sims, topn, pid):
    base_dir = os.path.join(outdir, "pass2", date)
    pids = pid.split(",")
    
    total_bet = 0
    total_return = 0
    hit_count = 0
    total_races = 0

    for pid in pids:
        race_dir = os.path.join(base_dir, pid)
        if not os.path.isdir(race_dir):
            continue

        files = sorted(glob.glob(os.path.join(race_dir, "result_*.json")))
        for file in files:
            result = load_json(file)
            total_races += 1
            total_bet += result.get("bet", 0)
            total_return += result.get("return", 0)
            if result.get("hit", False):
                hit_count += 1

    roi = round(total_return / total_bet, 4) if total_bet else 0
    hit_rate = round(hit_count / total_races, 4) if total_races else 0

    overall_data = {
        "total_races": total_races,
        "hit_count": hit_count,
        "total_bet": total_bet,
        "total_return": total_return,
        "roi": roi,
        "hit_rate": hit_rate,
        "params": {
            "SIMS": sims,
            "TOPN": topn,
            "UNIT": unit,
            "MODE": mode,
            "KEYMAN": {
                "enable": os.environ.get("K_ENABLE", ""),
                "thr": os.environ.get("K_THR", ""),
                "boost": os.environ.get("K_BOOST", ""),
                "aggr": os.environ.get("K_AGGR", ""),
                "buy_in_top3": os.environ.get("F_BUY_IN_TOP3", ""),
                "buy_thr": os.environ.get("F_BUY_THR", ""),
            },
            "FILTERS": {
                "require_odds": os.environ.get("F_REQUIRE_ODDS", ""),
                "min_ev": os.environ.get("F_MIN_EV", ""),
                "odds_bands": os.environ.get("F_ODDS_BANDS", ""),
                "odds_min": os.environ.get("F_ODDS_MIN", ""),
                "odds_max": os.environ.get("F_ODDS_MAX", ""),
                "exclude_first1": os.environ.get("F_EXCLUDE_FIRST1", ""),
                "only_first1": os.environ.get("F_ONLY_FIRST1", "")
            }
        }
    }

    overall_path = os.path.join(base_dir, "overall.json")
    with open(overall_path, "w", encoding="utf-8") as f:
        json.dump(overall_data, f, ensure_ascii=False, indent=2)

    print(f"✅ Wrote overall.json to {overall_path}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 7:
        print("Usage: aggregate_overall.py <outdir> <date> <mode> <unit> <sims> <topn> <pid>")
        sys.exit(1)
    main(*sys.argv[1:])
