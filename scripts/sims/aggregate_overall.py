# scripts/sims/aggregate_overall.py
import json
import os
import sys
from datetime import datetime

def main():
    if len(sys.argv) != 8:
        print("Usage: aggregate_overall.py <outdir> <date> <mode> <unit> <sims> <topn> <pid>")
        sys.exit(1)

    outdir, date, mode, unit, sims, topn, pid = sys.argv[1:]
    pass2_dir = os.path.join(outdir, "pass2")

    if not os.path.isdir(pass2_dir):
        print(f"[ERROR] Directory not found: {pass2_dir}")
        sys.exit(1)

    # 初期の統計構造（最低限）
    result = {
        "meta": {
            "date": date,
            "mode": mode,
            "unit": int(unit),
            "sims": int(sims),
            "topn": int(topn),
            "pid": pid.split(","),
            "generated_at": datetime.now().isoformat(),
        },
        "totals": {},
        "keyman": {},
        "filters": {}
    }

    # KEYMANパラメータ
    result["keyman"] = {
        "enable": os.getenv("K_ENABLE", "true").lower() == "true",
        "threshold": float(os.getenv("K_THR", "0.7")),
        "boost": float(os.getenv("K_BOOST", "0.15")),
        "aggr": float(os.getenv("K_AGGR", "0.0")),
    }

    # FILTERパラメータ
    result["filters"] = {
        "require_odds": os.getenv("F_REQUIRE_ODDS", "false").lower() == "true",
        "min_ev": float(os.getenv("F_MIN_EV", "0")),
        "odds_bands": os.getenv("F_ODDS_BANDS", ""),
        "odds_min": float(os.getenv("F_ODDS_MIN", "0")),
        "odds_max": float(os.getenv("F_ODDS_MAX", "0")),
        "exclude_first1": os.getenv("F_EXCLUDE_FIRST1", "false").lower() == "true",
        "only_first1": os.getenv("F_ONLY_FIRST1", "false").lower() == "true",
        "buy_in_top3": os.getenv("F_BUY_IN_TOP3", "false").lower() == "true",
        "buy_thr": float(os.getenv("F_BUY_THR", "0.7")),
    }

    # TODO: 成果物（回収率など）の収集は別途対応（必要に応じて）

    output_path = os.path.join(pass2_dir, "overall.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"[OK] Wrote overall.json to: {output_path}")

if __name__ == "__main__":
    main()
