import json
import os
import pandas as pd


def build_c_features(date: str, pid: str, race: str = ""):
    """C特徴量を生成して CSV 出力"""

    base_dir = f"public/integrated/v1/{date}/{pid}"
    outputs = []

    races = [race] if race else [f"{i}R" for i in range(1, 13)]

    for r in races:
        path = os.path.join(base_dir, f"{r}.json")
        if not os.path.exists(path):
            print(f"skip {path} (not found)")
            continue

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        row = {
            "date": date,
            "pid": pid,
            "race": r,
        }

        entries = data.get("entries", [])
        for e in entries:
            lane = e.get("lane")
            rc = e.get("racecard", {})
            ec = e.get("exhibition", {})
            ss = rc.get("startStats", {})
            ms = rc.get("motorStats", {})

            prefix = f"L{lane}_"

            feat = {
                "startCourse": e.get("startCourse"),
                "class": rc.get("classNumber"),
                "age": rc.get("age"),
                "avgST_rc": rc.get("avgST"),
                "ec_avgST": ec.get("avgST"),
                "flying": rc.get("flyingCount"),
                "late": rc.get("lateCount"),
                "ss_starts": ss.get("starts"),
                "ss_first": ss.get("first"),
                "ss_second": ss.get("second"),
                "ss_third": ss.get("third"),
                "ms_winRate": ms.get("winRate"),
                "ms_top2Rate": ms.get("top2Rate"),
                "ms_top3Rate": ms.get("top3Rate"),
            }

            # 勝ち・負け補正値
            feat["win_k"] = ms.get("wins", 0)
            feat["lose_k"] = ms.get("loses", 0)

            # ダミー特徴量例
            feat["d_avgST_rc"] = (rc.get("avgST") or 0) - 0.16
            feat["d_age"] = (rc.get("age") or 0) - 40
            feat["d_class"] = (rc.get("classNumber") or 0) - 3

            # ランク（相対評価用ダミー）
            feat["rank_avgST"] = 0
            feat["rank_age"] = 0
            feat["rank_class"] = 0

            # prefix 付けて row に追加
            for k, v in feat.items():
                row[f"{prefix}{k}"] = v

        # レース全体の平均特徴量（例）
        avgSTs = [rc.get("avgST") for rc in (e.get("racecard", {}) for e in entries) if rc.get("avgST") is not None]
        ages = [rc.get("age") for rc in (e.get("racecard", {}) for e in entries) if rc.get("age") is not None]
        classes = [rc.get("classNumber") for rc in (e.get("racecard", {}) for e in entries) if rc.get("classNumber") is not None]

        row["mean_avgST_rc"] = sum(avgSTs) / len(avgSTs) if avgSTs else None
        row["mean_age"] = sum(ages) / len(ages) if ages else None
        row["mean_class"] = sum(classes) / len(classes) if classes else None

        outputs.append(row)

    if not outputs:
        print("no outputs")
        return

    outdir = f"TENKAI/features_c/v1/{date}/{pid}"
    os.makedirs(outdir, exist_ok=True)
    outfile = os.path.join(outdir, f"{race or 'all'}.csv")

    df = pd.DataFrame(outputs)
    df.to_csv(outfile, index=False, encoding="utf-8")
    print(f"wrote {outfile}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--pid", required=True)
    parser.add_argument("--race", default="")
    args = parser.parse_args()

    build_c_features(args.date, args.pid, args.race)
