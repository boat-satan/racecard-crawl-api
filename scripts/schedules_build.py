# -*- coding: utf-8 -*-
import csv, json
from pathlib import Path
from typing import List, Dict
from schedules_sources import read_programs_cutoffs, merge_sources

def ensure_dir(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def write_csv(rows: List[Dict], path: Path):
    ensure_dir(path)
    cols = ["date","pid","place_name","race","cutoff_hm","off_hm","series","notes"]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in cols})

def write_json(rows: List[Dict], path: Path):
    ensure_dir(path)
    with path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYYMMDD（JST）")
    ap.add_argument("--repo-root", default=".", help="リポジトリのルートパス")
    ap.add_argument("--outdir", default="public/schedules/v1", help="出力ディレクトリ")
    args = ap.parse_args()

    root = Path(args.repo_root).resolve()
    primary = read_programs_cutoffs(root, args.date)
    rows = merge_sources(primary, [])  # まずはv2のみ

    if not rows:
        raise SystemExit(f"No schedule rows produced for date={args.date}.")

    outdir = root / args.outdir
    csv_path = outdir / f"{args.date}.csv"
    json_path = outdir / f"{args.date}.json"

    write_csv(rows, csv_path)
    write_json(rows, json_path)

    print(f"Wrote: {csv_path}")
    print(f"Wrote: {json_path}")

if __name__ == "__main__":
    main()
