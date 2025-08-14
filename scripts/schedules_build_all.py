# -*- coding: utf-8 -*-
import re, subprocess
from pathlib import Path

def is_date_dir(name: str) -> bool:
    return re.fullmatch(r"\d{8}", name) is not None

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-root", default=".", help="リポジトリのルートパス")
    ap.add_argument("--programs-root", default="public/programs/v2", help="日付ディレクトリが並ぶ場所")
    args = ap.parse_args()

    root = Path(args.repo_root).resolve()
    p_root = root / args.programs_root
    if not p_root.exists():
        raise SystemExit(f"Not found: {p_root}")

    dates = [p.name for p in sorted(p_root.iterdir()) if p.is_dir() and is_date_dir(p.name)]
    if not dates:
        raise SystemExit("No date directories found under programs/v2")

    for d in dates:
        print(f"[build] {d}")
        subprocess.run(["python", "scripts/schedules_build.py", "--date", d, "--repo-root", str(root)], check=True)

if __name__ == "__main__":
    main()
