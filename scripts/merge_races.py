# scripts/merge_races.py
# 統合データ(integrated)・オッズ(odds)・結果(results)を突合し、
# 3連単の組み合わせごとに1行で出力するCSVを public/merged/ 配下に生成します。

import os
import json
import pandas as pd

BASE_DIR = 'public'
INTEGRATED_ROOT = os.path.join(BASE_DIR, 'integrated', 'v1')
ODDS_ROOT       = os.path.join(BASE_DIR, 'odds',       'v1')
RESULTS_ROOT    = os.path.join(BASE_DIR, 'results',    'v1')
OUT_DIR         = os.path.join(BASE_DIR, 'merged')
OUT_PATH        = os.path.join(OUT_DIR, 'merged_trifecta_data.csv')

def safe_load(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def collect_entry_features(integ_json):
    """出走表から基本特徴（選手名/レーンなど）を抜き出し"""
    entries = integ_json.get('entries', []) or []
    feat = {}
    for e in entries:
        lane = e.get('lane')
        rc = e.get('racecard', {}) or {}
        name = rc.get('name')
        if lane is not None and name is not None:
            feat[f'lane_{lane}_name'] = name
    # 場・天候など（あれば）
    weather = (integ_json.get('weather') or {})
    feat['weather'] = weather.get('weather')
    feat['temperature'] = weather.get('temperature')
    feat['windSpeed'] = weather.get('windSpeed')
    feat['windDirection'] = weather.get('windDirection')
    feat['waterTemperature'] = weather.get('waterTemperature')
    feat['waveHeight'] = weather.get('waveHeight')
    return feat

def main():
    records = []

    if not os.path.isdir(INTEGRATED_ROOT):
        raise SystemExit(f'Not found: {INTEGRATED_ROOT}')

    for date in sorted(os.listdir(INTEGRATED_ROOT)):
        date_dir = os.path.join(INTEGRATED_ROOT, date)
        if not os.path.isdir(date_dir):
            continue

        for jcd in sorted(os.listdir(date_dir)):
            jcd_dir = os.path.join(date_dir, jcd)
            if not os.path.isdir(jcd_dir):
                continue

            for filename in sorted(os.listdir(jcd_dir)):
                if not filename.endswith('.json'):
                    continue

                race = filename[:-5]  # '10R.json' -> '10R'
                integ_path  = os.path.join(INTEGRATED_ROOT, date, jcd, filename)
                odds_path   = os.path.join(ODDS_ROOT,       date, jcd, filename)
                result_path = os.path.join(RESULTS_ROOT,    date, jcd, filename)

                # 必須ファイルが無いケースはスキップ
                if not (os.path.exists(odds_path) and os.path.exists(result_path)):
                    # 必要ならログを出す
                    # print(f'Skip: odds/results missing for {date}/{jcd}/{race}')
                    continue

                try:
                    integ = safe_load(integ_path)
                except Exception:
                    # 破損などはスキップ
                    continue
                try:
                    odds_data = safe_load(odds_path)
                    result = safe_load(result_path)
                except Exception:
                    continue

                # 出走表→特徴量
                entry_info = collect_entry_features(integ)

                # 結果の確定3連単
                winning_combo = (
                    (result.get('payouts') or {})
                    .get('trifecta', {})
                    .get('combo')
                )

                # オッズ（3連単）を展開
                trifecta = odds_data.get('trifecta') or []
                for item in trifecta:
                    combo = item.get('combo')
                    if combo is None:
                        continue
                    rec = {
                        'date': date,          # YYYYMMDD
                        'jcd': jcd,            # 場コード
                        'race': race,          # '10R' など
                        'combo': combo,        # '1-2-3'
                        'F': item.get('F'),
                        'S': item.get('S'),
                        'T': item.get('T'),
                        'odds': item.get('odds'),
                        'popularity_rank': item.get('popularityRank'),
                        'is_win': 1 if combo == winning_combo else 0,
                    }
                    rec.update(entry_info)
                    records.append(rec)

    # DataFrame & 出力
    df = pd.DataFrame.from_records(records)
    os.makedirs(OUT_DIR, exist_ok=True)
    df.to_csv(OUT_PATH, index=False, encoding='utf-8-sig')

    # ざっくりサマリ
    print(f'rows={len(df)}  races={df[\"date\"].nunique()} dates x {df[\"jcd\"].nunique()} places')
    print(f'output: {OUT_PATH}')

if __name__ == '__main__':
    main()
