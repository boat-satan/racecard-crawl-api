# scripts/merge_races.py
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
    entries = integ_json.get('entries', []) or []
    feat = {}
    for e in entries:
        lane = e.get('lane')
        rc = e.get('racecard', {}) or {}
        name = rc.get('name')
        if lane is not None and name is not None:
            feat[f'lane_{lane}_name'] = name

    weather = (integ_json.get('weather') or {})
    feat['weather']          = weather.get('weather')
    feat['temperature']      = weather.get('temperature')
    feat['windSpeed']        = weather.get('windSpeed')
    feat['windDirection']    = weather.get('windDirection')
    feat['waterTemperature'] = weather.get('waterTemperature')
    feat['waveHeight']       = weather.get('waveHeight')
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

                if not (os.path.exists(odds_path) and os.path.exists(result_path)):
                    continue

                try:
                    integ = safe_load(integ_path)
                    odds_data = safe_load(odds_path)
                    result = safe_load(result_path)
                except Exception:
                    continue

                entry_info = collect_entry_features(integ)

                # 結果の確定3連単（安全に取得）
                payouts = result.get('payouts') or {}
                trifecta_info = payouts.get('trifecta') or {}
                winning_combo = trifecta_info.get('combo')

                trifecta = odds_data.get('trifecta') or []
                for item in trifecta:
                    combo = item.get('combo')
                    if not combo:
                        continue
                    rec = {
                        'date': date,
                        'jcd': jcd,
                        'race': race,
                        'combo': combo,
                        'F': item.get('F'),
                        'S': item.get('S'),
                        'T': item.get('T'),
                        'odds': item.get('odds'),
                        'popularity_rank': item.get('popularityRank'),
                        'is_win': 1 if winning_combo and combo == winning_combo else 0,
                    }
                    rec.update(entry_info)
                    records.append(rec)

    df = pd.DataFrame.from_records(records)
    os.makedirs(OUT_DIR, exist_ok=True)
    df.to_csv(OUT_PATH, index=False, encoding='utf-8-sig')

    print(f"rows={len(df)}  races={df['date'].nunique()} dates x {df['jcd'].nunique()} places")
    print(f'output: {OUT_PATH}')

if __name__ == '__main__':
    main()
