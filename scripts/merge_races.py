# scripts/merge_races.py
# integrated/odds/results を突合し、日付ごとに CSV を出力します。
# 出力:
#   - public/merged/by_date/{YYYYMMDD}.csv
#   - public/merged/index_by_date.csv   ←日付ごとの件数サマリ

import os
import json
import pandas as pd

BASE_DIR = 'public'
INTEGRATED_ROOT = os.path.join(BASE_DIR, 'integrated', 'v1')
ODDS_ROOT       = os.path.join(BASE_DIR, 'odds',       'v1')
RESULTS_ROOT    = os.path.join(BASE_DIR, 'results',    'v1')
OUT_DIR         = os.path.join(BASE_DIR, 'merged')
BY_DATE_DIR     = os.path.join(OUT_DIR, 'by_date')
INDEX_PATH      = os.path.join(OUT_DIR, 'index_by_date.csv')

def safe_load(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def collect_entry_features(integ_json):
    """出走表から選手名・天候などの軽い特徴を抽出"""
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

    # 走査
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

                # 必須がなければスキップ
                if not (os.path.exists(odds_path) and os.path.exists(result_path)):
                    continue

                try:
                    integ = safe_load(integ_path)
                    odds_data = safe_load(odds_path)
                    result = safe_load(result_path)
                except Exception:
                    # 壊れている/読み取れないファイルはスキップ
                    continue

                entry_info = collect_entry_features(integ)

                # 確定3連単（安全に取得）
                payouts = result.get('payouts') or {}
                trifecta_info = payouts.get('trifecta') or {}
                winning_combo = trifecta_info.get('combo')

                # オッズの全組み合わせを展開
                for item in (odds_data.get('trifecta') or []):
                    combo = item.get('combo')
                    if not combo:
                        continue
                    rec = {
                        'date': date,                # YYYYMMDD
                        'jcd': jcd,                  # 場コード
                        'race': race,                # '10R'
                        'combo': combo,              # '1-2-3'
                        'F': item.get('F'),
                        'S': item.get('S'),
                        'T': item.get('T'),
                        'odds': item.get('odds'),
                        'popularity_rank': item.get('popularityRank'),
                        'is_win': 1 if (winning_combo and combo == winning_combo) else 0,
                    }
                    rec.update(entry_info)
                    records.append(rec)

    # DataFrame
    df = pd.DataFrame.from_records(records)

    # 出力先用意
    os.makedirs(BY_DATE_DIR, exist_ok=True)

    # 既存 by_date を一旦クリーン（古い残骸を防ぐ）
    for f in os.listdir(BY_DATE_DIR):
        if f.endswith('.csv'):
            try:
                os.remove(os.path.join(BY_DATE_DIR, f))
            except Exception:
                pass

    # 日付別に分割保存
    index_rows = []
    if not df.empty:
        for d, ddf in df.groupby('date'):
            out_path = os.path.join(BY_DATE_DIR, f'{d}.csv')
            ddf.to_csv(out_path, index=False, encoding='utf-8-sig')
            index_rows.append({
                'date': d,
                'rows': len(ddf),
                'unique_places': ddf['jcd'].nunique(),
                'unique_races': ddf['race'].nunique(),   # 例: '1R'〜'12R'
            })

    # インデックスCSV（サマリ）
    pd.DataFrame(index_rows).sort_values('date').to_csv(
        INDEX_PATH, index=False, encoding='utf-8-sig'
    )

    # ログ
    if df.empty:
        print('No rows to write.')
    else:
        print(f"total_rows={len(df)}  dates={df['date'].nunique()}  places={df['jcd'].nunique()}")
        print(f'by_date dir: {BY_DATE_DIR}')
        print(f'index: {INDEX_PATH}')

if __name__ == '__main__':
    main()
