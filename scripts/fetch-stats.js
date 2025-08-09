// scripts/fetch-stats.js
// 今日の出走表から選手の登録番号を集め、boatrace-db から
// ・コース別成績 ・コース別決まり手 ・展示タイム順位別成績 を取得して保存。

import fs from 'node:fs/promises';
import path from 'node:path';
import * as cheerio from 'cheerio';

const ROOT = path.resolve('public');
const PROGRAMS_DIR = path.join(ROOT, 'programs', 'v2', 'today'); // 既存の出走表置き場
const OUT_DIR = path.join(ROOT, 'stats', 'v1', 'racers');

async function ensureDir(p) {
  await fs.mkdir(p, { recursive: true }).catch(() => {});
}

function toNumber(s) {
  if (s == null) return null;
  const n = String(s).replace(/[^\d.\-]/g, '');
  return n === '' ? null : Number(n);
}

function pickText($, el) {
  return $(el).text().trim();
}

// テーブル見出しの“見出しテキスト”から次のtableを拾うヘルパ
function tableByHeading($, headingText) {
  // 見出しの候補：h2,h3,.ttl などを総当たり
  const headings = $('h1,h2,h3,h4,h5,.ttl,.title,.heading').filter((_, h) =>
    pickText($, h).includes(headingText)
  );
  if (headings.length === 0) return null;
  // 一番近い table
  let t = headings.first().nextAll('table').first();
  if (t.length === 0) {
    // 別構造の保険
    t = headings.first().parent().nextAll('table').first();
  }
  return t.length ? t : null;
}

// コース別成績テーブルをパース（1〜6コース）
function parseCourseStats($, table) {
  const result = {};
  $('tr', table).each((i, tr) => {
    const tds = $('th,td', tr).map((_, td) => pickText($, td)).get();
    // 例: [ "1コース", "出走", "勝率/1着率", "2連対率", "3連対率", "平均ST" ] 等、サイトにより表記差あり
    const head = tds.join(' ');
    // 行データ候補：先頭が「1」「1コース」「１コース」など
    const first = tds[0] || '';
    const m = first.match(/([1-6])/);
    if (!m) return;
    const c = m[1];

    // 残りから数値を拾う（許容的に）
    // よくある並び: 出走, 勝率(または1着率), 2連対率, 3連対率, 平均ST
    const nums = tds.slice(1).map(toNumber).filter(v => v !== null);
    // 最低でも 4〜5 個拾える想定
    const [starts, winOrRate, top2, top3, avgST] = nums;

    result[c] = {
      starts: starts ?? null,
      winPct: winOrRate ?? null,   // サイトによって「勝率(%) or 1着率(%)」の差あり
      top2Pct: top2 ?? null,
      top3Pct: top3 ?? null,
      avgST: avgST ?? null
    };
  });
  return Object.keys(result).length ? result : null;
}

// コース別決まり手（行が「1コース」「2コース」…、列に決まり手名が並ぶテーブル想定）
function parseMovesByCourse($, table) {
  // 1行目がヘッダ（決まり手の種類）
  const headers = $('tr', table).first().find('th,td').map((_, x) => pickText($, x)).get();
  const moveNames = headers.slice(1); // 先頭列はコース

  const out = {};
  $('tr', table).slice(1).each((_, tr) => {
    const cells = $('th,td', tr).map((_, td) => pickText($, td)).get();
    const first = cells[0] || '';
    const m = first.match(/([1-6])/);
    if (!m) return;
    const c = m[1];
    const row = {};
    moveNames.forEach((name, i) => {
      row[name] = toNumber(cells[i + 1]) ?? 0;
    });
    out[c] = row;
  });

  return Object.keys(out).length ? out : null;
}

// 展示タイム順位別成績（順位1〜6の行に、1着率/2連対率/3連対率 等が並ぶ想定）
function parseExTimeRankStats($, table) {
  const out = {};
  $('tr', table).each((_, tr) => {
    const cells = $('th,td', tr).map((_, td) => pickText($, td)).get();
    const label = cells[0] || '';
    const m = label.match(/([1-6])\s*位/); // 「1位」「１位」など
    if (!m) return;
    const rank = m[1];

    const nums = cells.slice(1).map(toNumber).filter(v => v !== null);
    // 例: [出走, 1着率, 2連対率, 3連対率]
    const [starts, winPct, top2Pct, top3Pct] = nums;
    out[rank] = { starts, winPct, top2Pct, top3Pct };
  });
  return Object.keys(out).length ? out : null;
}

// boatrace-db レーサーページを取得してパース
async function fetchOne(regno) {
  const url = `https://boatrace-db.net/racer/rcourse/regno/${regno}/`;
  const res = await fetch(url, { headers: { 'User-Agent': 'racecard-crawl-api (+github actions)' } });
  if (!res.ok) throw new Error(`fetch ${regno} ${res.status}`);
  const html = await res.text();
  const $ = cheerio.load(html);

  // セクションを見出しテキストで探す（サイト変更に強めのゆる選択）
  const courseTable = tableByHeading($, 'コース別成績');
  const movesTable  = tableByHeading($, 'コース別決まり手');
  const exRankTable = tableByHeading($, '展示タイム順位別成績');

  const courseStats = courseTable ? parseCourseStats($, courseTable) : null;
  const finishingMovesByCourse = movesTable ? parseMovesByCourse($, movesTable) : null;
  const exhibitionTimeRankStats = exRankTable ? parseExTimeRankStats($, exRankTable) : null;

  return {
    regno,
    fetchedAt: new Date().toISOString(),
    ...(courseStats ? { courseStats } : {}),
    ...(finishingMovesByCourse ? { finishingMovesByCourse } : {}),
    ...(exhibitionTimeRankStats ? { exhibitionTimeRankStats } : {})
  };
}

// 今日の出走 JSON から regno を集める
async function collectTodayRegnos() {
  const regnos = new Set();
  const venues = await fs.readdir(PROGRAMS_DIR);
  for (const v of venues) {
    const venueDir = path.join(PROGRAMS_DIR, v);
    const files = await fs.readdir(venueDir).catch(() => []);
    for (const f of files) {
      if (!f.endsWith('.json')) continue;
      const j = JSON.parse(await fs.readFile(path.join(venueDir, f), 'utf8'));
      if (!Array.isArray(j.boats)) continue;
      for (const b of j.boats) {
        if (b?.racer_number) regnos.add(b.racer_number);
      }
    }
  }
  return [...regnos];
}

async function main() {
  await ensureDir(OUT_DIR);
  const regnos = await collectTodayRegnos();
  console.log(`targets: ${regnos.length} racers`);

  const results = [];
  for (const regno of regnos) {
    try {
      const data = await fetchOne(regno);
      const outPath = path.join(OUT_DIR, `${regno}.json`);
      await fs.writeFile(outPath, JSON.stringify(data, null, 2));
      results.push({ regno, ok: true });
      // 小さなウェイト（優しさ）
      await new Promise(r => setTimeout(r, 150));
    } catch (e) {
      console.warn(`fail ${regno}:`, e.message);
      results.push({ regno, ok: false, error: e.message });
    }
  }

  // メタ
  await fs.writeFile(
    path.join(ROOT, 'debug', 'stats-meta-today.json'),
    JSON.stringify({ status: 200, count: results.length, ok: results.filter(r => r.ok).length, generatedAt: new Date().toISOString() }, null, 2)
  );
  console.log('done.');
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
