// scripts/fetch-beforeinfo-direct.js
// 使い方: node scripts/fetch-beforeinfo-direct.js 20250809 02 4R
// 例URL: https://www.boatrace.jp/owpc/pc/race/beforeinfo?hd=20250809&jcd=02&rno=4

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import fetch from 'node-fetch';
import * as cheerio from 'cheerio';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const [,, dateArg, pidArg, raceArg] = process.argv;
if (!dateArg || !pidArg || !raceArg) {
  console.error('Usage: node scripts/fetch-beforeinfo-direct.js <YYYYMMDD> <pid:01..24> <race:1R..12R|1..12>');
  process.exit(1);
}

const date = String(dateArg).replace(/-/g, '');
const pid  = String(pidArg).padStart(2, '0');
const raceNum = String(raceArg).toUpperCase().replace(/Ｒ/g,'R');
const rno = (() => {
  const m = raceNum.match(/\d+/);
  return m ? m[0] : raceNum;
})();

const url = `https://www.boatrace.jp/owpc/pc/race/beforeinfo?hd=${date}&jcd=${pid}&rno=${rno}`;

const getText = ($el) => ($el.text() || '').replace(/\s+/g,' ').trim();
const pickNumberFromHref = (href) => {
  if (!href) return '';
  const m = href.match(/toban=(\d{4})/);
  return m ? m[1] : '';
};

(async () => {
  const res = await fetch(url, { headers: { 'user-agent': 'Mozilla/5.0' }});
  if (!res.ok) {
    console.error(`Fetch failed: ${res.status} ${res.statusText}`);
    process.exit(1);
  }
  const html = await res.text();
  const $ = cheerio.load(html);

  // --- メイン表（枠ごと tbody 6本） ---
  const $mainTable = $('table.is-w748').first();
  const tbodies = $mainTable.find('tbody');

  // スタ展（右側の小さい表）: コース1〜6の順でSTが並ぶ
  const startExRows = $('table.is-w238 tbody.is-p10-0 > tr'); // 6ブロック
  const stByLane = {};
  let laneCursor = 1;
  startExRows.each((_, tr) => {
    // 各行に .table1_boatImage1Time が1つ
    const stTxt = getText($(tr).find('.table1_boatImage1Time'));
    if (stTxt) {
      // 例: ".19", "F.03" -> そのまま保存
      stByLane[laneCursor] = stTxt;
      laneCursor += 1;
    }
  });

  const entries = [];
  tbodies.each((_, tbody) => {
    const $tb = $(tbody);
    const $rows = $tb.find('tr');
    if ($rows.length === 0) return;

    const $r1 = $rows.eq(0);
    const tds = $r1.find('td');

    // 1行目の並びを前提（提供HTML準拠）
    const laneText = getText(tds.eq(0));
    const lane = parseInt(laneText, 10) || null;

    // 写真セル内 <a href="/owpc/pc/data/racersearch/profile?toban=4332">
    const href = tds.eq(1).find('a').attr('href') || '';
    const number = pickNumberFromHref(href); // "4332"

    // 選手名（3つ目のtd内のa）
    const name = getText(tds.eq(2).find('a'));

    // 体重（4つ目td・同tbody 2行目に調整重量があるが今回は省略）
    const weight = getText(tds.eq(3)) || '';

    // 展示タイム（5つ目）
    const tenji = getText(tds.eq(4)) || '';

    // チルト（6つ目）
    const tilt = getText(tds.eq(5)) || '';

    // ST（右の“スタート展示”表から lane で引く）
    const st = stByLane[lane] || '';

    entries.push({
      lane,
      number: number || '',
      name: name || '',
      weight,           // 例: "55.8kg"
      tenjiTime: tenji, // 例: "6.72"
      tilt,             // 例: "-0.5"
      st,               // 例: ".19" or "F.02"
      stFlag: /^F/i.test(st) ? 'F' : '' // 任意: フライング判定
    });
  });

  const out = {
    date,
    pid,
    race: `${rno}R`,
    source: url,
    mode: 'beforeinfo',
    generatedAt: new Date().toISOString(),
    entries
  };

  const outDir = path.join(__dirname, '..', 'data', 'beforeinfo', date, pid);
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, `${rno}R.json`);
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2), 'utf8');
  console.log(outPath);
})();
