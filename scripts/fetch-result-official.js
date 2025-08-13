#!/usr/bin/env node
/**
 * BOATRACE 公式 結果スクレイパ (ESM)
 * usage: node scripts/fetch-result-official.js YYYYMMDD PID RACE
 *   ex) node scripts/fetch-result-official.js 20250812 01 1
 * env:
 *   SKIP_EXISTING=true  …… 既存JSONがあればスキップ
 */
import fs from 'node:fs';
import path from 'node:path';
import { load } from 'cheerio';

// -------- utils
const sleep = ms => new Promise(r => setTimeout(r, ms));
const z2 = n => String(n).padStart(2, '0');

const toHalfDigits = (s='') =>
  String(s).replace(/[０-９]/g, ch => String.fromCharCode(ch.charCodeAt(0) - 0xFEE0));

function normalizePid(pid) {
  const m = String(pid).match(/\d+/);
  return m ? z2(parseInt(m[0], 10)) : String(pid);
}
function normalizeRace(r) {
  const m = String(r).match(/\d+/);
  const num = m ? parseInt(m[0], 10) : NaN;
  return { num, label: Number.isFinite(num) ? `${num}R` : String(r) };
}
function clean(t) {
  if (t == null) return '';
  return String(t).replace(/\u00a0/g, ' ')
                  .replace(/undefined/g, '')
                  .replace(/\s+/g, ' ')
                  .trim();
}
function yenToNumber(t) {
  const s = clean(t).replace(/[¥,\s]/g, '');
  const n = parseInt(s, 10);
  return Number.isFinite(n) ? n : null;
}
function timeNormalize(t) {
  const s = clean(t)
    .replace(/['’]\s*(\d{2})\s*(\d)/, (_,$1,$2)=>`'${$1}"${$2}`)
    .replace(/(\d)undefined(\d)/g, '$1"$2');
  return s.includes('"') ? s : s.replace(/(\d)$/, '"$1`);
}
function numberArrayFromCell($cell) {
  const raw = clean($cell.text());
  return toHalfDigits(raw).split(/[^0-9]+/).filter(Boolean).map(n => String(parseInt(n,10)));
}
function popularityFromCell($cell) {
  const n = parseInt(toHalfDigits(clean($cell.text())), 10);
  return Number.isFinite(n) ? n : null;
}

// -------- main
(async function main() {
  const [,, DATE, PID_IN, RACE_IN] = process.argv;
  if (!DATE || !PID_IN || !RACE_IN) {
    console.error('usage: node scripts/fetch-result-official.js YYYYMMDD PID RACE');
    process.exit(1);
  }

  const pid  = normalizePid(PID_IN);
  const race = normalizeRace(RACE_IN);
  const url  = `https://www.boatrace.jp/owpc/pc/race/raceresult?rno=${race.num}&jcd=${pid}&hd=${DATE}`;

  const outDir  = path.join('public', 'results', 'v1', DATE, pid);
  const outFile = path.join(outDir, `${race.label}.json`);

  if (process.env.SKIP_EXISTING === 'true' && fs.existsSync(outFile)) {
    console.log(`[skip] exists: ${outFile}`);
    process.exit(0);
  }

  console.log(`[fetch] ${url}`);
  const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (compatible; OddsBot/1.0)' }});
  if (!res.ok) {
    console.error(`[error] HTTP ${res.status}`);
    process.exit(2);
  }
  const html = await res.text();
  const $ = load(html);

  // --- 着順（全角数字対応）
  const order = [];
  let $finishTable = $('table:has(th:contains("レースタイム")):has(th:contains("ボートレーサー"))').first();
  if ($finishTable.length === 0) $finishTable = $('table:has(th:contains("着"))').first();

  $finishTable.find('tr').each((_, tr) => {
    const $tds = $(tr).find('td');
    if ($tds.length < 4) return;

    const pos  = parseInt(toHalfDigits(clean($tds.eq(0).text())), 10);
    const lane = parseInt(toHalfDigits(clean($tds.eq(1).text())), 10);

    const $info = $tds.eq(2);
    const infoText = toHalfDigits(clean($info.text()));
    const idMatch = infoText.match(/\b\d{4}\b/);
    const racerId = idMatch ? idMatch[0] : null;

    const name =
      clean($info.find('span').last().text()) ||
      infoText.replace(/\b\d{4}\b/, '').trim();

    const time = timeNormalize($tds.eq(3).text());

    if (Number.isFinite(pos) && Number.isFinite(lane)) {
      order.push({ pos, lane, racerId, name, time });
    }
  });

  // --- スタート情報
  const start = [];
  let startRemark = null;
  $('table:has(th:contains("スタート情報"))').first()
    .find('.table1_boatImage1').each((_, el) => {
      const lane = parseInt(toHalfDigits(clean($(el).find('.table1_boatImage1Number').text())), 10);
      const t = clean($(el).find('.table1_boatImage1TimeInner').text());
      const m = t.match(/([\-+.0-9]+)/);
      const st = m ? parseFloat(m[1]) : null;
      const remark = t.replace(m ? m[0] : '', '').trim();
      if (lane) start.push({ lane, st });
      if (remark && !startRemark) startRemark = remark;
    });

  // --- 決まり手
  const decision = clean($('table:has(th:contains("決まり手")) td').first().text()) || startRemark || null;

  // --- 水面気象
  const weatherBox = $('div.weather1');
  const weather = {
    condition: clean(weatherBox.find('.is-weather .weather1_bodyUnitLabel').text()) || null,
    airTemp:   parseFloat(clean(weatherBox.find('.is-direction .weather1_bodyUnitLabelData').text()).replace(/[^\d.\-]/g,'')),
    windSpeed: parseFloat(clean(weatherBox.find('.is-wind .weather1_bodyUnitLabelData').text()).replace(/[^\d.\-]/g,'')),
    windDirCode: (() => {
      const cls = weatherBox.find('.is-windDirection .weather1_bodyUnitImage').attr('class') || '';
      const m = cls.match(/is-wind(\d+)/);
      return m ? parseInt(m[1],10) : null;
    })(),
    waterTemp: parseFloat(clean(weatherBox.find('.is-waterTemperature .weather1_bodyUnitLabelData').text()).replace(/[^\d.\-]/g,'')),
    wave:      parseFloat(clean(weatherBox.find('.is-wave .weather1_bodyUnitLabelData').text()).replace(/[^\d.\-]/g,'')),
  };

  // --- 返還
  let refunds = [];
  const refundText = clean($('table:has(th:contains("返還")) .numberSet1').text());
  if (refundText) refunds = refundText.split(/[^0-9]+/).filter(Boolean).map(n => parseInt(n,10)).filter(Number.isFinite);

  // --- 払戻（勝式）
  const $payTable = $('table:has(th:contains("勝式"))').first();
  const payouts = { trifecta:null, trio:null, exacta:null, quinella:null, wide:[], win:null, place:[] };

  const tbodies = $payTable.find('tbody').toArray();
  const readLine = $row => {
    const tds = $row.find('td');
    return {
      combo: numberArrayFromCell($(tds[1])),
      amount: yenToNumber($(tds[2]).text()),
      popularity: popularityFromCell($(tds[3]))
    };
  };

  if (tbodies[0]) {
    const a = readLine($(tbodies[0]).find('tr').eq(0));
    if (a.combo.length === 3) payouts.trifecta = { combo: a.combo.join('-'), amount: a.amount, popularity: a.popularity };
  }
  if (tbodies[1]) {
    const a = readLine($(tbodies[1]).find('tr').eq(0));
    if (a.combo.length === 3) payouts.trio = { combo: a.combo.sort((x,y)=>x-y).join('='), amount: a.amount, popularity: a.popularity };
  }
  if (tbodies[2]) {
    const a = readLine($(tbodies[2]).find('tr').eq(0));
    if (a.combo.length === 2) payouts.exacta = { combo: a.combo.join('-'), amount: a.amount, popularity: a.popularity };
  }
  if (tbodies[3]) {
    const a = readLine($(tbodies[3]).find('tr').eq(0));
    if (a.combo.length === 2) payouts.quinella = { combo: a.combo.sort((x,y)=>x-y).join('='), amount: a.amount, popularity: a.popularity };
  }
  if (tbodies[4]) {
    $(tbodies[4]).find('tr').each((_, tr) => {
      const a = readLine($(tr));
      if (a.combo.length === 2 && a.amount) payouts.wide.push({ combo: a.combo.sort((x,y)=>x-y).join('='), amount: a.amount, popularity: a.popularity });
    });
  }
  if (tbodies[5]) {
    const a = readLine($(tbodies[5]).find('tr').eq(0));
    if (a.combo.length >= 1) payouts.win = { combo: a.combo[0], amount: a.amount };
  }
  if (tbodies[6]) {
    $(tbodies[6]).find('tr').each((_, tr) => {
      const a = readLine($(tr));
      if (a.combo.length >= 1 && a.amount) payouts.place.push({ combo: a.combo[0], amount: a.amount });
    });
  }

  const data = {
    date: DATE, pid, race: race.label,
    source: { result: url },
    generatedAt: new Date().toISOString(),
    order, start, decision, weather, refunds, payouts
  };

  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(outFile, JSON.stringify(data, null, 2));
  console.log(`[ok] ${outFile}`);
})();