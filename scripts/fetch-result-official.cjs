#!/usr/bin/env node
/**
 * BOATRACE 公式 結果スクレイパ
 * usage: node scripts/fetch-result-official.js YYYYMMDD PID RACE
 *   ex) node scripts/fetch-result-official.js 20250812 01 1
 * 環境変数:
 *   SKIP_EXISTING=true: 既に出力があればスキップ
 */
// --- ESM 環境でも require を使えるように（package.json に "type":"module" がある想定）
import { createRequire } from 'module'; const require = createRequire(import.meta.url);

const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

// -------- utils
const sleep = ms => new Promise(r => setTimeout(r, ms));

function z2(n) { return String(n).padStart(2, '0'); }
function normalizePid(pid) {
  // 01 or 1 -> 01
  const m = String(pid).match(/\d+/);
  if (!m) return String(pid);
  return z2(parseInt(m[0], 10));
}
function normalizeRace(r) {
  // "1", "1R", 1 -> {num:1, label:"1R"}
  const m = String(r).match(/\d+/);
  const num = m ? parseInt(m[0], 10) : NaN;
  return { num, label: Number.isFinite(num) ? `${num}R` : String(r) };
}
function clean(t) {
  if (t == null) return '';
  return String(t)
    .replace(/\u00a0/g, ' ')
    .replace(/undefined/g, '') // 画面キャプチャ由来のノイズ対策
    .replace(/\s+/g, ' ')
    .trim();
}
function yenToNumber(t) {
  const s = clean(t).replace(/[¥,\s]/g, '');
  const n = parseInt(s, 10);
  return Number.isFinite(n) ? n : null;
}
function timeNormalize(t) {
  // 1'51undefined1 -> 1'51"1, 1'51 1 -> 1'51"1
  const s = clean(t).replace(/['’]\s*(\d{2})\s*(\d)/, (_,$1,$2)=>`'${$1}"${$2}`)
                    .replace(/(\d)undefined(\d)/g, '$1"$2');
  if (s.includes('"')) return s;
  return s.replace(/(\d)$/, '"$1');
}
function numberArrayFromCell($cell) {
  // 3連単/2連単などの組番が span.numberSet1 に分割されているケースをテキストから素直に読む
  const raw = clean($cell.text());
  // ex: "4 - 6 - 2" | "2 = 4 = 6" | "4" など
  const nums = raw.split(/[^0-9]+/).filter(Boolean);
  return nums.map(n => String(parseInt(n,10)));
}
function popularityFromCell($cell) {
  const n = parseInt(clean($cell.text()), 10);
  return Number.isFinite(n) ? n : null;
}

// -------- main
(async function main() {
  const [,, DATE, PID_IN, RACE_IN] = process.argv;
  if (!DATE || !PID_IN || !RACE_IN) {
    console.error('usage: node scripts/fetch-result-official.js YYYYMMDD PID RACE');
    process.exit(1);
  }
  const pid = normalizePid(PID_IN);
  const race = normalizeRace(RACE_IN);

  const url = `https://www.boatrace.jp/owpc/pc/race/raceresult?rno=${race.num}&jcd=${pid}&hd=${DATE}`;

  const outDir = path.join('public', 'results', 'v1', DATE, pid);
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
  const $ = cheerio.load(html);

  // --- 着順テーブル（「着」ヘッダのあるテーブル）
  const $finishTable = $('table:has(th:contains("着"))').first();
  const order = [];
  $finishTable.find('tbody').each((_, tb) => {
    const tds = $(tb).find('td');
    if (tds.length < 4) return;
    const pos = parseInt(clean($(tds[0]).text()), 10);
    const lane = parseInt(clean($(tds[1]).text()), 10);
    const $info = $(tds[2]);
    const idMatch = clean($info.find('span').first().text()).match(/\d{4}/);
    const racerId = idMatch ? idMatch[0] : null;
    // 名前は最後の太字spanが安定
    const name = clean($info.find('span').last().text()) || clean($info.text()).replace(/\d{4}/, '').trim();
    const time = timeNormalize($(tds[3]).text());
    if (Number.isFinite(pos) && Number.isFinite(lane)) {
      order.push({ pos, lane, racerId, name, time });
    }
  });

  // --- スタート情報
  const start = [];
  let startRemark = null;
  const $startTable = $('table:has(th:contains("スタート情報"))').first();
  $startTable.find('.table1_boatImage1').each((_, el) => {
    const lane = parseInt(clean($(el).find('.table1_boatImage1Number').text()), 10);
    const t = clean($(el).find('.table1_boatImage1TimeInner').text());
    // 例: ".03   まくり" / ".11"
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
    airTemp: parseFloat(clean(weatherBox.find('.is-direction .weather1_bodyUnitLabelData').text()).replace(/[^\d.\-]/g,'')),
    windSpeed: parseFloat(clean(weatherBox.find('.is-wind .weather1_bodyUnitLabelData').text()).replace(/[^\d.\-]/g,'')),
    windDirCode: (function(){
      // 風向は class 名 is-windN から N を取る
      const cls = weatherBox.find('.is-windDirection .weather1_bodyUnitImage').attr('class') || '';
      const m = cls.match(/is-wind(\d+)/);
      return m ? parseInt(m[1],10) : null;
    })(),
    waterTemp: parseFloat(clean(weatherBox.find('.is-waterTemperature .weather1_bodyUnitLabelData').text()).replace(/[^\d.\-]/g,'')),
    wave: parseFloat(clean(weatherBox.find('.is-wave .weather1_bodyUnitLabelData').text()).replace(/[^\d.\-]/g,'')),
  };

  // --- 返還
  let refunds = [];
  const refundText = clean($('table:has(th:contains("返還")) .numberSet1').text());
  if (refundText) {
    const nums = refundText.split(/[^0-9]+/).filter(Boolean);
    refunds = nums.map(n => parseInt(n,10)).filter(Number.isFinite);
  }

  // --- 払戻「勝式」テーブル
  const $payTable = $('table:has(th:contains("勝式"))').first();
  const payouts = { trifecta:null, trio:null, exacta:null, quinella:null, wide:[], win:null, place:[] };

  // 各 tbody が 3連単/3連複/2連単/2連複/拡連複/単勝/複勝 の順で並ぶ前提（公式UI準拠）
  const tbodies = $payTable.find('tbody').toArray();

  function readLine($row) {
    const tds = $row.find('td');
    return {
      combo: numberArrayFromCell($(tds[1])),
      amount: yenToNumber($(tds[2]).text()),
      popularity: popularityFromCell($(tds[3]))
    };
  }

  // 3連単
  if (tbodies[0]) {
    const $rows = $(tbodies[0]).find('tr');
    const a = readLine($rows.eq(0));
    if (a.combo.length === 3) {
      payouts.trifecta = { combo: a.combo.join('-'), amount: a.amount, popularity: a.popularity };
    }
  }
  // 3連複
  if (tbodies[1]) {
    const $rows = $(tbodies[1]).find('tr');
    const a = readLine($rows.eq(0));
    if (a.combo.length === 3) {
      payouts.trio = { combo: a.combo.sort((x,y)=>x-y).join('='), amount: a.amount, popularity: a.popularity };
    }
  }
  // 2連単
  if (tbodies[2]) {
    const a = readLine($(tbodies[2]).find('tr').eq(0));
    if (a.combo.length === 2) {
      payouts.exacta = { combo: a.combo.join('-'), amount: a.amount, popularity: a.popularity };
    }
  }
  // 2連複
  if (tbodies[3]) {
    const a = readLine($(tbodies[3]).find('tr').eq(0));
    if (a.combo.length === 2) {
      payouts.quinella = { combo: a.combo.sort((x,y)=>x-y).join('='), amount: a.amount, popularity: a.popularity };
    }
  }
  // 拡連複（最大3行想定）
  if (tbodies[4]) {
    $(tbodies[4]).find('tr').each((_, tr) => {
      const a = readLine($(tr));
      if (a && a.combo && a.combo.length === 2 && a.amount) {
        payouts.wide.push({ combo: a.combo.sort((x,y)=>x-y).join('='), amount: a.amount, popularity: a.popularity });
      }
    });
  }
  // 単勝
  if (tbodies[5]) {
    const a = readLine($(tbodies[5]).find('tr').eq(0));
    if (a.combo.length >= 1) payouts.win = { combo: a.combo[0], amount: a.amount };
  }
  // 複勝（2行想定）
  if (tbodies[6]) {
    $(tbodies[6]).find('tr').each((_, tr) => {
      const a = readLine($(tr));
      if (a.combo.length >= 1 && a.amount) payouts.place.push({ combo: a.combo[0], amount: a.amount });
    });
  }

  const data = {
    date: DATE,
    pid,
    race: race.label,
    source: { result: url },
    generatedAt: new Date().toISOString(),
    order,          // 着順一覧
    start,          // スタート情報（lane, st）
    decision,       // 決まり手
    weather,        // 水面気象
    refunds,        // 返還枠番配列（空配列＝なし）
    payouts         // 払戻まとめ
  };

  // 出力
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(outFile, JSON.stringify(data, null, 2));
  console.log(`[ok] ${outFile}`);
})();