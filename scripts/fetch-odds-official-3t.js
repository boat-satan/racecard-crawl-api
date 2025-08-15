// scripts/fetch-odds-official-3t.js
// 公式サイトの 3連単オッズをスクレイピングして保存（堅牢化版）
// 出力: public/odds/v1/<date>/<pid>/<race>.json
//
// 使い方:
//   node scripts/fetch-odds-official-3t.js <YYYYMMDD> <pid:01..24> <race:1..12>
//   環境変数: TARGET_DATE / TARGET_PID / TARGET_RACE / SKIP_EXISTING=1
//
// 例:
//   node scripts/fetch-odds-official-3t.js 20250812 01 1
//
// 仕組み（前提）:
//   テーブルは 4行×列バンドルで、各バンドルの先頭セル(rowspan=4)が F(1着)。
//   バンドル内の4つの “小さな数字セル” は T(3着)。{1..6} から F と 4つのTを除いた残り1つが S(2着)。
//   （公式の基本レイアウトがこれ。変化形でも近似判定・防御的スキップで耐える）
//
// 強化点:
//  - 取得リトライ（指数バックオフ/タイムアウト）
//  - findOddsTable の検出強化・冗長チェック（行数÷4, rowspan の存在）
//  - 余分セル・カンマ入り数値・欠損・「-」「欠場」等の混入に耐える toNum
//  - 列ズレ耐性（row0で列バンドル数を確定→後続3行はその2セル×バンドル分だけを読み取り）
//  - 解析件数の健全性チェック&警告（P(6,3)=120 が理想。閾値未満でも保存はする）
//  - 既存ファイルスキップ・ディレクトリ作成の冪等化
//  - 出力 JSON に popularityRank を付加（オッズ昇順）
//

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { load as loadHTML } from "cheerio";

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const log  = (...a)=>console.log("[odds3t]", ...a);
const warn = (...a)=>console.warn("[odds3t][warn]", ...a);
const err  = (...a)=>console.error("[odds3t][error]", ...a);

const DATE = (process.env.TARGET_DATE || process.argv[2] || "").replace(/-/g,"");
const PID  = (process.env.TARGET_PID  || process.argv[3] || "").padStart(2,"0");
const RACE = String(process.env.TARGET_RACE || process.argv[4] || "").replace(/[^0-9]/g,"");
const SKIP_EXISTING = /^(1|true|yes)$/i.test(String(process.env.SKIP_EXISTING||""));

if (!/^\d{8}$/.test(DATE) || !/^\d{2}$/.test(PID) || !/^(?:[1-9]|1[0-2])$/.test(RACE)) {
  err("Usage: node scripts/fetch-odds-official-3t.js <YYYYMMDD> <pid:01..24> <race:1..12>");
  process.exit(1);
}

function officialOdds3tUrl({date, pid, race}) {
  // 公式: /owpc/pc/race/odds3t?rno=<race>&jcd=<pid>&hd=<date>
  return `https://www.boatrace.jp/owpc/pc/race/odds3t?rno=${race}&jcd=${pid}&hd=${date}`;
}

// ---------- HTTP(fetch) with retry & timeout ----------
async function fetchWithRetry(url, {tries=3, timeoutMs=12000} = {}) {
  let attempt = 0;
  let lastErr;
  while (attempt < tries) {
    attempt++;
    try {
      const ac = new AbortController();
      const t  = setTimeout(()=>ac.abort(new Error("fetch timeout")), timeoutMs);
      const res = await fetch(url, {
        signal: ac.signal,
        headers: {
          // user-agent / accept-language を明示
          "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
          "accept-language": "ja,en;q=0.8",
          "cache-control": "no-cache",
        }
      });
      clearTimeout(t);
      if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
      return await res.text();
    } catch (e) {
      lastErr = e;
      const backoff = 500 * Math.pow(2, attempt-1); // 0.5s, 1s, 2s...
      warn(`fetch fail (attempt ${attempt}/${tries}): ${e?.message || e}. retry in ${backoff}ms`);
      if (attempt < tries) await new Promise(r=>setTimeout(r, backoff));
    }
  }
  throw lastErr || new Error("fetch failed");
}

const norm = (s)=>String(s||"").replace(/\s+/g," ").trim();
const toNum = (s)=> {
  // カンマ・全角/ハイフン・「-」「--」「欠場」「F」等は NaN
  const txt = String(s||"")
    .replace(/[,\u2212]/g,"") // カンマ/全角マイナス
    .replace(/[^\d.\-]/g, (m)=> (m==="-") ? m : " "); // 数字/ピリオド/ハイフン以外を空白へ
  const m = txt.match(/-?\d+(?:\.\d+)?/);
  return m ? Number(m[0]) : NaN;
};

// ---------- odds table finder (robust) ----------
function findOddsTable($) {
  let table = null;

  // 1) 「3連単オッズ」見出しの近傍優先
  $("*").each((_, el) => {
    const t = norm($(el).text());
    if (/3\s*連\s*単\s*オッズ/.test(t)) {
      const near = $(el).nextAll("div.table1, div.is-table, table").first();
      if (near && near.length) {
        table = near.is("table") ? near : near.find("table").first();
        if (table && table.length) return false;
      }
    }
    return;
  });

  // 2) フォールバック: 全 table を走査
  if (!table) {
    $("table").each((_, t) => {
      const $t = $(t);
      const head = norm($t.find("thead").text());
      const body = norm($t.find("tbody").text());
      const rows = $t.find("tbody tr").length;
      // ヘッダに 1..6 など、tbody が数値だらけ、rows が 4 の倍数 などをヒントに
      if (/[123456]/.test(head) && rows > 0 && rows % 4 === 0 && /\d/.test(body)) {
        table = $t; return false;
      }
    });
  }

  if (!table || !table.length) return null;

  // 行数/rowspan の簡易妥当性チェック（失敗しても即捨てず、パース側で更に検証）
  const rows = table.find("tbody tr").length;
  if (rows % 4 !== 0) warn("tbody rows not multiple of 4:", rows);

  const hasRowspan4 = table.find("tbody tr th[rowspan='4'], tbody tr td[rowspan='4']").length > 0;
  if (!hasRowspan4) warn("no explicit rowspan=4 detected (layout variant?)");

  return table;
}

// ---------- parse (F,S,T,odds) ----------
function parseTrifecta($, $table) {
  const $tbody = $table.find("tbody").first();
  const trs = $tbody.find("tr").toArray();
  const all = [];

  for (let base = 0; base < trs.length; base += 4) {
    const r0 = $(trs[base]);
    const r1 = $(trs[base+1]);
    const r2 = $(trs[base+2]);
    const r3 = $(trs[base+3]);
    if (!r0 || !r1 || !r2 || !r3) break;

    // --- row0: 列バンドル（F + T,odds の1組）を検出 ---
    const cells0 = r0.find("th,td").toArray();
    const bundles = []; // [{F, values:[...], odds:[...]}]
    let k0 = 0;
    while (k0 < cells0.length) {
      const $c = $(cells0[k0]);
      let F = NaN;

      // rowspan=4 っぽいセルで F を拾う（属性が無くても、数字だけのセルはF候補）
      const rowspan = String($c.attr("rowspan")||"").trim();
      const fCandidate = toNum($c.text());
      const looksF = (rowspan === "4") && Number.isFinite(fCandidate);

      if (looksF) {
        F = fCandidate;
        k0++;
      } else {
        // 安全側: このセルは見出し/ダミー扱いでスキップ
        k0++;
        continue;
      }

      // 直後に (T,odds) が1組あるはず
      const tCell = cells0[k0++], oCell = cells0[k0++] || null;
      if (!tCell || !oCell) break;
      const v0 = toNum($(tCell).text());
      const o0 = toNum($(oCell).text());

      if (Number.isFinite(F) && Number.isFinite(v0) && Number.isFinite(o0)) {
        bundles.push({ F, values:[v0], odds:[o0] });
      }
    }

    if (bundles.length === 0) {
      // この4行ブロックはスキップ（変則や広告行など）
      continue;
    }

    // --- row1..row3: 同列順で (T,odds) を追加 ---
    const laterRows = [r1, r2, r3];
    for (const rr of laterRows) {
      const cells = rr.find("th,td").toArray();
      // 余分な先頭見出しセル等があっても、最初から 2*bundles.length セルだけ読む
      let k = 0;
      for (let b = 0; b < bundles.length; b++) {
        const tCell = cells[k++], oCell = cells[k++] || null;
        if (!tCell || !oCell) { warn("row short; skip this 4-row block"); k = -1; break; }
        bundles[b].values.push(toNum($(tCell).text()));
        bundles[b].odds.push(toNum($(oCell).text()));
      }
      if (k < 0) { // 途中破綻 → この4行ブロック全体を捨てる
        bundles.length = 0;
        break;
      }
    }
    if (bundles.length === 0) continue;

    // --- 各バンドルを (F,S,T,odds) に展開 ---
    for (const b of bundles) {
      const F = b.F;
      const vs = b.values.filter(Number.isFinite); // T候補（数値のみ）
      const os = b.odds;

      // 3着候補は最大4つ想定。それ未満なら残存行が欠けた可能性→スキップ
      if (vs.length < 2 || os.length !== b.values.length) {
        warn("bundle incomplete; skip", {F, len:vs.length});
        continue;
      }

      // S は {1..6} − {F} − {vs} の残り1つ
      const thirdSet = new Set(vs);
      const remain = [1,2,3,4,5,6].filter(n => n !== F && !thirdSet.has(n));
      if (remain.length !== 1) {
        // 公式変化形で vs 内に変な値が混入した等。防御的にスキップ
        warn("S resolve failed; skip bundle", {F, vs: [...thirdSet]});
        continue;
      }
      const S = remain[0];

      for (let j = 0; j < vs.length; j++) {
        const T = vs[j];
        const odds = os[j];
        if (Number.isFinite(T) && Number.isFinite(odds)) {
          all.push({ combo: `${F}-${S}-${T}`, F, S, T, odds });
        }
      }
    }
  }

  // 重複（同一 combo が複数回現れた）を後勝ち or 先勝ちで解消（ここでは先勝ち）
  const map = new Map();
  for (const e of all) {
    if (!map.has(e.combo)) map.set(e.combo, e);
  }
  const list = [...map.values()].filter(e => Number.isFinite(e.odds));

  // オッズ昇順（人気順相当）＋ popularityRank 付与
  list.sort((a,b)=> a.odds - b.odds);
  list.forEach((e, i)=> e.popularityRank = i+1);

  return list;
}

async function main() {
  const url = officialOdds3tUrl({ date: DATE, pid: PID, race: RACE });
  const outPath = path.join(__dirname, "..", "public", "odds", "v1", DATE, PID, `${RACE}R.json`);

  if (SKIP_EXISTING && fs.existsSync(outPath)) {
    log("skip existing:", path.relative(process.cwd(), outPath));
    return;
    }

  log("GET", url);
  const html = await fetchWithRetry(url, {tries: 3, timeoutMs: 12000});
  const $ = loadHTML(html, { xmlMode: false, decodeEntities: true });

  const $table = findOddsTable($);
  if (!$table || !$table.length) {
    throw new Error("odds table not found (layout changed?)");
  }

  const trifecta = parseTrifecta($, $table);
  if (trifecta.length === 0) {
    throw new Error("no trifecta odds parsed (parser mismatch?)");
  }

  // 健全性チェック（理想は P(6,3)=120）
  if (trifecta.length < 40) { // かなり少ない場合は警告
    warn(`parsed combos suspiciously small: ${trifecta.length}`);
  }

  const payload = {
    date: DATE, pid: PID, race: `${RACE}R`,
    source: { odds: url },
    generatedAt: new Date().toISOString(),
    trifecta // [{combo:"F-S-T", F,S,T, odds, popularityRank}, ...]  ※オッズ昇順
  };

  // ディレクトリ作成（冪等）
  for (const dir of [
    path.join(__dirname, "..", "public"),
    path.join(__dirname, "..", "public", "odds"),
    path.join(__dirname, "..", "public", "odds", "v1"),
    path.join(__dirname, "..", "public", "odds", "v1", DATE),
    path.join(__dirname, "..", "public", "odds", "v1", DATE, PID),
  ]) {
    try {
      await fsp.mkdir(dir, { recursive: true });
      // .keep は任意：存在してもOK／作成失敗は無視
      try { await fsp.writeFile(path.join(dir, ".keep"), ""); } catch {}
    } catch {}
  }

  await fsp.writeFile(outPath, JSON.stringify(payload, null, 2), "utf8");
  log("saved:", path.relative(process.cwd(), outPath), "items:", trifecta.length);
}

main().catch(e => { err(e?.stack || e?.message || String(e)); process.exit(1); });
