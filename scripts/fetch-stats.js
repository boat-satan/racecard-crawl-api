// scripts/fetch-stats.js
// Node v20 / ESM
// 出力: public/stats/v2/racers/<regno>.json
// 参照:
//   - rcourse 各コース (= 自艇nコース進入時): https://boatrace-db.net/racer/rcourse/regno/<regno>/course/<n>/
//   - rdemo   展示順位:                         https://boatrace-db.net/racer/rdemo/regno/<regno>/

import { load } from "cheerio";
import fs from "node:fs/promises";
import fssync from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

// -------------------------------
// 定数
// -------------------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PUBLIC_DIR = path.resolve(__dirname, "..", "public");
const TODAY_ROOTS = [
  path.join(PUBLIC_DIR, "programs", "v2", "today"),
  path.join(PUBLIC_DIR, "programs-slim", "v2", "today"),
];
const OUTPUT_DIR_V2 = path.join(PUBLIC_DIR, "stats", "v2", "racers");

// polite wait
const WAIT_MS_BETWEEN_RACERS       = Number(process.env.STATS_DELAY_MS || 3000); // 1選手ごと
const WAIT_MS_BETWEEN_COURSE_PAGES = Number(process.env.COURSE_WAIT_MS || 3000); // 各コースページ間

// env
const ENV_RACERS       = process.env.RACERS?.trim() || "";
const ENV_RACERS_LIMIT = Number(process.env.RACERS_LIMIT ?? "");
const ENV_BATCH        = Number(process.env.STATS_BATCH ?? "");
const FRESH_HOURS      = Number(process.env.FRESH_HOURS || 12); // 既存がこの時間以内ならスキップ

// -------------------------------
// ユーティリティ
// -------------------------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * fetchHtml: UA/Referer 付き + 簡易リトライ
 * - エラー系は 1 回だけリトライ（合計 2 トライ）
 */
async function fetchHtml(url, {
  retries    = 1,
  baseDelayMs= 2500,
  timeoutMs  = 20000,
} = {}) {
  const makeAC = () => new AbortController();

  for (let attempt = 0; attempt <= retries; attempt++) {
    const ac = makeAC();
    const timer = setTimeout(() => ac.abort(), timeoutMs);
    try {
      const res = await fetch(url, {
        signal: ac.signal,
        headers: {
          "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127 Safari/537.36",
          "accept": "text/html,application/xhtml+xml",
          "accept-language": "ja,en;q=0.9",
          "referer": "https://boatrace-db.net/",
          "cache-control": "no-cache",
        },
      });
      if (res.ok) {
        clearTimeout(timer);
        return await res.text();
      }
      const retriable = [401,403,404,429,500,502,503,504].includes(res.status);
      if (!retriable || attempt === retries) {
        const body = await res.text().catch(() => "");
        clearTimeout(timer);
        throw new Error(`HTTP ${res.status} ${res.statusText} @ ${url} ${body?.slice(0,120)}`);
      }
      const factor = [403,404,429,503].includes(res.status) ? 2.0 : 1.4;
      const delay  = Math.round((baseDelayMs * Math.pow(factor, attempt)) * (0.8 + Math.random()*0.4));
      clearTimeout(timer);
      await sleep(delay);
    } catch (err) {
      clearTimeout(timer);
      if (attempt === retries) throw new Error(`GET failed after ${retries+1} tries: ${url} :: ${err.message}`);
      const delay = Math.round((baseDelayMs * Math.pow(1.6, attempt)) * (0.8 + Math.random()*0.4));
      await sleep(delay);
    }
  }
  throw new Error(`unreachable fetch loop for ${url}`);
}

function normText(t) {
  return (t ?? "").replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
}
function toNumber(v) {
  if (v == null) return null;
  const s = String(v).replace(/[,%]/g, "");
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}
function parseTable($, $tbl) {
  const headers = [];
  $tbl.find("thead th, thead td").each((_, th) => headers.push(normText($(th).text())));
  if (headers.length === 0) {
    const firstRow = $tbl.find("tr").first();
    firstRow.find("th,td").each((_, th) => headers.push(normText($(th).text())));
  }
  const rows = [];
  $tbl.find("tbody tr").each((_, tr) => {
    const cells = [];
    $(tr).find("th,td").each((_, td) => cells.push(normText($(td).text())));
    if (cells.length) rows.push(cells);
  });
  return { headers, rows };
}
function headerIndex(headers, keyLike) {
  return headers.findIndex((h) => h.includes(keyLike));
}
function mustTableByHeader($, keyLikes) {
  const candidates = $("table");
  for (const el of candidates.toArray()) {
    const { headers } = parseTable($, $(el));
    const ok = keyLikes.every((k) => headers.some((h) => h.includes(k)));
    if (ok) return $(el);
  }
  return null;
}
function normalizeKimariteKey(k) {
  return k.replace("ま差し","まくり差し").replace("捲り差し","まくり差し").replace("捲り","まくり");
}

// -------------------------------
// 各コースページのパーサ
// -------------------------------
function parseAvgSTFromCoursePage($) {
  const $tbl = mustTableByHeader($, ["月日","場","レース","ST","結果"]);
  if (!$tbl) return null;
  const { headers, rows } = parseTable($, $tbl);
  const iST = headerIndex(headers, "ST");
  if (iST < 0) return null;
  let sum = 0, cnt = 0;
  for (const r of rows) {
    const st = r[iST];
    if (!st) continue;
    if (/^[FL]/i.test(st)) continue;
    const m = st.match(/-?\.?\d+(?:\.\d+)?/);
    if (!m) continue;
    const n = Number(m[0]);
    if (Number.isFinite(n)) { sum += Math.abs(n); cnt++; }
  }
  if (!cnt) return null;
  return Math.round((sum/cnt)*100)/100;
}

function parseLoseKimariteFromCoursePage($) {
  const $tbl = mustTableByHeader($, ["コース","出走数","1着数","逃げ","差し","まくり"]);
  if (!$tbl) return null;
  const { headers, rows } = parseTable($, $tbl);
  const iCourse = headerIndex(headers, "コース");
  const keys = headers.slice(3).map(normalizeKimariteKey);
  const lose = Object.fromEntries(keys.map(k => [k, 0]));
  for (const r of rows) {
    const label = r[iCourse] || "";
    if (label.includes("（自艇）")) continue; // 他艇のみ加算
    keys.forEach((k, i) => {
      const v = r[3+i];
      const num = v ? Number((v.match(/(\d+)/) || [])[1]) : NaN;
      if (Number.isFinite(num)) lose[k] += num;
    });
  }
  return lose;
}

// nコース進入時の全艇成績（自艇/他艇の成績行）
function parseEntryMatrixFromCoursePage($) {
  const $tbl = mustTableByHeader($, ["コース","出走数","1着数","2着数","3着数","1着率","2連対率","3連対率"]);
  if (!$tbl) return null;
  const { headers, rows } = parseTable($, $tbl);
  const idx = {
    course: headerIndex(headers, "コース"),
    starts: headerIndex(headers, "出走数"),
    w1    : headerIndex(headers, "1着数"),
    w2    : headerIndex(headers, "2着数"),
    w3    : headerIndex(headers, "3着数"),
    r1    : headerIndex(headers, "1着率"),
    r2    : headerIndex(headers, "2連対率"),
    r3    : headerIndex(headers, "3連対率"),
  };
  const result = { rows: [], self: null };
  for (const r of rows) {
    const label = r[idx.course] || "";
    const m = label.match(/([1-6])\s*コース/);
    if (!m) continue;
    const course = Number(m[1]);
    const isSelf = label.includes("（自艇）");
    const row = {
      course,
      isSelf,
      starts     : toNumber(r[idx.starts]),
      firstCount : toNumber(r[idx.w1]),
      secondCount: toNumber(r[idx.w2]),
      thirdCount : toNumber(r[idx.w3]),
      winRate    : toNumber(r[idx.r1]),
      top2Rate   : toNumber(r[idx.r2]),
      top3Rate   : toNumber(r[idx.r3]),
      raw: r,
    };
    result.rows.push(row);
    if (isSelf) result.self = row;
  }
  result.rows.sort((a,b)=>a.course-b.course);
  return result;
}

// nコース進入時の「全艇決まり手」テーブル：行ごとに保持（自艇行も含む）
function parseEntryKimariteRows($) {
  const $tbl = mustTableByHeader($, ["コース","出走数","1着数","逃げ","差し","まくり","まくり差し","抜き","恵まれ"]);
  if (!$tbl) return null;
  const { headers, rows } = parseTable($, $tbl);
  const idxCourse = headerIndex(headers, "コース");
  const keys = ["逃げ","差し","まくり","まくり差し","抜き","恵まれ"].map(normalizeKimariteKey);
  const out = [];
  for (const r of rows) {
    const label = r[idxCourse] || "";
    const m = label.match(/([1-6])\s*コース/);
    if (!m) continue;
    const course = Number(m[1]);
    const isSelf = label.includes("（自艇）");
    const detail = {};
    // 列位置は表の並びで探す
    const colIdx = Object.fromEntries(keys.map(k => [k, headerIndex(headers, k)]));
    for (const k of keys) {
      const i = colIdx[k];
      const v = i >= 0 ? r[i] : null;
      const num = v ? Number((v.match(/(\d+)/) || [])[1]) : null;
      detail[k] = Number.isFinite(num) ? num : 0;
    }
    out.push({ course, isSelf, detail, raw: r });
  }
  return out.length ? out : null;
}

// 展示タイム順位別
function parseExTimeRankFromRdemo($) {
  const $tbl = mustTableByHeader($, ["順位","出走数","1着率","2連対率","3連対率"]);
  if (!$tbl) return null;
  const { headers, rows } = parseTable($, $tbl);
  const iRank = headerIndex(headers, "順位");
  const iWin  = headerIndex(headers, "1着率");
  const iT2   = headerIndex(headers, "2連対率");
  const iT3   = headerIndex(headers, "3連対率");
  const items = [];
  for (const r of rows) {
    const rt = r[iRank] ?? r[0] ?? "";
    const m = rt.match(/([1-6])/);
    if (!m) continue;
    items.push({
      rank: Number(m[1]),
      winRate : iWin >= 0 ? toNumber(r[iWin]) : null,
      top2Rate: iT2  >= 0 ? toNumber(r[iT2])  : null,
      top3Rate: iT3  >= 0 ? toNumber(r[iT3])  : null,
      raw: r,
    });
  }
  items.sort((a,b)=>a.rank-b.rank);
  return items.length ? items : null;
}

// today配下から本日の出走選手を収集
async function collectRacersFromToday() {
  const set = new Set();
  for (const root of TODAY_ROOTS) {
    let entries = [];
    try { entries = await fs.readdir(root, { withFileTypes: true }); } catch { continue; }
    for (const d of entries) {
      if (!d.isDirectory()) continue;
      const dayDir = path.join(root, d.name);
      const files = await fs.readdir(dayDir).catch(() => []);
      for (const f of files) {
        if (!f.endsWith(".json")) continue;
        const full = path.join(dayDir, f);
        try {
          const json = JSON.parse(await fs.readFile(full, "utf8"));
          const boats = json?.boats || json?.program?.boats || [];
          for (const b of boats) {
            const r = b.racer_number ?? b.racerNumber ?? b.racer?.number;
            if (r) set.add(String(r));
          }
        } catch {}
      }
    }
  }
  return [...set];
}

async function ensureDir(p) { await fs.mkdir(p, { recursive: true }); }

function isFresh(file, hours = 12) {
  try {
    const st = fssync.statSync(file);
    return (Date.now() - st.mtimeMs) <= hours * 3600 * 1000;
  } catch { return false; }
}

// -------------------------------
// 1選手分
// -------------------------------
async function fetchOne(regno) {
  const uRdemo = `https://boatrace-db.net/racer/rdemo/regno/${regno}/`;

  const entryCourse = [];
  const coursePages = {};

  for (let c = 1; c <= 6; c++) {
    const url = `https://boatrace-db.net/racer/rcourse/regno/${regno}/course/${c}/`;
    coursePages[c] = url;
    try {
      const html = await fetchHtml(url);
      const $ = load(html);

      const avgST         = parseAvgSTFromCoursePage($);
      const loseKimarite  = parseLoseKimariteFromCoursePage($);
      const matrix        = parseEntryMatrixFromCoursePage($);
      const kimariteRows  = parseEntryKimariteRows($);

      // 自艇の勝ち決まり手（行から直接）
      const selfRow = kimariteRows?.find(r => r.isSelf);
      const winKimariteSelf = selfRow ? selfRow.detail : null;

      const selfSummary = matrix?.self ? {
        course     : matrix.self.course,
        starts     : matrix.self.starts,
        firstCount : matrix.self.firstCount,
        secondCount: matrix.self.secondCount,
        thirdCount : matrix.self.thirdCount,
      } : null;

      entryCourse.push({
        course: c,
        matrix: matrix ?? null,
        kimariteAllBoats: kimariteRows ? Object.fromEntries(
          // 全艇合計（必要なら利用、今はそのまま raw 代わりに保持）
          [["rows", kimariteRows]]
        ) : null,
        avgST: avgST ?? null,
        loseKimarite: loseKimarite ?? null,
        winKimariteSelf: winKimariteSelf ?? null,
        selfSummary,
      });

      await sleep(WAIT_MS_BETWEEN_COURSE_PAGES);
    } catch (e) {
      console.warn(`warn: entry-course page failed regno=${regno} course=${c}: ${e.message}`);
      entryCourse.push({
        course: c,
        matrix: null,
        kimariteAllBoats: null,
        avgST: null,
        loseKimarite: null,
        winKimariteSelf: null,
        selfSummary: null,
      });
    }
  }

  // 展示タイム順位別
  let exTimeRank = null;
  try {
    const html2 = await fetchHtml(uRdemo);
    const $2 = load(html2);
    exTimeRank = parseExTimeRankFromRdemo($2);
  } catch (e) {
    console.warn(`warn: rdemo fetch/parse failed for ${regno}: ${e.message}`);
  }

  return {
    schemaVersion: "2.0",
    regno: Number(regno),
    sources: { rdemo: uRdemo, coursePages },
    fetchedAt: new Date().toISOString(),
    entryCourse,   // [{course, matrix, kimariteAllBoats:{rows:[...]}, avgST, loseKimarite, winKimariteSelf, selfSummary}]
    exTimeRank,    // [{rank, winRate, top2Rate, top3Rate}]
    meta: { errors: [] },
  };
}

// -------------------------------
// メイン
// -------------------------------
async function main() {
  let racers = [];
  if (ENV_RACERS) {
    racers = ENV_RACERS.split(",").map((s) => s.trim()).filter(Boolean);
  } else {
    racers = await collectRacersFromToday();
  }
  if (ENV_RACERS_LIMIT && Number.isFinite(ENV_RACERS_LIMIT) && ENV_RACERS_LIMIT > 0) {
    racers = racers.slice(0, ENV_RACERS_LIMIT);
  }
  if (ENV_BATCH && Number.isFinite(ENV_BATCH) && ENV_BATCH > 0) {
    racers = racers.slice(0, ENV_BATCH);
  }

  if (racers.length === 0) {
    console.log("No racers to fetch. (Set RACERS env or put today programs)");
    return;
  }

  console.log(
    `process ${racers.length} racers (incremental, fresh<=${FRESH_HOURS}h)` +
    (ENV_RACERS ? " [env RACERS specified]" : "") +
    (ENV_RACERS_LIMIT ? ` [limit=${ENV_RACERS_LIMIT}]` : "") +
    (ENV_BATCH ? ` [batch=${ENV_BATCH}]` : "")
  );

  await ensureDir(OUTPUT_DIR_V2);

  let ok = 0, ng = 0;
  for (const regno of racers) {
    const outPath = path.join(OUTPUT_DIR_V2, `${regno}.json`);

    if (isFresh(outPath, FRESH_HOURS)) {
      console.log(`⏭️  skip fresh ${path.relative(PUBLIC_DIR, outPath)}`);
      continue;
    }

    try {
      const data = await fetchOne(regno);
      await fs.writeFile(outPath, JSON.stringify(data, null, 2), "utf8");
      console.log(`✅ wrote ${path.relative(PUBLIC_DIR, outPath)}`);
      ok++;
    } catch (e) {
      console.warn(`❌ ${regno}: ${e.message}`);
      ng++;
    }
    await sleep(WAIT_MS_BETWEEN_RACERS);
  }

  await ensureDir(path.join(PUBLIC_DIR, "debug"));
  await fs.writeFile(
    path.join(PUBLIC_DIR, "debug", "stats-meta.json"),
    JSON.stringify(
      {
        status: 200,
        fetchedAt: new Date().toISOString(),
        racers: racers.map((r) => Number(r)),
        success: ok,
        failed: ng,
      },
      null,
      2
    ),
    "utf8"
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
