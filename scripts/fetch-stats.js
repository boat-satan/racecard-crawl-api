// scripts/fetch-stats.js
// Node v20 / ESM / cheerio v1.x
// 出力: public/stats/v1/racers/<regno>.json
// 参照:
//  - https://boatrace-db.net/racer/rcourse/regno/<regno>/           (直近6か月 サマリ: コース別成績/決まり手)
//  - https://boatrace-db.net/racer/rcourse/regno/<regno>/course/N/  (各進入コース詳細 N=1..6: 全艇成績/全艇決まり手/全出走結果)
//  - https://boatrace-db.net/racer/rdemo/regno/<regno>/             (展示タイム順位別)

import { load } from "cheerio";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const PUBLIC_DIR   = path.resolve(__dirname, "..", "public");
const TODAY_ROOTS  = [
  path.join(PUBLIC_DIR, "programs", "v2", "today"),
  path.join(PUBLIC_DIR, "programs-slim", "v2", "today"),
];
const OUTPUT_DIR   = path.join(PUBLIC_DIR, "stats", "v1", "racers");

// マナー待ち（サイト推奨 3s/選手）。コース詳細は軽めに 600ms。
const WAIT_MS_BETWEEN_RACERS = Number(process.env.STATS_DELAY_MS ?? 3000);
const WAIT_MS_BETWEEN_COURSE = 600;

// 実行制御
const ENV_RACERS       = process.env.RACERS?.trim() || "";
const ENV_RACERS_LIMIT = Number(process.env.RACERS_LIMIT ?? "");

// --------------------------------- utils
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchHtml(url, { retries = 3, delayMs = 800 } = {}) {
  for (let i = 0; i <= retries; i++) {
    const res = await fetch(url, {
      headers: {
        "user-agent":
          "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118 Safari/537.36",
        accept: "text/html,application/xhtml+xml",
      },
    });
    if (res.ok) return await res.text();
    if (i < retries) await sleep(delayMs);
  }
  throw new Error(`GET ${url} failed after ${retries + 1} tries`);
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
  if (!headers.length) {
    const first = $tbl.find("tr").first();
    first.find("th,td").each((_, td) => headers.push(normText($(td).text())));
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
function findTableByHeaders($, requiredHeaders) {
  const candidates = $("table");
  for (const el of candidates.toArray()) {
    const { headers } = parseTable($, $(el));
    const ok = requiredHeaders.every((k) => headers.some((h) => h.includes(k)));
    if (ok) return $(el);
  }
  return null;
}

// --------------------------------- rcourse サマリ（直近6か月）
function parseCourseStatsSummary($) {
  // ヘッダ例: コース / 出走数 / 1着率 / 2連対率 / 3連対率 / 平均ST / 平均ST順
  const $tbl = findTableByHeaders($, ["コース", "出走数", "1着率", "2連対率", "3連対率"]);
  if (!$tbl) return null;

  const { headers, rows } = parseTable($, $tbl);
  const iCourse = headerIndex(headers, "コース");
  const iStarts = headerIndex(headers, "出走数");
  const iTop1   = headerIndex(headers, "1着率");
  const iTop2   = headerIndex(headers, "2連対率");
  const iTop3   = headerIndex(headers, "3連対率");

  const items = [];
  for (const r of rows) {
    const ct = r[iCourse] ?? r[0] ?? "";
    const m = ct.match(/([1-6])/);
    if (!m) continue;
    items.push({
      course: Number(m[1]),
      starts: iStarts >= 0 ? toNumber(r[iStarts]) : null,
      top1Rate: iTop1 >= 0 ? toNumber(r[iTop1]) : null,
      top2Rate: iTop2 >= 0 ? toNumber(r[iTop2]) : null,
      top3Rate: iTop3 >= 0 ? toNumber(r[iTop3]) : null,
      winRate: null, // サマリ表には勝率列が無い想定
      raw: r,
    });
  }
  items.sort((a, b) => a.course - b.course);
  return items.length ? items : null;
}

function parseKimariteSummary($) {
  // ヘッダ例: コース / 出走数 / 1着数 / 逃げ / 差し / まくり / ま差し / 抜き / 恵まれ
  const $tbl = findTableByHeaders($, ["コース", "出走数", "1着数", "逃げ", "差し", "まくり", "抜き", "恵まれ"]);
  if (!$tbl) return null;

  const { headers, rows } = parseTable($, $tbl);
  const iCourse = headerIndex(headers, "コース");
  const keys = headers.slice(3); // 1着数の次から各決まり手
  const items = [];
  for (const r of rows) {
    const ct = r[iCourse] ?? r[0] ?? "";
    const m = ct.match(/([1-6])/);
    if (!m) continue;

    const detail = {};
    keys.forEach((k, i) => {
      const v = r[3 + i];
      const percent = v?.match(/([-+]?\d+(\.\d+)?)\s*%/);
      const count   = v?.match(/(\d+)/);
      detail[k] = { count: count ? toNumber(count[1]) : toNumber(v), rate: percent ? toNumber(percent[1]) : null, raw: v ?? null };
    });

    items.push({ course: Number(m[1]), detail, raw: r });
  }
  items.sort((a, b) => a.course - b.course);
  return items.length ? items : null;
}

// --------------------------------- rcourse /course/N/（各進入コース詳細）
function parseAllBoatsStats($) {
  // ヘッダ例: コース / 出走数 / 1着数 / 2着数 / 3着数 / 1着率 / 2連対率 / 3連対率
  const $tbl = findTableByHeaders($, ["コース", "出走数", "1着数", "2着数", "3着数", "1着率", "2連対率", "3連対率"]);
  if (!$tbl) return null;

  const { headers, rows } = parseTable($, $tbl);
  const iCourse = headerIndex(headers, "コース");
  const iStarts = headerIndex(headers, "出走数");
  const iF      = headerIndex(headers, "1着数");
  const iS      = headerIndex(headers, "2着数");
  const iT      = headerIndex(headers, "3着数");
  const iTop1   = headerIndex(headers, "1着率");
  const iTop2   = headerIndex(headers, "2連対率");
  const iTop3   = headerIndex(headers, "3連対率");

  const items = [];
  for (const r of rows) {
    const label = r[iCourse] ?? r[0] ?? ""; // 例: "1コース（自艇）" / "2コース（他艇）"
    items.push({
      courseLabel: label,
      starts: iStarts >= 0 ? toNumber(r[iStarts]) : null,
      firsts: iF >= 0 ? toNumber(r[iF]) : null,
      seconds: iS >= 0 ? toNumber(r[iS]) : null,
      thirds: iT >= 0 ? toNumber(r[iT]) : null,
      top1Rate: iTop1 >= 0 ? toNumber(r[iTop1]) : null,
      top2Rate: iTop2 >= 0 ? toNumber(r[iTop2]) : null,
      top3Rate: iTop3 >= 0 ? toNumber(r[iTop3]) : null,
      raw: r,
    });
  }
  return items.length ? items : null;
}

function parseAllBoatsKimarite($) {
  // ヘッダ例: コース / 出走数 / 1着数 / 逃げ / 差し / まくり / ま差し / 抜き / 恵まれ
  const $tbl = findTableByHeaders($, ["コース", "出走数", "1着数", "逃げ", "差し", "まくり", "ま差し", "抜き", "恵まれ"]);
  if (!$tbl) return null;

  const { headers, rows } = parseTable($, $tbl);
  const iCourse = headerIndex(headers, "コース");
  const keys = headers.slice(3);
  const items = [];
  for (const r of rows) {
    const label = r[iCourse] ?? r[0] ?? ""; // 例: "1コース（自艇）" etc.
    const detail = {};
    keys.forEach((k, i) => {
      const v = r[3 + i];
      const percent = v?.match(/([-+]?\d+(\.\d+)?)\s*%/);
      const count   = v?.match(/(\d+)/);
      detail[k] = { count: count ? toNumber(count[1]) : toNumber(v), rate: percent ? toNumber(percent[1]) : null, raw: v ?? null };
    });
    items.push({ courseLabel: label, detail, raw: r });
  }
  return items.length ? items : null;
}

async function fetchCourseDetail(regno, inCourse) {
  const url = `https://boatrace-db.net/racer/rcourse/regno/${regno}/course/${inCourse}/`;
  const html = await fetchHtml(url);
  const $ = load(html);

  const allBoatsStats    = parseAllBoatsStats($);    // 進入コースN時の「全艇成績」テーブル
  const allBoatsKimarite = parseAllBoatsKimarite($); // 進入コースN時の「全艇決まり手」テーブル
  // 「全出走結果」テーブルは行数が多く重いので、必要になったら追加実装にする

  return { inCourse: Number(inCourse), url, allBoatsStats, allBoatsKimarite };
}

// --------------------------------- rdemo（展示タイム順位別）
function parseExTimeRank($) {
  // ヘッダ例: 順位 / 出走数 / 1着率 / 2連対率 / 3連対率
  const $tbl = findTableByHeaders($, ["順位", "出走数", "1着率", "2連対率", "3連対率"]);
  if (!$tbl) return null;

  const { headers, rows } = parseTable($, $tbl);
  const iRank = headerIndex(headers, "順位");
  const iW    = headerIndex(headers, "1着率");
  const iT2   = headerIndex(headers, "2連対率");
  const iT3   = headerIndex(headers, "3連対率");

  const items = [];
  for (const r of rows) {
    const rt = r[iRank] ?? r[0] ?? "";
    const m = rt.match(/([1-6])/);
    if (!m) continue;
    items.push({
      rank: Number(m[1]),
      winRate: iW  >= 0 ? toNumber(r[iW])  : null,
      top2Rate: iT2 >= 0 ? toNumber(r[iT2]) : null,
      top3Rate: iT3 >= 0 ? toNumber(r[iT3]) : null,
      raw: r,
    });
  }
  items.sort((a, b) => a.rank - b.rank);
  return items.length ? items : null;
}

// --------------------------------- 1選手ぶん
async function fetchOne(regno) {
  const uSummary = `https://boatrace-db.net/racer/rcourse/regno/${regno}/`;
  const uRdemo   = `https://boatrace-db.net/racer/rdemo/regno/${regno}/`;

  // rcourse サマリ
  let courseStats = null, courseKimarite = null;
  try {
    const html = await fetchHtml(uSummary);
    const $ = load(html);
    courseStats   = parseCourseStatsSummary($);
    courseKimarite = parseKimariteSummary($);
  } catch (e) {
    console.warn(`warn: rcourse summary failed for ${regno}: ${e.message}`);
  }

  // rcourse 各コース詳細（1..6）
  const byInCourse = [];
  for (let c = 1; c <= 6; c++) {
    try {
      const detail = await fetchCourseDetail(regno, c);
      byInCourse.push(detail);
      await sleep(WAIT_MS_BETWEEN_COURSE);
    } catch (e) {
      console.warn(`warn: rcourse course/${c} failed for ${regno}: ${e.message}`);
    }
  }

  // rdemo
  let exTimeRank = null;
  try {
    const html2 = await fetchHtml(uRdemo);
    const $2 = load(html2);
    exTimeRank = parseExTimeRank($2);
  } catch (e) {
    console.warn(`warn: rdemo failed for ${regno}: ${e.message}`);
  }

  return {
    regno: Number(regno),
    sources: {
      rcourse: uSummary,
      byCourse: Object.fromEntries(byInCourse.map(d => [d.inCourse, d.url])),
      rdemo: uRdemo,
    },
    fetchedAt: new Date().toISOString(),
    // サマリ（直近6か月）
    courseStats,      // [{ course, starts, top1Rate, top2Rate, top3Rate, winRate:null, raw }]
    courseKimarite,   // [{ course, detail:{逃げ:{count,rate},…}, raw }]
    // 進入コース別の詳細
    byInCourse: byInCourse.map(({ inCourse, allBoatsStats, allBoatsKimarite }) => ({
      inCourse,
      allBoatsStats,     // [{ courseLabel, starts, firsts, seconds, thirds, top1Rate, top2Rate, top3Rate, raw }]
      allBoatsKimarite,  // [{ courseLabel, detail:{逃げ:{count,rate},…}, raw }]
    })),
    // 展示タイム順位別
    exTimeRank,       // [{ rank, winRate, top2Rate, top3Rate, raw }]
  };
}

// --------------------------------- today から選手抽出
async function collectRacersFromToday() {
  const set = new Set();
  for (const root of TODAY_ROOTS) {
    let entries = [];
    try {
      entries = await fs.readdir(root, { withFileTypes: true });
    } catch { continue; }
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

// --------------------------------- main
async function main() {
  let racers = [];
  if (ENV_RACERS) {
    racers = ENV_RACERS.split(",").map(s => s.trim()).filter(Boolean);
  } else {
    racers = await collectRacersFromToday();
  }
  if (ENV_RACERS_LIMIT && Number.isFinite(ENV_RACERS_LIMIT) && ENV_RACERS_LIMIT > 0) {
    racers = racers.slice(0, ENV_RACERS_LIMIT);
  }

  if (!racers.length) {
    console.log("No racers to fetch. (Set RACERS env or put today programs)");
    return;
  }

  await ensureDir(OUTPUT_DIR);

  let ok = 0, ng = 0;
  for (const regno of racers) {
    try {
      const data = await fetchOne(regno);
      const outPath = path.join(OUTPUT_DIR, `${regno}.json`);
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
      { status: 200, fetchedAt: new Date().toISOString(), racers: racers.map(Number), success: ok, failed: ng },
      null,
      2
    ),
    "utf8"
  );
}

main().catch((e) => { console.error(e); process.exit(1); });
