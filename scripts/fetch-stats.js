// scripts/fetch-stats.js
// Node v20 / ESM / cheerio v1.x
// 出力: public/stats/v1/racers/<regno>.json
// 参照:
//   - rcourse 直近6か月: https://boatrace-db.net/racer/rcourse/regno/<regno>/
//   - rcourse 各コース:  https://boatrace-db.net/racer/rcourse/regno/<regno>/course/<n>/ (n=1..6)
//   - rdemo   展示順位:  https://boatrace-db.net/racer/rdemo/regno/<regno>/

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
const OUTPUT_DIR = path.join(PUBLIC_DIR, "stats", "v1", "racers");

// polite wait（最低でも3秒空ける）
const WAIT_MS_BETWEEN_RACERS = Math.max(3000, Number(process.env.STATS_DELAY_MS || 3000));
const WAIT_MS_BETWEEN_COURSE_PAGES = Math.max(3000, Number(process.env.COURSE_WAIT_MS || 3000));

// 既存を"新しければ"スキップ（デフォ 12h）
const FRESH_HOURS = Number(process.env.FRESH_HOURS || 12);
const FRESH_MS = FRESH_HOURS * 3600 * 1000;

// env
const ENV_RACERS       = process.env.RACERS?.trim() || "";
const ENV_RACERS_LIMIT = Number(process.env.RACERS_LIMIT ?? "");
const ENV_BATCH        = Number(process.env.STATS_BATCH ?? "");

// -------------------------------
// ユーティリティ
// -------------------------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * fetchHtml: UA/Referer/言語ヘッダ付き。
 * リトライ方針:
 *  - 404 は即失敗（再試行しない）
 *  - 403/429/5xx/ネットワークは「最大1回だけ」再試行
 */
async function fetchHtml(
  url,
  { retries = 1, baseDelayMs = 2000, timeoutMs = 20000 } = {}
) {
  const makeCtl = () => new AbortController();

  for (let attempt = 0; attempt <= retries; attempt++) {
    const ac = makeCtl();
    const timer = setTimeout(() => ac.abort(), timeoutMs);

    try {
      const res = await fetch(url, {
        signal: ac.signal,
        headers: {
          "user-agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
          "accept": "text/html,application/xhtml+xml",
          "accept-language": "ja,en;q=0.9",
          "referer": "https://boatrace-db.net/",
          "cache-control": "no-cache",
        },
      });

      clearTimeout(timer);

      if (res.ok) return await res.text();

      // 404 は即あきらめ
      if (res.status === 404) {
        const body = await res.text().catch(() => "");
        throw new Error(`HTTP 404 @ ${url} ${body?.slice(0, 120)}`);
      }

      // リトライ上限 or 非再試行系
      const retriable = [403, 429, 500, 502, 503, 504].includes(res.status);
      if (!(retriable && attempt < retries)) {
        const body = await res.text().catch(() => "");
        throw new Error(`HTTP ${res.status} ${res.statusText} @ ${url} ${body?.slice(0, 120)}`);
      }

      // 再試行前待機（指数+ジッター）
      const delay = Math.round(baseDelayMs * Math.pow(1.8, attempt) * (0.8 + Math.random() * 0.4));
      await sleep(delay);
      continue;
    } catch (err) {
      clearTimeout(timer);
      if (attempt >= retries) {
        throw new Error(`NET fetch failed @ ${url}: ${err.message}`);
      }
      const delay = Math.round(baseDelayMs * Math.pow(1.6, attempt) * (0.8 + Math.random() * 0.4));
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
  return k.replace("ま差し", "まくり差し").replace("捲り差し", "まくり差し").replace("捲り", "まくり");
}

// -------------------------------
// rcourse（直近6か月）: コース別成績/決まり手（一覧ページ）
// ※この表は「進入コース=1〜6（自艇/他艇混在）」の集計ブロック
// -------------------------------
function parseCourseStatsFromRcourse($) {
  const $tbl = mustTableByHeader($, ["コース", "出走数", "1着率", "2連対率", "3連対率"]);
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
      label: ct, // "1コース（自艇）" / "2コース（他艇）" など
      starts: iStarts >= 0 ? toNumber(r[iStarts]) : null,
      top1Rate: iTop1 >= 0 ? toNumber(r[iTop1]) : null,
      top2Rate: iTop2 >= 0 ? toNumber(r[iTop2]) : null,
      top3Rate: iTop3 >= 0 ? toNumber(r[iTop3]) : null,
      winRate: null,
      raw: r,
    });
  }
  items.sort((a,b)=>a.course-b.course);
  return items.length ? items : null;
}

function parseKimariteFromRcourse($) {
  const $tbl = mustTableByHeader($, ["コース", "出走数", "1着数", "逃げ", "差し", "まくり", "抜き", "恵まれ"]);
  if (!$tbl) return null;
  const { headers, rows } = parseTable($, $tbl);
  const iCourse = headerIndex(headers, "コース");
  const detailKeys = headers.slice(3).map(normalizeKimariteKey);

  const items = [];
  for (const r of rows) {
    const ct = r[iCourse] ?? r[0] ?? "";
    const m = ct.match(/([1-6])/);
    if (!m) continue;
    const detail = {};
    detailKeys.forEach((k, i) => {
      const v = r[3 + i];
      const percent = v?.match(/([-+]?\d+(\.\d+)?)\s*%/);
      const count = v?.match(/(\d+)/);
      detail[k] = {
        count: count ? toNumber(count[1]) : toNumber(v),
        rate: percent ? toNumber(percent[1]) : null,
        raw: v ?? null,
      };
    });
    items.push({ course: Number(m[1]), label: ct, detail, raw: r });
  }
  items.sort((a,b)=>a.course-b.course);
  return items.length ? items : null;
}

// -------------------------------
// rcourse/course/{n}: 「進入コース=n」のときの詳細
//  - 一覧テーブル: 「nコース進入時の全艇成績」= 自艇/他艇の成績が並ぶ
//  - 同ページ下部の「負け決まり手」なども合算用に読む
// -------------------------------
function parseCourseInDetail($) {
  const $tbl = mustTableByHeader($, ["コース", "出走数", "1着数", "1着率", "2連対率", "3連対率"]);
  if (!$tbl) return null;
  const { headers, rows } = parseTable($, $tbl);

  const iCourse = headerIndex(headers, "コース");
  const iStarts = headerIndex(headers, "出走数");
  const iTop1   = headerIndex(headers, "1着率");
  const iTop2   = headerIndex(headers, "2連対率");
  const iTop3   = headerIndex(headers, "3連対率");

  const lines = [];
  for (const r of rows) {
    const label = r[iCourse] || "";
    const m = label.match(/([1-6])/);
    if (!m) continue;
    lines.push({
      lane: Number(m[1]),
      label, // "3コース（自艇）" / "1コース（他艇）" ...
      starts: iStarts >= 0 ? toNumber(r[iStarts]) : null,
      top1Rate: iTop1 >= 0 ? toNumber(r[iTop1]) : null,
      top2Rate: iTop2 >= 0 ? toNumber(r[iTop2]) : null,
      top3Rate: iTop3 >= 0 ? toNumber(r[iTop3]) : null,
      raw: r,
    });
  }
  return lines.length ? lines : null;
}

function parseAvgSTFromCoursePage($) {
  const $tbl = mustTableByHeader($, ["月日", "場", "レース", "ST", "結果"]);
  if (!$tbl) return null;
  const { headers, rows } = parseTable($, $tbl);
  const iST = headerIndex(headers, "ST");
  if (iST < 0) return null;

  let sum = 0, cnt = 0;
  for (const r of rows) {
    const st = r[iST]; // ".15" / "F.01" / "L.10"
    if (!st) continue;
    if (/^[FL]/i.test(st)) continue;
    const m = st.match(/-?\.?\d+(?:\.\d+)?/);
    if (!m) continue;
    const n = Number(m[0]);
    if (Number.isFinite(n)) { sum += Math.abs(n); cnt++; }
  }
  if (!cnt) return null;
  return Math.round((sum / cnt) * 100) / 100;
}

function parseLoseKimariteFromCoursePage($) {
  const $tbl = mustTableByHeader($, ["コース", "出走数", "1着数", "逃げ", "差し", "まくり"]);
  if (!$tbl) return null;

  const { headers, rows } = parseTable($, $tbl);
  const iCourse = headerIndex(headers, "コース");
  const keys = headers.slice(3).map(normalizeKimariteKey);

  const lose = Object.fromEntries(keys.map(k => [k, 0]));
  for (const r of rows) {
    const label = r[iCourse] || "";
    if (label.includes("（自艇）")) continue; // 他艇の決まり手を合算
    keys.forEach((k, i) => {
      const v = r[3 + i];
      const num = v ? Number((v.match(/(\d+)/) || [])[1]) : NaN;
      if (Number.isFinite(num)) lose[k] += num;
    });
  }
  return lose;
}

// -------------------------------
// rdemo: 展示タイム順位別成績
// -------------------------------
function parseExTimeRankFromRdemo($) {
  const $tbl = mustTableByHeader($, ["順位", "出走数", "1着率", "2連対率", "3連対率"]);
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
      winRate: iWin >= 0 ? toNumber(r[iWin]) : null,
      top2Rate: iT2 >= 0 ? toNumber(r[iT2]) : null,
      top3Rate: iT3 >= 0 ? toNumber(r[iT3]) : null,
      raw: r,
    });
  }
  items.sort((a,b)=>a.rank-b.rank);
  return items.length ? items : null;
}

// -------------------------------
// 1選手分
// -------------------------------
async function fetchOne(regno) {
  const uRcourse = `https://boatrace-db.net/racer/rcourse/regno/${regno}/`;
  const uRdemo   = `https://boatrace-db.net/racer/rdemo/regno/${regno}/`;

  // 一覧（全体の1〜6コースブロック）
  let courseStats = null;
  let courseKimarite = null;
  try {
    const html = await fetchHtml(uRcourse);
    const $ = load(html);
    courseStats = parseCourseStatsFromRcourse($);
    courseKimarite = parseKimariteFromRcourse($);
  } catch (e) {
    console.warn(`warn: rcourse list fetch/parse failed for ${regno}: ${e.message}`);
  }

  // 各「進入コース=1..6」のページ詳細を収集
  const courseInDetails = []; // [{inCourse, allLanes:[{lane,label,starts,top1Rate,...}], avgST, loseKimarite}]
  const coursePages = {};
  for (let c = 1; c <= 6; c++) {
    const url = `https://boatrace-db.net/racer/rcourse/regno/${regno}/course/${c}/`;
    coursePages[c] = url;
    try {
      const html = await fetchHtml(url);
      const $ = load(html);
      const allLanes = parseCourseInDetail($);
      const avgST = parseAvgSTFromCoursePage($);
      const loseKimarite = parseLoseKimariteFromCoursePage($);
      courseInDetails.push({
        inCourse: c,
        allLanes: allLanes ?? null,
        avgST: avgST ?? null,
        loseKimarite: loseKimarite ?? null,
      });
    } catch (e) {
      console.warn(`warn: course page parse failed regno=${regno} course=${c}: ${e.message}`);
      courseInDetails.push({ inCourse: c, allLanes: null, avgST: null, loseKimarite: null });
    }
    await sleep(WAIT_MS_BETWEEN_COURSE_PAGES);
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
    version: "2.0",
    regno: Number(regno),
    sources: { rcourse: uRcourse, rdemo: uRdemo, coursePages },
    fetchedAt: new Date().toISOString(),
    // 一覧（「1コース（自艇）/2コース（他艇）…」の塊）
    courseStats,     // [{course,label,starts,top1Rate,top2Rate,top3Rate,winRate:null}]
    courseKimarite,  // [{course,label,detail:{逃げ:{count,rate},…}}]
    // 「進入コース=n」ページから取った詳細（自艇/他艇の行が並ぶ）
    courseInDetails, // [{inCourse, allLanes:[{lane,label,starts,top1Rate,top2Rate,top3Rate}], avgST, loseKimarite:{逃げ:xx,…}}]
    // 展示タイム順位別
    exTimeRank,      // [{rank, winRate, top2Rate, top3Rate}]
  };
}

// -------------------------------
// today配下から出走選手収集
// -------------------------------
async function collectRacersFromToday() {
  const set = new Set();
  for (const root of TODAY_ROOTS) {
    let entries = [];
    try {
      entries = await fs.readdir(root, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const d of entries) {
      if (!d.isDirectory()) continue;
      const dayDir = path.join(root, d.name);
      const files = await fs.readdir(dayDir).catch(() => []);
      for (const f of files) {
        if (!f.endsWith(".json")) continue;
        const full = path.join(dayDir, f);
        try {
          const json = JSON.parse(await fs.readFile(full, "utf8"));
          const boats =
            json?.boats ||
            json?.program?.boats ||
            json?.entries ||
            json?.Entries ||
            [];
          for (const b of boats) {
            const r = b.racer_number ?? b.number ?? b.racerNumber ?? b.racer?.number;
            if (r) set.add(String(r));
          }
        } catch {}
      }
    }
  }
  return [...set];
}

async function ensureDir(p) {
  await fs.mkdir(p, { recursive: true });
}

function isFresh(file) {
  try {
    const st = fssync.statSync(file);
    return Date.now() - st.mtimeMs < FRESH_MS;
  } catch { return false; }
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
    `process ${racers.length} racers` +
      (ENV_RACERS ? " (env RACERS specified)" : "") +
      (ENV_RACERS_LIMIT ? ` (limit=${ENV_RACERS_LIMIT})` : "") +
      (ENV_BATCH ? ` (batch=${ENV_BATCH})` : "") +
      ` | wait: racer=${WAIT_MS_BETWEEN_RACERS}ms, coursePage=${WAIT_MS_BETWEEN_COURSE_PAGES}ms, fresh=${FRESH_HOURS}h`
  );

  await ensureDir(OUTPUT_DIR);

  let ok = 0, ng = 0, skipped = 0;
  for (const regno of racers) {
    const outPath = path.join(OUTPUT_DIR, `${regno}.json`);

    if (isFresh(outPath)) {
      console.log(`⏭  skip fresh ${path.relative(PUBLIC_DIR, outPath)}`);
      skipped++;
      await sleep(WAIT_MS_BETWEEN_RACERS);
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
        skipped,
        waitMs: {
          betweenRacers: WAIT_MS_BETWEEN_RACERS,
          betweenCoursePages: WAIT_MS_BETWEEN_COURSE_PAGES,
        },
        freshHours: FRESH_HOURS,
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
