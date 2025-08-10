// scripts/fetch-stats.js  (v2 専用：public/stats/v2/racers/<regno>.json)
//
// 変更点（要旨）
// - 出力は v2 のみ（v1 書き出し廃止）
// - 12時間以内の既存ファイルはスキップ（mtime 判定）
// - 1回だけリトライ（=最大2回試行）
// - リクエスト間隔は 3s（選手間/各コースページ間）
// - 進入コース別の「自艇/他艇」の全艇成績は
//   /racer/rcourse/regno/<regno>/course/<n>/ を 1..6 参照して取得
// - 途中で1人分完了するたびに即書き出し（インクリメンタル）
// - ログの URL 末尾にコロンが付かないよう整形
//
// 参照：
//   - 進入コース別一覧: https://boatrace-db.net/racer/rcourse/regno/<regno>/course/<n>/ (n=1..6)
//     => 「nコース進入時の全艇成績」テーブル（1〜6コース × 自艇/他艇）
//     => 下部の一覧から平均ST（当該条件のレース群）も算出
//     => ページ内の「全艇決まり手」テーブルがあれば負け決まり手も集計
//   - 展示順位別成績: https://boatrace-db.net/racer/rdemo/regno/<regno>/
//
// ENV：
//   RACERS            : "3072,4103,..." 明示するとその人だけ
//   STATS_BATCH       : 先頭から N 人だけ処理
//   STATS_DELAY_MS    : 選手間の待機（ms）(default 3000)
//   COURSE_WAIT_MS    : 各コースページ間の待機（ms）(default 3000)

import { load } from "cheerio";
import fs from "node:fs/promises";
import fss from "node:fs";
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

// polite waits
const WAIT_MS_BETWEEN_RACERS = Number(process.env.STATS_DELAY_MS || 3000);
const WAIT_MS_BETWEEN_COURSE_PAGES = Number(process.env.COURSE_WAIT_MS || 3000);

// cache window
const FRESH_HOURS = 12;

// env
const ENV_RACERS = (process.env.RACERS || "").trim();
const ENV_BATCH = Number(process.env.STATS_BATCH || "");

// -------------------------------
// ユーティリティ
// -------------------------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function nowIso() {
  return new Date().toISOString();
}

function isFreshEnough(file, hours) {
  try {
    const st = fss.statSync(file);
    const ageMs = Date.now() - st.mtimeMs;
    return ageMs < hours * 3600 * 1000;
  } catch {
    return false;
  }
}

function normText(t) {
  return (t ?? "")
    .replace(/\u00A0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
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
  return k
    .replace("ま差し", "まくり差し")
    .replace("捲り差し", "まくり差し")
    .replace("捲り", "まくり");
}

/**
 * fetchHtml: UA/Referer/言語ヘッダ付き、リトライ 1回（合計2回）版
 */
async function fetchHtml(url, {
  retries = 1,
  baseDelayMs = 2500,
  timeoutMs = 20000,
} = {}) {
  const mkAC = () => new AbortController();

  for (let attempt = 0; attempt <= retries; attempt++) {
    const ac = mkAC();
    const t = setTimeout(() => ac.abort(), timeoutMs);

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
        clearTimeout(t);
        return await res.text();
      }

      const retriable = [403, 429, 500, 502, 503, 504].includes(res.status);
      const body = await res.text().catch(() => "");
      clearTimeout(t);

      if (!retriable || attempt === retries) {
        throw new Error(`HTTP ${res.status} ${res.statusText} @ ${url}`);
      }

      const factor = (res.status === 403 || res.status === 429 || res.status === 503) ? 2.0 : 1.3;
      const delay = Math.round((baseDelayMs * Math.pow(factor, attempt)) * (0.8 + Math.random() * 0.4));
      await sleep(delay);
      continue;

    } catch (err) {
      clearTimeout(t);
      if (attempt === retries) {
        throw new Error(`NET fetch failed @ ${url} :: ${err.message}`);
      }
      const delay = Math.round((baseDelayMs * Math.pow(1.6, attempt)) * (0.8 + Math.random() * 0.4));
      await sleep(delay);
    }
  }
  throw new Error(`unreachable fetch loop for ${url}`);
}

// -------------------------------
// ページパーサ
// -------------------------------

// 「nコース進入時の全艇成績」テーブル（自艇/他艇）
function parseAllBoatsStatsOnEntryCourse($) {
  // 見出しは「◯コース進入時の全艇成績」
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
    const label = r[iCourse] || "";
    const m = label.match(/([1-6])\s*コース/);
    if (!m) continue;
    const course = Number(m[1]);
    const self = /（自艇）/.test(label);
    const other = /（他艇）/.test(label);

    items.push({
      course,
      self: self ? true : (other ? false : null),
      starts: iStarts >= 0 ? toNumber(r[iStarts]) : null,
      top1Rate: iTop1 >= 0 ? toNumber(r[iTop1]) : null,
      top2Rate: iTop2 >= 0 ? toNumber(r[iTop2]) : null,
      top3Rate: iTop3 >= 0 ? toNumber(r[iTop3]) : null,
      raw: r,
    });
  }
  return items.length ? items : null;
}

// 同ページ下部のレース一覧から平均ST（F/Lは除外、絶対値平均）
function parseAvgSTFromEntryCoursePage($) {
  const $tbl = mustTableByHeader($, ["月日", "場", "レース", "ST", "結果"]);
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
  return Math.round((sum / cnt) * 100) / 100;
}

// 「全艇決まり手」的な表がある場合に負け決まり手合計を作る
function parseLoseKimariteFromEntryCoursePage($) {
  const $tbl = mustTableByHeader($, ["コース", "出走数", "1着数", "逃げ", "差し", "まくり"]);
  if (!$tbl) return null;
  const { headers, rows } = parseTable($, $tbl);
  const iCourse = headerIndex(headers, "コース");
  const keys = headers.slice(3).map(normalizeKimariteKey);

  const lose = Object.fromEntries(keys.map(k => [k, 0]));
  for (const r of rows) {
    const label = r[iCourse] || "";
    if (label.includes("（自艇）")) continue; // 自艇は勝ち手。負け側だけ合算
    keys.forEach((k, i) => {
      const v = r[3 + i];
      const num = v ? Number((v.match(/(\d+)/) || [])[1]) : NaN;
      if (Number.isFinite(num)) lose[k] += num;
    });
  }
  return lose;
}

// 展示タイム順位別
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
// 1選手分（v2 ペイロード）
// -------------------------------
async function fetchOneV2(regno) {
  const coursePages = {};
  const byEntryCourse = [];
  for (let entry = 1; entry <= 6; entry++) {
    const url = `https://boatrace-db.net/racer/rcourse/regno/${regno}/course/${entry}/`;
    coursePages[entry] = url;

    try {
      const html = await fetchHtml(url);
      const $ = load(html);

      const allBoatsStats = parseAllBoatsStatsOnEntryCourse($);
      const avgST = parseAvgSTFromEntryCoursePage($);
      const loseKimarite = parseLoseKimariteFromEntryCoursePage($);

      byEntryCourse.push({
        entryCourse: entry,
        allBoatsStats,   // [{course, self:true/false, starts, top1Rate, top2Rate, top3Rate}]
        avgST: avgST ?? null,
        loseKimarite: loseKimarite ?? null,
      });
    } catch (e) {
      console.warn(`warn: entry-course page failed regno=${regno} entry=${entry} -> ${e.message}`);
      byEntryCourse.push({
        entryCourse: entry,
        allBoatsStats: null,
        avgST: null,
        loseKimarite: null,
      });
    }
    await sleep(WAIT_MS_BETWEEN_COURSE_PAGES);
  }

  // 展示順位別
  let exTimeRank = null;
  const rdemoUrl = `https://boatrace-db.net/racer/rdemo/regno/${regno}/`;
  try {
    const html = await fetchHtml(rdemoUrl);
    exTimeRank = parseExTimeRankFromRdemo(load(html));
  } catch (e) {
    console.warn(`warn: rdemo fetch/parse failed regno=${regno} -> ${e.message}`);
  }

  return {
    schemaVersion: "2.0",
    regno: Number(regno),
    fetchedAt: nowIso(),
    sources: { rdemo: rdemoUrl, coursePages },
    byEntryCourse,   // 1..6
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
          // 可能性のあるフィールドを順に探す
          const boats =
            json?.boats ||
            json?.program?.boats ||
            json?.entries ||
            [];
          for (const b of boats) {
            const r =
              b.racer_number ?? b.racerNumber ?? b.number ?? b.racer?.number;
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
  if (ENV_BATCH && Number.isFinite(ENV_BATCH) && ENV_BATCH > 0) {
    racers = racers.slice(0, ENV_BATCH);
  }

  if (racers.length === 0) {
    console.log("No racers to fetch. (Set RACERS env or ensure today programs exist)");
    return;
  }

  await ensureDir(OUTPUT_DIR_V2);

  console.log(
    `process ${racers.length} racers (incremental, fresh<${FRESH_HOURS}h)` +
      (ENV_RACERS ? " [env RACERS specified]" : "") +
      (ENV_BATCH ? ` [batch=${ENV_BATCH}]` : "")
  );

  let ok = 0, skip = 0, ng = 0;
  for (const regno of racers) {
    const outPathV2 = path.join(OUTPUT_DIR_V2, `${regno}.json`);

    // 12h 以内はスキップ
    if (isFreshEnough(outPathV2, FRESH_HOURS)) {
      console.log(`⏭️  skip (fresh<${FRESH_HOURS}h): stats/v2/racers/${regno}.json`);
      skip++;
      await sleep(WAIT_MS_BETWEEN_RACERS);
      continue;
    }

    try {
      const data = await fetchOneV2(regno);
      await fs.writeFile(outPathV2, JSON.stringify(data, null, 2), "utf8");
      console.log(`✅ wrote stats/v2/racers/${regno}.json`);
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
        fetchedAt: nowIso(),
        racers: racers.map((r) => Number(r)),
        success: ok,
        skippedFresh: skip,
        failed: ng,
        cacheHours: FRESH_HOURS,
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
