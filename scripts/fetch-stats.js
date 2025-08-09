// scripts/fetch-stats.js
// Node v20 / ESM / cheerio v1.x
// 出力: public/stats/v1/racers/<regno>.json
// 参照:
//   - rcourse 直近6か月: https://boatrace-db.net/racer/rcourse/regno/<regno>/
//   - rcourse 各コース:  https://boatrace-db.net/racer/rcourse/regno/<regno>/course/<n>/ (n=1..6)
//   - rdemo   展示順位:  https://boatrace-db.net/racer/rdemo/regno/<regno>/
//
// 追加ポイント（A案: 途中コミット＆Push）
//   - 既存ファイルが “12時間以内の更新” ならスキップ（SKIP_WINDOW_HOURS=12）
//   - HTTPリトライは 1 回だけ（= 最大 2 トライ）
//   - GIT_CHECKPOINT_N 人ごと / GIT_CHECKPOINT_SEC 秒ごとに git add/commit/push
//   - SIGINT/SIGTERM でも最後に push 試行

import { load } from "cheerio";
import fs from "node:fs/promises";
import fssync from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";

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
const DEBUG_DIR  = path.join(PUBLIC_DIR, "debug");

// polite wait
const WAIT_MS_BETWEEN_RACERS = Number(process.env.STATS_DELAY_MS || 3000);
const WAIT_MS_BETWEEN_COURSE_PAGES = Number(process.env.COURSE_WAIT_MS || 1200);

// env
const ENV_RACERS       = process.env.RACERS?.trim() || "";
const ENV_RACERS_LIMIT = Number(process.env.RACERS_LIMIT ?? "");
const ENV_BATCH        = Number(process.env.STATS_BATCH ?? "");

// 既存ファイルスキップの時間窓（時間）
const SKIP_WINDOW_HOURS = Number(process.env.SKIP_WINDOW_HOURS || 12);

// チェックポイント push 設定
const GIT_CHECKPOINT_N   = Number(process.env.GIT_CHECKPOINT_N || 10);    // N人ごと
const GIT_CHECKPOINT_SEC = Number(process.env.GIT_CHECKPOINT_SEC || 300);  // 秒ごと（5分）
const GIT_COMMIT_MESSAGE = process.env.GIT_COMMIT_MESSAGE || "Update racer stats (checkpoint) [skip ci]";

// HTTP リトライ（最大試行回数 = 1回リトライ → 2トライ）
const MAX_RETRY = 1;
const BASE_DELAY_MS = 2500;
const TIMEOUT_MS = 20000;

// -------------------------------
// ユーティリティ
// -------------------------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function pathMtimeHours(p) {
  try {
    const st = await fs.stat(p);
    const ageMs = Date.now() - st.mtimeMs;
    return ageMs / 1000 / 3600;
  } catch {
    return Infinity;
  }
}

/**
 * fetchHtml: UA/Referer/言語ヘッダ付き、リトライ(1回)版
 */
async function fetchHtml(url) {
  for (let attempt = 0; attempt <= MAX_RETRY; attempt++) {
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), TIMEOUT_MS);

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

      if (res.ok) {
        clearTimeout(timer);
        return await res.text();
      }

      // リトライ対象
      const retriable = [403, 404, 429, 500, 502, 503, 504].includes(res.status);
      if (!retriable || attempt === MAX_RETRY) {
        const body = await res.text().catch(() => "");
        clearTimeout(timer);
        throw new Error(`HTTP ${res.status} ${res.statusText} @ ${url} ${body?.slice(0, 120)}`);
      }

      clearTimeout(timer);
      await sleep(Math.round(BASE_DELAY_MS * (0.8 + Math.random() * 0.4)));
      continue;

    } catch (err) {
      clearTimeout(err?.name === "AbortError" ? undefined : undefined);
      if (attempt === MAX_RETRY) {
        throw new Error(`GET failed after ${MAX_RETRY + 1} tries: ${url} :: ${err.message}`);
      }
      await sleep(Math.round(BASE_DELAY_MS * (0.8 + Math.random() * 0.4)));
    }
  }
  throw new Error(`unreachable fetch loop for ${url}`);
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

// -------------------------------
// rcourse（直近6か月）: コース別成績/決まり手（一覧）
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
    items.push({ course: Number(m[1]), detail, raw: r });
  }
  items.sort((a,b)=>a.course-b.course);
  return items.length ? items : null;
}

// -------------------------------
// rcourse/course/{n}: 平均ST & 負け決まり手
// -------------------------------
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
    if (label.includes("（自艇）")) continue;
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

  // 各コース詳細（平均ST/負け決まり手）
  const courseDetails = [];
  const coursePages = {};
  for (let c = 1; c <= 6; c++) {
    const url = `https://boatrace-db.net/racer/rcourse/regno/${regno}/course/${c}/`;
    coursePages[c] = url;
    try {
      const html = await fetchHtml(url);
      const $ = load(html);
      const avgST = parseAvgSTFromCoursePage($);
      const loseKimarite = parseLoseKimariteFromCoursePage($);
      courseDetails.push({
        course: c,
        avgST: avgST ?? null,
        loseKimarite: loseKimarite ?? null,
      });
      await sleep(WAIT_MS_BETWEEN_COURSE_PAGES);
    } catch (e) {
      console.warn(`warn: course page parse failed regno=${regno} course=${c}: ${e.message}`);
      courseDetails.push({ course: c, avgST: null, loseKimarite: null });
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
    regno: Number(regno),
    sources: { rcourse: uRcourse, rdemo: uRdemo, coursePages },
    fetchedAt: new Date().toISOString(),
    courseStats,
    courseKimarite,
    courseDetails,
    exTimeRank,
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
          const boats = json?.boats || json?.program?.boats || json?.entries || [];
          for (const b of boats) {
            const r =
              b.racer_number ?? b.racerNumber ?? b.racer?.number ??
              b.number ?? null;
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
// Git チェックポイント
// -------------------------------
let processedSinceLastPush = 0;
let lastPushAt = Date.now();

function shouldCheckpoint() {
  const byCount = GIT_CHECKPOINT_N > 0 && processedSinceLastPush >= GIT_CHECKPOINT_N;
  const byTime  = GIT_CHECKPOINT_SEC > 0 && (Date.now() - lastPushAt) / 1000 >= GIT_CHECKPOINT_SEC;
  return byCount || byTime;
}

function gitSafe(cmd) {
  try {
    return execSync(cmd, { stdio: "inherit" });
  } catch (e) {
    console.warn(`git cmd failed: ${cmd} :: ${e.message}`);
    return null;
  }
}

function gitCheckpointPush(label = "checkpoint") {
  try {
    // add 変更だけでOK
    gitSafe(`git add -A ${OUTPUT_DIR} ${DEBUG_DIR} || true`);
    // 差分なければ抜ける
    const diff = execSync("git diff --cached --quiet || echo CHANGED", { encoding: "utf8" }).trim();
    if (!diff) return;

    gitSafe(`git -c user.name="github-actions[bot]" -c user.email="41898282+github-actions[bot]@users.noreply.github.com" commit -m "${GIT_COMMIT_MESSAGE} (${label})"`);
    // push は1回だけ試行（衝突は後段の再base等、別ジョブで調整する方針）
    gitSafe("git push || true");

    processedSinceLastPush = 0;
    lastPushAt = Date.now();
  } catch (e) {
    console.warn(`checkpoint push failed: ${e.message}`);
  }
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

  await ensureDir(OUTPUT_DIR);
  await ensureDir(DEBUG_DIR);

  console.log(
    `process ${racers.length} racers` +
      (ENV_RACERS ? " (env RACERS specified)" : "") +
      (ENV_RACERS_LIMIT ? ` (limit=${ENV_RACERS_LIMIT})` : "") +
      (ENV_BATCH ? ` (batch=${ENV_BATCH})` : "") +
      ` | skip-if-mtime<${SKIP_WINDOW_HOURS}h`
  );

  // 中断時にも最後の push
  const onExit = () => {
    try { gitCheckpointPush("final"); } catch {}
    process.exit();
  };
  process.on("SIGINT", onExit);
  process.on("SIGTERM", onExit);

  let ok = 0, ng = 0;

  for (const regno of racers) {
    const outPath = path.join(OUTPUT_DIR, `${regno}.json`);
    const mtimeH = await pathMtimeHours(outPath);
    if (mtimeH < SKIP_WINDOW_HOURS) {
      console.log(`⏭  skip ${regno} (updated ${mtimeH.toFixed(1)}h ago < ${SKIP_WINDOW_HOURS}h)`);
    } else {
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

    processedSinceLastPush++;
    if (shouldCheckpoint()) gitCheckpointPush("periodic");
  }

  await fs.writeFile(
    path.join(DEBUG_DIR, "stats-meta.json"),
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

  // 最後にもう一度 Push
  gitCheckpointPush("final");
}

main().catch((e) => {
  console.error(e);
  // 最後に push だけ試す
  try { gitCheckpointPush("final-error"); } catch {}
  process.exit(1);
});
