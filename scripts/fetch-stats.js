// scripts/fetch-stats.js
// Node v20 / ESM / cheerio v1.x
//
// 出力: public/stats/v2/racers/<regno>.json
// 収集元:
//   - rcourse 直近6か月サマリ: https://boatrace-db.net/racer/rcourse/regno/<regno>/
//   - rcourse 各コース進入:   https://boatrace-db.net/racer/rcourse/regno/<regno>/course/<n>/ (n=1..6)
//   - rdemo   展示順位別:     https://boatrace-db.net/racer/rdemo/regno/<regno>/
//
// 仕様:
// - v2スキーマ: 1選手ファイルに「自艇/他艇（コース別）」「平均ST」「負け決まり手」「展示順位別」を集約
// - 既存ファイルが12時間以内ならスキップ（STATS_FRESH_HOURSで調整可）
// - 404はリトライ1回（他: 2〜3回程度）
// - todayの出走一覧から自動検出（public/programs/v2/today/*/*.json） or env RACERS で指定
//
// 使い方（一例）:
//   node scripts/fetch-stats.js
//   RACERS=4349,3156 node scripts/fetch-stats.js
//   STATS_DELAY_MS=2000 node scripts/fetch-stats.js

import { load as loadHTML } from "cheerio";
import fs from "node:fs/promises";
import fss from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

// -------------------------------
// 定数 / 環境
// -------------------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PUBLIC_DIR   = path.resolve(__dirname, "..", "public");
const OUTPUT_DIR   = path.join(PUBLIC_DIR, "stats", "v2", "racers");
const TODAY_FULL   = path.join(PUBLIC_DIR, "programs", "v2", "today");
const TODAY_SLIM   = path.join(PUBLIC_DIR, "programs-slim", "v2", "today");

const WAIT_MS_BETWEEN_RACERS         = Number(process.env.STATS_DELAY_MS || 3000);
const WAIT_MS_BETWEEN_COURSE_PAGES   = Number(process.env.COURSE_WAIT_MS || 1200);
const FRESH_HOURS                    = Number(process.env.STATS_FRESH_HOURS || 12); // 12時間でスキップ
const ENV_RACERS                     = (process.env.RACERS || "").trim();
const ENV_RACERS_LIMIT               = Number(process.env.RACERS_LIMIT ?? "");
const ENV_BATCH                      = Number(process.env.STATS_BATCH ?? "");

// リトライ方針
const RETRIES_404 = 1; // 指定通り: 404は1回だけ再試行
const RETRIES_DEF = 2; // 他は控えめ

// -------------------------------
// ユーティリティ
// -------------------------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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
async function ensureDir(p) {
  await fs.mkdir(p, { recursive: true });
}
function isFreshEnough(stat) {
  if (!stat?.mtimeMs) return false;
  const ageMs = Date.now() - stat.mtimeMs;
  return ageMs <= FRESH_HOURS * 3600 * 1000;
}

// 汎用フェッチ（404だけリトライ1、他は控えめ）
async function fetchHtml(url, {
  timeoutMs = 20000,
  baseDelayMs = 2200,
  jitter = 0.35
} = {}) {
  let tries = 0;
  let maxRetries = RETRIES_DEF;
  let lastStatus = 0;

  while (true) {
    tries++;

    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), timeoutMs);
    let res;
    try {
      res = await fetch(url, {
        signal: ac.signal,
        headers: {
          "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127 Safari/537.36",
          "accept": "text/html,application/xhtml+xml",
          "accept-language": "ja,en;q=0.9",
          "referer": "https://boatrace-db.net/",
          "cache-control": "no-cache",
        },
      });
    } catch (e) {
      clearTimeout(t);
      if (tries > maxRetries + 1) throw new Error(`NET ${e.message} @ ${url}`);
      const delay = Math.round(baseDelayMs * Math.pow(1.6, tries - 1) * (1 - jitter + Math.random() * 2*jitter));
      await sleep(delay);
      continue;
    }
    clearTimeout(t);

    lastStatus = res.status;

    if (res.ok) {
      return await res.text();
    }

    // ステータスに応じてリトライ回数を決め直す（404は特別に1）
    const retriable = [403, 404, 408, 429, 500, 502, 503, 504];
    if (!retriable.includes(res.status)) {
      const body = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status} ${res.statusText} @ ${url} ${body?.slice(0, 160)}`);
    }

    // 404 のときだけ最大1回再試行
    if (res.status === 404) {
      maxRetries = RETRIES_404;
    }

    if (tries > maxRetries + 1) {
      const body = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status} ${res.statusText} (exceeded retries) @ ${url} ${body?.slice(0, 160)}`);
    }

    const factor = (res.status === 403 || res.status === 429 || res.status === 503) ? 2.0 : 1.35;
    const delay = Math.round(baseDelayMs * Math.pow(factor, tries - 1) * (1 - jitter + Math.random() * 2*jitter));
    await sleep(delay);
  }
}

// -------------------------------
// 解析（/course/<n>/ ページ）
// -------------------------------

// 「◯コース進入時の全艇成績」：自艇＋他艇（k=1..6 だが自艇のk==nはothersに入れない）
function parseAllBoatsPerf($, selfCourse) {
  const $tbl = mustTableByHeader($, ["コース", "出走数", "1着率", "2連対率", "3連対率"]);
  if (!$tbl) return { self: null, others: {} };

  const { headers, rows } = parseTable($, $tbl);
  const iCourse = headerIndex(headers, "コース");
  const iStarts = headerIndex(headers, "出走数");
  const iTop1   = headerIndex(headers, "1着率");
  const iTop2   = headerIndex(headers, "2連対率");
  const iTop3   = headerIndex(headers, "3連対率");

  let self = null;
  const others = {};

  for (const r of rows) {
    const label = r[iCourse] || r[0] || "";
    const m = label.match(/([1-6])\s*コース/);
    if (!m) continue;
    const k = Number(m[1]);

    const rec = {
      starts: toNumber(r[iStarts]),
      top1Rate: toNumber(r[iTop1]),
      top2Rate: toNumber(r[iTop2]),
      top3Rate: toNumber(r[iTop3]),
      winKimarite: {} // 後で決まり手表から入れる
    };

    if (label.includes("（自艇）")) {
      self = rec;
    } else {
      // 自艇コースと同じ番号の"他艇"行は存在しない想定だが、あっても区別して格納
      others[k] = rec;
    }
  }

  return { self, others };
}

// 「◯コース進入時の全艇決まり手」：勝ち決まり手を自艇/他艇へ割当て
function parseAllBoatsWinKimarite($, selfCourse) {
  const $tbl = mustTableByHeader($, ["コース", "出走数", "1着数", "逃げ", "差し", "まくり"]);
  if (!$tbl) return { self: null, others: {} };

  const { headers, rows } = parseTable($, $tbl);
  const keys = headers.slice(3).map(normalizeKimariteKey);

  let self = null;
  const others = {};

  for (const r of rows) {
    const label = r[0] || "";
    const m = label.match(/([1-6])\s*コース/);
    if (!m) continue;
    const k = Number(m[1]);

    const obj = {};
    keys.forEach((k2, i) => {
      const raw = r[3 + i] || "0";
      const count = toNumber((raw.match(/(\d+)/) || [])[1] || 0);
      obj[k2] = count ?? 0;
    });

    if (label.includes("（自艇）")) self = obj;
    else others[k] = obj;
  }

  return { self, others };
}

// 平均ST（自艇コース視点）: 既存ロジック
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

// 負け決まり手（自艇コース視点）
function parseLoseKimariteFromCoursePage($) {
  const $tbl = mustTableByHeader($, ["コース", "出走数", "1着数", "逃げ", "差し", "まくり"]);
  if (!$tbl) return null;

  const { headers, rows } = parseTable($, $tbl);
  const iCourse = headerIndex(headers, "コース");
  const keys = headers.slice(3).map(normalizeKimariteKey);

  const lose = Object.fromEntries(keys.map(k => [k, 0]));
  for (const r of rows) {
    const label = r[iCourse] || "";
    if (label.includes("（自艇）")) continue; // 相手艇の勝ち決まり手 = 自艇の負け要因
    keys.forEach((k, i) => {
      const v = r[3 + i];
      const num = v ? Number((v.match(/(\d+)/) || [])[1]) : NaN;
      if (Number.isFinite(num)) lose[k] += num;
    });
  }
  return lose;
}

// 展示タイム順位別（rdemo）
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
    });
  }
  items.sort((a,b)=>a.rank-b.rank);
  return items.length ? items : null;
}

// -------------------------------
async function fetchOne(regno) {
  // 展示順位別
  let exTimeRank = null;
  const uRdemo = `https://boatrace-db.net/racer/rdemo/regno/${regno}/`;
  try {
    const html = await fetchHtml(uRdemo);
    const $ = loadHTML(html);
    exTimeRank = parseExTimeRankFromRdemo($);
  } catch (e) {
    console.warn(`warn: rdemo fetch/parse failed for ${regno}: ${e.message}`);
  }

  // 各コース詳細
  const courses = {};
  const coursePages = {};
  for (let c = 1; c <= 6; c++) {
    const url = `https://boatrace-db.net/racer/rcourse/regno/${regno}/course/${c}/`;
    coursePages[c] = url;
    try {
      const html = await fetchHtml(url);
      const $ = loadHTML(html);

      const avgST = parseAvgSTFromCoursePage($);
      const loseKimarite = parseLoseKimariteFromCoursePage($);

      const perf = parseAllBoatsPerf($, c);
      const winK = parseAllBoatsWinKimarite($, c);

      // 決まり手（勝ち）を自艇・他艇へ合体
      if (perf.self && winK.self) {
        perf.self.winKimarite = winK.self;
      }
      for (const k of Object.keys(perf.others || {})) {
        if (winK.others?.[k]) {
          perf.others[k].winKimarite = winK.others[k];
        }
      }

      courses[String(c)] = {
        avgST: avgST ?? null,
        loseKimarite: loseKimarite ?? null,
        self: perf.self ?? null,
        others: perf.others ?? {},
      };
      await sleep(WAIT_MS_BETWEEN_COURSE_PAGES);
    } catch (e) {
      console.warn(`warn: course page parse failed regno=${regno} course=${c}: ${e.message}`);
      courses[String(c)] = { avgST: null, loseKimarite: null, self: null, others: {} };
    }
  }

  return {
    schema: "racer-stats@2",
    regno: Number(regno),
    fetchedAt: new Date().toISOString(),
    sources: {
      rdemo: uRdemo,
      coursePages
    },
    exTimeRank: exTimeRank ?? null,
    courses
  };
}

// -------------------------------
// today配下から今日の出走選手を収集（フルの方を優先）
// -------------------------------
async function collectRacersFromToday() {
  const set = new Set();

  // programs/v2/today（フル）
  try {
    const pids = await fs.readdir(TODAY_FULL, { withFileTypes: true });
    for (const d of pids) {
      if (!d.isDirectory()) continue;
      const dir = path.join(TODAY_FULL, d.name);
      const files = await fs.readdir(dir).catch(() => []);
      for (const f of files) {
        if (!f.endsWith(".json")) continue;
        try {
          const json = JSON.parse(await fs.readFile(path.join(dir, f), "utf8"));
          const boats = json?.boats || json?.program?.boats || json?.entries || [];
          for (const b of boats) {
            // いろんなキー名に対応
            const r = b.racer_number ?? b.racerNumber ?? b.racer?.number ?? b.number ?? null;
            if (r) set.add(String(r));
          }
        } catch {}
      }
    }
  } catch {}

  // programs-slim は選手番号が無い想定が多いので補助的に読むだけ
  if (set.size === 0) {
    try {
      const pids = await fs.readdir(TODAY_SLIM, { withFileTypes: true });
      for (const d of pids) {
        if (!d.isDirectory()) continue;
        const dir = path.join(TODAY_SLIM, d.name);
        const files = await fs.readdir(dir).catch(() => []);
        for (const f of files) {
          if (!f.endsWith(".json")) continue;
          try {
            const json = JSON.parse(await fs.readFile(path.join(dir, f), "utf8"));
            const boats = json?.boats || json?.program?.boats || json?.entries || [];
            for (const b of boats) {
              const r = b.racer_number ?? b.racerNumber ?? b.racer?.number ?? b.number ?? null;
              if (r) set.add(String(r));
            }
          } catch {}
        }
      }
    } catch {}
  }

  return [...set];
}

// 既存が新鮮（<=12h）ならスキップ
async function shouldSkip(regno) {
  const outPath = path.join(OUTPUT_DIR, `${regno}.json`);
  try {
    const st = await fs.stat(outPath);
    return isFreshEnough(st);
  } catch {
    return false;
  }
}

// -------------------------------
// メイン
// -------------------------------
async function main() {
  await ensureDir(OUTPUT_DIR);

  let racers = [];
  if (ENV_RACERS) {
    racers = ENV_RACERS.split(",").map(s => s.trim()).filter(Boolean);
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
    console.log("No racers to fetch. (Put today programs or set RACERS)");
    return;
  }

  console.log(`process ${racers.length} racers` +
    (ENV_RACERS ? " (env RACERS specified)" : "") +
    (ENV_RACERS_LIMIT ? ` (limit=${ENV_RACERS_LIMIT})` : "") +
    (ENV_BATCH ? ` (batch=${ENV_BATCH})` : ""));

  let ok = 0, skip = 0, ng = 0;

  for (const regno of racers) {
    try {
      if (await shouldSkip(regno)) {
        console.log(`⏭  skip fresh ${regno} (<${FRESH_HOURS}h)`);
        skip++;
      } else {
        const data = await fetchOne(regno);
        const outPath = path.join(OUTPUT_DIR, `${regno}.json`);
        await fs.writeFile(outPath, JSON.stringify(data, null, 2), "utf8");
        console.log(`✅ wrote ${path.relative(PUBLIC_DIR, outPath)}`);
        ok++;
      }
    } catch (e) {
      console.warn(`❌ ${regno}: ${e.message}`);
      ng++;
    }
    await sleep(WAIT_MS_BETWEEN_RACERS);
  }

  // メタ（v2）
  await ensureDir(path.join(PUBLIC_DIR, "debug"));
  await fs.writeFile(
    path.join(PUBLIC_DIR, "debug", "stats-meta-v2.json"),
    JSON.stringify(
      {
        version: "v2",
        status: 200,
        fetchedAt: new Date().toISOString(),
        racers: racers.map((r) => Number(r)),
        success: ok,
        skipped: skip,
        failed: ng,
        freshHours: FRESH_HOURS,
        retry404: RETRIES_404,
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
