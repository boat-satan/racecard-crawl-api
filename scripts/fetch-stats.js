// scripts/fetch-stats.js
// Node v20 / ESM / cheerio v1.x
// 出力: public/stats/v1/racers/<regno>.json
// 取得元:
//  - index2: https://boatrace-db.net/racer/index2/regno/<regno>/
//  - rdemo : https://boatrace-db.net/racer/rdemo/regno/<regno>/

import { load } from "cheerio";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PUBLIC_DIR = path.resolve(__dirname, "..", "public");
const TODAY_ROOTS = [
  path.join(PUBLIC_DIR, "programs", "v2", "today"),
  path.join(PUBLIC_DIR, "programs-slim", "v2", "today"),
];

const OUTPUT_DIR = path.join(PUBLIC_DIR, "stats", "v1", "racers");

// ---------- utils ----------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchHtml(url, { retries = 3, delayMs = 800 } = {}) {
  let lastErr;
  for (let i = 0; i <= retries; i++) {
    try {
      const res = await fetch(url, {
        headers: {
          "user-agent":
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118 Safari/537.36",
          accept: "text/html,application/xhtml+xml",
        },
      });
      if (res.ok) return await res.text();
      lastErr = new Error(`${res.status} ${res.statusText}`);
    } catch (e) {
      lastErr = e;
    }
    if (i < retries) await sleep(delayMs);
  }
  throw new Error(`GET ${url} failed after ${retries + 1} tries: ${lastErr?.message ?? ""}`);
}

const norm = (t) =>
  String(t ?? "")
    .replace(/\u00A0/g, " ")
    .replace(/\s+/g, " ")
    .trim();

function num(v) {
  if (v == null) return null;
  const s = String(v).replace(/[,%]/g, "");
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function parseTable($tbl) {
  const headers = [];
  // theadが無いケース対策
  const $theadCells = $tbl.find("thead th, thead td");
  if ($theadCells.length) {
    $theadCells.each((_, th) => headers.push(norm($(th).text())));
  } else {
    const $first = $tbl.find("tr").first();
    $first.find("th,td").each((_, th) => headers.push(norm($(th).text())));
  }
  const rows = [];
  $tbl.find("tbody tr").each((_, tr) => {
    const cells = [];
    $(tr)
      .find("th,td")
      .each((_, td) => cells.push(norm($(td).text())));
    if (cells.length) rows.push(cells);
  });
  return { headers, rows };
}

// ---------- parsers: index2 ----------
function parseCourseStatsFromIndex2($) {
  // table.tRacerCourse が最優先
  let $tbl = $("table.tRacerCourse").first();
  if (!$tbl.length) {
    // 見出しフォールバック
    const $h = $(":contains('コース別成績')").filter((_, el) => /コース別成績/.test($(el).text())).first();
    if ($h.length) $tbl = $h.nextAll("table").first();
  }
  if (!$tbl.length) return null;

  const { headers, rows } = parseTable($tbl);
  const idx = {
    course: headers.findIndex((h) => h.includes("コース")),
    race: headers.findIndex((h) => h.includes("出走")),
    winCnt: headers.findIndex((h) => h.includes("1着数")),
    winRate: headers.findIndex((h) => h.includes("1着率")),
    top2: headers.findIndex((h) => h.includes("2連対率")),
    top3: headers.findIndex((h) => h.includes("3連対率")),
    st: headers.findIndex((h) => h.includes("平均ST")),
    stRank: headers.findIndex((h) => h.includes("平均ST順")),
  };

  const items = [];
  for (const r of rows) {
    const m = (r[idx.course] ?? r[0] ?? "").match(/([1-6])コース/);
    if (!m) continue;
    items.push({
      course: Number(m[1]),
      starts: num(r[idx.race]),
      winCount: num(r[idx.winCnt]),
      winRate: num(r[idx.winRate]),
      top2Rate: num(r[idx.top2]),
      top3Rate: num(r[idx.top3]),
      avgST: idx.st >= 0 ? num(r[idx.st]) : null,
      avgSTRank: idx.stRank >= 0 ? num(r[idx.stRank]) : null,
      raw: r,
    });
  }
  items.sort((a, b) => a.course - b.course);
  return items.length ? items : null;
}

function parseKimariteFromIndex2($) {
  let $tbl = $("table.tRacerTech").first();
  if (!$tbl.length) {
    const $h = $(":contains('コース別決まり手')")
      .filter((_, el) => /決まり手/.test($(el).text()))
      .first();
    if ($h.length) $tbl = $h.nextAll("table").first();
  }
  if (!$tbl.length) return null;

  const { headers, rows } = parseTable($tbl);
  const keys = headers.slice(3); // 1列目=コース,2=出走数,3=1着数, 以降=各決まり手
  const items = [];
  for (const r of rows) {
    const m = (r[0] ?? "").match(/([1-6])コース/);
    if (!m) continue;
    const detail = {};
    keys.forEach((k, i) => {
      const v = r[3 + i];
      const pct = v?.match(/([-+]?\d+(?:\.\d+)?)\s*%/);
      const cnt = v?.match(/(\d+)/);
      detail[k] = {
        count: cnt ? num(cnt[1]) : num(v),
        rate: pct ? num(pct[1]) : null,
        raw: v ?? null,
      };
    });
    items.push({ course: Number(m[1]), detail, raw: r });
  }
  items.sort((a, b) => a.course - b.course);
  return items.length ? items : null;
}

// ---------- parsers: rdemo ----------
function parseExTimeRankFromRdemo($) {
  // 見出しは「直近6か月の展示タイム順位別成績」
  // テーブルはクラスが安定しないのでヘッダで拾う
  let $tbl = $("table:contains('順位')").filter((_, t) => {
    const { headers } = parseTable($(t));
    return headers.some((h) => /順位/.test(h)) && headers.some((h) => /2連対率/.test(h));
  }).first();

  if (!$tbl.length) {
    const $h = $(":contains('展示タイム順位')").first();
    if ($h.length) $tbl = $h.nextAll("table").first();
  }
  if (!$tbl.length) return null;

  const { headers, rows } = parseTable($tbl);
  const idxRank = headers.findIndex((h) => h.includes("順位"));
  const idxWin = headers.findIndex((h) => h.includes("1着率"));
  const idxTop2 = headers.findIndex((h) => h.includes("2連対率"));
  const idxTop3 = headers.findIndex((h) => h.includes("3連対率"));

  const items = [];
  for (const r of rows) {
    const m = (r[idxRank] ?? r[0] ?? "").match(/([1-6])位/);
    if (!m) continue;
    items.push({
      rank: Number(m[1]),
      winRate: num(r[idxWin]),
      top2Rate: num(r[idxTop2]),
      top3Rate: num(r[idxTop3]),
      raw: r,
    });
  }
  items.sort((a, b) => a.rank - b.rank);
  return items.length ? items : null;
}

// ---------- collect racers from today ----------
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

async function ensureDir(p) {
  await fs.mkdir(p, { recursive: true });
}

// ---------- main fetch ----------
async function fetchOne(regno) {
  // index2（通算のコース別＆決まり手）
  const index2Url = `https://boatrace-db.net/racer/index2/regno/${regno}/`;
  // rdemo（展示タイム順位別）
  const rdemoUrl = `https://boatrace-db.net/racer/rdemo/regno/${regno}/`;

  let courseStats = null;
  let courseKimarite = null;
  let exTimeRank = null;

  try {
    const html = await fetchHtml(index2Url);
    const $ = load(html);
    courseStats = parseCourseStatsFromIndex2($);
    courseKimarite = parseKimariteFromIndex2($);
    if (!courseStats && !courseKimarite) {
      console.warn(`warn: index2 fetch/parse failed for ${regno}`);
    }
  } catch (e) {
    console.warn(`warn: index2 fetch/parse failed for ${regno}: ${e.message}`);
  }

  try {
    const html = await fetchHtml(rdemoUrl);
    const $ = load(html);
    exTimeRank = parseExTimeRankFromRdemo($);
    if (!exTimeRank) {
      console.warn(`warn: rdemo fetch/parse failed for ${regno}`);
    }
  } catch (e) {
    console.warn(`warn: rdemo fetch/parse failed for ${regno}: ${e.message}`);
  }

  return {
    regno: Number(regno),
    source: { index2: index2Url, rdemo: rdemoUrl },
    fetchedAt: new Date().toISOString(),
    courseStats,
    courseKimarite,
    exTimeRank,
  };
}

async function main() {
  await ensureDir(OUTPUT_DIR);

  // 単発テスト: RACERS="5044,3898" のように指定
  let racers = [];
  const env = process.env.RACERS?.trim();
  if (env) {
    racers = env.split(",").map((s) => s.trim()).filter(Boolean);
  } else {
    racers = await collectRacersFromToday();
  }

  if (racers.length === 0) {
    console.log("No racers to fetch. (Set RACERS env or put today programs)");
    return;
  }

  // 途中停止しても進捗がコミットされるよう1件ずつ書き出す
  let ok = 0, ng = 0;
  for (const regno of racers) {
    try {
      const data = await fetchOne(regno);
      const outPath = path.join(OUTPUT_DIR, `${regno}.json`);
      await fs.writeFile(outPath, JSON.stringify(data, null, 2), "utf8");
      console.log(`✅ wrote ${path.relative(PUBLIC_DIR, outPath)}`);
      ok++;
      // サーバの「3秒」指示に合わせて丁寧に
      await sleep(3000);
    } catch (e) {
      console.warn(`❌ ${regno}: ${e.message}`);
      ng++;
    }
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

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
