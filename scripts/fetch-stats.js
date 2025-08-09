// scripts/fetch-stats.js
// Node v20 / ESM / cheerio v1.x
// 出力: public/stats/v1/racers/<regno>.json
// 参照元：
//  - コース別成績/決まり手: https://boatrace-db.net/racer/index2/regno/<regno>/
//  - 展示タイム順位別成績:   https://boatrace-db.net/racer/rdemo/regno/<regno>/

import { load } from "cheerio";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

// -------------------------------
// 設定
// -------------------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PUBLIC_DIR = path.resolve(__dirname, "..", "public");
const TODAY_ROOTS = [
  path.join(PUBLIC_DIR, "programs", "v2", "today"),
  path.join(PUBLIC_DIR, "programs-slim", "v2", "today"),
];

const OUTPUT_DIR = path.join(PUBLIC_DIR, "stats", "v1", "racers");

// -------------------------------
// ユーティリティ
// -------------------------------
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
  return (t ?? "")
    .replace(/\u00A0/g, " ")
    .replace(/\s+/g, " ")
    .replace(/^[\s\n\t]+|[\s\n\t]+$/g, "");
}

function toNumber(v) {
  if (v == null) return null;
  const s = String(v).replace(/[,%]/g, "");
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

// 見出しを手掛かりに最も近い table を拾う
function findTableByTitle($, titleLike) {
  const $cands = $(
    `h1,h2,h3,h4,strong,b,legend,th,td,div,span,p:contains("${titleLike}")`
  );
  for (const el of $cands.toArray()) {
    const $el = $(el);
    const $tbl =
      $el.nextAll("table").first().filter(":has(thead,tbody)").first().length
        ? $el.nextAll("table").first()
        : $el.parent().nextAll("table").first().length
        ? $el.parent().nextAll("table").first()
        : $el.closest("section,div,article").find("table").first();
    if ($tbl && $tbl.length > 0) return $tbl;
  }
  return null;
}

function parseTable($tbl) {
  const headers = [];
  $tbl.find("thead th, thead td").each((_, th) => {
    headers.push(normText($(th).text()));
  });
  if (headers.length === 0) {
    const firstRow = $tbl.find("tr").first();
    firstRow.find("th,td").each((_, th) => headers.push(normText($(th).text())));
  }
  const rows = [];
  $tbl.find("tbody tr").each((_, tr) => {
    const cells = [];
    $(tr)
      .find("th,td")
      .each((_, td) => cells.push(normText($(td).text())));
    if (cells.length > 0) rows.push(cells);
  });
  return { headers, rows };
}

function headerIndexMap(headers, keys) {
  const map = {};
  for (const k of keys) {
    const idx = headers.findIndex((h) => h.includes(k));
    map[k] = idx >= 0 ? idx : -1;
  }
  return map;
}

// -------------------------------
// パース（index2: コース別成績／決まり手）
// -------------------------------
function parseCourseStatsFromIndex2($) {
  const $tbl =
    findTableByTitle($, "コース別成績") || findTableByTitle($, "コース成績");
  if (!$tbl) return null;

  const { headers, rows } = parseTable($tbl);
  const idx = headerIndexMap(headers, [
    "コース",
    "出走",
    "勝率",
    "1着率",
    "１着率",
    "2連対率",
    "２連対率",
    "3連対率",
    "３連対率",
  ]);

  const get = (row, key) => {
    const i =
      idx[key] >= 0
        ? idx[key]
        : key === "1着率" && idx["１着率"] >= 0
        ? idx["１着率"]
        : key === "2連対率" && idx["２連対率"] >= 0
        ? idx["２連対率"]
        : key === "3連対率" && idx["３連対率"] >= 0
        ? idx["３連対率"]
        : -1;
    return i >= 0 ? row[i] : null;
  };

  const items = [];
  for (const r of rows) {
    const courseTxt = get(r, "コース") ?? r[0] ?? "";
    const m = courseTxt.match(/([1-6])/);
    if (!m) continue;
    items.push({
      course: Number(m[1]),
      starts: toNumber(get(r, "出走")),
      winRate: toNumber(get(r, "勝率")),
      top1Rate: toNumber(get(r, "1着率")),
      top2Rate: toNumber(get(r, "2連対率")),
      top3Rate: toNumber(get(r, "3連対率")),
      raw: r,
    });
  }
  items.sort((a, b) => a.course - b.course);
  return items.length ? items : null;
}

function parseCourseKimariteFromIndex2($) {
  const $tbl =
    findTableByTitle($, "コース別決まり手") ||
    findTableByTitle($, "決まり手（コース別）") ||
    findTableByTitle($, "決まり手");
  if (!$tbl) return null;

  const { headers, rows } = parseTable($tbl);
  const kimariteKeys = headers.slice(1); // 先頭は「コース」

  const items = [];
  for (const r of rows) {
    const courseTxt = r[0] ?? "";
    const m = courseTxt.match(/([1-6])/);
    if (!m) continue;
    const detail = {};
    kimariteKeys.forEach((k, i) => {
      const v = r[i + 1];
      const count = v?.match(/(\d+)\s*(回|件|)/);
      const pct = v?.match(/([-+]?\d+(\.\d+)?)\s*%/);
      detail[k] = {
        count: count ? toNumber(count[1]) : toNumber(v),
        rate: pct ? toNumber(pct[1]) : null,
        raw: v ?? null,
      };
    });
    items.push({ course: Number(m[1]), detail, raw: r });
  }
  items.sort((a, b) => a.course - b.course);
  return items.length ? items : null;
}

// -------------------------------
// パース（rdemo: 展示タイム順位別）
// -------------------------------
function parseExTimeRankFromRdemo($) {
  const $tbl =
    findTableByTitle($, "展示タイム順位別成績") ||
    findTableByTitle($, "展示タイム順位");
  if (!$tbl) return null;

  const { headers, rows } = parseTable($tbl);
  const idx = headerIndexMap(headers, ["順位", "勝率", "2連対率", "3連対率"]);

  const items = [];
  for (const r of rows) {
    const rankTxt = idx["順位"] >= 0 ? r[idx["順位"]] : r[0] ?? "";
    const m = rankTxt.match(/([1-6])/);
    if (!m) continue;
    items.push({
      rank: Number(m[1]),
      winRate: idx["勝率"] >= 0 ? toNumber(r[idx["勝率"]]) : null,
      top2Rate: idx["2連対率"] >= 0 ? toNumber(r[idx["2連対率"]]) : null,
      top3Rate: idx["3連対率"] >= 0 ? toNumber(r[idx["3連対率"]]) : null,
      raw: r,
    });
  }
  items.sort((a, b) => a.rank - b.rank);
  return items.length ? items : null;
}

// -------------------------------
// 取得メイン（2ページ叩く）
// -------------------------------
async function fetchOne(regno) {
  const urlIndex2 = `https://boatrace-db.net/racer/index2/regno/${regno}/`;
  const urlRdemo  = `https://boatrace-db.net/racer/rdemo/regno/${regno}/`;

  // 個別に失敗しても全体は続行
  let courseStats = null;
  let courseKimarite = null;
  let exTimeRank = null;

  try {
    const html1 = await fetchHtml(urlIndex2);
    const $1 = load(html1);
    courseStats = parseCourseStatsFromIndex2($1);
    courseKimarite = parseCourseKimariteFromIndex2($1);
  } catch (e) {
    console.warn(`warn: index2 fetch/parse failed for ${regno}: ${e.message}`);
  }

  try {
    const html2 = await fetchHtml(urlRdemo);
    const $2 = load(html2);
    exTimeRank = parseExTimeRankFromRdemo($2);
  } catch (e) {
    console.warn(`warn: rdemo fetch/parse failed for ${regno}: ${e.message}`);
  }

  return {
    regno: Number(regno),
    sources: { index2: urlIndex2, rdemo: urlRdemo },
    fetchedAt: new Date().toISOString(),
    courseStats,
    courseKimarite,
    exTimeRank,
  };
}

// today ディレクトリ配下から出走選手を列挙
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

async function main() {
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

  await ensureDir(OUTPUT_DIR);

  let ok = 0, ng = 0;
  for (const regno of racers) {
    try {
      const data = await fetchOne(regno);
      const outPath = path.join(OUTPUT_DIR, `${regno}.json`);
      await fs.writeFile(outPath, JSON.stringify(data, null, 2), "utf8");
      console.log(`✅ wrote ${path.relative(PUBLIC_DIR, outPath)}`);
      ok++;
      await sleep(3000); // サイト負荷配慮
    } catch (e) {
      console.warn(`❌ ${regno}: ${e.message}`);
      ng++;
    }
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
