// scripts/fetch-stats.js
// Node v20 / ESM / cheerio v1.x
// 出力: public/stats/v1/racers/<regno>.json
// 参照元:
//  - https://boatrace-db.net/racer/index2/regno/<regno>/   (コース別成績・決まり手)
//  - https://boatrace-db.net/racer/rdemo/regno/<regno>/    (展示タイム順位別)

import { load } from "cheerio";
import fs from "node:fs/promises";
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

// polite wait（サイト側の推奨 3 秒）
const WAIT_MS_BETWEEN_RACERS = 3000;

// 環境変数でテスト/制限
//   RACERS="4349,3898"  ・・・対象を直接指定
//   RACERS_LIMIT="20"   ・・・先頭 N 件だけ実行（途中で止めてもOKにする用）
const ENV_RACERS       = process.env.RACERS?.trim() || "";
const ENV_RACERS_LIMIT = Number(process.env.RACERS_LIMIT ?? "");

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
    .trim();
}

function toNumber(v) {
  if (v == null) return null;
  const s = String(v).replace(/[,%]/g, "");
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

// thead を見てヘッダ配列と行データを返す
function parseTable($, $tbl) {
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
    if (cells.length) rows.push(cells);
  });

  return { headers, rows };
}

function headerIndex(headers, keyLike) {
  return headers.findIndex((h) => h.includes(keyLike));
}

function mustTableByHeader($, keyLikes) {
  // 1) 明示クラス狙い（index2想定）
  const candidates = $("table.tRacerCourse, table.tRacerTech, table.tRacerRboat1, table.tRacerRcourseTech, table.tRacerRboat2, table");
  for (const el of candidates.toArray()) {
    const { headers } = parseTable($, $(el));
    const ok = keyLikes.every((k) => headers.some((h) => h.includes(k)));
    if (ok) return $(el);
  }
  return null;
}

// -------------------------------
// パース（index2: コース別成績）
// -------------------------------
function parseCourseStatsFromIndex2($) {
  const $tbl = mustTableByHeader($, ["コース", "出走数", "1着率", "2連対率", "3連対率"]);
  if (!$tbl) return null;

  const { headers, rows } = parseTable($, $tbl);
  const iCourse = headerIndex(headers, "コース");
  const iStarts = headerIndex(headers, "出走数");
  const iWinRt  = headerIndex(headers, "1着率");
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
      top1Rate: iWinRt >= 0 ? toNumber(r[iWinRt]) : null,
      top2Rate: iTop2  >= 0 ? toNumber(r[iTop2])  : null,
      top3Rate: iTop3  >= 0 ? toNumber(r[iTop3])  : null,
      // 直近ページには「勝率」列が無いので winRate は null にしておく
      winRate: null,
      raw: r,
    });
  }
  items.sort((a, b) => a.course - b.course);
  return items.length ? items : null;
}

// -------------------------------
// パース（index2: コース別決まり手）
// -------------------------------
function parseKimariteFromIndex2($) {
  const $tbl = mustTableByHeader($, ["コース", "出走数", "1着数", "逃げ", "差し", "まくり", "抜き", "恵まれ"]);
  if (!$tbl) return null;

  const { headers, rows } = parseTable($, $tbl);
  const iCourse = headerIndex(headers, "コース");

  const detailKeys = headers.slice(3); // 「1着数」以降の各決まり手
  const items = [];
  for (const r of rows) {
    const ct = r[iCourse] ?? r[0] ?? "";
    const m = ct.match(/([1-6])/);
    if (!m) continue;

    const detail = {};
    detailKeys.forEach((k, i) => {
      const v = r[3 + i];
      // index2 は基本「回数」だけ。% があれば拾う。
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
  items.sort((a, b) => a.course - b.course);
  return items.length ? items : null;
}

// -------------------------------
// パース（rdemo: 展示タイム順位別成績）
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
  items.sort((a, b) => a.rank - b.rank);
  return items.length ? items : null;
}

// -------------------------------
// 1 選手分取得
// -------------------------------
async function fetchOne(regno) {
  const uIndex2 = `https://boatrace-db.net/racer/index2/regno/${regno}/`;
  const uRdemo  = `https://boatrace-db.net/racer/rdemo/regno/${regno}/`;

  // index2
  let courseStats = null;
  let courseKimarite = null;
  try {
    const html = await fetchHtml(uIndex2);
    const $ = load(html);
    courseStats = parseCourseStatsFromIndex2($);
    courseKimarite = parseKimariteFromIndex2($);
  } catch (e) {
    console.warn(`warn: index2 fetch/parse failed for ${regno}: ${e.message}`);
  }

  // rdemo
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
    sources: { index2: uIndex2, rdemo: uRdemo },
    fetchedAt: new Date().toISOString(),
    courseStats,     // [{course, starts, top1Rate, top2Rate, top3Rate, winRate:null}]
    courseKimarite,  // [{course, detail:{逃げ:{count,rate},…}}]
    exTimeRank,      // [{rank, winRate, top2Rate, top3Rate}]
  };
}

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

  if (racers.length === 0) {
    console.log("No racers to fetch. (Set RACERS env or put today programs)");
    return;
  }

  console.log(
    ENV_RACERS_LIMIT
      ? `batch mode: process first ${racers.length} racers`
      : `process ${racers.length} racers`
  );

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
    // polite wait（1選手ごとに3秒）
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
