// scripts/fetch-stats.js
// Node v20 (fetch 同梱) / ESM / cheerio v1.x
// 参照:
//  - コース別成績・決まり手: https://boatrace-db.net/racer/index2/regno/<regno>/
//  - 展示タイム順位別成績:   https://boatrace-db.net/racer/rdemo/regno/<regno>/
// 出力: public/stats/v1/racers/<regno>.json

import { load } from "cheerio";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { exec as _exec } from "node:child_process";
import { promisify } from "node:util";

const exec = promisify(_exec);

// --------------------------------------------------
// パス
// --------------------------------------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PUBLIC_DIR = path.resolve(__dirname, "..", "public");
const TODAY_ROOTS = [
  path.join(PUBLIC_DIR, "programs", "v2", "today"),
  path.join(PUBLIC_DIR, "programs-slim", "v2", "today"),
];

const OUTPUT_DIR = path.join(PUBLIC_DIR, "stats", "v1", "racers");

// --------------------------------------------------
// 設定（環境変数で上書き可）
// --------------------------------------------------
const STATS_DELAY_MS = Number(process.env.STATS_DELAY_MS ?? 3000) || 3000; // 1選手ごと待機ms
const STATS_BATCH = Number(process.env.STATS_BATCH ?? 0) || 0; // 0=無制限 / N人だけ処理
const PUSH_EACH = process.env.STATS_PUSH_EACH === "1"; // 1なら1人ごとにpush
// 手動抽出: RACERS="4349,3156"

// --------------------------------------------------
// ユーティリティ
// --------------------------------------------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchHtml(url, { retries = 3, delayMs = 1000 } = {}) {
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

function findNearestTableByHeading($, headingText) {
  // 見出しテキストにマッチする要素を広く拾って、その近傍/後続のtableを返す
  const $cands = $(
    `h1,h2,h3,h4,strong,b,legend,th,td,div,span,p:contains("${headingText}")`
  );
  for (const el of $cands.toArray()) {
    const $el = $(el);
    // 直後 or 親の後続
    let $tbl = $el.nextAll("table").first();
    if (!$tbl || $tbl.length === 0) $tbl = $el.parent().nextAll("table").first();
    // 同コンテナから最初のtable
    if ((!$tbl || $tbl.length === 0) && $el.closest("section,article,div").length) {
      $tbl = $el.closest("section,article,div").find("table").first();
    }
    if ($tbl && $tbl.length) return $tbl;
  }
  return null;
}

function parseTable($tbl) {
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

function headerIndexMap(headers, keys) {
  const map = {};
  for (const k of keys) {
    const idx = headers.findIndex((h) => h.includes(k));
    map[k] = idx >= 0 ? idx : -1;
  }
  return map;
}

// --------------------------------------------------
// パーサ: index2 (コース別成績 / 決まり手)
// --------------------------------------------------
function parseIndex2CourseStats($) {
  const $tbl =
    findNearestTableByHeading($, "コース別成績") ||
    findNearestTableByHeading($, "コース成績");
  if (!$tbl) return null;

  const { headers, rows } = parseTable($tbl);
  const idx = headerIndexMap(headers, [
    "コース", "出走数", "勝率", "1着率", "２連対率", "3連対率", "平均ST", "平均ST順",
  ]);

  const out = [];
  for (const r of rows) {
    const courseTxt = idx["コース"] >= 0 ? r[idx["コース"]] : r[0] ?? "";
    const m = courseTxt.match(/([1-6])/);
    if (!m) continue;
    out.push({
      course: Number(m[1]),
      starts: idx["出走数"] >= 0 ? toNumber(r[idx["出走数"]]) : null,
      winRate: idx["勝率"] >= 0 ? toNumber(r[idx["勝率"]]) : null,
      top1Rate: idx["1着率"] >= 0 ? toNumber(r[idx["1着率"]]) : null,
      top2Rate: idx["２連対率"] >= 0 ? toNumber(r[idx["２連対率"]]) : null,
      top3Rate: idx["3連対率"] >= 0 ? toNumber(r[idx["3連対率"]]) : null,
      avgST: idx["平均ST"] >= 0 ? toNumber(r[idx["平均ST"]]) : null,
      avgSTRank: idx["平均ST順"] >= 0 ? toNumber(r[idx["平均ST順"]]) : null,
      raw: r,
    });
  }
  out.sort((a, b) => a.course - b.course);
  return out.length ? out : null;
}

function parseIndex2Kimarite($) {
  const $tbl =
    findNearestTableByHeading($, "コース別決まり手") ||
    findNearestTableByHeading($, "決まり手（コース別）") ||
    findNearestTableByHeading($, "決まり手");
  if (!$tbl) return null;

  const { headers, rows } = parseTable($tbl);
  const kimariteKeys = headers.slice(2); // [コース,出走数, ...決まり手列]
  const out = [];
  for (const r of rows) {
    const courseTxt = r[0] ?? "";
    const m = courseTxt.match(/([1-6])/);
    if (!m) continue;
    const detail = {};
    kimariteKeys.forEach((k, i) => {
      const v = r[i + 2]; // 2列目以降
      const cnt = v?.match(/(\d+)/);
      const pct = v?.match(/([-+]?\d+(\.\d+)?)\s*%/);
      detail[k] = {
        count: cnt ? toNumber(cnt[1]) : null,
        rate: pct ? toNumber(pct[1]) : null,
        raw: v ?? null,
      };
    });
    out.push({ course: Number(m[1]), detail, raw: r });
  }
  out.sort((a, b) => a.course - b.course);
  return out.length ? out : null;
}

// --------------------------------------------------
// パーサ: rdemo (展示タイム順位別成績)
// --------------------------------------------------
function parseRdemoExTime($) {
  const $tbl =
    findNearestTableByHeading($, "展示タイム順位別成績") ||
    findNearestTableByHeading($, "展示タイム順位");
  if (!$tbl) return null;

  const { headers, rows } = parseTable($tbl);
  const idx = headerIndexMap(headers, ["順位", "勝率", "2連対率", "3連対率", "出走数", "1着数", "2着数", "3着数"]);
  const out = [];
  for (const r of rows) {
    const rankTxt = idx["順位"] >= 0 ? r[idx["順位"]] : r[0] ?? "";
    const m = rankTxt.match(/([1-6])/);
    if (!m) continue;
    out.push({
      rank: Number(m[1]),
      starts: idx["出走数"] >= 0 ? toNumber(r[idx["出走数"]]) : null,
      firsts: idx["1着数"] >= 0 ? toNumber(r[idx["1着数"]]) : null,
      seconds: idx["2着数"] >= 0 ? toNumber(r[idx["2着数"]]) : null,
      thirds: idx["3着数"] >= 0 ? toNumber(r[idx["3着数"]]) : null,
      winRate: idx["勝率"] >= 0 ? toNumber(r[idx["勝率"]]) : null,
      top2Rate: idx["2連対率"] >= 0 ? toNumber(r[idx["2連対率"]]) : null,
      top3Rate: idx["3連対率"] >= 0 ? toNumber(r[idx["3連対率"]]) : null,
      raw: r,
    });
  }
  out.sort((a, b) => a.rank - b.rank);
  return out.length ? out : null;
}

// --------------------------------------------------
// 1選手分の取得
// --------------------------------------------------
async function fetchOne(regno) {
  const reg = String(regno).trim();
  const urlIndex2 = `https://boatrace-db.net/racer/index2/regno/${reg}/`;
  const urlRdemo  = `https://boatrace-db.net/racer/rdemo/regno/${reg}/`;

  // index2
  let courseStats = null;
  let courseKimarite = null;
  try {
    const html = await fetchHtml(urlIndex2, { retries: 3, delayMs: 1500 });
    const $ = load(html);
    courseStats = parseIndex2CourseStats($);
    courseKimarite = parseIndex2Kimarite($);
  } catch (e) {
    console.warn(`warn: index2 fetch/parse failed for ${reg}: ${e.message}`);
  }

  // rdemo
  let exTimeRank = null;
  try {
    const html = await fetchHtml(urlRdemo, { retries: 3, delayMs: 1500 });
    const $ = load(html);
    exTimeRank = parseRdemoExTime($);
  } catch (e) {
    console.warn(`warn: rdemo fetch/parse failed for ${reg}: ${e.message}`);
  }

  return {
    regno: Number(reg),
    fetchedAt: new Date().toISOString(),
    sources: {
      index2: urlIndex2,
      rdemo: urlRdemo,
    },
    courseStats,     // [{course, starts, winRate, top1Rate, top2Rate, top3Rate, avgST, avgSTRank, raw}]
    courseKimarite,  // [{course, detail:{逃げ:{count,rate},…}, raw}]
    exTimeRank,      // [{rank, starts, firsts, seconds, thirds, winRate, top2Rate, top3Rate, raw}]
  };
}

// --------------------------------------------------
// today 配下から今日の出走選手を列挙
// --------------------------------------------------
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
      let files = [];
      try {
        files = await fs.readdir(dayDir);
      } catch {}
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

async function writeAndStage(relPath, data) {
  const abs = path.join(PUBLIC_DIR, relPath);
  await fs.mkdir(path.dirname(abs), { recursive: true });
  await fs.writeFile(abs, data, "utf8");
  await exec(`git add ${JSON.stringify(relPath)}`);
  return abs;
}

// --------------------------------------------------
// メイン
// --------------------------------------------------
async function main() {
  // Git user は workflow 側で設定済みを想定
  let racers = [];
  if (process.env.RACERS?.trim()) {
    racers = process.env.RACERS.split(",").map((s) => s.trim()).filter(Boolean);
  } else {
    racers = await collectRacersFromToday();
  }

  if (STATS_BATCH > 0 && racers.length > STATS_BATCH) {
    racers = racers.slice(0, STATS_BATCH);
    console.log(`batch mode: process first ${STATS_BATCH} racers`);
  }

  if (racers.length === 0) {
    console.log("No racers to fetch. (Set RACERS or put today programs)");
    return;
  }

  await ensureDir(OUTPUT_DIR);

  let ok = 0, ng = 0;
  let staged = false;

  for (const regno of racers) {
    try {
      const data = await fetchOne(regno);
      const rel = path.join("stats", "v1", "racers", `${regno}.json`);
      await writeAndStage(path.join("public", rel), JSON.stringify(data, null, 2));
      console.log(`✅ wrote ${rel}`);
      ok++;
      staged = true;

      if (PUSH_EACH) {
        await exec(`git commit -m "chore: stats ${regno} [skip ci]" || true`);
        await exec(`git push || true`);
        staged = false;
      }

      await sleep(STATS_DELAY_MS);
    } catch (e) {
      console.warn(`❌ ${regno}: ${e.message}`);
      ng++;
    }
  }

  // メタ
  const meta = {
    status: 200,
    fetchedAt: new Date().toISOString(),
    racers: racers.map(Number),
    success: ok,
    failed: ng,
    delayMs: STATS_DELAY_MS,
    batch: STATS_BATCH,
  };
  await writeAndStage(
    path.join("public", "debug", "stats-meta.json"),
    JSON.stringify(meta, null, 2)
  );
  staged = true;

  if (staged) {
    await exec(`git commit -m "chore: publish stats (${ok} ok/${ng} ng) [skip ci]" || true`);
    await exec(`git push || true`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
