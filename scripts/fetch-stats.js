// scripts/fetch-stats.js
// Node v20 (fetch 同梱) / ESM / cheerio v1.x を使用
// 出力: public/stats/v1/racers/<regno>.json
// 参照元: https://boatrace-db.net/racer/rcourse/regno/<regno>/

import { load } from "cheerio";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

// -------------------------------
// 設定
// -------------------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// どの選手を取得するか：
// 1) 環境変数 RACERS に「4349,3156」のようにカンマ区切りで指定
// 2) 未指定なら、public/programs/v2/today/**.json を全走査して出走選手を集める
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
    if (res.ok) {
      return await res.text();
    }
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

// 見出し（h2/h3/strong 等）を手掛かりに「次の table」を拾う。
// サイト構造差異に強めの探索。
function findTableByTitle($, titleLike) {
  // 含む要素を広めに探索
  const $cands = $(
    `h1,h2,h3,h4,strong,b,legend,th,td,div,span,p:contains("${titleLike}")`
  );

  for (const el of $cands.toArray()) {
    const $el = $(el);

    // 1) 同コンテナ内の table
    let $tbl =
      $el.nextAll("table").first() ||
      $el.parent().nextAll("table").first() ||
      $el.closest("section,div,article").find("table").first();

    if ($tbl && $tbl.length > 0) return $tbl;
  }
  return null;
}

// thead -> th をヘッダに、tbody -> tr/td を配列で取り出すジェネリックパーサ
function parseTable($tbl) {
  const headers = [];
  $tbl.find("thead th, thead td").each((_, th) => {
    headers.push(normText($(th).text()));
  });
  // thead がない場合、最初の tr をヘッダ扱い
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

// 指定した列名（部分一致可）を列インデックスにマップ
function headerIndexMap(headers, keys) {
  const map = {};
  for (const k of keys) {
    const idx = headers.findIndex((h) => h.includes(k));
    map[k] = idx >= 0 ? idx : -1;
  }
  return map;
}

// -------------------------------
// 各セクションのパース
// -------------------------------
function parseCourseStats($) {
  // 「コース別成績」テーブル
  const $tbl =
    findTableByTitle($, "コース別成績") || findTableByTitle($, "コース成績");
  if (!$tbl) return null;

  const { headers, rows } = parseTable($tbl);
  const idx = headerIndexMap(headers, [
    "コース",
    "出走",
    "勝率",
    "１着率",
    "2連対率",
    "３連対率",
  ]);

  const items = [];
  for (const r of rows) {
    // 1〜6コースの行だけ拾う
    const courseTxt =
      idx["コース"] >= 0 ? r[idx["コース"]] : r[0] ?? "";
    const m = courseTxt.match(/([1-6])/);
    if (!m) continue;

    items.push({
      course: Number(m[1]),
      starts: idx["出走"] >= 0 ? toNumber(r[idx["出走"]]) : null,
      winRate: idx["勝率"] >= 0 ? toNumber(r[idx["勝率"]]) : null,
      top1Rate: idx["１着率"] >= 0 ? toNumber(r[idx["１着率"]]) : null,
      top2Rate: idx["2連対率"] >= 0 ? toNumber(r[idx["2連対率"]]) : null,
      top3Rate: idx["３連対率"] >= 0 ? toNumber(r[idx["３連対率"]]) : null,
      raw: r,
    });
  }

  // コース順に整列
  items.sort((a, b) => a.course - b.course);
  return items.length ? items : null;
}

function parseCourseKimarite($) {
  // 「コース別決まり手」テーブル
  const $tbl =
    findTableByTitle($, "コース別決まり手") ||
    findTableByTitle($, "決まり手（コース別）") ||
    findTableByTitle($, "決まり手");

  if (!$tbl) return null;

  const { headers, rows } = parseTable($tbl);
  // 先頭列が「コース」、残りが決まり手（逃げ/差し/まくり/まくり差し/抜き/恵まれ… など）を想定
  // 見出し名はサイト側で多少違っても部分一致で拾う
  const kimariteKeys = headers.slice(1); // 1列目以外
  const items = [];
  for (const r of rows) {
    const courseTxt = r[0] ?? "";
    const m = courseTxt.match(/([1-6])/);
    if (!m) continue;

    const detail = {};
    kimariteKeys.forEach((k, i) => {
      const v = r[i + 1];
      // 例: "12 (30.0%)" / "30.0%" / "12" 等、数字や%をラフに受ける
      const percent = v?.match(/([-+]?\d+(\.\d+)?)\s*%/);
      const count = v?.match(/(\d+)\s*(回|件|)/);

      detail[k] = {
        count: count ? toNumber(count[1]) : null,
        rate: percent ? toNumber(percent[1]) : null,
        raw: v ?? null,
      };
    });

    items.push({
      course: Number(m[1]),
      detail,
      raw: r,
    });
  }

  items.sort((a, b) => a.course - b.course);
  return items.length ? items : null;
}

function parseExTimeRank($) {
  // 「展示タイム順位別成績」
  const $tbl =
    findTableByTitle($, "展示タイム順位別成績") ||
    findTableByTitle($, "展示タイム順位");

  if (!$tbl) return null;

  const { headers, rows } = parseTable($tbl);
  const idx = headerIndexMap(headers, ["順位", "勝率", "2連対率", "3連対率"]);

  const items = [];
  for (const r of rows) {
    const rankTxt =
      idx["順位"] >= 0 ? r[idx["順位"]] : r[0] ?? "";
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
// 取得メイン
// -------------------------------
async function fetchOne(regno) {
  const url = `https://boatrace-db.net/racer/rcourse/regno/${regno}/`;
  const html = await fetchHtml(url);
  const $ = load(html);

  const courseStats = parseCourseStats($);
  const courseKimarite = parseCourseKimarite($);
  const exTimeRank = parseExTimeRank($);

  return {
    regno: Number(regno),
    source: url,
    fetchedAt: new Date().toISOString(),
    courseStats, // [{ course, starts, winRate, top1Rate, top2Rate, top3Rate, raw }]
    courseKimarite, // [{ course, detail: { '逃げ':{count,rate}, … }, raw }]
    exTimeRank, // [{ rank, winRate, top2Rate, top3Rate, raw }]
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
    racers = env
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  } else {
    racers = await collectRacersFromToday();
  }

  if (racers.length === 0) {
    console.log("No racers to fetch. (Set RACERS env or put today programs)");
    return;
  }

  await ensureDir(OUTPUT_DIR);

  let ok = 0,
    ng = 0;
  for (const regno of racers) {
    try {
      const data = await fetchOne(regno);
      const outPath = path.join(OUTPUT_DIR, `${regno}.json`);
      await fs.writeFile(outPath, JSON.stringify(data, null, 2), "utf8");
      console.log(`✅ wrote ${path.relative(PUBLIC_DIR, outPath)}`);
      ok++;
      // polite
      await sleep(600);
    } catch (e) {
      console.warn(`❌ ${regno}: ${e.message}`);
      ng++;
    }
  }

  // メタ
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
