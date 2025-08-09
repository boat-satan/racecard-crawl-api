// scripts/fetch-exhibition-direct.js
// Usage:
//   TARGET_DATE=20250809 TARGET_PIDS=02 TARGET_RACES=7R node scripts/fetch-exhibition-direct.js --skip-existing
//   node scripts/fetch-exhibition-direct.js 20250809 02 1..12
//
// 出力先: public/exhibition/v1/<date>/<pid>/<race>.json
//
// 取得対象: beforeinfo（直前情報）
// 生成形式:
// {
//   date, pid, race, source, mode: "beforeinfo",
//   generatedAt,
//   entries: [
//     { lane, number, name, weight, tenjiTime, tilt, st, stFlag }
//   ]
// }

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { load as loadHTML } from "cheerio";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function log(...args) {
  console.log("[beforeinfo]", ...args);
}

function usageAndExit() {
  console.error(
    "Usage: node scripts/fetch-exhibition-direct.js <YYYYMMDD> <pid:01..24> <race: 1R|1..12|1,3,5R...>\n" +
      "   or set env TARGET_DATE / TARGET_PIDS / TARGET_RACES"
  );
  process.exit(1);
}

function normRaceToken(tok) {
  return parseInt(String(tok).replace(/[^0-9]/g, ""), 10);
}

function expandRaces(expr) {
  if (!expr) return [];
  const parts = String(expr).split(",").map((s) => s.trim()).filter(Boolean);
  const out = new Set();
  for (const p of parts) {
    const m = p.match(/^(\d+)[Rr]?\.\.(\d+)[Rr]?$/);
    if (m) {
      const a = parseInt(m[1], 10);
      const b = parseInt(m[2], 10);
      const [start, end] = a <= b ? [a, b] : [b, a];
      for (let i = start; i <= end; i++) out.add(i);
    } else {
      const n = normRaceToken(p);
      if (!Number.isNaN(n) && n >= 1 && n <= 12) out.add(n);
    }
  }
  return [...out].sort((a, b) => a - b);
}

function ensureDirSync(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

async function writeJSON(file, data) {
  ensureDirSync(path.dirname(file));
  await fsp.writeFile(file, JSON.stringify(data, null, 2));
}

const argvDate = process.argv[2];
const argvPid = process.argv[3];
const argvRace = process.argv[4];

const DATE = process.env.TARGET_DATE || argvDate || "";
const PIDS = (process.env.TARGET_PIDS || argvPid || "").split(",").map((s) => s.trim()).filter(Boolean);
const RACES_EXPR = process.env.TARGET_RACES || argvRace || "";
const SKIP_EXISTING = process.argv.includes("--skip-existing");

if (!DATE || PIDS.length === 0 || !RACES_EXPR) usageAndExit();

const RACES = expandRaces(RACES_EXPR);
if (RACES.length === 0) usageAndExit();

log(`date=${DATE} pids=${PIDS.join(",")} races=${RACES.join(",")}`);

async function fetchBeforeinfo({ date, pid, raceNo }) {
  const url = `https://www.boatrace.jp/owpc/pc/race/beforeinfo?hd=${date}&jcd=${pid}&rno=${raceNo}`;
  log("GET", url);
  const res = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0",
      "accept-language": "ja,en;q=0.8",
    },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
  const html = await res.text();
  return { url, html };
}

function parseBeforeinfo(html, { date, pid, raceNo, url }) {
  const $ = loadHTML(html);

  // ST情報取得
  const stByLane = {};
  $("div.table1_boatImage1").each((_, el) => {
    const lane = parseInt($(el).find(".table1_boatImage1Number").text().trim(), 10);
    const timeText = $(el).find(".table1_boatImage1Time").text().trim();
    if (lane >= 1 && lane <= 6) stByLane[lane] = timeText || "";
  });

  const entries = [];
  const tbodies = $("table.is-w748 tbody");

  tbodies.each((i, tbody) => {
    const lane = i + 1;
    const $tb = $(tbody);

    let number = "";
    let name = "";
    const profA = $tb.find('a[href*="toban="]').first();
    if (profA.length) {
      const m = (profA.attr("href") || "").match(/toban=(\d{4})/);
      if (m) number = m[1];
      name = profA.text().replace(/\s+/g, " ").trim();
    }

    const tds = $tb.find("tr").first().find("td");
    const weight = (tds.eq(0).text() || "").replace(/\s+/g, "").trim();
    const tenjiTime = (tds.eq(1).text() || "").trim();
    const tilt = (tds.eq(2).text() || "").trim();

    const st = stByLane[lane] || "";
    const stFlag = st.startsWith("F") ? "F" : "";

    entries.push({ lane, number, name, weight, tenjiTime, tilt, st, stFlag });
  });

  return {
    date,
    pid,
    race: `${raceNo}R`,
    source: url,
    mode: "beforeinfo",
    generatedAt: new Date().toISOString(),
    entries,
  };
}

async function main() {
  for (const pid of PIDS) {
    for (const raceNo of RACES) {
      const outPath = path.join(__dirname, "..", "public", "exhibition", "v1", DATE, pid, `${raceNo}R.json`);

      if (SKIP_EXISTING && fs.existsSync(outPath)) {
        log("skip existing:", path.relative(process.cwd(), outPath));
        continue;
      }

      try {
        const { url, html } = await fetchBeforeinfo({ date: DATE, pid, raceNo });
        const data = parseBeforeinfo(html, { date: DATE, pid, raceNo, url });
        await writeJSON(outPath, data);
        log("saved:", path.relative(process.cwd(), outPath));
      } catch (err) {
        console.error(`Failed: date=${DATE} pid=${pid} race=${raceNo} -> ${String(err)}`);
      }
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
