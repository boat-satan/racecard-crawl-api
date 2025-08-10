// scripts/fetch-exhibition-direct.js
// Usage:
//   node scripts/fetch-exhibition-direct.js 20250809 ALL ALL           # 全場・全R
//   node scripts/fetch-exhibition-direct.js 20250809 02,04 1..12       # 複数場・範囲R
//   node scripts/fetch-exhibition-direct.js 20250809 02 1,3,5,7        # 個別R
//   TARGET_DATE=20250809 TARGET_PIDS=ALL TARGET_RACES=ALL node scripts/fetch-exhibition-direct.js --skip-existing
//
// 出力: public/exhibition/v1/<date>/<pid>/<race>.json
// 取得: beforeinfo（直前情報）
// 形式:
// { date, pid, race, source, mode: "beforeinfo", generatedAt,
//   entries: [{ lane, number, name, weight, tenjiTime, tilt, st, stFlag }] }

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
    "Usage: node scripts/fetch-exhibition-direct.js <YYYYMMDD> <pid:01..24|ALL|comma> <race: 1..12|1..12範囲|comma|ALL>\n" +
      "  examples:\n" +
      "    node ... 20250809 ALL ALL\n" +
      "    node ... 20250809 02,04 1..12\n" +
      "    TARGET_DATE=20250809 TARGET_PIDS=02 TARGET_RACES=1,3,5 node ..."
  );
  process.exit(1);
}

function normRaceToken(tok) {
  return parseInt(String(tok).replace(/[^0-9]/g, ""), 10);
}

function expandRaces(expr) {
  if (!expr) return [];
  if (String(expr).toUpperCase() === "ALL") {
    return Array.from({ length: 12 }, (_, i) => i + 1); // 1..12
  }
  const parts = String(expr).split(",").map((s) => s.trim()).filter(Boolean);
  const out = new Set();
  for (const p of parts) {
    const m = p.match(/^(\d+)[Rr]?\.\.(\d+)[Rr]?$/);
    if (m) {
      const a = parseInt(m[1], 10);
      const b = parseInt(m[2], 10);
      const [start, end] = a <= b ? [a, b] : [b, a];
      for (let i = start; i <= end; i++) if (i >= 1 && i <= 12) out.add(i);
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

// ---------- 引数/環境 ----------
const argvDate = process.argv[2];
const argvPid  = process.argv[3];
const argvRace = process.argv[4];

const DATE       = process.env.TARGET_DATE  || argvDate  || "";
const PIDS_RAW   = process.env.TARGET_PIDS  || argvPid   || "";
const RACES_EXPR = process.env.TARGET_RACES || argvRace  || "";
const SKIP_EXISTING = process.argv.includes("--skip-existing");

let PIDS = [];
if (String(PIDS_RAW).toUpperCase() === "ALL") {
  PIDS = Array.from({ length: 24 }, (_, i) => String(i + 1).padStart(2, "0")); // 01..24
} else {
  PIDS = String(PIDS_RAW)
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

if (!DATE || PIDS.length === 0 || !RACES_EXPR) usageAndExit();

const RACES = expandRaces(RACES_EXPR);
if (RACES.length === 0) usageAndExit();

log(`date=${DATE} pids=${PIDS.join(",")} races=${RACES.join(",")}`);

// ---------- 取得 & 解析 ----------
async function fetchBeforeinfo({ date, pid, raceNo }) {
  const url = `https://www.boatrace.jp/owpc/pc/race/beforeinfo?hd=${date}&jcd=${pid}&rno=${raceNo}`;
  log("GET", url);
  const res = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0",
      "accept-language": "ja,en;q=0.8",
    },
  });
  if (!res.ok) {
    // 404など -> 例外にして上位で握りつぶす（ファイルは作らない）
    throw new Error(`HTTP ${res.status} ${res.statusText}`);
  }
  const html = await res.text();
  return { url, html };
}

function parseBeforeinfo(html, { date, pid, raceNo, url }) {
  const $ = loadHTML(html);

  // 右側「スタート展示」
  const stByLane = {};
  $("div.table1_boatImage1").each((_, el) => {
    const laneText =
      $(el).find(".table1_boatImage1Number").text().trim() ||
      $(el).find('[class*="table1_boatImage1Number"]').text().trim();
    const timeText =
      $(el).find(".table1_boatImage1Time").text().trim() ||
      $(el).find('[class*="table1_boatImage1Time"]').text().trim();
    const lane = parseInt(laneText, 10);
    if (lane >= 1 && lane <= 6) stByLane[lane] = timeText || "";
  });

  const entries = [];
  const tbodies = $("table.is-w748 tbody"); // 左の直前情報表は tbody×6

  tbodies.each((i, tbody) => {
    const lane = i + 1;
    const $tb = $(tbody);

    // 選手番号/名前
    let number = "";
    let name = "";
    const profAs = $tb.find('a[href*="toban="]');
    profAs.each((_, a) => {
      const href = $(a).attr("href") || "";
      const m = href.match(/toban=(\d{4})/);
      if (m) number = m[1];
      const t = $(a).text().replace(/\s+/g, " ").trim();
      if (t) name = t;
    });

    // 1行目の <td> 群から「kg を含むセル」を起点に weight / tenji / tilt
    const firstRowTds = $tb.find("tr").first().find("td").toArray();
    let weight = "", tenjiTime = "", tilt = "";
    const texts = firstRowTds.map((td) => ($(td).text() || "").replace(/\s+/g, "").trim());
    const kgIdx = texts.findIndex((t) => /kg$/i.test(t));
    if (kgIdx !== -1) {
      weight    = texts[kgIdx]     || "";
      tenjiTime = texts[kgIdx + 1] || "";
      tilt      = texts[kgIdx + 2] || "";
    } else {
      const kgCell = $tb.find("td").filter((_, td) => /kg$/i.test($(td).text().replace(/\s+/g, "").trim())).first();
      if (kgCell.length) {
        weight = kgCell.text().replace(/\s+/g, "").trim();
        const next1 = kgCell.next("td");
        const next2 = next1.next("td");
        tenjiTime = (next1.text() || "").trim();
        tilt      = (next2.text() || "").trim();
      }
    }

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
      const outPath = path.join(
        __dirname,
        "..",
        "public",
        "exhibition",
        "v1",
        DATE,
        pid,
        `${raceNo}R.json`
      );

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
        // 404/非公開等はログだけ出してスキップ（ファイル生成しない）
        console.error(`Failed: date=${DATE} pid=${pid} race=${raceNo} -> ${String(err)}`);
      }
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
