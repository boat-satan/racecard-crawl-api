// scripts/fetch-exhibition-direct.js
// Usage:
//   TARGET_DATE=20250809 TARGET_PIDS=02 TARGET_RACES=7R node scripts/fetch-exhibition-direct.js --skip-existing
//   node scripts/fetch-exhibition-direct.js 20250809 02 1..12
//   ### AUTO 運用（締切T-15分以降は常に取得）
//   TARGET_DATE=20250809 TARGET_PIDS=02 TARGET_RACES=auto node scripts/fetch-exhibition-direct.js --skip-existing
//
// 出力先: public/exhibition/v1/<date>/<pid>/<race>.json
//
// 取得対象: beforeinfo（直前情報）
// 生成形式:
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
    "Usage: node scripts/fetch-exhibition-direct.js <YYYYMMDD> <pid:01..24> <race: 1R|1..12|1,3,5R...|auto>\n" +
      "   or set env TARGET_DATE / TARGET_PIDS / TARGET_RACES"
  );
  process.exit(1);
}

function normRaceToken(tok) {
  return parseInt(String(tok).replace(/[^0-9]/g, ""), 10);
}

function expandRaces(expr) {
  if (!expr) return [];
  if (String(expr).toLowerCase() === "auto") return ["auto"];
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

// ---------- input ----------
const argvDate = process.argv[2];
const argvPid  = process.argv[3];
const argvRace = process.argv[4];

const DATE       = process.env.TARGET_DATE  || argvDate  || "";
const PIDS       = (process.env.TARGET_PIDS || argvPid   || "").split(",").map(s => s.trim()).filter(Boolean);
const RACES_EXPR = process.env.TARGET_RACES || argvRace  || "";
const SKIP_EXISTING = process.argv.includes("--skip-existing");

// AUTO 運用のトリガ分（既定15分）
const AUTO_TRIGGER_MIN = Number(process.env.AUTO_TRIGGER_MIN || 15);

if (!DATE || PIDS.length === 0 || !RACES_EXPR) usageAndExit();

const RACES = expandRaces(RACES_EXPR);
if (RACES.length === 0) usageAndExit();

log(`date=${DATE} pids=${PIDS.join(",")} races=${RACES.join(",")}`);

// ---------- helpers: 締切読取（柔軟にキーを探索） ----------
function toJstDate(dateYYYYMMDD, hhmm) {
  // hhmm: "HH:MM"
  return new Date(`${dateYYYYMMDD.slice(0,4)}-${dateYYYYMMDD.slice(4,6)}-${dateYYYYMMDD.slice(6,8)}T${hhmm}:00+09:00`);
}

function tryParseTimeString(s) {
  if (!s || typeof s !== "string") return null;
  // 例: "14:39", "14:39頃", "14:39締切"
  const m = s.match(/(\d{1,2}):(\d{2})/);
  if (!m) return null;
  const hh = m[1].padStart(2, "0");
  const mm = m[2];
  return `${hh}:${mm}`;
}

async function loadRaceDeadlineHHMM(date, pid, raceNo) {
  // 探索パス（programs と programs-slim の両方）
  const relPaths = [
    path.join("public", "programs", "v2", date, pid, `${raceNo}R.json`),
    path.join("public", "programs-slim", "v2", date, pid, `${raceNo}R.json`),
  ];
  for (const rel of relPaths) {
    const abs = path.join(__dirname, "..", rel);
    if (!fs.existsSync(abs)) continue;
    try {
      const j = JSON.parse(await fsp.readFile(abs, "utf8"));
      // よくあるキー候補を総当り
      const candidates = [
        j.deadlineJST, j.closeTimeJST, j.deadline, j.closingTime, j.startTimeJST, j.postTimeJST,
        j.scheduledTimeJST, j.raceCloseJST, j.startAt, j.closeAt,
        // ネスト候補
        j.info?.deadlineJST, j.info?.closeTimeJST, j.meta?.deadlineJST, j.meta?.closeTimeJST,
      ].filter(Boolean);

      for (const c of candidates) {
        // ISOの場合
        if (typeof c === "string" && c.includes("T") && c.match(/:\d{2}/)) {
          const dt = new Date(c);
          if (!isNaN(dt)) {
            const hh = String(dt.getHours()).padStart(2,"0");
            const mm = String(dt.getMinutes()).padStart(2,"0");
            return `${hh}:${mm}`;
          }
        }
        // HH:MM 抜き出し
        const hhmm = tryParseTimeString(String(c));
        if (hhmm) return hhmm;
      }

      // 文字列全走査（最後の手段）
      const raw = JSON.stringify(j);
      const m = raw.match(/(\d{1,2}):(\d{2})/);
      if (m) return `${m[1].padStart(2,"0")}:${m[2]}`;
    } catch {}
  }
  return null;
}

async function pickRacesAuto(date, pid) {
  const now = Date.now();
  const nowMin = Math.floor(now / 60000);
  const out = [];
  for (let r = 1; r <= 12; r++) {
    const hhmm = await loadRaceDeadlineHHMM(date, pid, r);
    if (!hhmm) continue; // 締切不明なら見送り
    const deadline = toJstDate(date, hhmm);
    const triggerMin = Math.floor((deadline.getTime() - AUTO_TRIGGER_MIN * 60000) / 60000);
    // 15分前以降は常に取得対象（締切後も拾う。上限を付けたい場合はここで制限可）
    if (nowMin >= triggerMin) out.push(r);
  }
  return out;
}

// ---------- core ----------
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
  const tbodies = $('table.is-w748 tbody'); // 左の直前情報表は tbody×6

  tbodies.each((i, tbody) => {
    const lane = i + 1;
    const $tb = $(tbody);

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

    const firstRowTds = $tb.find("tr").first().find("td").toArray();
    let weight = "", tenjiTime = "", tilt = "";

    const texts = firstRowTds.map(td =>
      ($(td).text() || "").replace(/\s+/g, "").trim()
    );
    const kgIdx = texts.findIndex(t => /kg$/i.test(t));
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
    // AUTO 指定なら、締切T-15分を過ぎたレースを自動選別
    let raceList;
    if (RACES.length === 1 && RACES[0] === "auto") {
      raceList = await pickRacesAuto(DATE, pid);
      log(`auto-picked races (${pid}): ${raceList.join(", ") || "(none)"}`);
      if (raceList.length === 0) continue;
    } else {
      raceList = RACES;
    }

    for (const raceNo of raceList) {
      const outPath = path.join(
        __dirname, "..", "public", "exhibition", "v1", DATE, pid, `${raceNo}R.json`
      );

      if (SKIP_EXISTING && fs.existsSync(outPath)) {
        log("skip existing:", path.relative(process.cwd(), outPath));
        continue;
      }

      try {
        const { url, html } = await fetchBeforeinfo({ date: DATE, pid, raceNo });
        const data = parseBeforeinfo(html, { date: DATE, pid, raceNo, url });
        // 404やテーブル欠損などで entries が空なら保存しない
        if (!data.entries || data.entries.length === 0) {
          log(`no entries -> skip save: ${DATE}/${pid}/${raceNo}R`);
          continue;
        }
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
