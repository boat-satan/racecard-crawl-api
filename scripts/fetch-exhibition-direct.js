// scripts/fetch-exhibition-direct.js
// Usage:
//   TARGET_DATE=20250809 TARGET_PIDS=02 TARGET_RACES=7R node scripts/fetch-exhibition-direct.js --skip-existing
//   node scripts/fetch-exhibition-direct.js 20250809 02 1..12
//
// 追加: TARGET_RACES="AUTO_T15" をサポート（締切T-15±許容分のレースだけ自動抽出）
//      WINDOW_OFFSET_MIN（デフォルト -15）, WINDOW_TOLERANCE_MIN（デフォルト 2）
//      NOW_JST（テスト用。例 "2025-08-10T07:45:00+09:00"）
//
// 出力先: public/exhibition/v1/<date>/<pid>/<race>.json
//
// 取得対象: beforeinfo（直前情報）
// 生成形式:
// {
//   date, pid, race, source, mode: "beforeinfo",
//   generatedAt,
//   entries: [{ lane, number, name, weight, tenjiTime, tilt, st, stFlag }]
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
      "   or set env TARGET_DATE / TARGET_PIDS / TARGET_RACES (AUTO_T15 可)"
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

// ---------- JST helpers ----------
function jstNow() {
  const override = process.env.NOW_JST; // e.g., "2025-08-10T07:45:00+09:00"
  if (override) return new Date(override);
  // JST = UTC+9
  return new Date(Date.now() + 9 * 60 * 60 * 1000);
}
function toMinutes(d) {
  return Math.floor(d.getTime() / 60000);
}
function parseTimeLike(value, fallbackDateYYYYMMDD) {
  if (!value) return null;

  // 1) ISOっぽい
  const iso = new Date(value);
  if (!Number.isNaN(iso.getTime())) return iso;

  // 2) "HH:MM" のみ → fallback 日付と合成して JST とみなす
  const m = String(value).match(/^(\d{1,2}):(\d{2})$/);
  if (m && fallbackDateYYYYMMDD) {
    const h = parseInt(m[1], 10);
    const mm = parseInt(m[2], 10);
    if (h >= 0 && h <= 23 && mm >= 0 && mm <= 59) {
      const y = fallbackDateYYYYMMDD.slice(0, 4);
      const mon = fallbackDateYYYYMMDD.slice(4, 6);
      const d = fallbackDateYYYYMMDD.slice(6, 8);
      // 明示的に +09:00 で組み立てる
      const s = `${y}-${mon}-${d}T${String(h).padStart(2,"0")}:${String(mm).padStart(2,"0")}:00+09:00`;
      const dt = new Date(s);
      if (!Number.isNaN(dt.getTime())) return dt;
    }
  }

  return null;
}

// ---------- Target races by T-15 ----------
async function autoPickRacesByWindow(date, pid, { offsetMin = -15, toleranceMin = 2 } = {}) {
  // public/programs*/v2/<date>/<pid>/*R.json を探し、締切/発走時刻らしきキーからトリガ時刻を計算
  const roots = [
    path.join(__dirname, "..", "public", "programs", "v2", date, pid),
    path.join(__dirname, "..", "public", "programs-slim", "v2", date, pid),
  ];
  const files = [];
  for (const dir of roots) {
    try {
      const entries = await fsp.readdir(dir, { withFileTypes: true });
      for (const e of entries) {
        if (e.isFile() && /^[1-9]|1[0-2]R\.json$/i.test(e.name)) {
          files.push(path.join(dir, e.name));
        }
      }
      if (files.length) break; // 見つかった方を採用
    } catch {}
  }
  if (!files.length) {
    log(`autoPick: no program files for ${date}/${pid}`);
    return [];
  }

  const now = jstNow();
  const nowMin = toMinutes(now);
  const picked = [];

  for (const file of files) {
    let raceNo = null;
    const m = path.basename(file).match(/^(\d{1,2})R\.json$/i);
    if (m) raceNo = parseInt(m[1], 10);

    try {
      const txt = await fsp.readFile(file, "utf8");
      const j = JSON.parse(txt);

      // 取り得る時刻キーを総当たりで抽出
      const candidates = [
        j.deadline, j.deadlineJst, j.deadlineAt, j.deadlineTime,
        j.startTime, j.startAt, j.postTime, j.offTime,
        j.raceDeadline, j.raceStartTime,
        j.closingTime, j.closeTime,
      ].filter(Boolean);

      // entries配下やmeta配下に埋まっているケースも一応見る
      const deep = (obj) => {
        if (!obj || typeof obj !== "object") return [];
        const vals = [];
        for (const k of Object.keys(obj)) {
          const v = obj[k];
          if (v && typeof v === "object") vals.push(...deep(v));
          else if (typeof v === "string" && /(\d{1,2}:\d{2}|T\d{2}:\d{2}:\d{2})/.test(v)) vals.push(v);
        }
        return vals;
      };
      const deepCandidates = deep(j);

      const all = [...candidates, ...deepCandidates];

      let best = null;
      for (const val of all) {
        const dt = parseTimeLike(val, date);
        if (dt) { best = dt; break; }
      }
      if (!best) continue;

      // T-15（可変）
      const triggerMs = best.getTime() + offsetMin * 60000;
      const triggerMin = Math.floor(triggerMs / 60000);
      if (Math.abs(triggerMin - nowMin) <= toleranceMin) {
        if (raceNo != null) picked.push(raceNo);
      }
    } catch {
      // 解析失敗はスキップ
    }
  }

  picked.sort((a,b)=>a-b);
  log(`autoPick: ${date}/${pid} -> ${picked.join(",") || "(none)"}`);
  return picked;
}

// ---------- input ----------
const argvDate = process.argv[2];
const argvPid  = process.argv[3];
const argvRace = process.argv[4];

const DATE       = process.env.TARGET_DATE  || argvDate  || "";
const PIDS       = (process.env.TARGET_PIDS || argvPid   || "").split(",").map(s => s.trim()).filter(Boolean);
const RACES_EXPR = process.env.TARGET_RACES || argvRace  || "";
const SKIP_EXISTING = process.argv.includes("--skip-existing");

// 新規: 自動T-15パラメータ
const WINDOW_OFFSET_MIN   = Number(process.env.WINDOW_OFFSET_MIN ?? "-15");
const WINDOW_TOLERANCE_MIN= Number(process.env.WINDOW_TOLERANCE_MIN ?? "2");

if (!DATE || PIDS.length === 0 || !RACES_EXPR) usageAndExit();

const RACES = RACES_EXPR === "AUTO_T15" ? null : expandRaces(RACES_EXPR);
if (RACES_EXPR !== "AUTO_T15" && RACES.length === 0) usageAndExit();

log(`date=${DATE} pids=${PIDS.join(",")} races=${RACES_EXPR === "AUTO_T15" ? "AUTO_T15" : RACES.join(",")}`);

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

  // 右側「スタート展示」: 各 .table1_boatImage1 に (lane, ST)
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

    // 選手番号/名前: toban=XXXX の a のうち、テキスト非空のものを優先
    let number = "";
    let name = "";
    const profAs = $tb.find('a[href*="toban="]');
    profAs.each((_, a) => {
      const href = $(a).attr("href") || "";
      const m = href.match(/toban=(\d{4})/);
      if (m) number = m[1];
      const t = $(a).text().replace(/\s+/g, " ").trim();
      if (t) name = t; // 画像リンクは空、名前セルは非空
    });

    // 1行目の <td> 群から「kg を含むセル」を基点に抽出
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
      // フォールバック（まれな崩れ対策）
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
    let raceList = RACES;
    if (RACES_EXPR === "AUTO_T15") {
      raceList = await autoPickRacesByWindow(DATE, pid, {
        offsetMin: WINDOW_OFFSET_MIN,
        toleranceMin: WINDOW_TOLERANCE_MIN,
      });
      if (!raceList.length) {
        log(`no target races in window for ${DATE}/${pid}; skip`);
        continue;
      }
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
