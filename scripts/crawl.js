// scripts/crawl.js
// Node v20 (fetch 同梱)
// 出力：
//   - public/programs-slim/v2/<date>/<pid>/<race>.json（軽量）+ index.json
//   - public/programs/v2/<date>/<pid>/<race>.json（フル）
// 環境変数：
//   TARGET_DATE=today | YYYYMMDD / YYYY-MM-DD（既定: today）
//   TARGET_PID=ALL | 02 | 場名 | カンマ区切り（既定: ALL → 全場）
//   TARGET_RACE=ALL | 1..12 | "1R" | カンマ区切り（既定: ALL → 全R）

import fs from "node:fs";
import path from "node:path";

const BASE_OUT_SLIM = "public/programs-slim/v2";
const BASE_OUT_FULL = "public/programs/v2";
const DEBUG_OUT     = "public/debug";

const ensureDir = (p) => fs.mkdirSync(p, { recursive: true });
const to2 = (s) => String(s).padStart(2, "0");
const outDirSlim = (date, pid) => path.join(BASE_OUT_SLIM, date, pid);
const outDirFull = (date, pid) => path.join(BASE_OUT_FULL, date, pid);

// ---- 入力 ----
const DATE_IN = (process.env.TARGET_DATE || "today").trim();
const DATE = DATE_IN.toLowerCase() === "today" ? "today" : DATE_IN.replace(/-/g, "");

const PID_IN = (process.env.TARGET_PID || "ALL").trim();
const WANT_ALL_PID = PID_IN.toUpperCase() === "ALL";
const PID_FILTERS = WANT_ALL_PID ? null
  : PID_IN.split(",").map(s => s.trim()).filter(Boolean).map(s => (/^\d+$/.test(s) ? to2(s) : s));

const RACE_IN = (process.env.TARGET_RACE || "ALL").trim();
const WANT_ALL_RACE = RACE_IN.toUpperCase() === "ALL";
const RACE_FILTERS = WANT_ALL_RACE ? null
  : RACE_IN.split(",").map(s => String(s).trim().toUpperCase())
      .map(s => s.endsWith("R") ? s : `${s}R`)
      .map(s => s.replace(/[^\d]/g, "") + "R");

// 取得元
const SRC = DATE === "today"
  ? "https://boatraceopenapi.github.io/programs/v2/today.json"
  : `https://boatraceopenapi.github.io/programs/v2/${DATE}.json`;

console.log("fetch:", SRC);

// ---- ユーティリティ ----
const pickRaceLabel = (v) => {
  const r = v.race ?? v.Race ?? v.race_number ?? v.RaceNumber;
  if (r == null) return null;
  const n = String(r).replace(/[^\d]/g, "");
  return n ? `${Number(n)}R` : null;
};
const pickStadiumCode = (v) => {
  const code = v.race_stadium_number ?? v.stadium_number ?? v.stadium ?? v.stadiumCode;
  return code != null ? to2(code) : null;
};
const pickStadiumName = (v) => v.race_stadium_name ?? v.stadium_name ?? v.stadiumName ?? null;

function emitOneRace(date, stadiumCode, stadiumName, raceLabel, raceDataLike) {
  const entriesSlim = (raceDataLike?.boats ?? raceDataLike?.entries ?? raceDataLike?.Entries ?? []).map(b => ({
    lane: b.racer_boat_number ?? b.lane ?? null,
    name: b.racer_name ?? b.name ?? null,
    class: b.racer_class_number ?? b.class ?? null,
  }));

  const deadline = raceDataLike?.race_closed_at ?? raceDataLike?.deadline ?? null;

  const fullPayload = {
    schemaVersion: "2.0",
    generatedAt: new Date().toISOString(),
    date,
    stadium: stadiumCode,
    stadiumName: stadiumName ?? null,
    race: raceLabel,
    deadline,
    gradeNumber: raceDataLike?.race_grade_number ?? null,
    title:       raceDataLike?.race_title ?? null,
    subtitle:    raceDataLike?.race_subtitle ?? null,
    distance:    raceDataLike?.race_distance ?? null,
    entries: (raceDataLike?.boats ?? raceDataLike?.entries ?? raceDataLike?.Entries ?? []).map(b => ({
      lane: b.racer_boat_number ?? b.lane ?? null,
      number: b.racer_number ?? b.number ?? null,
      name: b.racer_name ?? b.name ?? null,
      classNumber: b.racer_class_number ?? b.class ?? null,
      branchNumber: b.racer_branch_number ?? b.branch ?? null,
      birthplaceNumber: b.racer_birthplace_number ?? null,
      age: b.racer_age ?? null,
      weight: b.racer_weight ?? null,
      flyingCount: b.racer_flying_count ?? null,
      lateCount: b.racer_late_count ?? null,
      avgST: b.racer_average_start_timing ?? null,
      natTop1: b.racer_national_top_1_percent ?? null,
      natTop2: b.racer_national_top_2_percent ?? null,
      natTop3: b.racer_national_top_3_percent ?? null,
      locTop1: b.racer_local_top_1_percent ?? null,
      locTop2: b.racer_local_top_2_percent ?? null,
      locTop3: b.racer_local_top_3_percent ?? null,
      motorNumber: b.racer_assigned_motor_number ?? null,
      motorTop2: b.racer_assigned_motor_top_2_percent ?? null,
      motorTop3: b.racer_assigned_motor_top_3_percent ?? null,
      boatNumber: b.racer_assigned_boat_number ?? null,
      boatTop2: b.racer_assigned_boat_top_2_percent ?? null,
      boatTop3: b.racer_assigned_boat_top_3_percent ?? null
    }))
  };

  const slimDir = outDirSlim(date, stadiumCode);
  const fullDir = outDirFull(date, stadiumCode);
  ensureDir(slimDir); ensureDir(fullDir);

  fs.writeFileSync(
    path.join(slimDir, `${raceLabel}.json`),
    JSON.stringify({ race: raceLabel, deadline, entries: entriesSlim }, null, 2)
  );
  fs.writeFileSync(
    path.join(fullDir, `${raceLabel}.json`),
    JSON.stringify(fullPayload, null, 2)
  );

  const idxPath = path.join(slimDir, "index.json");
  let idx = { stadium: stadiumCode, stadiumName: stadiumName ?? null, races: [] };
  if (fs.existsSync(idxPath)) { try { idx = JSON.parse(fs.readFileSync(idxPath, "utf8")); } catch {} }
  const slimEntry = { race: raceLabel, deadline, entries: entriesSlim };
  const i = idx.races.findIndex(rr => rr.race === raceLabel);
  if (i >= 0) idx.races[i] = slimEntry; else idx.races.push(slimEntry);
  fs.writeFileSync(idxPath, JSON.stringify(idx, null, 2));

  console.log(`write: ${path.join(slimDir, `${raceLabel}.json`)}`);
}

// ---- main ----
try {
  const res = await fetch(SRC);
  const status = res.status;
  const text = await res.text();

  ensureDir(DEBUG_OUT);
  fs.writeFileSync(`${DEBUG_OUT}/src-${DATE}.txt`, text);
  fs.writeFileSync(`${DEBUG_OUT}/meta-${DATE}.json`, JSON.stringify({ status }, null, 2));
  console.log("debug:", `${DEBUG_OUT}/src-${DATE}.txt`);

  if (status !== 200) throw new Error(`source fetch ${status}`);

  let raw; try { raw = JSON.parse(text); } catch { raw = null; }
  let programs = null;
  if (Array.isArray(raw)) programs = raw;
  else if (raw && Array.isArray(raw.programs)) programs = raw.programs;
  else if (raw && Array.isArray(raw.venues))   programs = raw.venues;
  else if (raw && Array.isArray(raw.items))    programs = raw.items;

  if (!programs) {
    const slimRoot = path.join(BASE_OUT_SLIM, DATE);
    ensureDir(slimRoot);
    fs.writeFileSync(
      path.join(slimRoot, "index.json"),
      JSON.stringify({ date: DATE, races: [], reason: "unexpected source format", sampleKeys: raw ? Object.keys(raw) : null }, null, 2)
    );
    console.log("no programs found.");
    process.exit(0);
  }

  // A) フラット形式
  if (programs?.length && (programs[0].race_stadium_number !== undefined || programs[0].race_number !== undefined)) {
    for (const p of programs) {
      const stadiumCode = pickStadiumCode(p);
      const raceLabel   = pickRaceLabel(p);
      const stadiumName = pickStadiumName(p);
      if (!stadiumCode || !raceLabel) continue;
      if (PID_FILTERS && !PID_FILTERS.includes(stadiumCode) && !PID_FILTERS.includes(stadiumName ?? "")) continue;
      if (RACE_FILTERS && !RACE_FILTERS.includes(raceLabel)) continue;
      emitOneRace(DATE, stadiumCode, stadiumName, raceLabel, p);
    }
    process.exit(0);
  }

  // B) venue配列
  for (const v of programs) {
    const stadiumCode = to2(v.stadium ?? v.stadiumCode ?? v.stadium_number ?? "");
    const stadiumName = v.stadiumName ?? v.stadium_name ?? null;
    if (!stadiumCode) continue;
    if (PID_FILTERS && !PID_FILTERS.includes(stadiumCode) && !PID_FILTERS.includes(stadiumName ?? "")) continue;

    const racesArr = v.races ?? v.Races ?? [];
    for (const r of racesArr) {
      const raceLabel = pickRaceLabel(r);
      if (!raceLabel) continue;
      if (RACE_FILTERS && !RACE_FILTERS.includes(raceLabel)) continue;
      emitOneRace(DATE, stadiumCode, stadiumName, raceLabel, r);
    }
  }
  console.log("done.");
} catch (err) {
  console.error("error:", String(err));
  const slimRoot = path.join(BASE_OUT_SLIM, DATE);
  ensureDir(slimRoot);
  fs.writeFileSync(
    path.join(slimRoot, "index.json"),
    JSON.stringify({ date: DATE, error: String(err) }, null, 2)
  );
}
