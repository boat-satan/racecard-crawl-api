// scripts/build-all.js
// Node 20+（fetch同梱）
// 環境変数: TARGET_DATE=YYYYMMDD | "today"

import fs from "node:fs";
import path from "node:path";

const to2 = (s) => String(s).padStart(2, "0");
const ensureDir = (p) => fs.mkdirSync(p, { recursive: true });
const writeJSON = (p, obj) => {
  ensureDir(path.dirname(p));
  fs.writeFileSync(p, JSON.stringify(obj, null, 2));
};

const DATE_RAW = process.env.TARGET_DATE || "today";
const DATE = DATE_RAW.replace(/-/g, "");
const IS_TODAY = DATE.toLowerCase() === "today";

const SRC = IS_TODAY
  ? "https://boatraceopenapi.github.io/programs/v2/today.json"
  : `https://boatraceopenapi.github.io/programs/v2/${DATE}.json`;

const OUT_SLIM = "public/programs-slim/v2";
const OUT_FULL = "public/programs/v2";
const DEBUG_OUT = "public/debug";

console.log("fetch:", SRC);

(async () => {
  // ---- fetch & debug ----
  const res = await fetch(SRC);
  const text = await res.text();
  ensureDir(DEBUG_OUT);
  writeJSON(`${DEBUG_OUT}/meta-${DATE}.json`, { status: res.status });
  fs.writeFileSync(`${DEBUG_OUT}/src-${DATE}.txt`, text);
  if (!res.ok) {
    console.error("source fetch failed:", res.status);
    // 空のインデックスだけでも置く
    const base = IS_TODAY ? `${OUT_SLIM}/today` : `${OUT_SLIM}/${DATE}`;
    ensureDir(base);
    writeJSON(`${base}/index.json`, { date: DATE, stadiums: [], error: `source ${res.status}` });
    process.exit(0);
  }

  // ---- parse ----
  let raw; try { raw = JSON.parse(text); } catch { raw = null; }
  let programs = null;
  if (Array.isArray(raw)) programs = raw;
  else if (raw && Array.isArray(raw.programs)) programs = raw.programs; // いま主流（レース配列）
  else if (raw && Array.isArray(raw.venues)) programs = raw.venues;     // 旧: venue→races
  if (!programs) {
    console.error("unexpected source format");
    const base = IS_TODAY ? `${OUT_SLIM}/today` : `${OUT_SLIM}/${DATE}`;
    ensureDir(base);
    writeJSON(`${base}/index.json`, { date: DATE, stadiums: [], reason: "unexpected source format" });
    process.exit(0);
  }

  // ---- normalize to race array ----
  let races = [];
  if (programs.length && (programs[0].race_stadium_number !== undefined || programs[0].race_number !== undefined)) {
    races = programs;
  } else {
    for (const v of programs) {
      for (const r of (v.races || v.Races || [])) {
        races.push({
          race_stadium_number: v.stadium ?? v.stadiumCode ?? v.stadium_number,
          race_stadium_name:   v.stadiumName ?? v.stadium_name ?? null,
          race_number: String(r.race ?? r.Race).replace(/[^\d]/g, ""),
          race_closed_at: r.deadline ?? null,
          race_grade_number: r.gradeNumber ?? null,
          race_title: r.title ?? null,
          race_subtitle: r.subtitle ?? null,
          race_distance: r.distance ?? null,
          boats: (r.entries || r.Entries || []).map(e => ({
            racer_boat_number: e.lane,
            racer_number:      e.number,
            racer_name:        e.name,
            racer_class_number:e.class,
            racer_branch_number:e.branch,
          })),
        });
      }
    }
  }

  // ---- output roots ----
  const dateSlimBase = IS_TODAY ? `${OUT_SLIM}/today` : `${OUT_SLIM}/${DATE}`;
  const dateFullBase = IS_TODAY ? `${OUT_FULL}/today` : `${OUT_FULL}/${DATE}`;
  ensureDir(dateSlimBase);
  ensureDir(dateFullBase);

  // ---- write all races ----
  const pids = new Set();
  const touchedIdx = new Map(); // stadium index path -> bool
  let count = 0;

  for (const p of races) {
    const pid = to2(p.race_stadium_number ?? p.stadium ?? p.stadium_number);
    const rno = String(p.race_number);
    const raceName = `${rno}R`;
    pids.add(pid);

    // full payload
    const full = {
      schemaVersion: "2.0",
      generatedAt: new Date().toISOString(),
      date: IS_TODAY ? "today" : DATE,
      stadium: pid,
      stadiumName: p.race_stadium_name ?? null,
      race: raceName,
      deadline: p.race_closed_at ?? null,
      gradeNumber: p.race_grade_number ?? null,
      title: p.race_title ?? null,
      subtitle: p.race_subtitle ?? null,
      distance: p.race_distance ?? null,
      entries: (p.boats || []).map(b => ({
        lane: b.racer_boat_number ?? null,
        number: b.racer_number ?? null,
        name: b.racer_name ?? null,
        classNumber: b.racer_class_number ?? null,
        branchNumber: b.racer_branch_number ?? null,
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
        boatTop3: b.racer_assigned_boat_top_3_percent ?? null,
      })),
    };

    // slim payload
    const slim = {
      race: raceName,
      deadline: full.deadline,
      entries: full.entries.map(e => ({ lane: e.lane, name: e.name, class: e.classNumber ?? null })),
    };

    // write full/slim
    writeJSON(path.join(dateFullBase, pid, `${raceName}.json`), full);
    writeJSON(path.join(dateSlimBase, pid, `${raceName}.json`), slim);

    // stadium index (slim)
    const idxPath = path.join(dateSlimBase, pid, "index.json");
    let idx;
    if (!touchedIdx.get(idxPath) && fs.existsSync(idxPath)) {
      idx = JSON.parse(fs.readFileSync(idxPath, "utf8"));
    } else {
      idx = { stadium: pid, stadiumName: full.stadiumName ?? null, races: [] };
    }
    const i = idx.races.findIndex(r => r.race === raceName);
    const slimEntry = { race: raceName, deadline: slim.deadline, entries: slim.entries };
    if (i >= 0) idx.races[i] = slimEntry; else idx.races.push(slimEntry);
    writeJSON(idxPath, idx);
    touchedIdx.set(idxPath, true);

    count++;
  }

  // date index (slim)
  writeJSON(path.join(dateSlimBase, "index.json"), {
    date: IS_TODAY ? "today" : DATE,
    stadiums: [...pids].sort(),
  });

  // today alias（DATE指定時のみ）
  if (!IS_TODAY) {
    for (const base of [OUT_FULL, OUT_SLIM]) {
      const src = `${base}/${DATE}`;
      const dst = `${base}/today`;
      try {
        fs.rmSync(dst, { recursive: true, force: true });
        fs.cpSync(src, dst, { recursive: true });
      } catch (e) {
        console.error("today alias error:", e);
      }
    }
    console.log("today alias updated from", DATE);
  }

  console.log(`done: ${count} races written -> ${IS_TODAY ? "today" : DATE}`);
})();
