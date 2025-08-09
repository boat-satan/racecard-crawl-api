// scripts/build-all.js
import fs from "node:fs";
import path from "node:path";

const to2 = (s) => String(s).padStart(2,"0");
const ensureDir = (p) => fs.mkdirSync(p, { recursive: true });

const DATE   = (process.env.TARGET_DATE || "today").replace(/-/g,"");
const SRC = DATE.toLowerCase()==="today"
  ? "https://boatraceopenapi.github.io/programs/v2/today.json"
  : `https://boatraceopenapi.github.io/programs/v2/${DATE}.json`;

const OUT_SLIM = "public/programs-slim/v2";
const OUT_FULL = "public/programs/v2";
const DEBUG_OUT = "public/debug";

console.log("fetch:", SRC);
const res = await fetch(SRC);
const text = await res.text();
ensureDir(DEBUG_OUT);
fs.writeFileSync(`${DEBUG_OUT}/src-${DATE}.txt`, text);
fs.writeFileSync(`${DEBUG_OUT}/meta-${DATE}.json`, JSON.stringify({ status: res.status }, null, 2));
if (!res.ok) {
  console.error("source fetch failed:", res.status);
  process.exit(0);
}

let raw; try { raw = JSON.parse(text); } catch { raw = null; }
let programs = null;
if (Array.isArray(raw)) programs = raw;
else if (raw && Array.isArray(raw.programs)) programs = raw.programs;   // ←今の形式
else if (raw && Array.isArray(raw.venues)) programs = raw.venues;

if (!programs) {
  console.error("unexpected source format");
  process.exit(0);
}

// 1) もし venue 形式なら races を平坦化
let races = [];
if (programs.length && (programs[0].race_stadium_number !== undefined || programs[0].race_number !== undefined)) {
  races = programs;
} else {
  for (const v of programs) {
    for (const r of (v.races || [])) {
      races.push({
        race_stadium_number: v.stadium ?? v.stadiumCode ?? v.stadium_number,
        race_stadium_name: v.stadiumName ?? v.stadium_name ?? null,
        race_number: String(r.race ?? r.Race).replace(/[^\d]/g,""),
        race_closed_at: r.deadline ?? null,
        boats: (r.entries || r.Entries || []).map(e => ({
          racer_boat_number: e.lane,
          racer_number: e.number,
          racer_name: e.name,
          racer_class_number: e.class,
          racer_branch_number: e.branch
        }))
      });
    }
  }
}

// 2) 出力（全場×全R）
let count = 0;
for (const p of races) {
  const pid = to2(p.race_stadium_number ?? p.stadium ?? p.stadium_number);
  const rno = String(p.race_number);
  const raceName = `${rno}R`;

  const payloadFull = {
    schemaVersion: "2.0",
    generatedAt: new Date().toISOString(),
    date: DATE,
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
      boatTop3: b.racer_assigned_boat_top_3_percent ?? null
    }))
  };

  const payloadSlim = {
    race: raceName,
    deadline: payloadFull.deadline,
    entries: payloadFull.entries.map(e => ({ lane: e.lane, name: e.name, class: e.classNumber ?? null }))
  };

  const dirFull = path.join(OUT_FULL, DATE, pid);
  const dirSlim = path.join(OUT_SLIM, DATE, pid);
  ensureDir(dirFull); ensureDir(dirSlim);

  fs.writeFileSync(path.join(dirFull, `${raceName}.json`), JSON.stringify(payloadFull, null, 2));
  fs.writeFileSync(path.join(dirSlim, `${raceName}.json`), JSON.stringify(payloadSlim, null, 2));

  // index.json（スリム）
  const idxPath = path.join(dirSlim, "index.json");
  let idx = { stadium: pid, stadiumName: payloadFull.stadiumName ?? null, races: [] };
  if (fs.existsSync(idxPath)) idx = JSON.parse(fs.readFileSync(idxPath,"utf8"));
  const i = idx.races.findIndex(r => r.race === raceName);
  const slimEntry = { race: raceName, deadline: payloadSlim.deadline, entries: payloadSlim.entries };
  if (i >= 0) idx.races[i] = slimEntry; else idx.races.push(slimEntry);
  fs.writeFileSync(idxPath, JSON.stringify(idx, null, 2));

  count++;
}

console.log(`done: ${count} races written for ${DATE}`);
