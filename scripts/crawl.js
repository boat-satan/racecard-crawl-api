// scripts/crawl.js
// Node v20 (fetch 同梱)
// 出力：
//   - public/programs-slim/v2/<date>/<pid>/<race>.json（軽量）+ index.json
//   - public/programs/v2/<date>/<pid>/<race>.json（フル：元データ寄せ）
// 環境変数：
//   TARGET_DATE=today | YYYYMMDD / YYYY-MM-DD
//   TARGET_PID=02 | 場名
//   TARGET_RACE=1..12 | "1R" など

import fs from "node:fs";
import path from "node:path";

// ---------- 定数 ----------
const BASE_OUT_SLIM = "public/programs-slim/v2";
const BASE_OUT_FULL = "public/programs/v2";
const DEBUG_OUT     = "public/debug";

// ---------- helpers ----------
const ensureDir = (p) => fs.mkdirSync(p, { recursive: true });
const outDirSlim = (date, pid) => path.join(BASE_OUT_SLIM, date, pid);
const outDirFull = (date, pid) => path.join(BASE_OUT_FULL, date, pid);
const to2 = (s) => String(s).padStart(2, "0");

// inputs
const DATE_IN = (process.env.TARGET_DATE || "today").trim();
const DATE = DATE_IN.toLowerCase() === "today" ? "today" : DATE_IN.replace(/-/g, "");
const PID_IN = (process.env.TARGET_PID || "02").trim();  // "02" or 場名
const PID = /^\d+$/.test(PID_IN) ? to2(PID_IN) : PID_IN;
const RACE_Q = (process.env.TARGET_RACE || "1").trim();
const RACE = /R$/i.test(RACE_Q) ? RACE_Q.toUpperCase() : `${RACE_Q}R`;
const RNO  = String(RACE_Q).replace(/[^\d]/g, ""); // 数字だけ（1..12）

const SRC = DATE === "today"
  ? "https://boatraceopenapi.github.io/programs/v2/today.json"
  : `https://boatraceopenapi.github.io/programs/v2/${DATE}.json`;

console.log("fetch:", SRC);

// ---------- main ----------
try {
  const res = await fetch(SRC);
  const status = res.status;
  const text = await res.text();

  // デバッグ保存
  ensureDir(DEBUG_OUT);
  fs.writeFileSync(`${DEBUG_OUT}/src-${DATE}.txt`, text);
  fs.writeFileSync(`${DEBUG_OUT}/meta-${DATE}.json`, JSON.stringify({ status }, null, 2));
  console.log("debug:", `${DEBUG_OUT}/src-${DATE}.txt`);

  if (status !== 200) throw new Error(`source fetch ${status}`);

  // JSON 化 & 配列抽出（複数スキーマ対応）
  let raw; try { raw = JSON.parse(text); } catch { raw = null; }
  let programs = null;
  if (Array.isArray(raw)) programs = raw;
  else if (raw && Array.isArray(raw.programs)) programs = raw.programs; // v2標準
  else if (raw && Array.isArray(raw.venues))   programs = raw.venues;
  else if (raw && Array.isArray(raw.items))    programs = raw.items;

  const dirSlim = outDirSlim(DATE, PID);
  ensureDir(dirSlim);

  if (!programs) {
    fs.writeFileSync(path.join(dirSlim, "index.json"),
      JSON.stringify({
        stadium: PID,
        stadiumName: null,
        races: [],
        reason: "unexpected source format",
        sampleKeys: raw ? Object.keys(raw) : null
      }, null, 2)
    );
    console.log("write:", path.join(dirSlim, "index.json"));
    process.exit(0);
  }

  // ----- レースデータの探し方（2系統） -----
  let raceData = null;
  let stadiumName = null;
  let stadiumCode = PID;

  // A) レース配列（race_stadium_number, race_number を直接持つ）
  if (programs?.length && (programs[0].race_stadium_number !== undefined || programs[0].race_number !== undefined)) {
    raceData = programs.find(p =>
      (String(p.race_stadium_number ?? p.stadium_number ?? p.stadium)?.padStart(2, "0") === PID) &&
      (String(p.race_number) === String(RNO))
    );
    if (raceData) {
      stadiumName = raceData.race_stadium_name ?? raceData.stadium_name ?? null;
      stadiumCode = to2(raceData.race_stadium_number ?? raceData.stadium_number ?? PID);
    }
  }

  // B) 従来の「場 → races」形式
  if (!raceData) {
    const venue = programs.find(v =>
      v.stadium === PID || v.stadiumCode === PID || v.stadium_number === PID ||
      v.stadiumName === PID || v.stadium_name === PID
    );
    if (venue) {
      stadiumName = venue.stadiumName ?? venue.stadium_name ?? null;
      stadiumCode = venue.stadium ?? venue.stadiumCode ?? venue.stadium_number ?? PID;
      const racesArr = venue.races ?? venue.Races ?? [];
      raceData = racesArr.find(x => String(x.race ?? x.Race).replace(/[^\d]/g, "") === String(RNO));
    }
  }

  // ------ payload（スリム基準） ------
  let payload;
  if (raceData) {
    if (raceData.boats) {
      // A 直持ち（v2標準）
      payload = {
        date: DATE,
        stadium: to2(stadiumCode),
        stadiumName: stadiumName,
        race: `${Number(raceData.race_number)}R`,
        deadline: raceData.race_closed_at ?? null,
        entries: (raceData.boats || []).map(b => ({
          lane: b.racer_boat_number ?? null,
          number: b.racer_number ?? null,
          name: b.racer_name ?? null,
          class: b.racer_class_number ?? null,
          branch: b.racer_branch_number ?? null
        }))
      };
    } else {
      // B venue.races
      const r = raceData;
      payload = {
        date: DATE,
        stadium: to2(stadiumCode),
        stadiumName: stadiumName,
        race: (r.race ?? r.Race ?? RACE).toString().replace(/[^\d]/g, "") + "R",
        deadline: r.deadline ?? null,
        entries: (r.entries ?? r.Entries ?? []).map(e => ({
          lane: e.lane ?? null,
          number: e.number ?? null,
          name: e.name ?? null,
          class: e.class ?? null,
          branch: e.branch ?? null
        }))
      };
    }
  } else {
    payload = {
      date: DATE,
      stadium: to2(stadiumCode),
      stadiumName: stadiumName,
      race: RACE,
      deadline: null,
      entries: [],
      reason: "race not found"
    };
  }

  // ---------- “フル” と “スリム” を同時保存 ----------
  // フル用（source を最大限活かして保存）
  const fullPayload = (() => {
    const base = {
      schemaVersion: "2.0",
      generatedAt: new Date().toISOString(),
      date: payload.date,
      stadium: payload.stadium,
      stadiumName: payload.stadiumName ?? null,
      race: payload.race,
      deadline: payload.deadline ?? null
    };
    if (raceData) {
      // v2標準に多いフィールドを拾う（無ければ null）
      base.gradeNumber = raceData.race_grade_number ?? null;
      base.title       = raceData.race_title ?? null;
      base.subtitle    = raceData.race_subtitle ?? null;
      base.distance    = raceData.race_distance ?? null;
    }
    base.entries = (raceData?.boats || payload.entries || []).map(b => ({
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
    }));
    return base;
  })();

  // 保存先ディレクトリ
  const fullDir = outDirFull(DATE, payload.stadium);
  ensureDir(fullDir);

  // フル保存
  fs.writeFileSync(path.join(fullDir, `${payload.race}.json`), JSON.stringify(fullPayload, null, 2));

  // スリム保存（API互換：最低限）
  const slimDir = outDirSlim(DATE, payload.stadium);
  ensureDir(slimDir);

  const racePathSlim = path.join(slimDir, `${payload.race}.json`);
  fs.writeFileSync(
    racePathSlim,
    JSON.stringify(
      {
        race: payload.race,
        deadline: payload.deadline ?? null,
        entries: (payload.entries || []).map(e => ({
          lane: e.lane,
          name: e.name,
          class: e.class ?? null
        }))
      },
      null,
      2
    )
  );

  // 場の index.json（スリム）
  const idxPath = path.join(slimDir, "index.json");
  let idx = { stadium: payload.stadium, stadiumName: payload.stadiumName ?? null, races: [] };
  if (fs.existsSync(idxPath)) {
    try {
      idx = JSON.parse(fs.readFileSync(idxPath, "utf8"));
    } catch {
      idx = { stadium: payload.stadium, stadiumName: payload.stadiumName ?? null, races: [] };
    }
  }
  const slimEntry = {
    race: payload.race,
    deadline: payload.deadline ?? null,
    entries: (payload.entries || []).map(x => ({ lane: x.lane, name: x.name, class: x.class ?? null }))
  };
  const i = idx.races.findIndex(rr => rr.race === payload.race);
  if (i >= 0) idx.races[i] = slimEntry; else idx.races.push(slimEntry);
  fs.writeFileSync(idxPath, JSON.stringify(idx, null, 2));

  console.log("write slim:", racePathSlim);
  console.log("write full:", path.join(fullDir, `${payload.race}.json`));
} catch (err) {
  // エラー時も index.json を最低限出す
  const dirSlim = outDirSlim(DATE, PID);
  ensureDir(dirSlim);
  fs.writeFileSync(
    path.join(dirSlim, "index.json"),
    JSON.stringify({ stadium: PID, stadiumName: null, races: [], error: String(err) }, null, 2)
  );
  console.error("error:", String(err));
}
