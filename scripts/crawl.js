// scripts/crawl.js
import fs from "node:fs";
import path from "node:path";

const BASE_OUT = "public/programs-slim/v2";
const DEBUG_OUT = "public/debug";

// ---------- helpers ----------
const ensureDir = (p) => fs.mkdirSync(p, { recursive: true });
const outDir = (date, pid) => path.join(BASE_OUT, date, pid);
const to2 = (s) => String(s).padStart(2, "0");

// inputs
const DATE   = (process.env.TARGET_DATE || "today").replace(/-/g, "");
const PID_IN = process.env.TARGET_PID || "02";   // "02" or 場名
const PID    = /^\d+$/.test(PID_IN) ? to2(PID_IN) : PID_IN;
const RACE_Q = process.env.TARGET_RACE || "1";
const RACE   = /R$/i.test(RACE_Q) ? RACE_Q.toUpperCase() : `${RACE_Q}R`;
const RNO    = String(RACE_Q).replace(/[^\d]/g, ""); // 数字だけ（1..12）

const SRC = DATE.toLowerCase() === "today"
  ? "https://boatraceopenapi.github.io/programs/v2/today.json"
  : `https://boatraceopenapi.github.io/programs/v2/${DATE}.json`;

console.log("fetch:", SRC);

// ---------- main ----------
try {
  const res = await fetch(SRC);
  const status = res.status;
  const text = await res.text();

  // デバッグ保存（何が返ってきたか見えるように）
  ensureDir(DEBUG_OUT);
  fs.writeFileSync(`${DEBUG_OUT}/src-${DATE}.txt`, text);
  fs.writeFileSync(`${DEBUG_OUT}/meta-${DATE}.json`, JSON.stringify({ status }, null, 2));
  console.log("debug:", `${DEBUG_OUT}/src-${DATE}.txt`);

  if (status !== 200) throw new Error(`source fetch ${status}`);

  // JSONにして配列を取り出す（複数スキーマに対応）
  let raw; try { raw = JSON.parse(text); } catch { raw = null; }

  // programs 候補を抽出
  let programs = null;
  if (Array.isArray(raw)) programs = raw;
  else if (raw && Array.isArray(raw.programs)) programs = raw.programs;      // ←今回のケース
  else if (raw && Array.isArray(raw.venues)) programs = raw.venues;
  else if (raw && Array.isArray(raw.items)) programs = raw.items;

  const dir = outDir(DATE, PID);
  ensureDir(dir);

  if (!programs) {
    fs.writeFileSync(path.join(dir, "index.json"),
      JSON.stringify({ stadium: PID, stadiumName: null, races: [], reason: "unexpected source format", sampleKeys: raw ? Object.keys(raw) : null }, null, 2));
    console.log("write:", path.join(dir, "index.json"));
    process.exit(0);
  }

  // ----- レースデータの探し方（2系統） -----
  let raceData = null;
  let stadiumName = null;
  let stadiumCode = PID;

  // A) programs が「レース配列」形式（race_stadium_number, race_number を直接持つ）
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

  // ------ 出力オブジェクトを作る ------
  let payload;
  if (raceData) {
    // A 直持ち形式
    if (raceData.boats) {
      payload = {
        date: DATE,
        stadium: to2(stadiumCode),
        stadiumName: stadiumName,
        race: RACE,
        deadline: raceData.race_closed_at ?? null,
        entries: (raceData.boats || []).map(b => ({
          lane: b.racer_boat_number ?? b.lane ?? null,
          number: b.racer_number ?? b.number ?? null,
          name: b.racer_name ?? b.name ?? null,
          class: b.racer_class_number ?? b.class ?? null,
          branch: b.racer_branch_number ?? b.branch ?? null
        }))
      };
    } else {
      // B venue.races 形式
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

  // ------ ファイル出力（必ず書く） ------
  const racePath = path.join(dir, `${RACE}.json`);
  fs.writeFileSync(racePath, JSON.stringify(payload, null, 2));

  // venue index.json（簡易）
  const slim = (payload.entries?.length)
    ? { race: payload.race, deadline: payload.deadline, entries: payload.entries.map(x => ({ lane: x.lane, name: x.name, class: x.class })) }
    : { race: payload.race, deadline: null, entries: [], reason: payload.reason };
  const idxPath = path.join(dir, "index.json");
  let idx = { stadium: payload.stadium, stadiumName: payload.stadiumName, races: [] };
  if (fs.existsSync(idxPath)) idx = JSON.parse(fs.readFileSync(idxPath, "utf8"));
  const i = idx.races.findIndex(rr => rr.race === payload.race);
  if (i >= 0) idx.races[i] = slim; else idx.races.push(slim);
  fs.writeFileSync(idxPath, JSON.stringify(idx, null, 2));

  console.log("write:", racePath);
} catch (err) {
  const dir = outDir(DATE, PID);
  ensureDir(dir);
  fs.writeFileSync(path.join(dir, "index.json"),
    JSON.stringify({ stadium: PID, stadiumName: null, races: [], error: String(err) }, null, 2));
  console.error("error:", String(err));
}
