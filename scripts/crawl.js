import fs from "node:fs";
import path from "node:path";

const BASE_OUT = "public/programs-slim/v2";

function ensureDir(p){ fs.mkdirSync(p, { recursive:true }); }
function outDir(date, pid){ return path.join(BASE_OUT, date, pid); }

// env
const DATE    = (process.env.TARGET_DATE || "today").replace(/-/g,"");
const PID     = process.env.TARGET_PID || "02";      // 場コード or 場名
const RACE_Q  = process.env.TARGET_RACE || "1";
const RACE    = /R$/i.test(RACE_Q) ? RACE_Q.toUpperCase() : `${RACE_Q}R`;

// 元データ
const SRC = DATE.toLowerCase()==="today"
  ? "https://boatraceopenapi.github.io/programs/v2/today.json"
  : `https://boatraceopenapi.github.io/programs/v2/${DATE}.json`;

console.log("fetch:", SRC);

try {
  const res = await fetch(SRC);
  if (!res.ok) throw new Error(`source fetch ${res.status}`);
  const programs = await res.json();

  const dir = outDir(DATE, PID);
  ensureDir(dir);

  if (!Array.isArray(programs)) {
    // フォーマット不正でも痕跡を残す
    fs.writeFileSync(path.join(dir, "index.json"),
      JSON.stringify({ stadium: PID, stadiumName: null, races: [], reason: "unexpected source format" }, null, 2));
    process.exit(0);
  }

  // 場を探す（コード or 名前）
  const venue = programs.find(v => v.stadium === PID || v.stadiumName === PID);
  if (!venue) {
    // ★ここで必ず index.json を出す（空でもOK）
    fs.writeFileSync(path.join(dir, "index.json"),
      JSON.stringify({ stadium: PID, stadiumName: null, races: [], reason: "stadium not found" }, null, 2));
    console.log("warn: stadium not found:", PID);
    process.exit(0);
  }

  // レースを探す
  const r = (venue.races || []).find(x => x.race === RACE);
  const payload = r ? {
    date: DATE,
    stadium: venue.stadium,
    stadiumName: venue.stadiumName,
    race: r.race,
    deadline: r.deadline || null,
    entries: (r.entries || []).map(e => ({
      lane: e.lane,
      number: e.number ?? null,
      name: e.name,
      class: e.class ?? null,
      branch: e.branch ?? null
    }))
  } : {
    date: DATE,
    stadium: venue.stadium,
    stadiumName: venue.stadiumName,
    race: RACE,
    deadline: null,
    entries: [],
    reason: "race not found"
  };

  // ★必ず書く（見に行けるようにする）
  fs.writeFileSync(path.join(dir, `${RACE}.json`), JSON.stringify(payload, null, 2));

  // 場の index.json も（簡易）
  const slim = r ? { race: payload.race, deadline: payload.deadline,
                     entries: payload.entries.map(x=>({lane:x.lane,name:x.name,class:x.class})) }
                 : { race: RACE, deadline: null, entries: [], reason: payload.reason };
  const idxPath = path.join(dir, "index.json");
  let idx = { stadium: venue.stadium, stadiumName: venue.stadiumName, races: [] };
  if (fs.existsSync(idxPath)) idx = JSON.parse(fs.readFileSync(idxPath,"utf8"));
  const i = idx.races.findIndex(rr => rr.race === RACE);
  if (i>=0) idx.races[i] = slim; else idx.races.push(slim);
  fs.writeFileSync(idxPath, JSON.stringify(idx, null, 2));

  console.log("write:", path.join(dir, `${RACE}.json`));
} catch (err) {
  // 取得自体が失敗した時も痕跡を出す
  const dir = outDir(DATE, PID);
  ensureDir(dir);
  fs.writeFileSync(path.join(dir, "index.json"),
    JSON.stringify({ stadium: PID, stadiumName: null, races: [], error: String(err) }, null, 2));
  console.error("error:", String(err));
}
