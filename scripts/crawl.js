import fs from "node:fs";
import path from "node:path";

const BASE_OUT = "public/programs-slim/v2";
function ensureDir(p){ fs.mkdirSync(p, { recursive:true }); }
function outDir(date, pid){ return path.join(BASE_OUT, date, pid); }

const DATE   = (process.env.TARGET_DATE || "today").replace(/-/g,"");
const PID    = (process.env.TARGET_PID || "02").padStart(2,"0");   // 02 形式に
const RACE_Q = process.env.TARGET_RACE || "1";
const RACE   = /R$/i.test(RACE_Q) ? RACE_Q.toUpperCase() : `${RACE_Q}R`;

const SRC = DATE.toLowerCase()==="today"
  ? "https://boatraceopenapi.github.io/programs/v2/today.json"
  : `https://boatraceopenapi.github.io/programs/v2/${DATE}.json`;

console.log("fetch:", SRC);

try {
  const res = await fetch(SRC);
  const status = res.status;
  const bodyText = await res.text(); // ←まずテキストで受ける（HTMLの可能性もある）
  // デバッグ保存（あとでブラウザで見られる）
  const dbgDir = "public/debug";
  ensureDir(dbgDir);
  fs.writeFileSync(`${dbgDir}/src-${DATE}.txt`, bodyText);
  fs.writeFileSync(`${dbgDir}/meta-${DATE}.json`, JSON.stringify({ status }, null, 2));
  console.log("debug saved:", `${dbgDir}/src-${DATE}.txt`);

  if (status !== 200) throw new Error(`source fetch ${status}`);

  // いろんな形に対応して配列を抜く
  let raw;
  try { raw = JSON.parse(bodyText); } catch { raw = null; }
  const programs =
    Array.isArray(raw) ? raw :
    (raw && Array.isArray(raw.programs)) ? raw.programs :
    (raw && Array.isArray(raw.venues))   ? raw.venues :
    (raw && Array.isArray(raw.items))    ? raw.items : null;

  const dir = outDir(DATE, PID);
  ensureDir(dir);

  if (!programs) {
    fs.writeFileSync(path.join(dir, "index.json"),
      JSON.stringify({ stadium: PID, stadiumName: null, races: [], reason: "unexpected source format", sampleKeys: raw ? Object.keys(raw) : null }, null, 2));
    process.exit(0);
  }

  // 場を探す（コード or 名称）
  const venue = programs.find(v =>
    v.stadium === PID || v.stadiumId === PID || v.stadiumCode === PID ||
    v.stadiumName === PID || v.場コード === PID || v.場名 === PID
  );
  if (!venue) {
    fs.writeFileSync(path.join(dir, "index.json"),
      JSON.stringify({ stadium: PID, stadiumName: null, races: [], reason: "stadium not found" }, null, 2));
    console.log("warn: stadium not found:", PID);
    process.exit(0);
  }

  const r = (venue.races || venue.Races || []).find(x => (x.race||x.Race) === RACE);
  const payload = r ? {
    date: DATE,
    stadium: venue.stadium ?? venue.stadiumCode ?? PID,
    stadiumName: venue.stadiumName ?? null,
    race: r.race ?? r.Race,
    deadline: r.deadline ?? null,
    entries: (r.entries || r.Entries || []).map(e => ({
      lane: e.lane ?? e.Lane,
      number: e.number ?? null,
      name: e.name ?? e.Name,
      class: e.class ?? null,
      branch: e.branch ?? null
    }))
  } : {
    date: DATE,
    stadium: venue.stadium ?? venue.stadiumCode ?? PID,
    stadiumName: venue.stadiumName ?? null,
    race: RACE,
    deadline: null,
    entries: [],
    reason: "race not found"
  };

  // ★必ず出力（あとで見に行ける）
  fs.writeFileSync(path.join(dir, `${RACE}.json`), JSON.stringify(payload, null, 2));

  // index.json も更新
  const slim = (payload.entries?.length)
    ? { race: payload.race, deadline: payload.deadline, entries: payload.entries.map(x=>({lane:x.lane,name:x.name,class:x.class})) }
    : { race: payload.race, deadline: null, entries: [], reason: payload.reason };
  const idxPath = path.join(dir, "index.json");
  let idx = { stadium: payload.stadium, stadiumName: payload.stadiumName, races: [] };
  if (fs.existsSync(idxPath)) idx = JSON.parse(fs.readFileSync(idxPath,"utf8"));
  const i = idx.races.findIndex(rr => rr.race === RACE);
  if (i>=0) idx.races[i] = slim; else idx.races.push(slim);
  fs.writeFileSync(idxPath, JSON.stringify(idx, null, 2));

  console.log("write:", path.join(dir, `${RACE}.json`));
} catch (err) {
  const dir = outDir(DATE, PID);
  ensureDir(dir);
  fs.writeFileSync(path.join(dir, "index.json"),
    JSON.stringify({ stadium: PID, stadiumName: null, races: [], error: String(err) }, null, 2));
  console.error("error:", String(err));
}
