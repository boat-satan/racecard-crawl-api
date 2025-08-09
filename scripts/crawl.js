import fs from "node:fs";
import path from "node:path";

const BASE_OUT = "public/programs-slim/v2";

function ensureDir(p){ fs.mkdirSync(p, { recursive:true }); }
function outPath(date, pid){ return path.join(BASE_OUT, date, pid); }

// env（workflowから渡す）
const DATE    = (process.env.TARGET_DATE || "today").replace(/-/g,"");
const PID     = process.env.TARGET_PID || "02"; // 場コード
const RACE_Q  = process.env.TARGET_RACE || "1"; // 1 or 1R
const RACE    = /R$/i.test(RACE_Q) ? RACE_Q.toUpperCase() : `${RACE_Q}R`;

const src = DATE.toLowerCase()==="today"
  ? "https://boatraceopenapi.github.io/programs/v2/today.json"
  : `https://boatraceopenapi.github.io/programs/v2/${DATE}.json`;

console.log("fetch:", src);

const res = await fetch(src);
if (!res.ok) {
  console.error("source fetch failed:", res.status);
  process.exit(0); // 失敗でもワークフローは続ける
}
const data = await res.json();
if (!Array.isArray(data)) {
  console.error("unexpected source format");
  process.exit(0);
}

// stadium は "02" などのコード or 名前で合致させる
const venue = data.find(v => v.stadium === PID || v.stadiumName === PID);
if (!venue) {
  console.warn("stadium not found:", PID);
} else {
  const r = (venue.races || []).find(x => x.race === RACE);
  if (!r) {
    console.warn("race not found:", RACE);
  } else {
    const payload = {
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
    };
    const dir = outPath(DATE, venue.stadium);
    ensureDir(dir);
    fs.writeFileSync(path.join(dir, `${RACE}.json`), JSON.stringify(payload, null, 2));
    // 場のindexも軽く出す
    const slim = { race: payload.race, deadline: payload.deadline, entries: payload.entries.map(x=>({lane:x.lane,name:x.name,class:x.class})) };
    const idxPath = path.join(dir, "index.json");
    let idx = { stadium: venue.stadium, stadiumName: venue.stadiumName, races: [] };
    if (fs.existsSync(idxPath)) idx = JSON.parse(fs.readFileSync(idxPath,"utf8"));
    const i = idx.races.findIndex(rr => rr.race === RACE);
    if (i>=0) idx.races[i] = slim; else idx.races.push(slim);
    fs.writeFileSync(idxPath, JSON.stringify(idx, null, 2));
    console.log("write:", path.join(dir, `${RACE}.json`));
  }
}
