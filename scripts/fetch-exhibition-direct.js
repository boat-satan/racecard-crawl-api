// scripts/fetch-exhibition-direct.js
// 出力: public/exhibition/v1/<date>/<pid>/<race>.json
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { load as loadHTML } from "cheerio";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function log(...args) { console.log("[beforeinfo]", ...args); }
function usageAndExit() {
  console.error("Usage: node scripts/fetch-exhibition-direct.js <YYYYMMDD> <pid:01..24> <race: 1R|1..12|1,3,5R...|auto>");
  process.exit(1);
}
const normRaceToken = (tok)=> parseInt(String(tok).replace(/[^0-9]/g,""),10);
function expandRaces(expr){
  if (!expr) return [];
  if (String(expr).toLowerCase()==="auto") return ["auto"];
  const parts = String(expr).split(",").map(s=>s.trim()).filter(Boolean);
  const out = new Set();
  for (const p of parts){
    const m = p.match(/^(\d+)[Rr]?\.\.(\d+)[Rr]?$/);
    if (m){ const a=+m[1], b=+m[2]; const [s,e]=a<=b?[a,b]:[b,a]; for(let i=s;i<=e;i++) out.add(i); }
    else { const n = normRaceToken(p); if (!Number.isNaN(n)&&n>=1&&n<=12) out.add(n); }
  }
  return [...out].sort((a,b)=>a-b);
}
function ensureDirSync(dir){ fs.mkdirSync(dir,{recursive:true}); }
async function writeJSON(file, data){ ensureDirSync(path.dirname(file)); await fsp.writeFile(file, JSON.stringify(data,null,2)); }

const argvDate = process.argv[2];
const argvPid  = process.argv[3];
const argvRace = process.argv[4];

const DATE       = process.env.TARGET_DATE  || argvDate  || "";
const PIDS       = (process.env.TARGET_PIDS || argvPid   || "").split(",").map(s=>s.trim()).filter(Boolean);
const RACES_EXPR = process.env.TARGET_RACES || argvRace  || "";
const SKIP_EXISTING = process.argv.includes("--skip-existing");
const AUTO_TRIGGER_MIN = Number(process.env.AUTO_TRIGGER_MIN || 15);

if (!DATE || PIDS.length===0 || !RACES_EXPR) usageAndExit();
const RACES = expandRaces(RACES_EXPR); if (RACES.length===0) usageAndExit();

function toJstDate(dateYYYYMMDD, hhmm){
  return new Date(`${dateYYYYMMDD.slice(0,4)}-${dateYYYYMMDD.slice(4,6)}-${dateYYYYMMDD.slice(6,8)}T${hhmm}:00+09:00`);
}
function tryParseTimeString(s){
  if (!s || typeof s!=="string") return null;
  const m = s.match(/(\d{1,2}):(\d{2})/); if (!m) return null;
  const hh = m[1].padStart(2,"0"), mm=m[2]; return `${hh}:${mm}`;
}
async function loadRaceDeadlineHHMM(date, pid, raceNo){
  const relPaths = [
    path.join("public","programs","v2",date,pid,`${raceNo}R.json`),
    path.join("public","programs-slim","v2",date,pid,`${raceNo}R.json`),
  ];
  for (const rel of relPaths){
    const abs = path.join(__dirname,"..",rel);
    if (!fs.existsSync(abs)) continue;
    try{
      const j = JSON.parse(await fsp.readFile(abs,"utf8"));
      const candidates = [
        j.deadlineJST,j.closeTimeJST,j.deadline,j.closingTime,j.startTimeJST,j.postTimeJST,
        j.scheduledTimeJST,j.raceCloseJST,j.startAt,j.closeAt,
        j.info?.deadlineJST,j.info?.closeTimeJST,j.meta?.deadlineJST,j.meta?.closeTimeJST
      ].filter(Boolean);
      for (const c of candidates){
        if (typeof c==="string" && c.includes("T") && c.match(/:\d{2}/)){
          const dt = new Date(c); if (!isNaN(dt)){
            const hh=String(dt.getHours()).padStart(2,"0"), mm=String(dt.getMinutes()).padStart(2,"0");
            return `${hh}:${mm}`;
          }
        }
        const hhmm = tryParseTimeString(String(c)); if (hhmm) return hhmm;
      }
      const raw = JSON.stringify(j); const m = raw.match(/(\d{1,2}):(\d{2})/);
      if (m) return `${m[1].padStart(2,"0")}:${m[2]}`;
    }catch{}
  }
  return null;
}
async function pickRacesAuto(date, pid){
  const nowMin = Math.floor(Date.now()/60000), out=[];
  for (let r=1;r<=12;r++){
    const hhmm = await loadRaceDeadlineHHMM(date,pid,r); if (!hhmm) continue;
    const triggerMin = Math.floor((toJstDate(date,hhmm).getTime() - AUTO_TRIGGER_MIN*60000)/60000);
    if (nowMin >= triggerMin) out.push(r);
  }
  return out;
}

async function fetchBeforeinfo({date,pid,raceNo}){
  const url = `https://www.boatrace.jp/owpc/pc/race/beforeinfo?hd=${date}&jcd=${pid}&rno=${raceNo}`;
  log("GET", url);
  const res = await fetch(url, { headers: { "user-agent":"Mozilla/5.0", "accept-language":"ja,en;q=0.8" }});
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
  const html = await res.text();
  return { url, html };
}

// ★ 風向クラス番号 → 日本語方位（1=北 … 13=西 … 16=北北西）
const WIND_DIR_MAP = {
  1:"北", 2:"北北東", 3:"北東", 4:"東北東", 5:"東",
  6:"東南東", 7:"南東", 8:"南南東", 9:"南", 10:"南南西",
  11:"南西", 12:"西南西", 13:"西", 14:"西北西", 15:"北西", 16:"北北西"
};

function parseBeforeinfo(html, {date,pid,raceNo,url}){
  const $ = loadHTML(html);

  // スタ展 ST（右側の図）
  const stByLane = {};
  $("div.table1_boatImage1").each((_, el) => {
    const laneText = $(el).find(".table1_boatImage1Number,[class*='table1_boatImage1Number']").text().trim();
    const timeText = $(el).find(".table1_boatImage1Time,[class*='table1_boatImage1Time']").text().trim();
    const lane = parseInt(laneText,10);
    if (lane>=1 && lane<=6) stByLane[lane] = timeText || "";
  });

  // --- 天候・水面気象（右側「水面気象情報」） ---
  const wRoot = $(".weather1");
  const weatherText = wRoot.find(".weather1_bodyUnit.is-weather .weather1_bodyUnitLabelTitle").text().trim() || null;

  // 気温
  const tempTxt = wRoot.find(".weather1_bodyUnit.is-direction .weather1_bodyUnitLabelData").first().text().trim();
  const temperature = tempTxt ? parseFloat(tempTxt.replace(/[^\d.]/g,"")) : null;

  // 風速
  const windTxt = wRoot.find(".weather1_bodyUnit.is-wind .weather1_bodyUnitLabelData").text().trim();
  const windSpeed = windTxt ? parseFloat(windTxt.replace(/[^\d.]/g,"")) : null;

  // ★風向（クラス名 is-windNN を読む）
  let windDirection = null;
  const dirClass = wRoot.find(".weather1_bodyUnit.is-windDirection .weather1_bodyUnitImage").attr("class") || "";
  const mDir = dirClass.match(/is-wind(\d{1,2})/);
  if (mDir) {
    const key = parseInt(mDir[1],10);
    windDirection = WIND_DIR_MAP[key] || null;
  }

  // 水温
  const waterTxt = wRoot.find(".weather1_bodyUnit.is-waterTemperature .weather1_bodyUnitLabelData").text().trim();
  const waterTemperature = waterTxt ? parseFloat(waterTxt.replace(/[^\d.]/g,"")) : null;

  // 波高（cm → m）
  const waveTxt = wRoot.find(".weather1_bodyUnit.is-wave .weather1_bodyUnitLabelData").text().trim();
  const waveHeight = waveTxt ? (parseFloat(waveTxt.replace(/[^\d.]/g,""))/100) : null;

  // 安定板使用（見出しラベルに存在するか）
  const stabilizer = $(".title16_titleLabels__add2020 .label2:contains('安定板')").length > 0;

  // --- 直前情報テーブル（左側） ---
  const entries = [];
  const tbodies = $('table.is-w748 tbody');
  tbodies.each((i, tbody) => {
    const lane = i + 1;
    const $tb = $(tbody);

    let number = "", name = "";
    $tb.find('a[href*="toban="]').each((_, a) => {
      const href = $(a).attr("href") || "";
      const m = href.match(/toban=(\d{4})/); if (m) number = m[1];
      const t = $(a).text().replace(/\s+/g," ").trim(); if (t) name = t;
    });

    const tds = $tb.find("tr").first().find("td").toArray();
    const texts = tds.map(td => ($(td).text()||"").replace(/\s+/g,"").trim());
    let weight="", tenjiTime="", tilt="";
    const kgIdx = texts.findIndex(t=>/kg$/i.test(t));
    if (kgIdx!==-1){ weight=texts[kgIdx]||""; tenjiTime=texts[kgIdx+1]||""; tilt=texts[kgIdx+2]||""; }

    const st = stByLane[lane] || "";
    const stFlag = st.startsWith("F") ? "F" : "";

    entries.push({ lane, number, name, weight, tenjiTime, tilt, st, stFlag });
  });

  return {
    date, pid, race: `${raceNo}R`, source: url, mode: "beforeinfo",
    generatedAt: new Date().toISOString(),
    weather: {
      weather: weatherText ? weatherText.replace(/(り|のち.*)?$/,"") : null, // 例: "曇り"→"曇"
      temperature,
      windSpeed,
      windDirection,
      waterTemperature,
      waveHeight,
      stabilizer
    },
    entries
  };
}

async function main(){
  for (const pid of PIDS){
    let raceList;
    if (RACES.length===1 && RACES[0]==="auto"){
      raceList = await pickRacesAuto(DATE,pid);
      log(`auto-picked races (${pid}): ${raceList.join(", ") || "(none)"}`);
      if (raceList.length===0) continue;
    } else {
      raceList = RACES;
    }

    for (const raceNo of raceList){
      const outPath = path.join(__dirname,"..","public","exhibition","v1",DATE,pid,`${raceNo}R.json`);
      if (SKIP_EXISTING && fs.existsSync(outPath)){ log("skip existing:", path.relative(process.cwd(), outPath)); continue; }
      try{
        const { url, html } = await fetchBeforeinfo({date:DATE,pid,raceNo});
        const data = parseBeforeinfo(html,{date:DATE,pid,raceNo,url});
        if (!data.entries || data.entries.length===0){ log(`no entries -> skip save: ${DATE}/${pid}/${raceNo}R`); continue; }
        await writeJSON(outPath, data);
        log("saved:", path.relative(process.cwd(), outPath));
      }catch(err){
        console.error(`Failed: date=${DATE} pid=${pid} race=${raceNo} -> ${String(err)}`);
      }
    }
  }
}

main().catch(e=>{ console.error(e); process.exit(1); });
