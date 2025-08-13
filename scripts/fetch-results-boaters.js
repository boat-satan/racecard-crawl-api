// scripts/fetch-results-boaters.js (debug強化/odds対応/スキップ対応)
// 出力:
//   結果  : public/results/v1/<date>/<pid>/<race>.json
//   オッズ: public/odds/v1/<date>/<pid>/<race>.json  ← 3連単のみ
//   失敗時: public/debug/boaters/<date>/<pid>/<race>-{result|odds}.html
// 使い方:
//   node scripts/fetch-results-boaters.js <YYYYMMDD> <pid:01..24|01,05|all> <race:1R|1..12|1,3,5|auto>
//     [--skip-existing] [--with-odds] [--debug-raw]
// 環境変数: TARGET_DATE / TARGET_PIDS / TARGET_RACES / RESULT_AUTO_AFTER_MIN
//          SKIP_EXISTING=1 / WITH_ODDS=1 / DEBUG_RAW=1

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { load as loadHTML } from "cheerio";

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const log = (...a)=>console.log("[result]", ...a);
const usageAndExit = () => {
  console.error("Usage: node scripts/fetch-results-boaters.js <YYYYMMDD> <pid:01..24|01,05|all> <race: 1R|1..12|1,3,5|auto> [--skip-existing] [--with-odds] [--debug-raw]");
  process.exit(1);
};

/* ====== 入力/フラグ ====== */
const argvDate = process.argv[2];
const argvPid  = process.argv[3];
const argvRace = process.argv[4];

const DATE = (process.env.TARGET_DATE || argvDate || "").replace(/-/g,"");
let PIDS = (process.env.TARGET_PIDS || argvPid || "").split(",").map(s=>s.trim()).filter(Boolean);
const RACES_EXPR = process.env.TARGET_RACES || argvRace || "";
const AUTO_AFTER_MIN = Number(process.env.RESULT_AUTO_AFTER_MIN || 10);

const SKIP_EXISTING =
  process.argv.includes("--skip-existing") ||
  /^(1|true|yes)$/i.test(String(process.env.SKIP_EXISTING || ""));
const WITH_ODDS =
  process.argv.includes("--with-odds") ||
  /^(1|true|yes)$/i.test(String(process.env.WITH_ODDS || ""));
const DEBUG_RAW =
  process.argv.includes("--debug-raw") ||
  /^(1|true|yes)$/i.test(String(process.env.DEBUG_RAW || ""));

if (!DATE || !RACES_EXPR || (!PIDS.length && argvPid!=="all")) usageAndExit();
if (PIDS.length===1 && PIDS[0]==="all") PIDS = Array.from({length:24},(_,i)=>String(i+1).padStart(2,"0"));

/* ====== 共通ユーティリティ ====== */
const pidToSlug = {
  "01":"kiryu","02":"toda","03":"edogawa","04":"heiwajima","05":"tamagawa",
  "06":"hamanako","07":"gamagori","08":"tokoname","09":"tsu","10":"mikuni",
  "11":"biwako","12":"suminoe","13":"amagasaki","14":"naruto","15":"marugame",
  "16":"kojima","17":"miyajima","18":"tokuyama","19":"shimonoseki","20":"wakamatsu",
  "21":"ashiya","22":"fukuoka","23":"karatsu","24":"omura",
};
const norm = (s)=>String(s||"").replace(/\s+/g," ").trim();
function yyyy_mm_dd(yymmdd){ return `${yymmdd.slice(0,4)}-${yymmdd.slice(4,6)}-${yymmdd.slice(6,8)}`; }
function toJST(yymmdd, hhmm){
  return new Date(`${yymmdd.slice(0,4)}-${yymmdd.slice(4,6)}-${yymmdd.slice(6,8)}T${hhmm}:00+09:00`);
}
function ensureDirSync(d){ fs.mkdirSync(d, { recursive:true }); }
async function writeJSON(file, data){ ensureDirSync(path.dirname(file)); await fsp.writeFile(file, JSON.stringify(data,null,2), "utf8"); }
function ensureKeep(dir){ try { fs.writeFileSync(path.join(dir, ".keep"), ""); } catch {} }
function dbgPath(date,pid,race,kind){
  return path.join(__dirname,"..","public","debug","boaters",date,pid,`${race}${kind}.html`);
}
async function dumpRawIfNeeded(html, date, pid, race, kind){
  if (!DEBUG_RAW) return;
  const out = dbgPath(date,pid,race,kind);
  ensureDirSync(path.dirname(out));
  await fsp.writeFile(out, html, "utf8");
  log(`debug html saved: ${path.relative(process.cwd(), out)}`);
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
const RACES = expandRaces(RACES_EXPR);

/* ====== fetch ====== */
async function fetchText(url){
  const res = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
      "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "accept-language": "ja,en;q=0.8",
      "referer": "https://boaters-boatrace.com/",
      "upgrade-insecure-requests": "1",
      "cache-control": "no-cache",
      "pragma": "no-cache"
    }
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
  return await res.text();
}

/* ====== URL ====== */
const boatersResultUrl = ({date,pid,raceNo}) =>
  `https://boaters-boatrace.com/race/${pidToSlug[pid]}/${yyyy_mm_dd(date)}/${raceNo}R/race-result`;
const boatersOddsUrl = ({date,pid,raceNo}) =>
  `https://boaters-boatrace.com/race/${pidToSlug[pid]}/${yyyy_mm_dd(date)}/${raceNo}R/odds`;

/* ====== 解析: 結果 ====== */
function parseBoaters(html){
  const $ = loadHTML(html);
  let entries = [];

  // 1) table ベース
  $("table").each((_, t)=>{
    const head = $(t).find("th").map((_,th)=>norm($(th).text())).get().join("|");
    if (!/(着|着順)/.test(head) || !/(枠|枠番|コース)/.test(head)) return;
    const out = [];
    $(t).find("tbody tr").each((__, tr)=>{
      const cells = $(tr).find("th,td").map((___, td)=>norm($(td).text())).get();
      if (cells.length < 2) return;
      const finish = Number(cells[0]);
      const lane   = Number(cells[1]);
      if (!Number.isFinite(finish) || !Number.isFinite(lane)) return;
      const numberM = cells.join(" ").match(/\b(\d{4})\b/);
      const number  = numberM ? numberM[1] : null;
      const name    = cells.find(c=>/[\u3040-\u30FF\u4E00-\u9FFF]/.test(c)) || "";
      const time    = cells.find(c=>/\d+'\d{2}"\d/.test(c)) || null;
      out.push({ finish, lane, number, name: name.replace(/\s+/g,""), time });
    });
    if (out.length) entries = out;
  });

  // 2) div レイアウトのフォールバック: 「1着 1 枠」「2着 3 枠」風のパターン
  if (entries.length === 0){
    const body = norm($("body").text());
    // 例: "着順 1 1 2 3 3 2 ..." のように出てくることがある
    const m = body.match(/着順([^]+?)スタート情報|着順([^]+?)勝式/);
    const blob = m ? (m[1]||m[2]||"") : "";
    const nums = blob.match(/\b[1-6]\b/g) || [];
    // 先頭から (着順, 枠) のペアを推定（粗いが何も無いより良い）
    const out = [];
    for (let i=0;i<nums.length-1 && out.length<6; i+=2){
      const finish = Number(nums[i]);
      const lane   = Number(nums[i+1]);
      if (finish>=1 && finish<=6 && lane>=1 && lane<=6) out.push({ finish, lane, number:null, name:"", time:null });
    }
    if (out.length) entries = out;
  }

  // スタート情報
  let startInfo = [];
  let stTable = null;
  $("h2,h3,section,div").each((_, el)=>{
    const t = norm($(el).text());
    if (/スタート情報/.test(t)) {
      const near = $(el).nextAll("table").first();
      if (near && near.length) stTable = near;
      return false;
    }
  });
  if (stTable){
    stTable.find("tbody tr").each((_, tr)=>{
      const cells = $(tr).find("th,td").map((__, td)=>norm($(td).text())).get();
      if (cells.length>=2) {
        const lane = Number(cells[0]);
        const st   = cells[1].replace(/^([0-9])$/, ".$1");
        if (Number.isFinite(lane) && /^\.\d{2}$/.test(st)) startInfo.push({ lane, ST: st });
      }
    });
  }

  // 決まり手
  let kimarite = null;
  $("*").each((_, el)=>{
    const t = norm($(el).text());
    const mm = t.match(/決まり手\s*[:：]?\s*([^\s]+)/);
    if (mm){ kimarite = mm[1]; return false; }
  });

  // 払戻
  const payout = [];
  $("table").each((_, t)=>{
    const head = $(t).find("th").map((_,th)=>norm($(th).text())).get().join("|");
    if (!/勝式/.test(head)) return;
    $(t).find("tbody tr").each((__, tr)=>{
      const cells = $(tr).find("th,td").map((___,td)=>norm($(td).text())).get();
      if (cells.length<2) return;
      const kind  = cells.find(c=>/(3連単|3連複|2連単|2連複|拡連複|単勝|複勝)/.test(c)) || null;
      const combo = (cells.find(c=>/^[1-6](?:[-=][1-6]){0,2}$/.test(c)) || cells.find(c=>/[1-6].*[-=].*[1-6]/) || null);
      const yenM  = cells.join(" ").match(/([\d,]+)\s*円?/);
      const popM  = cells.join(" ").match(/人気\s*:?[\s]*([0-9]+)/) || cells.join(" ").match(/(\d+)\s*$/);
      if (kind && yenM) {
        payout.push({
          kind: kind.replace(/\s/g,""),
          combo: combo ? combo.replace(/\s/g,"") : null,
          amount: Number(yenM[1].replace(/,/g,"")),
          popularity: popM ? Number(popM[1]) : null
        });
      }
    });
  });

  // 水面気象
  const all = norm($("body").text());
  const weather = {};
  const tempM = all.match(/気温\s*([\d.]+)℃/);        if (tempM) weather.temperature = Number(tempM[1]);
  const windM = all.match(/風速\s*([\d.]+)m/);         if (windM) weather.windSpeed = Number(windM[1]);
  const waterM = all.match(/水温\s*([\d.]+)℃/);        if (waterM) weather.waterTemperature = Number(waterM[1]);
  const waveM = all.match(/波高\s*([\d.]+)cm|波高\s*([\d.]+)m/);
  if (waveM) { const cm = waveM[1], m = waveM[2]; weather.waveHeight = cm ? Number(cm)/100 : Number(m); }
  const weatherWord = (all.match(/(晴|曇|雨|雪)/) || [])[1] || null;
  if (weatherWord) weather.weather = weatherWord;

  return { entries, startInfo, kimarite, weather, payout };
}

/* ====== 解析: 3連単オッズ ====== */
function parseBoatersOdds(html){
  const $ = loadHTML(html);
  let odds = [];

  let table = null;
  $("h1,h2,h3,section,div").each((_, el)=>{
    const t = norm($(el).text());
    if (/(3連単|三連単)/.test(t)) {
      const near = $(el).nextAll("table").first();
      if (near && near.length) { table = near; return false; }
    }
  });
  if (!table){
    $("table").each((_, t)=>{
      const head = $(t).find("th").map((_,th)=>norm($(th).text())).get().join("|");
      if (/(3連単|三連単)/.test(head) || /(組番|オッズ)/.test(head)) { table = $(t); return false; }
    });
  }

  const pushOdds = (combo, val) => {
    const c = String(combo).replace(/\s/g,"");
    const v = Number(String(val).replace(/,/g,""));
    if (/^[1-6]-[1-6]-[1-6]$/.test(c) && Number.isFinite(v)) odds.push({ combo:c, odds:v });
  };

  if (table){
    table.find("tbody tr").each((_, tr)=>{
      const cells = $(tr).find("th,td").map((__,td)=>norm($(td).text())).get();
      const combo = cells.find(c=>/^[1-6]-[1-6]-[1-6]$/.test(c));
      const val   = cells.find(c=>/^\d+(?:\.\d+)?$/.test(c));
      if (combo && val) pushOdds(combo, val);
      else {
        for (let i=0;i<cells.length-1;i+=2){
          if (/^[1-6]-[1-6]-[1-6]$/.test(cells[i]) && /^\d+(?:\.\d+)?$/.test(cells[i+1])) {
            pushOdds(cells[i], cells[i+1]);
          }
        }
      }
    });
  }

  // 本文フォールバック
  if (odds.length===0){
    const body = norm($("body").text());
    const re = /([1-6]-[1-6]-[1-6])\s+(\d+(?:\.\d+)?)/g;
    let m; while ((m = re.exec(body))) pushOdds(m[1], m[2]);
  }

  // 重複除去＆安い順
  const map = new Map();
  for (const o of odds){ if (!map.has(o.combo) || map.get(o.combo).odds !== o.odds) map.set(o.combo, o); }
  odds = [...map.values()].sort((a,b)=>a.odds-b.odds);
  return odds;
}

/* ====== 保存: 結果 ====== */
async function runOneResult({date,pid,raceNo}){
  const rootDir = path.join(__dirname,"..","public","results","v1");
  const dayDir  = path.join(rootDir, date);
  const pidDir  = path.join(dayDir, pid);
  const outPath = path.join(pidDir, `${raceNo}R.json`);

  if (SKIP_EXISTING && fs.existsSync(outPath)) {
    log(`skip existing (result): ${path.relative(process.cwd(), outPath)}`);
    return false;
  }

  fs.mkdirSync(pidDir, { recursive: true });
  ensureKeep(rootDir); ensureKeep(dayDir); ensureKeep(pidDir);

  const url = boatersResultUrl({date,pid,raceNo});
  log("GET", url);
  const html = await fetchText(url);

  const parsed = parseBoaters(html);
  if (!parsed.entries || parsed.entries.length===0){
    log(`no parsed results -> skip save: ${date}/${pid}/${raceNo}R`);
    await dumpRawIfNeeded(html, date, pid, `${raceNo}R`, "-result");
    return false;
  }

  const payload = {
    date, pid, race: `${raceNo}R`,
    source: { result: url },
    generatedAt: new Date().toISOString(),
    result: {
      entries: parsed.entries,
      startInfo: parsed.startInfo,
      kimarite: parsed.kimarite,
      weather: parsed.weather
    },
    payout: parsed.payout || []
  };
  await writeJSON(outPath, payload);
  log("saved (result):", path.relative(process.cwd(), outPath));
  return true;
}

/* ====== 保存: 3連単オッズ ====== */
async function runOneOdds({date,pid,raceNo}){
  const rootDir = path.join(__dirname,"..","public","odds","v1");
  const dayDir  = path.join(rootDir, date);
  const pidDir  = path.join(dayDir, pid);
  const outPath = path.join(pidDir, `${raceNo}R.json`);

  if (SKIP_EXISTING && fs.existsSync(outPath)) {
    log(`skip existing (odds): ${path.relative(process.cwd(), outPath)}`);
    return false;
  }

  fs.mkdirSync(pidDir, { recursive: true });
  ensureKeep(rootDir); ensureKeep(dayDir); ensureKeep(pidDir);

  const url = boatersOddsUrl({date,pid,raceNo});
  log("GET (odds)", url);
  const html = await fetchText(url);
  const trifecta = parseBoatersOdds(html);

  if (!trifecta || trifecta.length===0){
    log(`no trifecta odds -> skip save: ${date}/${pid}/${raceNo}R`);
    await dumpRawIfNeeded(html, date, pid, `${raceNo}R`, "-odds");
    return false;
  }

  const payload = {
    date, pid, race: `${raceNo}R`,
    source: { odds: url },
    generatedAt: new Date().toISOString(),
    trifecta
  };
  await writeJSON(outPath, payload);
  log("saved (odds):", path.relative(process.cwd(), outPath));
  return true;
}

/* ====== programsからオート選択 ====== */
async function loadRaceDeadlineHHMM(date, pid, raceNo){
  const rels = [
    path.join("public","programs","v2",date,pid,`${raceNo}R.json`),
    path.join("public","programs-slim","v2",date,pid,`${raceNo}R.json`),
  ];
  for (const rel of rels){
    const abs = path.join(__dirname,"..",rel);
    if (!fs.existsSync(abs)) continue;
    try{
      const j = JSON.parse(await fsp.readFile(abs,"utf8"));
      const cand = [
        j.deadlineJST,j.closeTimeJST,j.deadline,j.closingTime,
        j.startTimeJST,j.postTimeJST,j.scheduledTimeJST,
        j.info?.deadlineJST,j.meta?.deadlineJST
      ].filter(Boolean);
      for (const c of cand){
        if (typeof c==="string" && c.includes("T")) {
          const dt = new Date(c); if (!isNaN(dt)) {
            const hh = String(dt.getHours()).padStart(2,"0");
            const mm = String(dt.getMinutes()).padStart(2,"0");
            return `${hh}:${mm}`;
          }
        }
        const m = String(c||"").match(/(\d{1,2}):(\d{2})/);
        if (m) return `${m[1].padStart(2,"0")}:${m[2]}`;
      }
    }catch{}
  }
  return null;
}
async function pickRacesAuto(date, pid){
  const nowMin = Math.floor(Date.now()/60000), out=[];
  for (let r=1;r<=12;r++){
    const hhmm = await loadRaceDeadlineHHMM(date,pid,r); if (!hhmm) continue;
    const triggerMin = Math.floor((toJST(date,hhmm).getTime()+AUTO_AFTER_MIN*60000)/60000);
    if (nowMin >= triggerMin) out.push(r);
  }
  return out;
}

/* ====== メイン ====== */
async function main(){
  for (const pid of PIDS){
    let raceList;
    if (RACES.length===1 && RACES[0]==="auto"){
      raceList = await pickRacesAuto(DATE, pid);
      log(`auto-picked races (${pid}): ${raceList.join(", ") || "(none)"}`);
      if (raceList.length===0) continue;
    } else {
      raceList = RACES;
    }
    for (const r of raceList){
      try {
        await runOneResult({date:DATE, pid, raceNo:r});
        if (WITH_ODDS) await runOneOdds({date:DATE, pid, raceNo:r});
      } catch(e){
        console.error(`Failed: date=${DATE} pid=${pid} rno=${r} -> ${e.message}`);
      }
    }
  }
}

main().catch(e=>{ console.error(e); process.exit(1); });