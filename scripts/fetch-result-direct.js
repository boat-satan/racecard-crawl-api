// 出力: public/results/v1/<date>/<pid>/<race>.json
// 使い方:
//   node scripts/fetch-results-direct.js <YYYYMMDD> <pid:01..24|01,05|all> <race:1R|1..12|1,3,5|auto>
//   環境変数: TARGET_DATE / TARGET_PIDS / TARGET_RACES / RESULT_AUTO_AFTER_MIN
//
// 変更点（完全差し替え）:
// - 公式 owpc ではなく Boaters (https://boaters-boatrace.com) から取得
// - URL 例: https://boaters-boatrace.com/race/kiryu/2025-08-12/1R/race-result
// - 払戻金も同一ページから抽出（Boatersは結果ページ内に払戻表が併記される想定）
//
// 備考:
// - 場コード(pid)→スラッグの対応は下の PID_TO_SLUG を必要に応じて調整してください
// - HTML構造差異に強い「見出しテキスト駆動＋フォールバック」パーサで実装

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { load as loadHTML } from "cheerio";

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const log = (...a)=>console.log("[result]", ...a);
const usageAndExit = () => {
  console.error("Usage: node scripts/fetch-results-direct.js <YYYYMMDD> <pid:01..24|01,05|all> <race: 1R|1..12|1,3,5|auto>");
  process.exit(1);
};

// ---------- CLI / ENV ----------
const argvDate = process.argv[2];
const argvPid  = process.argv[3];
const argvRace = process.argv[4];

const DATE = (process.env.TARGET_DATE || argvDate || "").replace(/-/g,"");
let PIDS = (process.env.TARGET_PIDS || argvPid || "").split(",").map(s=>s.trim()).filter(Boolean);
const RACES_EXPR = process.env.TARGET_RACES || argvRace || "";
const AUTO_AFTER_MIN = Number(process.env.RESULT_AUTO_AFTER_MIN || 10);

if (!DATE || !RACES_EXPR || (!PIDS.length && argvPid!=="all")) usageAndExit();

// ---------- pid -> venue slug（要確認・調整可） ----------
const PID_TO_SLUG = {
  "01":"kiryu",     "02":"toda",      "03":"edogawa",  "04":"heiwajima",
  "05":"tamagawa",  "06":"hamanako",  "07":"gamagori", "08":"tokoname",
  "09":"tsu",       "10":"mikuni",    "11":"biwako",   "12":"suminoe",
  "13":"amagasaki", "14":"ashiya",    "15":"naruto",   "16":"marugame",
  "17":"kojima",    "18":"miyajima",  "19":"tokuyama", "20":"shimonoseki",
  "21":"wakamatsu", "22":"kokura",    "23":"karatsu",  "24":"omiya" // ★必要なら修正
};

if (PIDS.length===1 && PIDS[0]==="all") PIDS = Object.keys(PID_TO_SLUG);

// ---------- RACES 解析 ----------
const normRaceToken = (tok)=> parseInt(String(tok).replace(/[^0-9]/g,""),10);
function expandRaces(expr){
  if (!expr) return [];
  if (String(expr).toLowerCase()==="auto") return ["auto"];
  if (String(expr).toLowerCase()==="all")  return Array.from({length:12},(_,i)=>i+1);
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
if (RACES.length===0) usageAndExit();

function toJST(dateYYYYMMDD, hhmm){
  return new Date(`${dateYYYYMMDD.slice(0,4)}-${dateYYYYMMDD.slice(4,6)}-${dateYYYYMMDD.slice(6,8)}T${hhmm}:00+09:00`);
}

// programs から締切時刻（近似）を拾う（auto用）
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
    const triggerMin = Math.floor((toJST(date,hhmm).getTime()+AUTO_AFTER_MIN*60000)/60000); // 締切＋X分後
    if (nowMin >= triggerMin) out.push(r);
  }
  return out;
}

// ---------- Fetchers ----------
async function fetchText(url){
  const res = await fetch(url, {
    headers: {
      "user-agent":"Mozilla/5.0",
      "accept-language":"ja,en;q=0.8",
      "cache-control":"no-cache"
    }
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
  return await res.text();
}
function toBoatersDate(dateYYYYMMDD){
  return `${dateYYYYMMDD.slice(0,4)}-${dateYYYYMMDD.slice(4,6)}-${dateYYYYMMDD.slice(6,8)}`;
}
function buildBoatersUrl({date,pid,raceNo}){
  const slug = PID_TO_SLUG[String(pid).padStart(2,"0")];
  if (!slug) throw new Error(`unknown pid slug: ${pid}`);
  const d = toBoatersDate(date);
  const r = `${raceNo}R`;
  return `https://boaters-boatrace.com/race/${slug}/${d}/${r}/race-result`;
}

// ---------- Parsers（見出しテキスト駆動＋フォールバック） ----------
const norm = (s)=>String(s||"").replace(/\s+/g," ").trim();

function parseBoatersResult(html){
  const $ = loadHTML(html);
  const bodyText = norm($("body").text());

  // 着順テーブル（「着順」や「結果」ブロック）
  let entries = [];
  $("section,div,table").each((_, box)=>{
    const t = norm($(box).text());
    if (!/(着順|結果)/.test(t)) return;
    // trベースで抽出
    const rows = $(box).find("tr");
    const out = [];
    rows.each((i, tr)=>{
      const cells = $(tr).find("th,td,div,span").toArray().map(el=>norm($(el).text())).filter(Boolean);
      // 想定: [着順, 枠(艇番), 選手名(登録番号含むことあり), タイム ...]
      if (cells.length>=3) {
        const fin = Number(cells[0]);
        const lane = Number(cells[1]);
        if (Number.isFinite(fin) && fin>=1 && fin<=6 && Number.isFinite(lane) && lane>=1 && lane<=6){
          const mNo = cells.join(" ").match(/\b(\d{4})\b/);
          const number = mNo ? mNo[1] : null;
          const nameCell = cells.slice(2).find(s=>/[\u3040-\u30FF\u4E00-\u9FFF]/.test(s)) || "";
          const timeCell = cells.find(s=>/\d+'\d{2}"\d/.test(s)) || null;
          out.push({ finish: fin, lane, number, name: nameCell.replace(/\s+/g,""), time: timeCell });
        }
      }
    });
    if (out.length) entries = out;
  });

  // スタート情報（「スタート」見出し）
  const startInfo = [];
  let startBox = null;
  $("section,div,table").each((_, el)=>{
    const t = norm($(el).text());
    if (/スタート/.test(t)) { startBox = $(el); return false; }
  });
  if (startBox){
    const tokens = norm(startBox.text()).split(/\s+/);
    for (let i=0;i<tokens.length;i++){
      const lane = Number(tokens[i]);
      const st = tokens[i+1];
      if (Number.isFinite(lane) && /^\.\d{2}$/.test(st)){ startInfo.push({ lane, ST: st }); i++; }
    }
  } else {
    // フォールバック
    const m = bodyText.match(/スタート[^\d]*(.*)$/);
    if (m){
      const tokens = m[1].split(/\s+/);
      for (let i=0;i<tokens.length;i++){
        const lane = Number(tokens[i]);
        const st = tokens[i+1];
        if (Number.isFinite(lane) && /^\.\d{2}$/.test(st)){ startInfo.push({ lane, ST: st }); i++; }
      }
    }
  }

  // 決まり手（「決まり手」）
  let kimarite = null;
  $("*").each((_, el)=>{
    const t = norm($(el).text());
    const m = t.match(/決まり手[:：]?\s*([^\s]+)/);
    if (m){ kimarite = m[1]; return false; }
  });
  if (!kimarite){
    const m = bodyText.match(/決まり手[:：]?\s*([^\s]+)/);
    if (m) kimarite = m[1];
  }

  // 払戻（ページ内に「払戻」や「払戻金」セクションがあるはず）
  const payouts = [];
  $("section,div,table").each((_, box)=>{
    const t = norm($(box).text());
    if (!/(払戻|払戻金)/.test(t)) return;
    $(box).find("tr").each((_, tr)=>{
      const cells = $(tr).find("th,td,div,span").toArray().map(el=>norm($(el).text())).filter(Boolean);
      if (cells.length<2) return;
      const line = cells.join(" ");
      const kindM = line.match(/(3連単|3連複|2連単|2連複|拡連複|単勝|複勝)/);
      if (!kindM) return;
      const kind = kindM[1];
      const yenM  = line.match(/¥\s*([\d,]+)/) || line.match(/([\d,]+)\s*円/);
      const popM  = line.match(/(\d+)\s*人気/);
      // 組番（= と - と 空白を許容）
      const after = line.slice(line.indexOf(kind)+kind.length).trim();
      const combM = after.match(/([1-6](?:[-＝= ][1-6]){0,2})/);
      const combo = combM ? combM[1].replace(/\s+/g,"").replace(/＝/g,"=") : null;
      const amount = yenM ? Number(yenM[1].replace(/,/g,"")) : null;
      const popularity = popM ? Number(popM[1]) : null;
      if (kind && combo && amount!=null){
        payouts.push({ kind, combo, amount, popularity });
      }
    });
  });

  // 水面気象（ページ下部にあれば拾う）
  const weather = {};
  const tempM = bodyText.match(/気温\s*([\d.]+)℃/);        if (tempM) weather.temperature = Number(tempM[1]);
  const windM = bodyText.match(/風速\s*([\d.]+)m/);         if (windM) weather.windSpeed = Number(windM[1]);
  const waterM = bodyText.match(/水温\s*([\d.]+)℃/);        if (waterM) weather.waterTemperature = Number(waterM[1]);
  const waveM = bodyText.match(/波高\s*([\d.]+)cm/) || bodyText.match(/波高\s*([\d.]+)m/);
  if (waveM) weather.waveHeight = /cm/.test(waveM[0]) ? Number(waveM[1])/100 : Number(waveM[1]);
  const weatherWord = (bodyText.match(/(晴|曇|雨|雪)/) || [])[1] || null;
  if (weatherWord) weather.weather = weatherWord;

  return { entries, startInfo, kimarite, weather, payouts };
}

// ---------- I/O ----------
function ensureDirSync(dir){ fs.mkdirSync(dir, { recursive:true }); }
async function writeJSON(file, data){
  ensureDirSync(path.dirname(file));
  await fsp.writeFile(file, JSON.stringify(data,null,2), "utf8");
}

// ---------- Main unit ----------
async function runOne({date,pid,raceNo}){
  const outPath = path.join(__dirname,"..","public","results","v1",date,pid,`${raceNo}R.json`);
  const url = buildBoatersUrl({date,pid,raceNo});

  log("GET", url);
  const html = await fetchText(url);

  const parsed = parseBoatersResult(html);

  // 結果ゼロなら保存しない（未確定等）
  if (!parsed.entries || parsed.entries.length===0){
    log(`no parsed results -> skip save: ${date}/${pid}/${raceNo}R`);
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
    payout: parsed.payouts || []
  };

  await writeJSON(outPath, payload);
  log("saved:", path.relative(process.cwd(), outPath));
  return true;
}

// ---------- Main ----------
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
      try { await runOne({date:DATE, pid, raceNo:r}); }
      catch(e){ console.error(`Failed: date=${DATE} pid=${pid} rno=${r} -> ${e.message}`); }
    }
  }
}

main().catch(e=>{ console.error(e); process.exit(1); });