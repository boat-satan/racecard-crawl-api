// 出力: public/results/v1/<date>/<pid>/<race>.json
// 使い方:
//   node scripts/fetch-results-boaters.js <YYYYMMDD> <pid:01..24|01,05|all> <race:1R|1..12|1,3,5|auto>
//   環境変数: TARGET_DATE / TARGET_PIDS / TARGET_RACES / RESULT_AUTO_AFTER_MIN
// 依存: cheerio

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { load as loadHTML } from "cheerio";

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const log = (...a)=>console.log("[result]", ...a);
const usageAndExit = () => {
  console.error("Usage: node scripts/fetch-results-boaters.js <YYYYMMDD> <pid:01..24|01,05|all> <race: 1R|1..12|1,3,5|auto>");
  process.exit(1);
};

/* ====== 入力 ====== */
const argvDate = process.argv[2];
const argvPid  = process.argv[3];
const argvRace = process.argv[4];

const DATE = (process.env.TARGET_DATE || argvDate || "").replace(/-/g,"");
let PIDS = (process.env.TARGET_PIDS || argvPid || "").split(",").map(s=>s.trim()).filter(Boolean);
const RACES_EXPR = process.env.TARGET_RACES || argvRace || "";
const AUTO_AFTER_MIN = Number(process.env.RESULT_AUTO_AFTER_MIN || 10);

if (!DATE || !RACES_EXPR || (!PIDS.length && argvPid!=="all")) usageAndExit();
if (PIDS.length===1 && PIDS[0]==="all") PIDS = Array.from({length:24},(_,i)=>String(i+1).padStart(2,"0"));

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
if (RACES.length===0) usageAndExit();

/* ====== ユーティリティ ====== */
const pidToSlug = {
  "01":"kiryu","02":"toda","03":"edogawa","04":"heiwajima","05":"tamagawa",
  "06":"hamanako","07":"gamagori","08":"tokoname","09":"tsu","10":"mikuni",
  "11":"biwako","12":"suminoe","13":"amagasaki","14":"naruto","15":"marugame",
  "16":"kojima","17":"miyajima","18":"tokuyama","19":"shimonoseki","20":"wakamatsu",
  "21":"ashiya","22":"fukuoka","23":"karatsu","24":"omura",
};
function yyyy_mm_dd(dateYYYYMMDD){
  return `${dateYYYYMMDD.slice(0,4)}-${dateYYYYMMDD.slice(4,6)}-${dateYYYYMMDD.slice(6,8)}`;
}
function toJST(dateYYYYMMDD, hhmm){
  return new Date(`${dateYYYYMMDD.slice(0,4)}-${dateYYYYMMDD.slice(4,6)}-${dateYYYYMMDD.slice(6,8)}T${hhmm}:00+09:00`);
}

// programs から締切時刻（あれば）
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
    const triggerMin = Math.floor((toJST(date,hhmm).getTime()+AUTO_AFTER_MIN*60000)/60000); // 締切＋X分
    if (nowMin >= triggerMin) out.push(r);
  }
  return out;
}

async function fetchText(url){
  const res = await fetch(url, { headers: { "user-agent":"Mozilla/5.0", "accept-language":"ja,en;q=0.8" }});
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
  return await res.text();
}
function ensureDirSync(dir){ fs.mkdirSync(dir, { recursive:true }); }
async function writeJSON(file, data){
  ensureDirSync(path.dirname(file));
  await fsp.writeFile(file, JSON.stringify(data,null,2), "utf8");
}
const norm = (s)=>String(s||"").replace(/\s+/g," ").trim();

/* ====== URL (Boaters) ====== */
const boatersResultUrl = ({date,pid,raceNo}) => {
  const slug = pidToSlug[pid]; if (!slug) throw new Error(`unknown pid ${pid}`);
  return `https://boaters-boatrace.com/race/${slug}/${yyyy_mm_dd(date)}/${raceNo}R/race-result`;
};

/* ====== Parser (Boaters 専用) ====== */
function parseBoaters(html){
  const $ = loadHTML(html);

  // --- 着順表 ---
  // ヘッダに「着順 / 枠番 / ボートレーサー」などがある table を探す
  let entries = [];
  $("table").each((_, t)=>{
    const head = $(t).find("th").map((_,th)=>norm($(th).text())).get().join("|");
    if (!/(着|着順)/.test(head) || !/(枠|枠番)/.test(head)) return;

    const out = [];
    $(t).find("tbody tr").each((__, tr)=>{
      const cells = $(tr).find("th,td").map((___, td)=>norm($(td).text())).get();
      // 代表的: 着順,枠番,選手名(登録番号),タイム など
      if (cells.length<2) return;
      const finish = Number(cells[0]);
      const lane   = Number(cells[1]);
      if (!Number.isFinite(finish) || !Number.isFinite(lane)) return;

      // 登録番号 4桁
      const numberM = cells.join(" ").match(/\b(\d{4})\b/);
      const number = numberM ? numberM[1] : null;

      // 選手名（漢字/かな含む最長テキストを拾う）
      const name = cells.map(c=>c).find(c=>/[\u3040-\u30FF\u4E00-\u9FFF]/.test(c))||"";

      // レースタイム 1'51"1
      const time = (cells.find(c=>/\d+'\d{2}"\d/.test(c))||null);

      out.push({ finish, lane, number, name: name.replace(/\s+/g,""), time });
    });
    if (out.length) entries = out;
  });

  // --- スタート情報（ST） ---
  let startInfo = [];
  // Boaters は「スタート情報」テキストの近くに table があることが多い
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
      // 例: 枠番, ST, （決まり手が同じテーブルにある場合も）
      if (cells.length>=2) {
        const lane = Number(cells[0]);
        const st   = cells[1].replace(/^([0-9])$/, ".$1"); // ".11" 形式に正規化試み
        if (Number.isFinite(lane) && /^\.\d{2}$/.test(st)) startInfo.push({ lane, ST: st });
      }
    });
  }
  if (startInfo.length===0){
    // フォールバック：本文テキストから lane .XX パターン
    const body = norm($("body").text());
    const m = body.match(/スタート情報([^]+?)勝式|スタート情報([^]+?)水面気象情報/);
    const blob = m ? (m[1]||m[2]||"") : "";
    const tokens = blob.split(/[\s\/,]+/);
    for (let i=0;i<tokens.length;i++){
      const lane = Number(tokens[i]);
      const st = tokens[i+1];
      if (Number.isFinite(lane) && /^\.\d{2}$/.test(st)) { startInfo.push({ lane, ST: st }); i++; }
    }
  }

  // --- 決まり手 ---
  let kimarite = null;
  $("*").each((_, el)=>{
    const t = norm($(el).text());
    const m = t.match(/決まり手\s*[:：]?\s*([^\s]+)/);
    if (m){ kimarite = m[1]; return false; }
  });

  // --- 払戻（ページ内の「勝式」テーブル群） ---
  const payout = [];
  $("table").each((_, t)=>{
    const head = $(t).find("th").map((_,th)=>norm($(th).text())).get().join("|");
    if (!/勝式/.test(head)) return;
    $(t).find("tbody tr").each((__, tr)=>{
      const cells = $(tr).find("th,td").map((___,td)=>norm($(td).text())).get();
      if (cells.length<2) return;

      // 形式: 勝式 / 組番 / 払戻金 / 人気 など
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

  // --- 水面気象（ざっくりテキスト抽出） ---
  const all = norm($("body").text());
  const weather = {};
  const tempM = all.match(/気温\s*([\d.]+)℃/);        if (tempM) weather.temperature = Number(tempM[1]);
  const windM = all.match(/風速\s*([\d.]+)m/);         if (windM) weather.windSpeed = Number(windM[1]);
  const waterM = all.match(/水温\s*([\d.]+)℃/);        if (waterM) weather.waterTemperature = Number(waterM[1]);
  const waveM = all.match(/波高\s*([\d.]+)cm|波高\s*([\d.]+)m/);
  if (waveM) {
    const cm = waveM[1], m = waveM[2];
    weather.waveHeight = cm ? Number(cm)/100 : Number(m);
  }
  const weatherWord = (all.match(/(晴|曇|雨|雪)/) || [])[1] || null;
  if (weatherWord) weather.weather = weatherWord;

  return { entries, startInfo, kimarite, weather, payout };
}

/* ====== 実行 ====== */
async function runOne({date,pid,raceNo}){
  const outPath = path.join(__dirname,"..","public","results","v1",date,pid,`${raceNo}R.json`);
  const url = boatersResultUrl({date,pid,raceNo});
  log("GET", url);
  const html = await fetchText(url);

  const parsed = parseBoaters(html);
  // 着順が拾えない場合は保存しない
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
    payout: parsed.payout || []
  };
  await writeJSON(outPath, payload);
  log("saved:", path.relative(process.cwd(), outPath));
  return true;
}

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