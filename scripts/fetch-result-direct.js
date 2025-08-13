// 出力: public/results/v1/<date>/<pid>/<race>.json
// 使い方:
//   node scripts/fetch-results-direct.js <YYYYMMDD> <pid:01..24|01,05|all> <race:1R|1..12|1,3,5|auto>
//   環境変数: TARGET_DATE / TARGET_PIDS / TARGET_RACES / RESULT_AUTO_AFTER_MIN
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

function toJST(dateYYYYMMDD, hhmm){
  return new Date(`${dateYYYYMMDD.slice(0,4)}-${dateYYYYMMDD.slice(4,6)}-${dateYYYYMMDD.slice(6,8)}T${hhmm}:00+09:00`);
}

// programs から締切時刻をゆるく拾う（あれば）
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
    const triggerMin = Math.floor((toJST(date,hhmm).getTime()+AUTO_AFTER_MIN*60000)/60000); // 「締切＋X分」後をトリガ
    if (nowMin >= triggerMin) out.push(r);
  }
  return out;
}

// ---------- Fetchers ----------
async function fetchText(url){
  const res = await fetch(url, { headers: { "user-agent":"Mozilla/5.0", "accept-language":"ja,en;q=0.8" }});
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
  return await res.text();
}
const resultUrl = ({date,pid,raceNo}) => `https://www.boatrace.jp/owpc/pc/race/result?hd=${date}&jcd=${pid}&rno=${raceNo}`;
const payUrl    = ({date,pid,raceNo}) => `https://www.boatrace.jp/owpc/pc/race/pay?hd=${date}&jcd=${pid}&rno=${raceNo}`;

// ---------- Parsers（クラス名に依存しない、見出しテキスト駆動） ----------

// ユーティリティ: 行テキスト→連続空白を1つへ
const norm = (s)=>String(s||"").replace(/\s+/g," ").trim();

// 1) 結果ページ（着順表・スタート情報・気象・決まり手）
function parseResult(html){
  const $ = loadHTML(html);
  const pageText = $("body").text();

  // 着順テーブル: 見出しに「着」「枠」「ボートレーサー」が並ぶ table を探す
  let entries = [];
  $("table,div,section").each((_, box)=>{
    const txt = norm($(box).text());
    if (!/着/.test(txt) || !/枠/.test(txt) || !/ボートレーサー/.test(txt)) return;
    // 行候補
    const rows = $(box).find("tr").length ? $(box).find("tr") : $(box).children("div");
    const out = [];
    rows.each((i, tr)=>{
      const cells = $(tr).find("th,td,div").toArray().map(el=>norm($(el).text())).filter(Boolean);
      // だいたい: [着, 枠, 選手名(と登録番号), タイム]
      if (cells.length>=3 && /^[1-6]$/.test(cells[0]) && /^[1-6]$/.test(cells[1])) {
        const finish = Number(cells[0]);
        const lane   = Number(cells[1]);
        // 登録番号4桁が含まれることが多い
        const mNo = cells.find(t=>/\d{4}/.test(t))?.match(/(\d{4})/);
        const number = mNo ? mNo[1] : null;
        // 名前は全角空白を潰して抽出
        const nameCell = cells.slice(2).find(t=>/[\u3040-\u30FF\u4E00-\u9FFF]/.test(t)) || "";
        // レースタイムは 1'51"1 の形
        const timeCell = cells.find(t=>/\d+'\d{2}"\d/.test(t)) || null;

        out.push({ finish, lane, number, name: nameCell.replace(/\s+/g,""), time: timeCell });
      }
    });
    if (out.length) entries = out;
  });

  // スタート情報（行に「.11」「.03 まくり」等）
  const startInfo = [];
  // まず、見出し「スタート情報」を含む塊を探す
  let startBox = null;
  $("section,div,table").each((_, el)=>{
    const t = norm($(el).text());
    if (/スタート情報/.test(t)) { startBox = $(el); return false; }
  });
  if (startBox) {
    const lines = norm(startBox.text()).split(/\s+/);
    // 例: "1 .11 2 .15 3 .17 4 .03 まくり 6 .04 5 .05"
    for (let i=0;i<lines.length;i++){
      const lane = Number(lines[i]);
      const st = lines[i+1];
      if (Number.isFinite(lane) && /^\.\d{2}$/.test(st)) {
        // 次のトークンが決まり手っぽい場合（ひとまず無視。決まり手は別で拾う）
        startInfo.push({ lane, ST: st });
        i++;
      }
    }
  } else {
    // フォールバック: body テキストから抽出
    const m = norm(pageText).match(/スタート情報([^\n]+)/);
    if (m){
      const tokens = m[1].trim().split(/\s+/);
      for (let i=0;i<tokens.length;i++){
        const lane = Number(tokens[i]);
        const st = tokens[i+1];
        if (Number.isFinite(lane) && /^\.\d{2}$/.test(st)) { startInfo.push({ lane, ST: st }); i++; }
      }
    }
  }

  // 決まり手
  let kimarite = null;
  $("*").each((_, el)=>{
    const t = norm($(el).text());
    if (/決まり手/.test(t)) {
      const mm = t.match(/決まり手\s*([^\s]+)/);
      if (mm) { kimarite = mm[1]; return false; }
    }
  });
  if (!kimarite) {
    const m = norm(pageText).match(/決まり手\s*([^\s]+)/);
    if (m) kimarite = m[1];
  }

  // 水面気象情報
  const weather = {};
  const all = norm(pageText);
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

  return { entries, startInfo, kimarite, weather };
}

// 2) 払戻ページ（式別ごとに組番・金額・人気）
function parsePay(html){
  const $ = loadHTML(html);
  const pays = [];
  // 各テーブル/ブロックを走査して「勝式」「組番」「払戻金」「人気」らしき行を抽出
  $("table,div,section").each((_, box)=>{
    const txt = norm($(box).text());
    if (!/勝式/.test(txt) && !/3連単|3連複|2連単|2連複|拡連複|単勝|複勝/.test(txt)) return;

    // 行を拾う
    $(box).find("tr").each((_, tr)=>{
      const cells = $(tr).find("th,td,div").toArray().map(el=>norm($(el).text()));
      const line = cells.join(" ");
      // 例: "3連単 4-6-2 ¥1,960 8"
      const kindM = line.match(/(3連単|3連複|2連単|2連複|拡連複|単勝|複勝)/);
      const yenM  = line.match(/¥?([\d,]+)\b/);
      const popM  = line.match(/\b(\d+)\s*$/); // 行末の数字を人気とみなす（安全性のため後段でフィルタ）
      if (kindM && yenM) {
        // 組番抽出（=や-や空白を許容）
        let comb = null;
        const after = line.slice(line.indexOf(kindM[1]) + kindM[1].length).trim();
        const combM = after.match(/([1-6][=-][1-6](?:[=-][1-6])?|[1-6])+/);
        if (combM) comb = combM[0].replace(/=/g,"=").replace(/-/g,"-");
        const amount = Number(yenM[1].replace(/,/g,""));
        const popularity = popM ? Number(popM[1]) : null;
        pays.push({ kind: kindM[1], combo: comb, amount, popularity });
      }
    });
  });

  // フォールバック（ページ全体テキストから）
  if (pays.length===0){
    const t = norm($("body").text());
    const re = /(3連単|3連複|2連単|2連複|拡連複|単勝|複勝)\s+([1-6= -]+)\s+¥?([\d,]+)\s+(\d+)/g;
    let m; while ((m = re.exec(t))) {
      pays.push({ kind: m[1], combo: norm(m[2]).replace(/\s+/g,""), amount: Number(m[3].replace(/,/g,"")), popularity: Number(m[4]) });
    }
  }
  return pays;
}

// ---------- I/O ----------
function ensureDirSync(dir){ fs.mkdirSync(dir, { recursive:true }); }
async function writeJSON(file, data){
  ensureDirSync(path.dirname(file));
  await fsp.writeFile(file, JSON.stringify(data,null,2), "utf8");
}

// ---------- Main ----------
async function runOne({date,pid,raceNo}){
  const outPath = path.join(__dirname,"..","public","results","v1",date,pid,`${raceNo}R.json`);

  const urlR = resultUrl({date,pid,raceNo});
  const urlP = payUrl({date,pid,raceNo});
  log("GET", urlR);
  const htmlR = await fetchText(urlR);
  log("GET", urlP);
  const htmlP = await fetchText(urlP);

  const parsedR = parseResult(htmlR);
  const parsedP = parsePay(htmlP);

  // 「結果」がゼロ件なら保存しない（開始前など）
  if (!parsedR.entries || parsedR.entries.length===0){
    log(`no parsed results -> skip save: ${date}/${pid}/${raceNo}R`);
    return false;
  }

  const payload = {
    date, pid, race: `${raceNo}R`,
    source: { result: urlR, pay: urlP },
    generatedAt: new Date().toISOString(),
    result: parsedR,
    payout: parsedP
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