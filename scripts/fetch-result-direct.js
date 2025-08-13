// scripts/fetch-result-direct.js
// 出力: public/results/v1/<date>/<pid>/<race>.json
// 使い方:
//   node scripts/fetch-result-direct.js <YYYYMMDD> <pid:01..24> <race: 1R|1..12|1,3,5R...|auto> [--skip-existing]
//
// 環境変数も fetch-exhibition-direct.js と同一:
//   TARGET_DATE=20250812 TARGET_PIDS=05 TARGET_RACES=auto node scripts/fetch-result-direct.js
//
// 仕様:
//  - boatrace.jp からレース結果ページ（raceresult）と払戻ページ（pay）を直取得
//  - 主要フィールドのみ堅牢に抽出（着順/艇/選手/進入/ST/決まり手/記録、失格・返還、払戻）
//  - "auto" はプログラム時刻を参照し、締切から一定分経過したものだけ実行

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { load as loadHTML } from "cheerio";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function log(...args) { console.log("[result]", ...args); }
function usageAndExit() {
  console.error("Usage: node scripts/fetch-result-direct.js <YYYYMMDD> <pid:01..24> <race: 1R|1..12|1,3,5R...|auto>");
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

// 締切から何分後に「結果取得OK」とするか（auto用）
const AUTO_AFTER_MIN = Number(process.env.RESULT_AUTO_AFTER_MIN || 10);

/* ---------- programs 参照（auto判定に利用・exhibitionと同一実装） ---------- */
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
async function pickRacesAutoFinished(date, pid){
  const nowMin = Math.floor(Date.now()/60000), out=[];
  for (let r=1;r<=12;r++){
    const hhmm = await loadRaceDeadlineHHMM(date,pid,r); if (!hhmm) continue;
    const okMin = Math.floor((toJstDate(date,hhmm).getTime() + AUTO_AFTER_MIN*60000)/60000);
    if (nowMin >= okMin) out.push(r);
  }
  return out;
}

/* ---------- fetch helpers ---------- */
async function fetchPage(url){
  const res = await fetch(url, { headers: { "user-agent":"Mozilla/5.0", "accept-language":"ja,en;q=0.8" }});
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
  return await res.text();
}
async function fetchRacePages({date,pid,raceNo}){
  // ★URLは公式のPCサイト想定（beforeinfo と同じドメイン）
  const base = "https://www.boatrace.jp/owpc/pc/race";
  const urlResult = `${base}/raceresult?hd=${date}&jcd=${pid}&rno=${raceNo}`;
  const urlPay    = `${base}/pay?hd=${date}&jcd=${pid}&rno=${raceNo}`; // 払戻一覧ページ
  log("GET", urlResult);
  const htmlResult = await fetchPage(urlResult);
  let htmlPay = "";
  try { log("GET", urlPay); htmlPay = await fetchPage(urlPay); } catch(e){ log("pay page not found (skip)"); }
  return { urlResult, urlPay, htmlResult, htmlPay };
}

/* ---------- 共通: 風向（クラス is-windNN → 方位） ---------- */
const WIND_DIR_MAP = {
  1:"北", 2:"北北東", 3:"北東", 4:"東北東", 5:"東",
  6:"東南東", 7:"南東", 8:"南南東", 9:"南", 10:"南南西",
  11:"南西", 12:"西南西", 13:"西", 14:"西北西", 15:"北西", 16:"北北西"
};

/* ---------- 解析: 結果ページ ---------- */
function parseRaceResult(html, {date,pid,raceNo,urlResult}){
  const $ = loadHTML(html);

  // ---- 天候・水面（結果ページにも出ることが多い） ----
  const wRoot = $(".weather1, .result__weather, .weather"); // ★要調整ポイント: 代表クラス群
  const weatherText =
    wRoot.find(".weather1_bodyUnit.is-weather .weather1_bodyUnitLabelTitle, .weather__summary").first().text().trim() || null;
  const temperature = (() => {
    const t = wRoot.find(".weather1_bodyUnit.is-direction .weather1_bodyUnitLabelData").first().text().trim()
      || wRoot.find(".weather1_bodyUnit.is-temperature .weather1_bodyUnitLabelData").first().text().trim();
    return t ? parseFloat(t.replace(/[^\d.]/g,"")) : null;
  })();
  const windSpeed = (() => {
    const t = wRoot.find(".weather1_bodyUnit.is-wind .weather1_bodyUnitLabelData").first().text().trim();
    return t ? parseFloat(t.replace(/[^\d.]/g,"")) : null;
  })();
  let windDirection = null;
  const dirClass = wRoot.find(".weather1_bodyUnit.is-windDirection .weather1_bodyUnitImage").attr("class") || "";
  const mDir = dirClass.match(/is-wind(\d{1,2})/);
  if (mDir) { const key = parseInt(mDir[1],10); windDirection = WIND_DIR_MAP[key] || null; }
  const waterTemperature = (() => {
    const t = wRoot.find(".weather1_bodyUnit.is-waterTemperature .weather1_bodyUnitLabelData").first().text().trim();
    return t ? parseFloat(t.replace(/[^\d.]/g,"")) : null;
  })();
  const waveHeight = (() => {
    const t = wRoot.find(".weather1_bodyUnit.is-wave .weather1_bodyUnitLabelData").first().text().trim();
    return t ? (parseFloat(t.replace(/[^\d.]/g,""))/100) : null; // cm→m
  })();

  // ---- 着順テーブル ----
  // ★要調整ポイント: 結果の行クラスは開催で微妙に違うため、汎用に tbody>tr を走査しつつ、列の意味は正規化する
  const results = [];
  $("table, .table1, .result_table")
    .filter((_,el)=>/着順|到達|着|進入|ST/.test($(el).text()))
    .first()
    .find("tbody tr")
    .each((_, tr) => {
      const $tds = $(tr).find("td");
      if ($tds.length < 3) return;

      const rawTexts = $tds.toArray().map(td => $(td).text().replace(/\s+/g," ").trim());
      const txt = (i)=> rawTexts[i] || "";

      // 代表的な並び（例）
      // [着, 枠, 登録番号・選手, 決まり手, 進入, ST, タイム] 等
      const finish = parseInt((txt(0).match(/\d+/)||[])[0],10);
      const lane   = parseInt((txt(1).match(/\d+/)||[])[0],10);
      let number="", name="";
      const a = $(tr).find('a[href*="toban="]').first();
      if (a.length){
        const href = a.attr("href")||"";
        const m = href.match(/toban=(\d{4})/); if (m) number = m[1];
        name = a.text().replace(/\s+/g," ").trim();
      } else {
        // フォールバック: 「XXXX 選手名」混在テキストから抽出
        const m2 = rawTexts.join(" ").match(/(\d{4})\s*([^\d\s][^\s]+)/);
        if (m2){ number=m2[1]; name=m2[2]; }
      }

      // 進入コース & ST
      const course = parseInt((rawTexts.find(t=>/進入|コース/.test(t))||"").replace(/[^0-9]/g,""),10) || null;
      const stCell = rawTexts.find(t=>t.match(/^[-0-9.]*F?$/) || /ST/.test(t)) || "";
      const st = (stCell.includes("ST")? stCell.replace(/.*ST/,""): stCell).trim();
      const startType = (/F/i.test(stCell) || /^F/.test(st)) ? "F" : (/L/i.test(stCell) ? "L" : "N");

      // 決まり手・記録
      const technique = rawTexts.find(t=>/まくり|まくり差し|差し|逃げ|抜き|恵まれ|叩き/.test(t)) || "";
      const timeTxt = rawTexts.find(t=>/^\d+:\d{2}\.\d$/.test(t)) || ""; // 例: 1:47.3

      // 失格・返還など（例: 欠/妨/落/沈 等がセルに単独で出るケース）
      const status = rawTexts.find(t=>/欠|妨|落|沈|失|返還|転/.test(t)) || "";

      if (Number.isFinite(finish) && Number.isFinite(lane)) {
        results.push({
          finish, lane, number, name,
          course: Number.isFinite(course) ? course : null,
          ST: st || null, startType,
          technique: technique || null,
          time: timeTxt || null,
          status: status || null
        });
      }
    });

  // 決まり手（ページ見出し側に単独で出ている時用）
  let decision = $(".result__point, .is-decided, .racedata1_bodyUnitLabelTitle:contains('決まり手')").text().replace(/\s+/g," ").trim() || null;
  if (!decision) {
    decision = $(".result_info:contains('決まり手')").text().replace(/.*決まり手[:：]\s*/,"").trim() || null;
  }

  return {
    date, pid, race: `${raceNo}R`, source: urlResult, mode: "raceresult",
    generatedAt: new Date().toISOString(),
    weather: {
      weather: weatherText ? weatherText.replace(/(り|のち.*)?$/,"") : null,
      temperature, windSpeed, windDirection, waterTemperature, waveHeight
    },
    decision: decision || null,
    results
  };
}

/* ---------- 解析: 払戻ページ ---------- */
function parsePay(html, {date,pid,raceNo,urlPay}){
  if (!html) return null;
  const $ = loadHTML(html);

  // 券種ごとに「組番」「配当」「人気」を拾う
  function pickTableLike(keyword){
    const tbl = $("table, .table1, .pay_table")
      .filter((_,el)=>$(el).text().includes(keyword)).first();
    if (!tbl.length) return [];
    const out = [];
    tbl.find("tbody tr").each((_,tr)=>{
      const tds = $(tr).find("td");
      if (!tds.length) return;

      const texts = tds.toArray().map(td => $(td).text().replace(/\s+/g," ").trim());
      // 代表: [式別, 組番, 金額, 人気] or [組番, 金額, 人気]
      const nums = texts.join(" ");
      const comb = (nums.match(/(\d-){1,2}\d/) || nums.match(/\d{3}/) || [""])[0]; // 例: 1-2-3 / 12 / 123
      const amount = parseInt((nums.match(/([\d,]+)円/)||[])[1]?.replace(/,/g,"") || (texts.find(t=>t.includes("円"))||"").replace(/[^\d]/g,""), 10);
      const fav = parseInt((nums.match(/(\d+)人気/)||[])[1] || (texts.find(t=>/人気/.test(t))||"").replace(/[^\d]/g,""),10) || null;
      if (comb && amount) out.push({ combination: comb, payout: amount, popularity: fav });
    });
    return out;
  }

  // よく使う種別
  const trifecta   = pickTableLike("三連単");
  const trio       = pickTableLike("三連複");
  const exacta     = pickTableLike("二連単");
  const quinella   = pickTableLike("二連複");
  const wide       = pickTableLike("拡連複");

  return {
    date, pid, race: `${raceNo}R`, source: urlPay, mode: "pay",
    generatedAt: new Date().toISOString(),
    refunds: { trifecta, trio, exacta, quinella, wide }
  };
}

/* ---------- main ---------- */
async function main(){
  if (!DATE || (PIDS.length===0) || !RACES_EXPR) usageAndExit();
  const RACES = expandRaces(RACES_EXPR); if (RACES.length===0) usageAndExit();

  for (const pid of PIDS){
    let raceList;
    if (RACES.length===1 && RACES[0]==="auto"){
      raceList = await pickRacesAutoFinished(DATE,pid);
      log(`auto-picked finished races (${pid}): ${raceList.join(", ") || "(none)"}`);
      if (raceList.length===0) continue;
    } else {
      raceList = RACES;
    }

    for (const raceNo of raceList){
      const outPath = path.join(__dirname,"..","public","results","v1",DATE,pid,`${raceNo}R.json`);
      if (SKIP_EXISTING && fs.existsSync(outPath)){ log("skip existing:", path.relative(process.cwd(), outPath)); continue; }

      try{
        const { urlResult, urlPay, htmlResult, htmlPay } = await fetchRacePages({date:DATE,pid,raceNo});
        const parsedResult = parseRaceResult(htmlResult,{date:DATE,pid,raceNo,urlResult});
        if (!parsedResult?.results?.length){
          log(`no parsed results -> skip save: ${DATE}/${pid}/${raceNo}R`);
          continue;
        }
        const parsedPay = parsePay(htmlPay, {date:DATE,pid,raceNo,urlPay}) || undefined;

        const payload = {
          meta: { date: DATE, pid, race: `${raceNo}R`, generatedAt: new Date().toISOString() },
          result: parsedResult,
          pay: parsedPay || null
        };

        await writeJSON(outPath, payload);
        log("saved:", path.relative(process.cwd(), outPath));
      }catch(err){
        console.error(`Failed: date=${DATE} pid=${pid} race=${raceNo} -> ${String(err)}`);
      }
    }
  }
}

main().catch(e=>{ console.error(e); process.exit(1); });