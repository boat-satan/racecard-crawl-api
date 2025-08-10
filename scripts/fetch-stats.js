// scripts/fetch-stats.js  (v2 / incremental & sessioned)
// Node v20 ESM

import { load } from "cheerio";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";
import { Agent as HttpAgent } from "node:http";
import { Agent as HttpsAgent } from "node:https";

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const PUBLIC_DIR   = path.resolve(__dirname, "..", "public");
const OUTPUT_DIR_V2= path.join(PUBLIC_DIR, "stats", "v2", "racers");

// programs today（出走者候補収集）
const TODAY_ROOTS = [
  path.join(PUBLIC_DIR, "programs", "v2", "today"),
  path.join(PUBLIC_DIR, "programs-slim", "v2", "today"),
];

// --- SETTINGS ---
const WAIT_MS_BETWEEN_RACERS = Number(process.env.STATS_DELAY_MS || 3000); // 3s
const COURSE_WAIT_MS         = Number(process.env.COURSE_WAIT_MS || 3000); // 3s
const FRESH_HOURS            = 12;                                         // 12h 以内はスキップ
const RETRIES_CONNECT        = 1;  // 接続系の再試行 1 回（= 合計2トライ）
const RETRIES_404            = 1;  // 404 の再試行 1 回（= 合計2トライ）

const ENV_RACERS = (process.env.RACERS || "").trim();
const ENV_BATCH  = Number(process.env.STATS_BATCH || "");

// --- Tiny cookie jar ---
const cookieJar = {};
function setCookieFromHeaders(headers) {
  const setCookie = headers.get("set-cookie");
  if (!setCookie) return;
  // 複数行対応
  const parts = Array.isArray(setCookie) ? setCookie : [setCookie];
  for (const line of parts) {
    const [kv] = String(line).split(";");
    const [k, v] = kv.split("=");
    if (k && v) cookieJar[k.trim()] = v.trim();
  }
}
function cookieHeader() {
  const pairs = Object.entries(cookieJar).map(([k,v]) => `${k}=${v}`);
  return pairs.join("; ");
}

// undici/node-fetch の keep-alive を切る
const httpAgent  = new HttpAgent({ keepAlive: false });
const httpsAgent = new HttpsAgent({ keepAlive: false });

// fetch with headers / retries / cookie
async function fetchHtml(url, {
  timeoutMs = 20000,
  retry404  = RETRIES_404,
  retryConn = RETRIES_CONNECT,
} = {}) {
  let attempts = 0;
  let lastErr  = null;

  while (true) {
    attempts++;
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);

    try {
      const res = await fetch(url, {
        signal: ctrl.signal,
        // @ts-ignore node >=18: dispatcher を使うよりエージェント指定が簡単
        agent: (url.startsWith("https:") ? httpsAgent : httpAgent),
        redirect: "follow",
        headers: {
          "user-agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
          "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "accept-language": "ja,en;q=0.9",
          "cache-control": "no-cache",
          "upgrade-insecure-requests": "1",
          "referer": "https://boatrace-db.net/",
          ...(Object.keys(cookieJar).length ? { "cookie": cookieHeader() } : {}),
        },
      });

      // Cookie 捕捉
      setCookieFromHeaders(res.headers);

      if (res.status === 404 && retry404 > 0) {
        retry404--;
        clearTimeout(t);
        await sleep(1000); // 少し待って再試行
        continue;
      }

      if (!res.ok) {
        const body = await res.text().catch(() => "");
        clearTimeout(t);
        throw new Error(`HTTP ${res.status} ${res.statusText} @ ${url} :: ${body.slice(0,120)}`);
      }

      const html = await res.text();
      clearTimeout(t);
      return html;

    } catch (e) {
      clearTimeout(t);
      lastErr = e;
      if (retryConn > 0) {
        retryConn--;
        // 1.6倍バックオフ + ジッター
        const wait = Math.round(1200 * (1.6 ** (attempts - 1)) * (0.9 + Math.random()*0.2));
        await sleep(wait);
        continue;
      }
      break;
    }
  }
  throw lastErr ?? new Error(`fetch failed: ${url}`);
}

function normText(t){ return (t ?? "").replace(/\u00A0/g," ").replace(/\s+/g," ").trim(); }
function toNumber(v){ if(v==null) return null; const n=Number(String(v).replace(/[,%]/g,"")); return Number.isFinite(n)?n:null; }

function parseTable($, $tbl) {
  const headers = [];
  $tbl.find("thead th, thead td").each((_, th) => headers.push(normText($(th).text())));
  if (headers.length === 0) {
    const first = $tbl.find("tr").first();
    first.find("th,td").each((_, td) => headers.push(normText($(td).text())));
  }
  const rows = [];
  $tbl.find("tbody tr").each((_, tr) => {
    const cells = [];
    $(tr).find("th,td").each((_, td) => cells.push(normText($(td).text())));
    if (cells.length) rows.push(cells);
  });
  return { headers, rows };
}
function headerIndex(headers, keyLike){ return headers.findIndex(h=>h.includes(keyLike)); }
function mustTableByHeader($, keys){
  const cand = $("table").toArray();
  for (const el of cand) {
    const { headers } = parseTable($, $(el));
    if (keys.every(k => headers.some(h => h.includes(k)))) return $(el);
  }
  return null;
}
const normK = (k)=>k.replace("ま差し","まくり差し").replace("捲り差し","まくり差し").replace("捲り","まくり");

// ===== 直近6ヶ月：全艇視点（rcourse root） =====
function parseCourseStatsFromRcourse($){
  const $tbl = mustTableByHeader($, ["コース","出走数","1着率","2連対率","3連対率"]);
  if (!$tbl) return null;
  const { headers, rows } = parseTable($, $tbl);
  const iC = headerIndex(headers,"コース");
  const iS = headerIndex(headers,"出走数");
  const i1 = headerIndex(headers,"1着率");
  const i2 = headerIndex(headers,"2連対率");
  const i3 = headerIndex(headers,"3連対率");

  const items=[];
  for(const r of rows){
    const ct=r[iC]??r[0]??"";
    const m=ct.match(/([1-6])/); if(!m) continue;
    items.push({
      course:Number(m[1]),
      starts: iS>=0?toNumber(r[iS]):null,
      top1Rate:i1>=0?toNumber(r[i1]):null,
      top2Rate:i2>=0?toNumber(r[i2]):null,
      top3Rate:i3>=0?toNumber(r[i3]):null,
      winRate:null,
      raw:r
    });
  }
  items.sort((a,b)=>a.course-b.course);
  return items.length?items:null;
}
function parseKimariteFromRcourse($){
  const $tbl = mustTableByHeader($, ["コース","出走数","1着数","逃げ","差し","まくり","抜き","恵まれ"]);
  if (!$tbl) return null;
  const { headers, rows } = parseTable($, $tbl);
  const iC = headerIndex(headers,"コース");
  const keys = headers.slice(3).map(normK);
  const items=[];
  for(const r of rows){
    const ct=r[iC]??r[0]??""; const m=ct.match(/([1-6])/); if(!m) continue;
    const detail={};
    keys.forEach((k,i)=>{
      const v=r[3+i];
      const num = v?.match(/(\d+)/);
      const pct = v?.match(/([-+]?\d+(?:\.\d+)?)\s*%/);
      detail[k]={ count:num?toNumber(num[1]):toNumber(v), rate:pct?toNumber(pct[1]):null, raw:v??null };
    });
    items.push({ course:Number(m[1]), detail, raw:r });
  }
  items.sort((a,b)=>a.course-b.course);
  return items.length?items:null;
}

// ===== 進入コース別（自艇視点） =====
function parseAvgSTFromCoursePage($){
  const $tbl=mustTableByHeader($,["月日","場","レース","ST","結果"]); if(!$tbl) return null;
  const { headers, rows } = parseTable($,$tbl);
  const iST = headerIndex(headers,"ST"); if(iST<0) return null;
  let sum=0, cnt=0;
  for(const r of rows){
    const st=r[iST]; if(!st) continue;
    if(/^[FL]/i.test(st)) continue;
    const m=st.match(/-?\.?\d+(?:\.\d+)?/); if(!m) continue;
    const n=Number(m[0]); if(Number.isFinite(n)){ sum+=Math.abs(n); cnt++; }
  }
  return cnt?Math.round((sum/cnt)*100)/100:null;
}
function parseLoseKimariteFromCoursePage($){
  const $tbl = mustTableByHeader($,["コース","出走数","1着数","逃げ","差し","まくり"]); if(!$tbl) return null;
  const { headers, rows } = parseTable($,$tbl);
  const iC = headerIndex(headers,"コース");
  const keys = headers.slice(3).map(normK);
  const lose = Object.fromEntries(keys.map(k=>[k,0]));
  for(const r of rows){
    const label = r[iC]||"";
    if(label.includes("（自艇）")) continue; // 相手艇のみ集計
    keys.forEach((k,i)=>{
      const v=r[3+i];
      const num = v ? Number((v.match(/(\d+)/)||[])[1]) : NaN;
      if(Number.isFinite(num)) lose[k]+=num;
    });
  }
  return lose;
}

function parseExTimeRankFromRdemo($){
  const $tbl=mustTableByHeader($,["順位","出走数","1着率","2連対率","3連対率"]); if(!$tbl) return null;
  const { headers, rows } = parseTable($,$tbl);
  const iR=headerIndex(headers,"順位"), i1=headerIndex(headers,"1着率"), i2=headerIndex(headers,"2連対率"), i3=headerIndex(headers,"3連対率");
  const items=[];
  for(const r of rows){
    const rt=r[iR]??r[0]??""; const m=rt.match(/([1-6])/); if(!m) continue;
    items.push({ rank:Number(m[1]), winRate:i1>=0?toNumber(r[i1]):null, top2Rate:i2>=0?toNumber(r[i2]):null, top3Rate:i3>=0?toNumber(r[i3]):null, raw:r });
  }
  items.sort((a,b)=>a.rank-b.rank);
  return items.length?items:null;
}

// ---- gather racers from today ----
async function collectRacersFromToday(){
  const set=new Set();
  for(const root of TODAY_ROOTS){
    let entries=[]; try{ entries=await fs.readdir(root,{ withFileTypes:true }); }catch{ continue; }
    for(const d of entries){
      if(!d.isDirectory()) continue;
      const dir=path.join(root,d.name);
      const files=await fs.readdir(dir).catch(()=>[]);
      for(const f of files){
        if(!f.endsWith(".json")) continue;
        try{
          const j=JSON.parse(await fs.readFile(path.join(dir,f),"utf8"));
          const boats=j?.boats || j?.program?.boats || j?.entries || [];
          for(const b of boats){
            const r=b.racer_number ?? b.racerNumber ?? b.number ?? b.racer?.number;
            if(r) set.add(String(r));
          }
        }catch{}
      }
    }
  }
  return [...set];
}

async function ensureDir(p){ await fs.mkdir(p,{recursive:true}); }
async function isFreshFile(p, hours=FRESH_HOURS){
  try{
    const st = await fs.stat(p);
    const ageMs = Date.now() - st.mtimeMs;
    return ageMs < hours*3600*1000;
  }catch{ return false; }
}

// ---- one racer ----
async function fetchOne(regno){
  // 1) warm up (top)
  try{
    await fetchHtml("https://boatrace-db.net/");
    await sleep(300 + Math.floor(Math.random()*200)); // 0.3~0.5s
  }catch{}

  const urls = {
    rcourse: `https://boatrace-db.net/racer/rcourse/regno/${regno}/`,
    rdemo:   `https://boatrace-db.net/racer/rdemo/regno/${regno}/`,
  };

  const metaErrors = [];

  // list
  let courseStats=null, courseKimarite=null;
  try{
    const html = await fetchHtml(urls.rcourse);
    const $ = load(html);
    courseStats   = parseCourseStatsFromRcourse($);
    courseKimarite= parseKimariteFromRcourse($);
  }catch(e){
    metaErrors.push({ where:"rcourse", message: String(e.message||e) });
  }

  // per-course (1..6) 自艇視点
  const courseDetails=[];
  const entryCourse = {};
  for(let c=1;c<=6;c++){
    const u = `https://boatrace-db.net/racer/rcourse/regno/${regno}/course/${c}/`;
    entryCourse[c]=u;
    try{
      const html = await fetchHtml(u);
      const $ = load(html);
      const avgST = parseAvgSTFromCoursePage($);
      const lose  = parseLoseKimariteFromCoursePage($);
      courseDetails.push({ course:c, avgST:avgST??null, loseKimarite: lose??null });
    }catch(e){
      metaErrors.push({ where:`course:${c}`, message:String(e.message||e) });
      courseDetails.push({ course:c, avgST:null, loseKimarite:null });
    }
    await sleep(COURSE_WAIT_MS);
  }

  // rdemo
  let exTimeRank=null;
  try{
    const html = await fetchHtml(urls.rdemo);
    const $ = load(html);
    exTimeRank = parseExTimeRankFromRdemo($);
  }catch(e){
    metaErrors.push({ where:"rdemo", message:String(e.message||e) });
  }

  return {
    schemaVersion: "2.0",
    regno: Number(regno),
    sources: { ...urls, coursePages: entryCourse },
    fetchedAt: new Date().toISOString(),
    courseStats,      // 全艇視点（「1コース（自艇）」〜「6コース（他艇）」の行をフラット化）
    courseKimarite,   // 全艇視点の決まり手内訳
    courseDetails,    // 自艇が c コース進入時の平均ST & 相手艇の勝ち手内訳（loseKimarite）
    exTimeRank,       // 展示タイム順位別
    meta: { errors: metaErrors }
  };
}

async function main(){
  await ensureDir(OUTPUT_DIR_V2);

  let racers = [];
  if (ENV_RACERS) {
    racers = ENV_RACERS.split(",").map(s=>s.trim()).filter(Boolean);
  } else {
    racers = await collectRacersFromToday();
  }
  if (ENV_BATCH && Number.isFinite(ENV_BATCH) && ENV_BATCH>0) {
    racers = racers.slice(0, ENV_BATCH);
  }

  if (racers.length===0){
    console.log("No racers to fetch.");
    return;
  }

  console.log(`process ${racers.length} racers (incremental, fresh<${FRESH_HOURS}h)` + (ENV_RACERS ? " [env RACERS specified]" : ""));

  let ok=0, ng=0;
  for (const regno of racers){
    const outPath = path.join(OUTPUT_DIR_V2, `${regno}.json`);

    // fresh skip
    if (await isFreshFile(outPath, FRESH_HOURS)) {
      console.log(`⏭  skip fresh ${path.relative(PUBLIC_DIR,outPath)}`);
      continue;
    }

    try{
      const data = await fetchOne(regno);
      // null だらけなら保存しない
      const hasAny =
        (data.courseStats && data.courseStats.length) ||
        (data.courseKimarite && data.courseKimarite.length) ||
        (data.courseDetails && data.courseDetails.length) ||
        (data.exTimeRank && data.exTimeRank.length);

      if (hasAny) {
        await fs.writeFile(outPath, JSON.stringify(data, null, 2), "utf8");
        console.log(`✅ wrote ${path.relative(PUBLIC_DIR,outPath)}`);
        ok++;
      } else {
        console.warn(`⚠️  empty data for ${regno}, not writing file`);
        ng++;
      }
    }catch(e){
      console.warn(`❌ ${regno}: ${e.message}`);
      ng++;
    }

    await sleep(WAIT_MS_BETWEEN_RACERS);
  }

  await ensureDir(path.join(PUBLIC_DIR,"debug"));
  await fs.writeFile(
    path.join(PUBLIC_DIR,"debug","stats-v2-meta.json"),
    JSON.stringify({ status:200, fetchedAt:new Date().toISOString(), success:ok, failed:ng }, null, 2),
    "utf8"
  );
}

main().catch(e=>{ console.error(e); process.exit(1); });
