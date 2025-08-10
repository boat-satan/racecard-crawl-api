// scripts/fetch-stats.js
// Node v20 / ESM
// 出力: public/stats/v2/racers/<regno>.json（※日付なしで最新を上書き）
// 参照:
//  - rcourse 各コース: https://boatrace-db.net/racer/rcourse/regno/<regno>/course/<n>/
//  - rdemo   展示順位: https://boatrace-db.net/racer/rdemo/regno/<regno>/

import { load } from "cheerio";
import fs from "node:fs/promises";
import fssync from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

// -------------------------------
// 定数/入出力
// -------------------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const PUBLIC_DIR    = path.resolve(__dirname, "..", "public");
const OUTPUT_DIR_V2 = path.join(PUBLIC_DIR, "stats", "v2", "racers"); // ★日付なし固定保存

// polite wait（最低3秒を保証）
const WAIT_MS_BETWEEN_RACERS       = Math.max(3000, Number(process.env.STATS_DELAY_MS || 3000));
const WAIT_MS_BETWEEN_COURSE_PAGES = Math.max(3000, Number(process.env.COURSE_WAIT_MS || 3000));

// env
const ENV_RACERS       = process.env.RACERS?.trim() || "";
const ENV_RACERS_LIMIT = Number(process.env.RACERS_LIMIT ?? "");
const ENV_BATCH        = Number(process.env.STATS_BATCH ?? "");
const FRESH_HOURS      = Number(process.env.FRESH_HOURS || 12);

// 絞り込み（PID/RACE）— 例: TARGET_PID="02,06", TARGET_RACE="1,3R,12"
const PID_IN = (process.env.TARGET_PID || "").trim();
const PID_FILTERS = PID_IN && PID_IN.toUpperCase() !== "ALL"
  ? PID_IN.split(",").map(s=>s.trim()).filter(Boolean).map(s => (/^\d+$/.test(s) ? s.padStart(2,"0") : s))
  : null;

const RACE_IN = (process.env.TARGET_RACE || "").trim();
const RACE_FILTERS = RACE_IN && RACE_IN.toUpperCase() !== "ALL"
  ? RACE_IN.split(",").map(s => String(s).trim().toUpperCase())
      .map(s => s.endsWith("R") ? s : `${s}R`)
      .map(s => s.replace(/[^\d]/g, "") + "R")
  : null;

if (PID_FILTERS)  console.log("filters: pid =", PID_FILTERS.join(","));
if (RACE_FILTERS) console.log("filters: race =", RACE_FILTERS.join(","));

// -------------------------------
// ユーティリティ
// -------------------------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * fetchHtml: UA/Referer付き、リトライは1回だけ
 */
async function fetchHtml(url, {
  retries = 1,            // ★リトライ 1 回
  baseDelayMs = 3000,     // ★失敗時の待機も最低3秒
  timeoutMs = 45000,
} = {}) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), timeoutMs);
    try {
      const res = await fetch(url, {
        signal: ac.signal,
        headers: {
          "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127 Safari/537.36",
          "accept": "text/html,application/xhtml+xml",
          "accept-language": "ja,en;q=0.9",
          "referer": "https://boatrace-db.net/",
          "cache-control": "no-cache",
        },
      });
      const body = await res.text().catch(() => "");
      clearTimeout(timer);

      if (res.ok) return body;

      const retriable = [401,404,429,500,502,503,504].includes(res.status);
      if (!retriable || attempt === retries) {
        throw new Error(`HTTP ${res.status} ${res.statusText} @ ${url}`);
      }
      await sleep(baseDelayMs); // リトライ前に最低3秒
    } catch (err) {
      clearTimeout(timer);
      if (attempt === retries) throw new Error(`GET failed after ${retries+1} tries: ${url} :: ${err.message}`);
      await sleep(baseDelayMs);
    }
  }
  throw new Error("unreachable");
}

function normText(t){ return (t ?? "").replace(/\u00A0/g," ").replace(/\s+/g," ").trim(); }
function toNumber(v){
  if(v==null) return null;
  const n = Number(String(v).replace(/[,%]/g,""));
  return Number.isFinite(n) ? n : null;
}
function parseTable($, $tbl){
  const headers=[]; $tbl.find("thead th, thead td").each((_,th)=>headers.push(normText($(th).text())));
  if(!headers.length){
    const first=$tbl.find("tr").first();
    first.find("th,td").each((_,td)=>headers.push(normText($(td).text())));
  }
  const rows=[];
  $tbl.find("tbody tr").each((_,tr)=>{
    const cells=[]; $(tr).find("th,td").each((_,td)=>cells.push(normText($(td).text())));
    if(cells.length) rows.push(cells);
  });
  return { headers, rows };
}
const headerIndex = (hs,key)=>hs.findIndex(h=>h.includes(key));
function mustTableByHeader($, keyLikes){
  const candidates = $("table");
  for (const el of candidates.toArray()){
    const { headers } = parseTable($, $(el));
    if (keyLikes.every(k => headers.some(h => h.includes(k)))) return $(el);
  }
  return null;
}
function normalizeKimariteKey(k){
  return k.replace("ま差し","まくり差し").replace("捲り差し","まくり差し").replace("捲り","まくり");
}

// -------------------------------
// 各ページパーサ
// -------------------------------
function parseAvgSTFromCoursePage($){
  const $tbl = mustTableByHeader($, ["月日","場","レース","ST","結果"]); if(!$tbl) return null;
  const { headers, rows } = parseTable($, $tbl);
  const iST = headerIndex(headers,"ST"); if(iST<0) return null;
  let sum=0,cnt=0;
  for(const r of rows){
    const st=r[iST]; if(!st) continue;
    if(/^[FL]/i.test(st)) continue; // F/Lは除外
    const m = st.match(/-?\.?\d+(?:\.\d+)?/); if(!m) continue;
    const n=Number(m[0]); if(Number.isFinite(n)){ sum+=Math.abs(n); cnt++; }
  }
  return cnt? Math.round((sum/cnt)*100)/100 : null;
}

function parseLoseKimariteFromCoursePage($){
  const $tbl = mustTableByHeader($, ["コース","出走数","1着数","逃げ","差し","まくり"]); if(!$tbl) return null;
  const { headers, rows } = parseTable($, $tbl);
  const iCourse = headerIndex(headers,"コース");
  const keys = headers.slice(3).map(normalizeKimariteKey);
  const lose = Object.fromEntries(keys.map(k=>[k,0]));
  for(const r of rows){
    const label = r[iCourse] || "";
    if(label.includes("（自艇）")) continue; // 他艇のみ
    keys.forEach((k,i)=>{
      const v=r[3+i]; const num=v?Number((v.match(/(\d+)/)||[])[1]):NaN;
      if(Number.isFinite(num)) lose[k]+=num;
    });
  }
  return lose;
}

function parseEntryMatrixFromCoursePage($){
  const $tbl = mustTableByHeader($, ["コース","出走数","1着数","2着数","3着数","1着率","2連対率","3連対率"]); if(!$tbl) return null;
  const { headers, rows } = parseTable($, $tbl);
  const idx = {
    course: headerIndex(headers,"コース"),
    starts: headerIndex(headers,"出走数"),
    w1: headerIndex(headers,"1着数"),
    w2: headerIndex(headers,"2着数"),
    w3: headerIndex(headers,"3着数"),
    r1: headerIndex(headers,"1着率"),
    r2: headerIndex(headers,"2連対率"),
    r3: headerIndex(headers,"3連対率"),
  };
  const result={ rows:[], self:null };
  for(const r of rows){
    const label=r[idx.course]||""; const m=label.match(/([1-6])\s*コース/); if(!m) continue;
    const course=Number(m[1]); const isSelf=label.includes("（自艇）");
    const row={
      course,isSelf,
      starts:toNumber(r[idx.starts]),
      firstCount:toNumber(r[idx.w1]),
      secondCount:toNumber(r[idx.w2]),
      thirdCount:toNumber(r[idx.w3]),
      winRate:toNumber(r[idx.r1]),
      top2Rate:toNumber(r[idx.r2]),
      top3Rate:toNumber(r[idx.r3]),
      raw:r,
    };
    result.rows.push(row); if(isSelf) result.self=row;
  }
  result.rows.sort((a,b)=>a.course-b.course);
  return result;
}

// 「全艇決まり手」（自艇行の横列＝勝ち決まり手）
function parseEntryKimariteRows($){
  const $tbl = mustTableByHeader($, ["決まり手","逃げ","差し","まくり","まくり差し","抜き","恵まれ"]); if(!$tbl) return null;
  const { headers, rows } = parseTable($, $tbl);
  const kStart = headers[0]?.includes("決まり手") ? 1 : 0;
  const keys   = headers.slice(kStart).map(normalizeKimariteKey);

  const resultRows=[];
  for(const r of rows){
    const label=r[0]||""; const m=label.match(/([1-6])\s*コース/); if(!m) continue;
    const course=Number(m[1]); const isSelf=label.includes("（自艇）");
    const detail={};
    for(let i=0;i<keys.length;i++){
      const v=r[kStart+i]; const num=v?Number((v.match(/(\d+)/)||[])[1]):NaN;
      detail[keys[i]]=Number.isFinite(num)?num:0;
    }
    resultRows.push({ course, isSelf, detail, raw:r });
  }
  resultRows.sort((a,b)=>a.course-b.course);
  return { rows: resultRows };
}

// rdemo: 展示タイム順位
function parseExTimeRankFromRdemo($){
  const $tbl = mustTableByHeader($, ["順位","出走数","1着率","2連対率","3連対率"]); if(!$tbl) return null;
  const { headers, rows } = parseTable($, $tbl);
  const iRank=headerIndex(headers,"順位"), iWin=headerIndex(headers,"1着率"),
        iT2=headerIndex(headers,"2連対率"), iT3=headerIndex(headers,"3連対率");
  const items=[];
  for(const r of rows){
    const rt=r[iRank] ?? r[0] ?? ""; const m=rt.match(/([1-6])/); if(!m) continue;
    items.push({ rank:Number(m[1]),
      winRate: iWin>=0?toNumber(r[iWin]):null,
      top2Rate:iT2>=0?toNumber(r[iT2]):null,
      top3Rate:iT3>=0?toNumber(r[iT3]):null,
      raw:r });
  }
  items.sort((a,b)=>a.rank-b.rank);
  return items.length?items:null;
}

// -------------------------------
// 出走表の探索ルート（最新YYYYMMDD → today → 直下）
// -------------------------------
function listDateDirs(base){
  try{
    return fssync.readdirSync(base,{withFileTypes:true})
      .filter(d=>d.isDirectory() && /^\d{8}$/.test(d.name))
      .map(d=>d.name)
      .sort((a,b)=>b.localeCompare(a)); // desc
  }catch{ return []; }
}

function* candidateProgramRoots(){
  const bases = [
    path.join(PUBLIC_DIR, "programs", "v2"),
    path.join(PUBLIC_DIR, "programs-slim", "v2"),
  ];
  // 1) 最新YYYYMMDD
  for (const base of bases) {
    const dates = listDateDirs(base);
    if (dates.length) yield path.join(base, dates[0]);
  }
  // 2) today ディレクトリ
  for (const base of bases) {
    const todayDir = path.join(base, "today");
    if (fssync.existsSync(todayDir)) yield todayDir;
  }
  // 3) フラット直下（古い構成や手動配置）
  for (const base of bases) yield base;
}

// 出走選手収集（PID/RACEフィルタ対応）
async function collectRacersFromPrograms(){
  const set=new Set();
  const readJson=(p)=>{ try{ return JSON.parse(fssync.readFileSync(p,"utf8")); } catch{ return null; } };

  let usedRoot = null;

  for (const root of candidateProgramRoots()){
    if (!fssync.existsSync(root)) continue;

    const entries = fssync.readdirSync(root, { withFileTypes: true });

    // 1) 直置き race json
    for (const e of entries) {
      if (e.isFile() && e.name.endsWith(".json") && e.name !== "index.json") {
        if (RACE_FILTERS && !RACE_FILTERS.includes(e.name.replace(".json","").toUpperCase())) continue;
        const j = readJson(path.join(root, e.name));
        const boats = j?.entries || j?.boats || [];
        for (const b of boats) {
          const r = b.number ?? b.racer_number ?? b.racer?.number;
          if (r) set.add(String(r));
        }
      }
    }

    // 2) PID配下
    for (const e of entries) {
      if (!e.isDirectory()) continue;
      const pidDirName = e.name; // "02" 等
      // PIDフィルタ：ディレクトリ名 or stadiumName(index.json) の両対応
      if (PID_FILTERS && !PID_FILTERS.includes(pidDirName)) {
        // stadiumNameでの照合（任意）
        const idxPath = path.join(root, pidDirName, "index.json");
        let okByName = false;
        if (fssync.existsSync(idxPath)) {
          try {
            const idx = JSON.parse(fssync.readFileSync(idxPath, "utf8"));
            const name = idx?.stadiumName ?? "";
            if (name && PID_FILTERS.some(pf => pf === name)) okByName = true;
          } catch {}
        }
        if (!okByName) continue;
      }

      const files = fssync.readdirSync(path.join(root, pidDirName)).filter(f=>f.endsWith(".json") && f!=="index.json");
      for (const f of files) {
        if (RACE_FILTERS && !RACE_FILTERS.includes(f.replace(".json","").toUpperCase())) continue;
        const j = readJson(path.join(root, pidDirName, f));
        const boats = j?.entries || j?.boats || [];
        for (const b of boats) {
          const r = b.number ?? b.racer_number ?? b.racer?.number;
          if (r) set.add(String(r));
        }
      }
    }

    if (set.size) { usedRoot = root; break; } // どこかで見つかればそれを採用
  }

  if (usedRoot) console.log("program root:", path.relative(PUBLIC_DIR, usedRoot));
  return [...set];
}

async function ensureDir(p){ await fs.mkdir(p,{recursive:true}); }
function isFresh(file, hours=12){
  try{ const st=fssync.statSync(file); return (Date.now()-st.mtimeMs) <= hours*3600*1000; }
  catch{ return false; }
}

// -------------------------------
// 1選手分
// -------------------------------
async function fetchOne(regno){
  const uRdemo = `https://boatrace-db.net/racer/rdemo/regno/${regno}/`;
  const entryCourse=[], coursePages={};

  for(let c=1;c<=6;c++){
    const url=`https://boatrace-db.net/racer/rcourse/regno/${regno}/course/${c}/`;
    coursePages[c]=url;
    try{
      const html=await fetchHtml(url);
      const $=load(html);

      const avgST        = parseAvgSTFromCoursePage($);
      const loseKimarite = parseLoseKimariteFromCoursePage($);
      const matrix       = parseEntryMatrixFromCoursePage($);
      const kRows        = parseEntryKimariteRows($);

      // 自艇行の横列＝勝ち決まり手
      const selfRow = kRows?.rows?.find(r=>r.isSelf);
      const winKimariteSelf = selfRow ? { ...selfRow.detail } : null;

      const selfSummary = matrix?.self ? {
        course: matrix.self.course,
        starts: matrix.self.starts,
        firstCount: matrix.self.firstCount,
        secondCount: matrix.self.secondCount,
        thirdCount: matrix.self.thirdCount,
      } : null;

      entryCourse.push({
        course: c,
        matrix: matrix ?? null,
        kimariteAllBoats: kRows ?? null,
        avgST: avgST ?? null,
        loseKimarite: loseKimarite ?? null,
        winKimariteSelf: winKimariteSelf ?? null,
        selfSummary,
      });

      await sleep(WAIT_MS_BETWEEN_COURSE_PAGES);
    } catch(e){
      console.warn(`warn: entry-course page failed regno=${regno} course=${c}: ${e.message}`);
      entryCourse.push({ course:c, matrix:null, kimariteAllBoats:null, avgST:null, loseKimarite:null, winKimariteSelf:null, selfSummary:null });
    }
  }

  // 展示タイム順位
  let exTimeRank=null;
  try{
    const html=await fetchHtml(uRdemo);
    const $=load(html);
    exTimeRank = parseExTimeRankFromRdemo($);
  } catch(e){
    console.warn(`warn: rdemo fetch/parse failed for ${regno}: ${e.message}`);
  }

  return {
    schemaVersion: "2.0",
    regno: Number(regno),
    sources: { rdemo: uRdemo, coursePages },
    fetchedAt: new Date().toISOString(),
    entryCourse,
    exTimeRank,
    meta: { errors: [] },
  };
}

// -------------------------------
// メイン
// -------------------------------
async function main(){
  let racers=[];
  if(ENV_RACERS){
    racers = ENV_RACERS.split(",").map(s=>s.trim()).filter(Boolean);
  }else{
    racers = await collectRacersFromPrograms();
  }
  if(ENV_RACERS_LIMIT && Number.isFinite(ENV_RACERS_LIMIT) && ENV_RACERS_LIMIT>0){
    racers = racers.slice(0, ENV_RACERS_LIMIT);
  }
  if(ENV_BATCH && Number.isFinite(ENV_BATCH) && ENV_BATCH>0){
    racers = racers.slice(0, ENV_BATCH);
  }

  if(!racers.length){
    console.log("No racers to fetch. (Set RACERS env or put programs under public/programs*/v2)");
    return;
  }

  console.log(
    `process ${racers.length} racers (incremental, fresh<=${FRESH_HOURS}h)` +
    (ENV_RACERS ? " [env RACERS specified]" : "") +
    (ENV_RACERS_LIMIT ? ` [limit=${ENV_RACERS_LIMIT}]` : "") +
    (ENV_BATCH ? ` [batch=${ENV_BATCH}]` : "")
  );

  await ensureDir(OUTPUT_DIR_V2);

  let ok=0, ng=0;
  for(const regno of racers){
    const outPath = path.join(OUTPUT_DIR_V2, `${regno}.json`);
    if(isFresh(outPath, FRESH_HOURS)){
      console.log(`⏭️  skip fresh ${path.relative(PUBLIC_DIR, outPath)}`);
      continue;
    }
    try{
      const data = await fetchOne(regno);
      await fs.writeFile(outPath, JSON.stringify(data, null, 2), "utf8");
      console.log(`✅ wrote ${path.relative(PUBLIC_DIR, outPath)}`);
      ok++;
    }catch(e){
      console.warn(`❌ ${regno}: ${e.message}`);
      ng++;
    }
    await sleep(WAIT_MS_BETWEEN_RACERS); // ★確実に3秒以上
  }

  await ensureDir(path.join(PUBLIC_DIR, "debug"));
  await fs.writeFile(
    path.join(PUBLIC_DIR, "debug", "stats-meta.json"),
    JSON.stringify({ status:200, fetchedAt:new Date().toISOString(), racers: racers.map(Number), success:ok, failed:ng }, null, 2),
    "utf8"
  );
}

main().catch(e => { console.error(e); process.exit(1); });
