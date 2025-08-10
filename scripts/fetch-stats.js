// scripts/fetch-stats.js
// Node v20 / ESM
// 出力: public/stats/v2/racers/<regno>.json（※日付なしで最新を上書き）
// 参照:
//  - rcourse 各コース: https://boatrace-db.net/racer/rcourse/regno/<regno>/course/<n>/
//  - rdemo   展示順位: https://boatrace-db.net/racer/rdemo/regno/<regno>/
//
// 仕様（この版）:
//  - 出走表の参照は “日付に依存しない”。
//    - まず programs/v2/ と programs-slim/v2/ の「日付ディレクトリ（YYYYMMDD）」のうち “最新” を自動検出
//    - 見つからなければフラット直下（= 日付階層なし）を走査
//  - PID/RACE フィルタ対応（TARGET_PID, TARGET_RACE）
//  - 勝ち決まり手は “自艇行” を優先的に読み取り（無ければ 0 扱い）
//  - stats は public/stats/v2/racers/<regno>.json に “増分上書き”
//  - 既存JSONが新しければスキップ（FRESH_HOURS）

import { load } from "cheerio";
import fs from "node:fs/promises";
import fssync from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

// -------------------------------
// 定数
// -------------------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const PUBLIC_DIR    = path.resolve(__dirname, "..", "public");
const OUTPUT_DIR_V2 = path.join(PUBLIC_DIR, "stats", "v2", "racers"); // ★日付なし固定保存

// polite wait
const WAIT_MS_BETWEEN_RACERS       = Number(process.env.STATS_DELAY_MS || 3000);
const WAIT_MS_BETWEEN_COURSE_PAGES = Number(process.env.COURSE_WAIT_MS || 3000);

// env
const ENV_RACERS       = process.env.RACERS?.trim() || "";   // "4349,3156" など（指定時はこれ優先）
const ENV_RACERS_LIMIT = Number(process.env.RACERS_LIMIT ?? "");
const ENV_BATCH        = Number(process.env.STATS_BATCH ?? "");
const FRESH_HOURS      = Number(process.env.FRESH_HOURS || 12);
const OVERWRITE        = String(process.env.STATS_OVERWRITE || "0") === "1";

// 参照する出走表のフィルタ（任意）
const PID_IN = (process.env.TARGET_PID || "").trim();   // "ALL" | "02" | "桐生" | "02,03"
const RACE_IN= (process.env.TARGET_RACE|| "").trim();   // "ALL" | "1" | "1R" | "1,2,12"

const WANT_ALL_PID  = !PID_IN || PID_IN.toUpperCase() === "ALL";
const PID_FILTERS   = WANT_ALL_PID ? null
  : PID_IN.split(",").map(s => s.trim()).filter(Boolean).map(s => (/^\d+$/.test(s) ? s.padStart(2,"0") : s));

const WANT_ALL_RACE = !RACE_IN || RACE_IN.toUpperCase() === "ALL";
const RACE_FILTERS  = WANT_ALL_RACE ? null
  : RACE_IN.split(",").map(s => String(s).trim().toUpperCase())
      .map(s => s.endsWith("R") ? s : `${s}R`)
      .map(s => s.replace(/[^\d]/g, "") + "R");

// -------------------------------
// ユーティリティ
// -------------------------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchHtml(url, {
  retries = 1, baseDelayMs = 2500, timeoutMs = 20000,
} = {}) {
  const mkAC = () => new AbortController();
  for (let attempt = 0; attempt <= retries; attempt++) {
    const ac = mkAC();
    const t = setTimeout(() => ac.abort(), timeoutMs);
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
      if (res.ok) { clearTimeout(t); return await res.text(); }
      const retriable = [401,403,404,429,500,502,503,504].includes(res.status);
      if (!retriable || attempt === retries) {
        const body = await res.text().catch(()=> "");
        clearTimeout(t);
        throw new Error(`HTTP ${res.status} ${res.statusText} @ ${url} ${body?.slice(0,120)}`);
      }
      const factor = [403,404,429,503].includes(res.status) ? 2.0 : 1.4;
      const delay  = Math.round((baseDelayMs * Math.pow(factor, attempt)) * (0.8 + Math.random()*0.4));
      clearTimeout(t);
      await sleep(delay);
    } catch (err) {
      clearTimeout(t);
      if (attempt === retries) throw new Error(`GET failed after ${retries+1} tries: ${url} :: ${err.message}`);
      const delay = Math.round((baseDelayMs * Math.pow(1.6, attempt)) * (0.8 + Math.random()*0.4));
      await sleep(delay);
    }
  }
  throw new Error("unreachable");
}

function normText(t){ return (t ?? "").replace(/\u00A0/g," ").replace(/\s+/g," ").trim(); }
function toNumber(v){ if(v==null) return null; const n = Number(String(v).replace(/[,%]/g,"")); return Number.isFinite(n)?n:null; }
function parseTable($, $tbl){
  const headers=[]; $tbl.find("thead th, thead td").each((_,th)=>headers.push(normText($(th).text())));
  if(!headers.length){ const first=$tbl.find("tr").first(); first.find("th,td").each((_,td)=>headers.push(normText($(td).text()))); }
  const rows=[]; $tbl.find("tbody tr").each((_,tr)=>{ const cells=[]; $(tr).find("th,td").each((_,td)=>cells.push(normText($(td).text()))); if(cells.length) rows.push(cells); });
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

// --- 各コースページのパーサ ---
function parseAvgSTFromCoursePage($){
  const $tbl = mustTableByHeader($, ["月日","場","レース","ST","結果"]); if(!$tbl) return null;
  const { headers, rows } = parseTable($, $tbl);
  const iST = headerIndex(headers,"ST"); if(iST<0) return null;
  let sum=0,cnt=0;
  for(const r of rows){
    const st=r[iST]; if(!st) continue;
    if(/^[FL]/i.test(st)) continue;
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
    if(label.includes("（自艇）")) continue;
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
// 出走選手の収集（“日付なし運用”）
// 1) programs/v2 / programs-slim/v2 の日付ディレクトリ(YYYYMMDD)を列挙して最新を選ぶ
// 2) 最新が見つかればその配下を走査、無ければ直下（フラット）を走査
// 3) PID/RACE フィルタを適用し、entries/boats から登録番号を収集
// -------------------------------
function pickRaceLabel(obj){
  const r = obj?.race ?? obj?.Race ?? obj?.race_number ?? obj?.RaceNumber;
  if (r == null) return null;
  const n = String(r).replace(/[^\d]/g,"");
  return n ? `${Number(n)}R` : null;
}
function pickStadiumCode(obj){
  const code = obj?.race_stadium_number ?? obj?.stadium_number ?? obj?.stadium ?? obj?.stadiumCode;
  return code != null ? String(code).padStart(2,"0") : null;
}
function pickStadiumName(obj){
  return obj?.race_stadium_name ?? obj?.stadium_name ?? obj?.stadiumName ?? null;
}
function listDateDirs(root){
  if (!fssync.existsSync(root)) return [];
  return fssync.readdirSync(root, { withFileTypes: true })
    .filter(d => d.isDirectory() && /^\d{8}$/.test(d.name))
    .map(d => d.name)
    .sort((a,b)=> b.localeCompare(a)); // 新しい順
}
function* candidateProgramRoots(){
  const roots = [
    path.join(PUBLIC_DIR, "programs", "v2"),
    path.join(PUBLIC_DIR, "programs-slim", "v2"),
  ];
  // まず両方の“最新日付”があればそれを優先
  for (const base of roots) {
    const dates = listDateDirs(base);
    if (dates.length) yield path.join(base, dates[0]);
  }
  // フラット直下の可能性
  for (const base of roots) yield base;
}
function collectRegnosFromJson(json){
  const regnos = [];
  const boats = json?.entries || json?.boats || [];
  for (const b of boats) {
    const r = b.number ?? b.racer_number ?? b.racer?.number;
    if (r) regnos.push(String(r));
  }
  return regnos;
}
function collectRegnosFromVenueJson(json){
  const regnos = [];
  const venues = Array.isArray(json?.programs) ? json.programs
               : Array.isArray(json?.venues)   ? json.venues
               : Array.isArray(json?.items)    ? json.items
               : Array.isArray(json)           ? json
               : [];
  for (const v of venues) {
    const stadiumCode = pickStadiumCode(v);
    const stadiumName = pickStadiumName(v);
    if (PID_FILTERS && !PID_FILTERS.includes(stadiumCode ?? "") && !PID_FILTERS.includes(stadiumName ?? "")) continue;

    const racesArr = v.races ?? v.Races ?? [];
    for (const r of racesArr) {
      const raceLabel = pickRaceLabel(r);
      if (!raceLabel) continue;
      if (RACE_FILTERS && !RACE_FILTERS.includes(raceLabel)) continue;
      regnos.push(...collectRegnosFromJson(r));
    }
  }
  return regnos;
}
async function collectRacersFromPrograms(){
  const set = new Set();
  const readJson = (p) => { try { return JSON.parse(fssync.readFileSync(p, "utf8")); } catch { return null; } };

  for (const root of candidateProgramRoots()) {
    if (!fssync.existsSync(root)) continue;

    // 1) フラット（各ファイルが1R分）
    let usedSomething = false;
    const stadiumDirs = fssync.existsSync(root)
      ? fssync.readdirSync(root, { withFileTypes: true }).filter(d => d.isDirectory()).map(d => d.name)
      : [];

    for (const pid of stadiumDirs) {
      const pidDir = path.join(root, pid);
      // PID フィルタ
      if (PID_FILTERS && !PID_FILTERS.includes(pid)) {
        // stadiumName で判定する術がないので、ここはコードのみ
        // （名前フィルタを使いたい場合は「フルJSON側」で venue 形式を読む想定）
      }

      const files = fssync.readdirSync(pidDir).filter(f => f.endsWith(".json") && f !== "index.json");
      for (const f of files) {
        const json = readJson(path.join(pidDir, f));
        if (!json) continue;

        // v2標準（フラット要素）なら race_stadium_number / race_number を持つ
        if (json?.race_number !== undefined || json?.race_stadium_number !== undefined) {
          const scode = pickStadiumCode(json);
          const sname = pickStadiumName(json);
          if (PID_FILTERS && !PID_FILTERS.includes(scode ?? "") && !PID_FILTERS.includes(sname ?? "")) continue;
          const raceLabel = pickRaceLabel(json);
          if (!raceLabel) continue;
          if (RACE_FILTERS && !RACE_FILTERS.includes(raceLabel)) continue;

          collectRegnosFromJson(json).forEach(r => set.add(r));
          usedSomething = true;
        } else {
          // venue.races 形式の可能性
          const regnos = collectRegnosFromVenueJson(json);
          if (regnos.length) {
            regnos.forEach(r => set.add(r));
            usedSomething = true;
          }
        }
      }
    }
    if (usedSomething && set.size) return [...set]; // 最初に見つかった候補を採用
  }
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

      // 自艇行の横列＝勝ち決まり手（見つからなければ null）
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
  try{ const html=await fetchHtml(uRdemo); const $=load(html); exTimeRank = parseExTimeRankFromRdemo($); }
  catch(e){ console.warn(`warn: rdemo fetch/parse failed for ${regno}: ${e.message}`); }

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

  console.log(`process ${racers.length} racers (incremental, fresh<=${FRESH_HOURS}h)` +
    (ENV_RACERS ? " [env RACERS specified]" : "") +
    (ENV_RACERS_LIMIT ? ` [limit=${ENV_RACERS_LIMIT}]` : "") +
    (ENV_BATCH ? ` [batch=${ENV_BATCH}]` : "") +
    (PID_FILTERS ? ` [pid=${PID_IN}]` : "") +
    (RACE_FILTERS ? ` [race=${RACE_IN}]` : "")
  );

  await ensureDir(OUTPUT_DIR_V2);

  let ok=0, ng=0;
  for(const regno of racers){
    const outPath = path.join(OUTPUT_DIR_V2, `${regno}.json`);
    if(!OVERWRITE && isFresh(outPath, FRESH_HOURS)){
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
    await sleep(WAIT_MS_BETWEEN_RACERS);
  }

  await ensureDir(path.join(PUBLIC_DIR, "debug"));
  await fs.writeFile(
    path.join(PUBLIC_DIR, "debug", "stats-meta.json"),
    JSON.stringify({ status:200, fetchedAt:new Date().toISOString(), racers: racers.map(Number), success:ok, failed:ng }, null, 2),
    "utf8"
  );
}

main().catch(e => { console.error(e); process.exit(1); });
