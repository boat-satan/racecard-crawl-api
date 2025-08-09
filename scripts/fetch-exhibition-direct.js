// scripts/fetch-exhibition-direct.js
// Node v20 / ESM / cheerio v1.x
// 目的: 公式サイト(boatrace.jp)のレースカードから展示情報を直接クロール
// 出力: public/exhibition/v1/<YYYYMMDD>/<PID>/<R>.json
// 使い方:
//   TARGET_DATE=20250809 TARGET_PIDS=02 TARGET_RACES=1R node scripts/fetch-exhibition-direct.js --skip-existing
//   （環境変数が空なら today / 全場 / 1..12R を対象）
//
// メモ:
// - 公式HTMLの構造は更新される可能性があるため、CSSセレクタは下の SELECTORS で一括管理。
// - 404や公開前は空配列で返しつつ placeholder を保存（後続の --skip-existing が効くように）。

import fs from "node:fs/promises";
import path from "node:path";
import { load } from "cheerio";

const ROOT = process.cwd();
const OUT_ROOT = path.join(ROOT, "public", "exhibition", "v1");
const DEBUG_DIR = path.join(ROOT, "public", "debug");

const ENV_DATE  = (process.env.TARGET_DATE  || "today").replace(/-/g, "");
const ENV_PIDS  = (process.env.TARGET_PIDS  || "").trim();  // "02,09" など
const ENV_RACES = (process.env.TARGET_RACES || "").trim();  // "1R,2R" or "1,2"

// --- レートリミット（丁寧に） ---
const WAIT_MS = 900; // ~1秒/req
const sleep = (ms)=> new Promise(r=> setTimeout(r, ms));

// --- boatrace.jp racelist の想定URL ---
function urlFor({ date, pid, rno }) {
  const hd  = date === "today" ? todayYYYYMMDD() : date;
  const jcd = pid.padStart(2, "0");
  const r   = String(rno).replace(/[^\d]/g, "");
  return `https://www.boatrace.jp/owpc/pc/race/racelist?hd=${hd}&jcd=${jcd}&rno=${r}`;
}

// --- セレクタ一括管理（必要に応じて調整） ---
const SELECTORS = {
  // 展示タイムのテーブル（racelist内の「展示タイム」欄）
  exTimeRows: 'table.is-exhibition-time tbody tr',   // 各行 = 枠番順
  exTime_cell_lane: 'th,td.is-fix',                  // 枠番 or レーン
  exTime_cell_time: 'td:nth-child(2)',               // 展示タイム
  // 進入・ST のテーブル（「進入」や「ST」が載る欄）
  entryRows: 'table.is-hiranuma tbody tr, table.is-course-entry tbody tr',
  entry_cell_lane: 'th,td.is-fix',
  entry_cell_course: 'td:nth-child(2)',              // 進入コース
  entry_cell_st: 'td:nth-child(3)',                  // ST（.14, F.01 など）
};

// --- パース補助 ---
function norm(t){ return (t??"").replace(/\u00A0/g," ").replace(/\s+/g," ").trim(); }
function toNum(s){
  if (!s) return null;
  const m = String(s).match(/-?\d+(?:\.\d+)?/);
  if (!m) return null;
  const n = Number(m[0]);
  return Number.isFinite(n) ? n : null;
}
function toLane(s){
  const m = String(s).match(/(\d+)/);
  return m ? Number(m[1]) : null;
}
function toCourse(s){ return toLane(s); }
function toST(s){ // ".14" / "F.01" / "L.10"
  const S = String(s).toUpperCase().trim();
  if (!S) return null;
  if (S.startsWith("F") || S.startsWith("L")) return S; // 文字付きは文字のまま保持
  const n = toNum(S);
  return n==null ? null : Number(n.toFixed(2)); // 0.14 形式にしておく
}

// --- HTML取得 ---
async function fetchHtml(url,{retries=2,delay=800}={}){
  for (let i=0;i<=retries;i++){
    const res = await fetch(url, {
      headers: {
        "user-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118 Safari/537.36",
        "accept":"text/html,application/xhtml+xml",
      }
    });
    if (res.ok) return await res.text();
    if (res.status === 404) {
      const text = await res.text().catch(()=> "");
      const e = new Error("HTTP 404");
      e.status = 404; e.body = text;
      throw e;
    }
    if (i<retries) await sleep(delay);
  }
  throw new Error("fetch failed");
}

// --- today ヘルパ ---
function todayYYYYMMDD(){
  const jst = new Date(Date.now() + 9*3600*1000); // ざっくりJST
  const y = jst.getUTCFullYear();
  const m = String(jst.getUTCMonth()+1).padStart(2,"0");
  const d = String(jst.getUTCDate()).padStart(2,"0");
  return `${y}${m}${d}`;
}

// --- 保存 ---
async function ensureDir(p){ await fs.mkdir(p, { recursive:true }); }
async function writeJson(p, obj){
  await ensureDir(path.dirname(p));
  await fs.writeFile(p, JSON.stringify(obj, null, 2), "utf8");
}

// --- 既存スキップ ---
async function exists(p){ try{ await fs.access(p); return true; } catch { return false; } }

// --- パーサ本体 ---
function parseExhibition($){
  const exTimes = new Map(); // lane -> exTime
  $(SELECTORS.exTimeRows).each((_, tr)=>{
    const lane = toLane(norm($(tr).find(SELECTORS.exTime_cell_lane).first().text()));
    const time = toNum(norm($(tr).find(SELECTORS.exTime_cell_time).first().text()));
    if (lane) exTimes.set(lane, time);
  });

  const entries = []; // 1..6
  $(SELECTORS.entryRows).each((_, tr)=>{
    const lane = toLane(norm($(tr).find(SELECTORS.entry_cell_lane).first().text()));
    const course = toCourse(norm($(tr).find(SELECTORS.entry_cell_course).first().text()));
    const st = toST(norm($(tr).find(SELECTORS.entry_cell_st).first().text()));
    if (!lane) return;
    entries.push({ lane, course, st, exTime: exTimes.get(lane) ?? null });
  });

  // 万が一「進入/STテーブル」が無い場合、展示タイムだけでも枠順で出す
  if (entries.length === 0 && exTimes.size) {
    for (const [lane, exTime] of exTimes.entries()) {
      entries.push({ lane, course: null, st: null, exTime });
    }
  }

  // lane順にソート
  entries.sort((a,b)=> (a.lane??99)-(b.lane??99));
  return { entries };
}

// --- 1レース処理 ---
async function fetchOne({ date, pid, rno, skipExisting }){
  const d = date === "today" ? todayYYYYMMDD() : date;
  const p2 = pid.padStart(2,"0");
  const R = String(rno).replace(/[^\d]/g,"");
  const outPath = path.join(OUT_ROOT, d, p2, `${R}R.json`);

  if (skipExisting && await exists(outPath)) {
    return { skipped:true, outPath };
  }

  const url = urlFor({ date, pid:p2, rno:R });
  try{
    const html = await fetchHtml(url);
    const $ = load(html);
    const { entries } = parseExhibition($);

    const payload = {
      date: d,
      pid: p2,
      race: `${R}R`,
      source: url,
      generatedAt: new Date().toISOString(),
      entries, // [{lane, course, st, exTime}]
    };

    await writeJson(outPath, payload);
    return { ok:true, outPath, count: entries.length };
  } catch(e){
    // 404等でも placeholder を出す（既存スキップの判定に使える）
    const payload = {
      status: "unavailable",
      source: url,
      error: String(e.message || e),
      generatedAt: new Date().toISOString(),
    };
    await writeJson(outPath, payload);

    // デバッグHTML保存（404以外）
    if (!e.status || e.status !== 404) {
      await ensureDir(DEBUG_DIR);
      const fn = path.join(DEBUG_DIR, `exhibition-${d}-${p2}-${R}.err.txt`);
      await fs.writeFile(fn, (e.body || e.stack || String(e)), "utf8");
    }
    return { ok:false, outPath, error: String(e.message||e) };
  }
}

// --- 対象の展開 ---
function parseList(envCsv, fallback){
  if (envCsv && envCsv.trim()) {
    return envCsv.split(",").map(s=> s.trim()).filter(Boolean);
  }
  return fallback;
}
function range(n1, n2){ const a=[]; for(let i=n1;i<=n2;i++) a.push(i); return a; }

// --- メイン ---
(async function main(){
  const date  = ENV_DATE;             // YYYYMMDD or today
  const pids  = parseList(ENV_PIDS,  ["02"]);        // デフォ江戸川
  const races = parseList(ENV_RACES, range(1,12).map(n=> `${n}`)); // 1..12

  const skipExisting = process.argv.includes("--skip-existing");

  console.log(`exhibition(build): date=${date} pids=[${pids.join(",")}] races=[${races.join(",")}] skipExisting=${skipExisting}`);

  let done=0, skipped=0;
  for (const pid of pids) {
    for (const r of races) {
      const rno = String(r).replace(/[^\d]/g,"");
      const res = await fetchOne({ date, pid, rno, skipExisting });
      if (res.skipped) { skipped++; continue; }
      if (res.ok) done++;
      await sleep(WAIT_MS);
    }
  }
  console.log(`done exhibitions: ok=${done} skipped=${skipped}`);
})().catch(e=>{
  console.error(e);
  process.exit(1);
});
