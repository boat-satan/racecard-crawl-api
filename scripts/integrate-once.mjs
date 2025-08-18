// scripts/integrate-once.mjs
// Node v20 / ESM
// 用途: サーバーなしで 1レース分の統合JSONを生成
// 入力: public/programs*/v2/<date>/<pid>/<race>.json（出走表）
//       public/exhibition/v1/<date>/<pid>/<race>.json（展示）
//       public/stats/v2/racers/<regno>.json（選手スタッツ）
// 出力: public/integrated/v1/<date>/<pid>/<race>.json

import fs from "node:fs/promises";
import fssync from "node:fs";
import path from "node:path";

const ROOT = process.cwd();
const to2 = (s) => String(s).padStart(2, "0");

// -------- 引数/環境変数 --------
function pickArg(name, fallback){
  const val = process.env[name] ?? getCliArg(`--${name.toLowerCase()}`);
  return (val ?? fallback);
}
function getCliArg(flag){
  const i = process.argv.indexOf(flag);
  return i> -1 ? process.argv[i+1] : undefined;
}
const DATE = (pickArg("DATE","today") || "").replace(/-/g,"");
const PID  = to2(pickArg("PID","02"));
const RACE = (pickArg("RACE","1R") || "").toUpperCase().replace(/[^\d]/g,"") + "R";

// -------- ユーティリティ --------
const readJson = async (p) => JSON.parse(await fs.readFile(p,"utf8"));
const exists = (p) => fssync.existsSync(p);
async function ensureDir(p){ await fs.mkdir(p,{recursive:true}); }

// 出走表の探索（programs / programs-slim のどちらかにある想定）
function* candidateProgramRoots(){
  yield path.join(ROOT,"public","programs","v2", DATE);
  yield path.join(ROOT,"public","programs-slim","v2", DATE);
  yield path.join(ROOT,"public","programs","v2","today");
  yield path.join(ROOT,"public","programs-slim","v2","today");
  yield path.join(ROOT,"public","programs","v2");
  yield path.join(ROOT,"public","programs-slim","v2");
}

function findRacecardPath(){
  for(const base of candidateProgramRoots()){
    const p = path.join(base, PID, `${RACE}.json`);
    if (exists(p)) return p;
  }
  return null;
}

// --- ここだけ差し替え：スタッツ抽出 + 欠損フォールバック実装 ---
function sliceStatsForCourse(stats, courseN){
  if (!stats) return null;
  const n = Number(courseN);
  const entryCourse = Array.isArray(stats.entryCourse) ? stats.entryCourse : [];
  const hit = entryCourse.find(ec => Number(ec.course) === n);
  const kAll = hit?.kimariteAllBoats?.rows || stats.kimariteAllBoats?.rows || [];

  // フォールバック計算ヘルパ
  const pickSelfDetail = () => {
    const selfRow = (kAll || []).find(r => Number(r.course) === n && r.isSelf && r.detail);
    return selfRow?.detail || null;
  };
  const sumLoseDetail = () => {
    const init = { "逃げ":0,"差し":0,"まくり":0,"まくり差し":0,"抜き":0,"恵まれ":0 };
    for (const r of (kAll || [])) {
      if (Number(r.course) !== n) continue;
      if (r.isSelf) continue;
      const d = r.detail || {};
      for (const k of Object.keys(init)) init[k] += Number(d[k] ?? 0);
    }
    return init;
  };

  if (!hit) {
    return {
      entryCourse: { course: n, avgST: null, selfSummary: null, winKimariteSelf: null, loseKimarite: null, matrixSelf: null },
      exTimeRank: stats.exTimeRank ?? null,
      regno: stats.regno ?? null,
      fetchedAt: stats.fetchedAt ?? null,
      schemaVersion: stats.schemaVersion ?? null,
      notFound: true
    };
  }

  const winSelf    = hit.winKimariteSelf ?? pickSelfDetail();
  const loseAll    = hit.loseKimarite    ?? sumLoseDetail();
  const matrixSelf = hit.matrix?.self ?? null;

  return {
    entryCourse: {
      course: n,
      avgST: hit.avgST ?? null,
      selfSummary: hit.selfSummary ?? null,
      winKimariteSelf: winSelf ?? null,
      loseKimarite: loseAll ?? null,
      matrixSelf
    },
    exTimeRank: stats.exTimeRank ?? null,
    regno: stats.regno ?? null,
    fetchedAt: stats.fetchedAt ?? null,
    schemaVersion: stats.schemaVersion ?? null
  };
}

// -------- メイン統合 --------
async function integrateOnce(){
  // 1) 入力の場所
  const racecardPath = findRacecardPath();
  if (!racecardPath){
    throw new Error(`racecard not found for ${DATE}/${PID}/${RACE} under public/programs*/v2`);
  }
  const exhibitionPath = path.join(ROOT, "public", "exhibition", "v1", DATE, PID, `${RACE}.json`);
  if (!exists(exhibitionPath)){
    throw new Error(`exhibition not found: ${path.relative(ROOT, exhibitionPath)}`);
  }

  // 2) 読み込み
  const racecard   = await readJson(racecardPath);
  const exhibition = await readJson(exhibitionPath);

  // 3) 進入コースの決定: 展示 startCourse があれば優先、なければ lane を採用
  const exEntries = Array.isArray(exhibition.entries) ? exhibition.entries : [];
  const rcEntries = racecard.entries || racecard.boats || [];

  // 4) 各艇のスタッツ抽出（進入コース n）
  const integratedEntries = [];
  for (const e of rcEntries){
    const number = Number(e.number ?? e.racer_number ?? e.racer?.number);
    const laneRC = Number(e.lane ?? e.boat ?? e.racer_boat_number);

    // 展示側の対応行
    const ex = exEntries.find(x => Number(x.number ?? x.racer_number ?? x.id) === number)
            || exEntries.find(x => Number(x.lane) === laneRC);

    const startCourse = ex?.exhibition?.startCourse != null
      ? Number(ex.exhibition.startCourse)
      : (ex?.lane != null ? Number(ex.lane) : laneRC);

    // スタッツ読み込み（必要な時だけ）
    let statsSlice = null;
    try{
      const statPath = path.join(ROOT, "public", "stats", "v2", "racers", `${number}.json`);
      if (exists(statPath)){
        const statsFull = await readJson(statPath);
        statsSlice = sliceStatsForCourse(statsFull, startCourse);
      }
    }catch{ /* ignore per boat */ }

    integratedEntries.push({
      number,
      lane: laneRC,
      startCourse,
      racecard: e,
      exhibition: ex || null,
      stats: statsSlice,
    });
  }

  // 5) 出力
  const out = {
    schemaVersion: "1.0",
    date: DATE, pid: PID, race: RACE,
    generatedAt: new Date().toISOString(),
    sources: {
      racecard: path.relative(ROOT, racecardPath),
      exhibition: path.relative(ROOT, exhibitionPath),
      statsDir: "public/stats/v2/racers",
    },
    // ★ 追加: 展示で取得した水面気象＋安定板情報をそのまま反映
    weather: exhibition.weather ?? null,
    entries: integratedEntries,
  };

  const outDir = path.join(ROOT, "public", "integrated", "v1", DATE, PID);
  await ensureDir(outDir);
  const outPath = path.join(outDir, `${RACE}.json`);
  await fs.writeFile(outPath, JSON.stringify(out, null, 2), "utf8");

  console.log("wrote:", path.relative(ROOT, outPath));
}

integrateOnce().catch(e => { console.error(e); process.exit(1); });
