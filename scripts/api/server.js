// scripts/api/integrate.js
// Node v20 / ESM
// 統合ロジック本体（サーバから呼ぶ）
// 入力: date, pid, race, options
// 出力: 統合JSON（ファイルにも保存）

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";

const ROOT = process.cwd();
const PUB  = path.join(ROOT, "public");

// 既定の入出力場所
const SRC_RACECARD   = (date,pid,race) => path.join(PUB, "programs",      "v2", date, pid, `${race}.json`);
const SRC_EXHIBITION = (date,pid,race) => path.join(PUB, "exhibition",    "v1", date, pid, `${race}.json`);
const SRC_STATS_DIR  = path.join(PUB, "stats", "v2", "racers");
const OUT_INTEGRATED = (date,pid,race) => path.join(PUB, "integrated", "v1", date, pid, `${race}.json`);

const to2 = s => String(s).padStart(2,"0");
const normRace = (r) => {
  if (!r) return null;
  const n = String(r).replace(/[^\d]/g,"");
  if (!n) return null;
  const i = Number(n);
  if (!Number.isFinite(i) || i<1 || i>12) return null;
  return `${i}R`;
};

function isFresh(p, hours){
  try{
    const st = fs.statSync(p);
    return (Date.now() - st.mtimeMs) <= hours*3600*1000;
  }catch{ return false; }
}

async function readJson(p){
  try{
    const t = await fsp.readFile(p, "utf8");
    return JSON.parse(t);
  }catch(e){
    const code = (e && e.code) || "READ_ERROR";
    const msg = code === "ENOENT" ? `not found: ${p}` : `invalid json: ${p}`;
    const err = new Error(msg);
    err.code = code;
    err.path = p;
    throw err;
  }
}

// 展示から startCourse を推定（lane優先 → startCourse → 不明なら lane）
function pickStartCourse(exEntry){
  const lane = Number(exEntry?.lane) || null;
  const sc   = Number(exEntry?.exhibition?.startCourse ?? exEntry?.startCourse) || null;
  return sc || lane || null;
}

// 指定コースのスタッツだけを抽出
function sliceStatsForCourse(fullStats, courseNo){
  if (!fullStats || !Number.isFinite(courseNo)) return null;
  const ec = Array.isArray(fullStats.entryCourse) ? fullStats.entryCourse : fullStats.entryCourse && fullStats.entryCourse.course ? [fullStats.entryCourse] : [];
  const found = ec.find(c => c.course === courseNo) || null;
  return found ? {
    course: found.course,
    avgST: found.avgST ?? null,
    loseKimarite: found.loseKimarite ?? null,
    winKimariteSelf: found.winKimariteSelf ?? null,
    selfSummary: found.selfSummary ?? null,
  } : null;
}

export async function integrateOnce(dateIn, pidIn, raceIn, {
  freshHours = Number(process.env.FRESH_HOURS || 12),
  allowCache = true,
} = {}) {
  // 正規化
  const date = (dateIn && dateIn !== "today")
    ? String(dateIn).replace(/-/g,"")
    : String(new Date().toLocaleString("ja-JP", { timeZone:"Asia/Tokyo" }))
        .replace(/\D/g,"").slice(0,8); // JST今日
  const pid  = to2(pidIn);
  const race = normRace(raceIn);
  if (!race) {
    const err = new Error(`invalid race: ${raceIn}`);
    err.status = 422;
    throw err;
  }

  const outPath = OUT_INTEGRATED(date, pid, race);

  // キャッシュ利用
  if (allowCache && isFresh(outPath, freshHours)) {
    const cached = await readJson(outPath);
    return { cached: true, path: outPath, payload: cached };
  }

  // 入力ソース
  const racecardPath   = SRC_RACECARD(date,pid,race);
  const exhibitionPath = SRC_EXHIBITION(date,pid,race);
  const statsDir       = SRC_STATS_DIR;

  // 読み込み
  const racecard   = await readJson(racecardPath).catch(e => { e.status = (e.code==="ENOENT"?404:422); throw e; });
  const exhibition = await readJson(exhibitionPath).catch(e => { e.status = (e.code==="ENOENT"?404:422); throw e; });

  // 統合
  const exEntries = exhibition.entries || [];
  const rcEntries = racecard.entries  || racecard.boats || [];

  // 参照しやすい map 作成（番号キー）
  const rcByNo = new Map();
  for (const b of rcEntries) {
    const num = Number(b?.number ?? b?.racer_number);
    if (num) rcByNo.set(num, {
      lane: Number(b.lane ?? b.course ?? b.racer_boat_number ?? null),
      number: num,
      name: b.name ?? b.racer?.name ?? null,
      classNumber: b.classNumber ?? b.class_number ?? null,
      branchNumber: b.branchNumber ?? b.branch_number ?? null,
      birthplaceNumber: b.birthplaceNumber ?? b.birthplace_number ?? null,
      age: b.age ?? null,
      weight: b.weight ?? null,
      flyingCount: b.flyingCount ?? b.f ?? null,
      lateCount: b.lateCount ?? b.l ?? null,
      avgST: b.avgST ?? b.avg_st ?? null,
      natTop1: b.natTop1 ?? null,
      natTop2: b.natTop2 ?? null,
      natTop3: b.natTop3 ?? null,
      locTop1: b.locTop1 ?? null,
      locTop2: b.locTop2 ?? null,
      locTop3: b.locTop3 ?? null,
      motorNumber: b.motorNumber ?? b.motor?.number ?? null,
      motorTop2: b.motorTop2 ?? b.motor?.top2 ?? null,
      motorTop3: b.motorTop3 ?? b.motor?.top3 ?? null,
      boatNumber: b.boatNumber ?? b.boat?.number ?? null,
      boatTop2: b.boatTop2 ?? b.boat?.top2 ?? null,
      boatTop3: b.boatTop3 ?? b.boat?.top3 ?? null,
    });
  }

  const payload = {
    schemaVersion: "1.0",
    date, pid, race,
    generatedAt: new Date().toISOString(),
    sources: {
      racecard:   path.posix.join("public","programs","v2",date,pid,`${race}.json`),
      exhibition: path.posix.join("public","exhibition","v1",date,pid,`${race}.json`),
      statsDir:   path.posix.join("public","stats","v2","racers"),
    },
    entries: [],
  };

  // 展示の6艇をベースに1艇ずつ統合
  for (const ex of exEntries) {
    const number = Number(ex.number ?? ex?.raw?.number);
    const lane   = Number(ex.lane) || null;

    // 出走表
    const rc = number ? rcByNo.get(number) : null;

    // 進入コース（展示）
    const startCourse = pickStartCourse({ lane, exhibition: ex.exhibition });

    // スタッツ（regno.jsonを読んでコース抽出 + 展示順位別）
    let stats = null;
    if (number && fs.existsSync(path.join(statsDir, `${number}.json`))) {
      try {
        const sraw = JSON.parse(fs.readFileSync(path.join(statsDir, `${number}.json`), "utf8"));
        const entryCourse = sliceStatsForCourse(sraw, startCourse ?? lane ?? null);
        stats = {
          entryCourse,
          exTimeRank: Array.isArray(sraw.exTimeRank) ? sraw.exTimeRank : null,
          regno: sraw.regno ?? number,
          fetchedAt: sraw.fetchedAt ?? null,
          schemaVersion: sraw.schemaVersion ?? null,
        };
      } catch {}
    }

    payload.entries.push({
      number,
      lane,
      startCourse,
      racecard: rc || null,
      exhibition: ex,
      stats,
    });
  }

  // 保存
  await fsp.mkdir(path.dirname(outPath), { recursive: true });
  await fsp.writeFile(outPath, JSON.stringify(payload, null, 2), "utf8");

  return { cached:false, path: outPath, payload };
}

// ------------------------------------------------------
// HTTP ハンドラ（Express などから使う）
// GET /api/integrate/v1/:date/:pid/:race
export async function handleIntegrate(req, res){
  try{
    const { date, pid, race } = req.params;
    const { freshHours, noCache } = req.query;
    const result = await integrateOnce(date, pid, race, {
      freshHours: freshHours ? Number(freshHours) : undefined,
      allowCache: noCache ? false : true,
    });
    res.status(200).json(result.payload);
  }catch(e){
    const status = e.status || (e.code==="ENOENT" ? 404 : 500);
    res.status(status).json({ error: e.message || String(e) });
  }
}
