// scripts/api/merge-race-data.js
// Node v20 / ESM
//
// 目的:
//  - 出走表(programs v2) / 展示データ(exhibition v1) / スタッツ(stats v2)
//    の3つを読み込み、各艇の展示進入コース(n)に合わせたスタッツ要約だけを付与して統合出力。
// 出力先:
//  - public/merged/v1/<date>/<pid>/<race>.json
//
// 使い方例:
//   TARGET_DATE=20250810 TARGET_PID=04 TARGET_RACE=1 node scripts/api/merge-race-data.js
//
// 依存:
//  - scripts/lib/extract-stats.js

import fs from "node:fs/promises";
import fssync from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildRacerStatsForCourse, normalizeCourseNumber } from "../lib/extract-stats.js";

// --------- 入力指定・定数 ----------
const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const ROOT = path.resolve(__dirname, "..", "..");
const PUB  = path.join(ROOT, "public");

// 既存ディレクトリ構成（必要なら環境変数で差し替え可）
const PROG_ROOT   = process.env.PROG_ROOT   || path.join(PUB, "programs", "v2");
const PROG_SLIM   = process.env.PROG_SLIM   || path.join(PUB, "programs-slim", "v2");
const EXHIB_ROOT  = process.env.EXHIB_ROOT  || path.join(PUB, "exhibition", "v1");
const STATS_ROOT  = process.env.STATS_ROOT  || path.join(PUB, "stats", "v2", "racers");
const MERGED_ROOT = process.env.MERGED_ROOT || path.join(PUB, "merged", "v1");

// 引数（環境変数）
const DATE_IN = (process.env.TARGET_DATE || "today").replace(/-/g, "");
const PID_IN  = (process.env.TARGET_PID  || "02").padStart(2, "0");
const RNO     = String(process.env.TARGET_RACE || "1").replace(/[^\d]/g, "");
const RACE    = `${RNO}R`;

// --------- ユーティリティ ----------
const readJSON = async (p) => JSON.parse(await fs.readFile(p, "utf8"));
const exists = (p) => { try { fssync.accessSync(p); return true; } catch { return false; } };

const to2 = (s) => String(s).padStart(2, "0");

/**
 * programs の探索:
 *  1) programs/v2/<date>/<pid>/<race>.json
 *  2) programs-slim/v2/<date>/<pid>/<race>.json
 *  3) programs/v2/today/<pid>/<race>.json
 *  4) programs-slim/v2/today/<pid>/<race>.json
 *  5) programs*/v2/<pid>/<race>.json（直下）
 */
function* candidateProgramFiles(date, pid, race) {
  yield path.join(PROG_ROOT, date, pid, `${race}.json`);
  yield path.join(PROG_SLIM, date, pid, `${race}.json`);
  yield path.join(PROG_ROOT, "today", pid, `${race}.json`);
  yield path.join(PROG_SLIM, "today", pid, `${race}.json`);
  yield path.join(PROG_ROOT, pid, `${race}.json`);
  yield path.join(PROG_SLIM, pid, `${race}.json`);
}

/**
 * プログラムJSONを読む（上記の探索順で最初に見つかったもの）
 */
async function loadProgram(date, pid, race) {
  for (const f of candidateProgramFiles(date, pid, race)) {
    if (exists(f)) return await readJSON(f);
  }
  throw new Error(`program not found for ${date}/${pid}/${race}`);
}

/**
 * 展示JSON
 *  - 固定の場所: exhibition/v1/<date>/<pid>/<race>.json
 */
async function loadExhibition(date, pid, race) {
  const file = path.join(EXHIB_ROOT, date, pid, `${race}.json`);
  if (!exists(file)) throw new Error(`exhibition not found: ${file}`);
  return await readJSON(file);
}

/**
 * スタッツ(1人分)を読む
 *  - 固定の場所: stats/v2/racers/<regno>.json
 */
async function loadStats(regno) {
  const file = path.join(STATS_ROOT, `${regno}.json`);
  if (!exists(file)) return null; // 無ければ null（そのまま欠損扱い）
  try {
    return await readJSON(file);
  } catch {
    return null;
  }
}

/**
 * 出走表 entry から「艇番(lane)/登録番号(regno)」を柔軟に取り出す
 */
function pickLaneAndRegno(entry) {
  const lane  = entry?.lane ?? entry?.boat_number ?? entry?.racer_boat_number ?? entry?.boat ?? null;
  const regno = entry?.number ?? entry?.racer_number ?? entry?.racer?.number ?? entry?.id ?? null;
  return {
    lane: lane != null ? Number(lane) : null,
    regno: regno != null ? Number(regno) : null,
  };
}

/**
 * 展示エントリから「展示進入コース(startCourse)」を取る
 *  - scripts/crawl-exhibition.js の正規化出力を想定
 */
function pickStartCourseFromExEntry(exEntry) {
  const sc = exEntry?.exhibition?.startCourse ?? exEntry?.startCourse ?? null;
  return normalizeCourseNumber(sc);
}

/**
 * 進入コースに応じてスタッツを間引き
 */
function buildStatsSlice(stats, startCourse) {
  if (!stats || !startCourse) return null;
  return buildRacerStatsForCourse(stats, startCourse);
}

/**
 * 統合 1 レーサー分
 */
function mergeOne(programEntry, exhibitionEntry, statsSlice) {
  // 基本プロフ（出走表側）
  const { lane, regno } = pickLaneAndRegno(programEntry);

  // 展示サマリ（最低限）
  const ex = exhibitionEntry?.exhibition ?? null;

  return {
    lane,
    regno,
    // 出走表の元情報は rawProgram にそのまま
    rawProgram: programEntry ?? null,

    exhibition: ex ? {
      time: ex.time ?? null,
      rank: ex.rank ?? null,
      startTiming: ex.startTiming ?? null,
      startCourse: ex.startCourse ?? null,
      pitOut: ex.pitOut ?? null,
      lapRank: ex.lapRank ?? null,
      note: ex.note ?? null,
    } : null,

    // スタッツ（(n)コース進入時 + 展示順位別）
    stats: statsSlice ?? null,
  };
}

/**
 * 統合メイン
 */
async function mergeRace(date, pid, race) {
  const program = await loadProgram(date, pid, race);
  const exhibition = await loadExhibition(date, pid, race);

  // 出走一覧（プログラム側の entries/boats 擬似互換）
  const progEntries = program?.entries || program?.boats || [];
  const exEntries   = exhibition?.entries || [];

  // number(regno) をキーに、展示の startCourse を控える（laneでも可だがregnoの方が頑健）
  const exByRegno = new Map();
  for (const e of exEntries) {
    const regno = Number(e?.number ?? e?.racer_number ?? e?.id ?? null);
    const sc = pickStartCourseFromExEntry(e);
    if (regno && sc) exByRegno.set(regno, { entry: e, startCourse: sc });
  }

  const mergedEntries = [];
  for (const pe of progEntries) {
    const { regno } = pickLaneAndRegno(pe);
    // 展示側ひけない場合もある
    const exHit = regno ? exByRegno.get(regno) : null;
    const startCourse = exHit?.startCourse ?? null;

    // スタッツ読み込み -> (n)コースで間引き
    const fullStats = regno ? await loadStats(regno) : null;
    const statsSlice = startCourse ? buildStatsSlice(fullStats, startCourse) : null;

    mergedEntries.push(
      mergeOne(pe, exHit?.entry ?? null, statsSlice)
    );
  }

  // 出力形
  return {
    schemaVersion: "1.0",
    date,
    stadium: to2(program?.pid ?? program?.stadium ?? PID_IN),
    race,
    generatedAt: new Date().toISOString(),
    // メタ（参照元）
    sources: {
      program: { resolvedFrom: "programs v2 / programs-slim v2", date, pid, race },
      exhibition: { root: path.relative(ROOT, EXHIB_ROOT), date, pid, race },
      stats: { root: path.relative(ROOT, STATS_ROOT) },
    },
    entries: mergedEntries,
    // 必要なら元JSONも最小限で残す
    raw: {
      program: {
        pid: program?.pid ?? null,
        stadiumName: program?.stadiumName ?? null,
        raceName: program?.raceName ?? null,
      },
      exhibitionMeta: {
        sourceShape: exhibition?.sourceShape ?? null,
      }
    }
  };
}

/**
 * 保存
 */
async function saveMerged(date, pid, race, payload) {
  const dir = path.join(MERGED_ROOT, date, pid);
  await fs.mkdir(dir, { recursive: true });
  const file = path.join(dir, `${race}.json`);
  await fs.writeFile(file, JSON.stringify(payload, null, 2), "utf8");
  console.log("write:", path.relative(ROOT, file));
}

// --------- CLI 実行部 ----------
(async () => {
  try {
    const payload = await mergeRace(DATE_IN, PID_IN, RACE);
    await saveMerged(DATE_IN, PID_IN, RACE, payload);
  } catch (e) {
    console.error(e.message || e);
    process.exit(1);
  }
})();
