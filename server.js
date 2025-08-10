// server.js
// Node v20 / ESM
// 目的: 出走表(program)・展示(exhibition)・スタッツ(stats)を読み込み、
//       レース単位で統合JSONを返すAPIを提供する（保存はしない最小版）
//
// エンドポイント:
//   GET /api/integrated/:date/:pid/:race
//     例) /api/integrated/20250810/04/1R
//
// ディレクトリ前提:
//   public/programs/v2/<date>/<pid>/<race>.json        (or programs-slim/v2/...)
//   public/exhibition/v1/<date>/<pid>/<race>.json
//   public/stats/v2/racers/<regno>.json
//
// 注: スタッツは「展示の進入コース n に対応する entryCourse[n]」と
//     「展示順位別(exTimeRank)」のみを抽出して返します。

import { createServer } from "node:http";
import fs from "node:fs/promises";
import fssync from "node:fs";
import path from "node:path";
import url from "node:url";

const ROOT = process.cwd();

// ---------- ヘルパ ----------
const to2 = (s) => String(s).padStart(2, "0");
const normRace = (r) => (String(r).toUpperCase().endsWith("R") ? String(r).toUpperCase() : `${r}R`);

async function readJsonIfExists(p) {
  try {
    const t = await fs.readFile(p, "utf8");
    return JSON.parse(t);
  } catch {
    return null;
  }
}
function exists(p) {
  try { return fssync.existsSync(p); } catch { return false; }
}

// programs の検索（programs/v2 → programs-slim/v2）
function findProgramPath(date, pid, race) {
  const candidates = [
    path.join(ROOT, "public", "programs", "v2", date, pid, `${race}.json`),
    path.join(ROOT, "public", "programs-slim", "v2", date, pid, `${race}.json`),
  ];
  return candidates.find(exists) || candidates[0]; // なければ一つ目を返す（後で404にする）
}

function exPath(date, pid, race) {
  return path.join(ROOT, "public", "exhibition", "v1", date, pid, `${race}.json`);
}

function statsPath(regno) {
  return path.join(ROOT, "public", "stats", "v2", "racers", `${regno}.json`);
}

// lane/番号対応のユーティリティ
function getEntryNumberOfLane(programJson, lane) {
  const entries = programJson?.entries || programJson?.boats || [];
  const hit = entries.find(e => Number(e.lane ?? e.boat ?? e.racer_boat_number) === Number(lane));
  return hit ? Number(hit.number ?? hit.racer_number ?? hit.id) : null;
}

// 展示からコース(進入)推定: startCourse があればそれ、なければ lane フォールバック
function getStartCourseFromExEntry(exEntry) {
  const sc = exEntry?.exhibition?.startCourse ?? exEntry?.startCourse;
  const ln = exEntry?.lane ?? exEntry?.racer_boat_number ?? exEntry?.boat;
  return sc != null ? Number(sc) : (ln != null ? Number(ln) : null);
}

// スタッツからコース別要約を抽出
function pickStatsForCourse(statsJson, courseNum) {
  if (!statsJson || !Array.isArray(statsJson.entryCourse)) return null;
  const ec = statsJson.entryCourse.find(c => Number(c.course) === Number(courseNum));
  if (!ec) return null;
  return {
    course: Number(courseNum),
    avgST: ec.avgST ?? null,
    selfSummary: ec.selfSummary ?? null,
    matrixSelf: ec.matrix?.self ?? null,
    winKimariteSelf: ec.winKimariteSelf ?? null,
    loseKimarite: ec.loseKimarite ?? null,
  };
}

// ---------- ハンドラ ----------
async function handleIntegrated(req, res, params) {
  const date = params.date.replace(/-/g, "");
  const pid = to2(params.pid);
  const race = normRace(params.race);

  // 入力ファイルパス
  const programFile = findProgramPath(date, pid, race);
  const exhibitionFile = exPath(date, pid, race);

  // 読み込み
  const program = await readJsonIfExists(programFile);
  const exhibition = await readJsonIfExists(exhibitionFile);

  // 存在チェック
  if (!program) {
    res.writeHead(404, { "content-type": "application/json; charset=utf-8" });
    res.end(JSON.stringify({ error: "program not found", path: path.relative(ROOT, programFile) }, null, 2));
    return;
  }
  if (!exhibition) {
    res.writeHead(404, { "content-type": "application/json; charset=utf-8" });
    res.end(JSON.stringify({ error: "exhibition not found", path: path.relative(ROOT, exhibitionFile) }, null, 2));
    return;
  }

  // laneごとに番号と進入コースを確定 → スタッツ読み込み＆必要部分だけ抽出
  const exEntries = exhibition.entries || [];
  const statsByLane = {};

  for (const ex of exEntries) {
    const lane = ex?.lane != null ? Number(ex.lane) : null;
    const number =
      ex?.number != null ? Number(ex.number) : (lane != null ? getEntryNumberOfLane(program, lane) : null);
    const courseNum = getStartCourseFromExEntry(ex);

    if (!number || !courseNum) continue;

    // スタッツ読み込み（キャッシュ簡易化）
    const spath = statsPath(number);
    const statsJson = await readJsonIfExists(spath);

    // 必要部分だけ
    const picked = pickStatsForCourse(statsJson, courseNum);
    const exTimeRank = statsJson?.exTimeRank ?? null;

    statsByLane[lane] = {
      number,
      course: courseNum,
      stats: picked,
      exTimeRank,
      statsSource: exists(spath) ? path.relative(ROOT, spath) : null,
    };
  }

  // 統合オブジェクト
  const payload = {
    schemaVersion: "1.0",
    generatedAt: new Date().toISOString(),
    input: { date, pid, race },
    sources: {
      program: path.relative(ROOT, programFile),
      exhibition: path.relative(ROOT, exhibitionFile),
    },
    // そのまま欲しい人向けに原本も返す（必要に応じて front で省略可）
    program,
    exhibition,
    // GPTs へ渡す要点: lane→(racer, 進入コースのスタッツ要約, 展示順位別)
    statsByLane,
  };

  res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(payload, null, 2));
}

// ---------- ルーター ----------
const server = createServer(async (req, res) => {
  try {
    const u = url.parse(req.url, true);
    const m = req.method || "GET";

    // /api/integrated/:date/:pid/:race
    const rx = /^\/api\/integrated\/([^/]+)\/([^/]+)\/([^/]+)\/?$/;
    const mm = u.pathname.match(rx);
    if (m === "GET" && mm) {
      const [, date, pid, race] = mm;
      await handleIntegrated(req, res, { date, pid, race });
      return;
    }

    res.writeHead(404, { "content-type": "application/json; charset=utf-8" });
    res.end(JSON.stringify({ error: "not found" }, null, 2));
  } catch (e) {
    res.writeHead(500, { "content-type": "application/json; charset=utf-8" });
    res.end(JSON.stringify({ error: e.message }, null, 2));
  }
});

const PORT = Number(process.env.PORT || 3000);
server.listen(PORT, () => {
  console.log(`Integrated API listening on http://localhost:${PORT}`);
});
