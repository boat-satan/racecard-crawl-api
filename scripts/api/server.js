// scripts/api/server.js
// Node v20 / ESM
//
// 目的:
//  - HTTPで /api/merged?date=YYYYMMDD&pid=NN&race=1..12 を受け取り
//    出走表 + 展示 + (展示進入コースに合わせた)スタッツ要約 を統合して JSON を返す。
//  - 事前に保存された public/merged/v1/<date>/<pid>/<race>.json があれば
//    ?cache=1 でそれを優先返却。無ければその場で統合して返す。
//    （将来的に別バッチで保存運用しても、オンデマンドでもOK）
//
// 起動:
//   node scripts/api/server.js
//   PORT=8787 node scripts/api/server.js
//
// 例:
//   curl 'http://localhost:8787/api/merged?date=20250810&pid=04&race=1'
//   curl 'http://localhost:8787/api/merged?date=today&pid=04&race=1&cache=1'

import http from "node:http";
import url from "node:url";
import path from "node:path";
import fs from "node:fs/promises";
import fssync from "node:fs";
import { fileURLToPath } from "node:url";
import { buildRacerStatsForCourse, normalizeCourseNumber } from "../lib/extract-stats.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const ROOT = path.resolve(__dirname, "..", "..");
const PUB  = path.join(ROOT, "public");

// 既定の格納ルート（環境変数で差し替え可）
const PROG_ROOT   = process.env.PROG_ROOT   || path.join(PUB, "programs", "v2");
const PROG_SLIM   = process.env.PROG_SLIM   || path.join(PUB, "programs-slim", "v2");
const EXHIB_ROOT  = process.env.EXHIB_ROOT  || path.join(PUB, "exhibition", "v1");
const STATS_ROOT  = process.env.STATS_ROOT  || path.join(PUB, "stats", "v2", "racers");
const MERGED_ROOT = process.env.MERGED_ROOT || path.join(PUB, "merged", "v1");

const PORT = Number(process.env.PORT || 8787);

// ---------- small utils ----------
const exists = (p) => { try { fssync.accessSync(p); return true; } catch { return false; } };
const readJSON = async (p) => JSON.parse(await fs.readFile(p, "utf8"));
const to2 = (s) => String(s).padStart(2, "0");
const ok = (res, obj) => {
  const body = JSON.stringify(obj, null, 2);
  res.writeHead(200, {
    "content-type": "application/json; charset=utf-8",
    "access-control-allow-origin": "*",
  });
  res.end(body);
};
const bad = (res, code, msg, extra={}) => {
  const body = JSON.stringify({ status: code, error: msg, ...extra }, null, 2);
  res.writeHead(code, {
    "content-type": "application/json; charset=utf-8",
    "access-control-allow-origin": "*",
  });
  res.end(body);
};

// ---------- source loaders ----------
function* candidateProgramFiles(date, pid, race) {
  yield path.join(PROG_ROOT, date, pid, `${race}.json`);
  yield path.join(PROG_SLIM, date, pid, `${race}.json`);
  yield path.join(PROG_ROOT, "today", pid, `${race}.json`);
  yield path.join(PROG_SLIM, "today", pid, `${race}.json`);
  yield path.join(PROG_ROOT, pid, `${race}.json`);
  yield path.join(PROG_SLIM, pid, `${race}.json`);
}
async function loadProgram(date, pid, race) {
  for (const f of candidateProgramFiles(date, pid, race)) {
    if (exists(f)) return await readJSON(f);
  }
  throw new Error(`program not found for ${date}/${pid}/${race}`);
}
async function loadExhibition(date, pid, race) {
  const file = path.join(EXHIB_ROOT, date, pid, `${race}.json`);
  if (!exists(file)) throw new Error(`exhibition not found: ${file}`);
  return await readJSON(file);
}
async function loadStats(regno) {
  const file = path.join(STATS_ROOT, `${regno}.json`);
  if (!exists(file)) return null;
  try { return await readJSON(file); } catch { return null; }
}

// ---------- pickers / builders ----------
function pickLaneAndRegno(entry) {
  const lane  = entry?.lane ?? entry?.boat_number ?? entry?.racer_boat_number ?? entry?.boat ?? null;
  const regno = entry?.number ?? entry?.racer_number ?? entry?.racer?.number ?? entry?.id ?? null;
  return { lane: lane != null ? Number(lane) : null, regno: regno != null ? Number(regno) : null };
}
function pickStartCourseFromExEntry(exEntry) {
  const sc = exEntry?.exhibition?.startCourse ?? exEntry?.startCourse ?? null;
  return normalizeCourseNumber(sc);
}
function buildStatsSlice(stats, startCourse) {
  if (!stats || !startCourse) return null;
  return buildRacerStatsForCourse(stats, startCourse);
}
function mergeOne(programEntry, exhibitionEntry, statsSlice) {
  const { lane, regno } = pickLaneAndRegno(programEntry);
  const ex = exhibitionEntry?.exhibition ?? null;
  return {
    lane,
    regno,
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
    stats: statsSlice ?? null,
  };
}
async function mergeRace(date, pid, race) {
  const program   = await loadProgram(date, pid, race);
  const exhibition= await loadExhibition(date, pid, race);

  const progEntries = program?.entries || program?.boats || [];
  const exEntries   = exhibition?.entries || [];

  const exByRegno = new Map();
  for (const e of exEntries) {
    const regno = Number(e?.number ?? e?.racer_number ?? e?.id ?? null);
    const sc = pickStartCourseFromExEntry(e);
    if (regno && sc) exByRegno.set(regno, { entry: e, startCourse: sc });
  }

  const mergedEntries = [];
  for (const pe of progEntries) {
    const { regno } = pickLaneAndRegno(pe);
    const hit = regno ? exByRegno.get(regno) : null;
    const startCourse = hit?.startCourse ?? null;
    const fullStats = regno ? await loadStats(regno) : null;
    const statsSlice = startCourse ? buildStatsSlice(fullStats, startCourse) : null;
    mergedEntries.push(mergeOne(pe, hit?.entry ?? null, statsSlice));
  }

  return {
    schemaVersion: "1.0",
    date,
    stadium: to2(program?.pid ?? program?.stadium ?? pid),
    race,
    generatedAt: new Date().toISOString(),
    sources: {
      program: { where: "programs v2 / programs-slim v2" },
      exhibition: { where: path.relative(ROOT, EXHIB_ROOT) },
      stats: { where: path.relative(ROOT, STATS_ROOT) },
    },
    entries: mergedEntries,
    raw: {
      program: {
        pid: program?.pid ?? null,
        stadiumName: program?.stadiumName ?? null,
        raceName: program?.raceName ?? null,
      },
      exhibitionMeta: {
        sourceShape: exhibition?.sourceShape ?? null,
      },
    },
  };
}

// ---------- server ----------
const server = http.createServer(async (req, res) => {
  // CORS preflight
  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET,OPTIONS",
      "access-control-allow-headers": "content-type",
      "access-control-max-age": "600",
    });
    return res.end();
  }

  const u = new url.URL(req.url, `http://${req.headers.host}`);
  if (u.pathname !== "/api/merged") {
    return bad(res, 404, "not found");
  }

  try {
    const dateIn = (u.searchParams.get("date") || "today").replace(/-/g, "");
    const pid    = (u.searchParams.get("pid")  || "02").padStart(2, "0");
    const rno    = String(u.searchParams.get("race") || "1").replace(/[^\d]/g, "");
    if (!rno) return bad(res, 400, "invalid race");
    const race   = `${rno}R`;
    const useCache = u.searchParams.get("cache") === "1";

    // cache優先
    const cacheFile = path.join(MERGED_ROOT, dateIn, pid, `${race}.json`);
    if (useCache && exists(cacheFile)) {
      const json = await readJSON(cacheFile);
      return ok(res, { ...json, _cache: true });
    }

    const payload = await mergeRace(dateIn, pid, race);

    // 即時返却（保存はここではしない / 必要なら ?save=1 で保存）
    if (u.searchParams.get("save") === "1") {
      const dir = path.dirname(cacheFile);
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(cacheFile, JSON.stringify(payload, null, 2), "utf8");
    }

    return ok(res, payload);
  } catch (e) {
    return bad(res, 500, "merge failed", { detail: e?.message || String(e) });
  }
});

server.listen(PORT, () => {
  console.log(`▶ merged API listening on http://localhost:${PORT}/api/merged`);
});
