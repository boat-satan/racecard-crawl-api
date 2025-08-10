// server.js
// Node v20 / ESM
// 統合API: 出走表 + 展示データ + スタッツ を統合して返す
//  - GET /api/integrate/v1/:date/:pid/:race  例: /api/integrate/v1/20250810/04/1R
//  - 静的ファイルは /public を配信

import express from "express";
import path from "node:path";
import fs from "node:fs/promises";
import fssync from "node:fs";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const app = express();
const PORT = Number(process.env.PORT || 3000);
const PUBLIC_DIR = path.join(__dirname, "public");

// ---- helpers -------------------------------------------------
const to2 = (s) => String(s).padStart(2, "0");
const normRace = (r) =>
  (String(r).toUpperCase().endsWith("R") ? String(r).toUpperCase() : `${r}R`);

async function readJsonIfExists(p) {
  try { return JSON.parse(await fs.readFile(p, "utf8")); } catch { return null; }
}

function pickProgramEntryShape(p) {
  const list = (p && (p.entries || p.boats)) || [];
  return list.map((e) => ({
    lane: Number(e.lane ?? e.course ?? e.c ?? e.slip ?? null),
    number: Number(e.number ?? e.racer_number ?? e.id ?? null),
    name: e.name ?? e.racer_name ?? null,
    raw: e,
  }));
}

function pickExhibitionEntryShape(x) {
  const list = (x && x.entries) || [];
  return list.map((e) => ({
    lane: Number(e.lane ?? null),
    number: Number(e.number ?? null),
    exRank: e.exhibition?.rank ?? e.exRank ?? null,
    exTime: e.exhibition?.time ?? e.exTime ?? e.tenjiTime ?? null,
    startCourse: e.exhibition?.startCourse ?? e.startCourse ?? null,
    startTiming: e.exhibition?.startTiming ?? e.startTiming ?? e.st ?? null,
    raw: e,
  }));
}

function sliceStatsForCourse(stats, courseN) {
  if (!stats || !courseN) return null;
  const arr = Array.isArray(stats.entryCourse) ? stats.entryCourse : [stats.entryCourse].filter(Boolean);
  const ec = arr.find((c) => Number(c.course) === Number(courseN));
  if (!ec) return null;
  return {
    course: Number(courseN),
    selfSummary: ec.selfSummary ?? null,
    winKimariteSelf: ec.winKimariteSelf ?? null,
    loseKimarite: ec.loseKimarite ?? null,
    matrixSelf: ec.matrix?.self ?? null,
  };
}

function pickExRankStats(stats, exRank) {
  if (!stats || !exRank) return null;
  const item = (stats.exTimeRank || []).find((r) => Number(r.rank) === Number(exRank));
  return item
    ? { rank: Number(item.rank), winRate: item.winRate ?? null, top2Rate: item.top2Rate ?? null, top3Rate: item.top3Rate ?? null }
    : null;
}

function findFirstExisting(paths) {
  for (const p of paths) if (fssync.existsSync(p)) return p;
  return null;
}

// ---- ready ---------------------------------------------------
app.get("/ready", (_req, res) => res.json({ ok: true }));

// ---- integrate API ------------------------------------------
app.get("/api/integrate/v1/:date/:pid/:race", async (req, res) => {
  try {
    const date = String(req.params.date).replace(/-/g, "");
    const pid  = to2(req.params.pid);
    const race = normRace(req.params.race);
    const wantDebug = String(req.query.debug || "") === "1";

    // programs 探索候補
    const programCandidates = [
      path.join(PUBLIC_DIR, "programs", "v2", date,  pid, `${race}.json`),
      path.join(PUBLIC_DIR, "programs-slim", "v2", date,  pid, `${race}.json`),
      path.join(PUBLIC_DIR, "programs", "v2", "today", pid, `${race}.json`),
      path.join(PUBLIC_DIR, "programs-slim", "v2", "today", pid, `${race}.json`),
      path.join(PUBLIC_DIR, "programs", "v2", pid, `${race}.json`),
      path.join(PUBLIC_DIR, "programs-slim", "v2", pid, `${race}.json`),
    ];
    const programFile = findFirstExisting(programCandidates);
    if (!programFile) {
      return res.status(404).json({ error: "program not found", tried: programCandidates.map(p=>path.relative(PUBLIC_DIR,p)) });
    }
    const program = await readJsonIfExists(programFile);

    // exhibition 探索候補
    const exhibitionCandidates = [
      path.join(PUBLIC_DIR, "exhibition", "v1", date,  pid, `${race}.json`),
      path.join(PUBLIC_DIR, "exhibition", "v1", "today", pid, `${race}.json`),
      path.join(PUBLIC_DIR, "exhibition", "v1", pid, `${race}.json`),
    ];
    const exhibitionFile = findFirstExisting(exhibitionCandidates);
    if (!exhibitionFile) {
      return res.status(404).json({ error: "exhibition not found", tried: exhibitionCandidates.map(p=>path.relative(PUBLIC_DIR,p)) });
    }
    const exhibition = await readJsonIfExists(exhibitionFile);

    const progEntries = pickProgramEntryShape(program);
    const exEntries   = pickExhibitionEntryShape(exhibition);

    const mergedEntries = [];
    for (const pe of progEntries) {
      const ex = exEntries.find((e) => e.number === pe.number) || null;
      const statsFile = path.join(PUBLIC_DIR, "stats", "v2", "racers", `${pe.number}.json`);
      const stats = fssync.existsSync(statsFile) ? await readJsonIfExists(statsFile) : null;

      const courseN = ex?.startCourse ?? null;
      const statsForCourse = courseN ? sliceStatsForCourse(stats, courseN) : null;
      const exRankStats = ex?.exRank ? pickExRankStats(stats, ex.exRank) : null;

      mergedEntries.push({
        lane: pe.lane ?? ex?.lane ?? null,
        number: pe.number,
        name: pe.name ?? null,
        exhibition: ex ? {
          rank: ex.exRank ?? null,
          time: ex.exTime ?? null,
          startCourse: ex.startCourse ?? null,
          startTiming: ex.startTiming ?? null,
        } : null,
        stats: { forCourse: statsForCourse, exRankStats },
      });
    }

    const payload = {
      schemaVersion: "1.0",
      generatedAt: new Date().toISOString(),
      params: { date, pid, race },
      sources: {
        program: path.relative(PUBLIC_DIR, programFile),
        exhibition: path.relative(PUBLIC_DIR, exhibitionFile),
        statsDir: "stats/v2/racers/",
      },
      program: { stadium: program?.stadiumName ?? program?.stadium ?? null, raw: wantDebug ? program : undefined },
      exhibition: { raw: wantDebug ? exhibition : undefined },
      entries: mergedEntries,
      debug: wantDebug ? {
        programTried: programCandidates.map(p=>path.relative(PUBLIC_DIR,p)),
        exhibitionTried: exhibitionCandidates.map(p=>path.relative(PUBLIC_DIR,p)),
      } : undefined,
    };

    res.set("cache-control", "public, max-age=30");
    res.json(payload);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e?.message || e) });
  }
});

// ---- static --------------------------------------------------
app.use(express.static(PUBLIC_DIR, { extensions: ["html"] }));

app.listen(PORT, () => {
  console.log(`server listening on http://localhost:${PORT}`);
});
