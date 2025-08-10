// server.js
// Node v20 / ESM
// 統合API: 出走表 + 展示データ + スタッツ を統合して返す
//  - GET /api/integrate/v1/:date/:pid/:race
//    例: /api/integrate/v1/20250810/04/1R
//  - 既存の静的ファイルも /public 配下を配信
//  - ★ 追加: /ready と /api/integrate/v1/ping のヘルスチェック

import express from "express";
import path from "node:path";
import fs from "node:fs/promises";
import fssync from "node:fs";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const app = express();
const PORT = Number(process.env.PORT || 3000);

// ルート
const PUBLIC_DIR = path.join(__dirname, "public");

// -------- ヘルパ --------
const to2 = (s) => String(s).padStart(2, "0");
const normRace = (r) =>
  (String(r).toUpperCase().endsWith("R") ? String(r).toUpperCase() : `${r}R`);

async function readJsonIfExists(p) {
  try {
    const t = await fs.readFile(p, "utf8");
    return JSON.parse(t);
  } catch {
    return null;
  }
}

function pickProgramEntryShape(p) {
  // programs の形揺れ対応
  if (!p) return [];
  const list = p.entries || p.boats || [];
  return list.map((e) => ({
    lane: Number(e.lane ?? e.course ?? e.c ?? e.slip ?? null),
    number: Number(e.number ?? e.racer_number ?? e.id ?? null),
    name: e.name ?? e.racer_name ?? null,
    raw: e,
  }));
}

function pickExhibitionEntryShape(x) {
  if (!x) return [];
  const list = x.entries || [];
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
  const ec = Array.isArray(stats.entryCourse)
    ? stats.entryCourse.find((c) => c.course === Number(courseN))
    : null;
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
  const item = Array.isArray(stats.exTimeRank)
    ? stats.exTimeRank.find((r) => Number(r.rank) === Number(exRank))
    : null;
  if (!item) return null;
  return {
    rank: Number(item.rank),
    winRate: item.winRate ?? null,
    top2Rate: item.top2Rate ?? null,
    top3Rate: item.top3Rate ?? null,
  };
}

// -------- ヘルスチェック（★追加） --------
app.get("/", (_req, res) => res.status(200).send("OK"));
app.get("/ready", (_req, res) => res.status(200).send("OK"));
app.get("/api/integrate/v1/ping", (_req, res) =>
  res.json({ ok: true, ts: new Date().toISOString() })
);

// -------- 統合API --------
app.get("/api/integrate/v1/:date/:pid/:race", async (req, res) => {
  try {
    const date = String(req.params.date).replace(/-/g, "");
    const pid  = to2(req.params.pid);
    const race = normRace(req.params.race);

    // 1) 出走表（programs / programs-slim のどちらかに存在する方を採用）
    const programPaths = [
      path.join(PUBLIC_DIR, "programs", "v2", date, pid, `${race}.json`),
      path.join(PUBLIC_DIR, "programs-slim", "v2", date, pid, `${race}.json`),
      // “today” や直下構成にフォールバック
      path.join(PUBLIC_DIR, "programs", "v2", "today", pid, `${race}.json`),
      path.join(PUBLIC_DIR, "programs-slim", "v2", "today", pid, `${race}.json`),
      path.join(PUBLIC_DIR, "programs", "v2", pid, `${race}.json`),
      path.join(PUBLIC_DIR, "programs-slim", "v2", pid, `${race}.json`),
    ];
    let program = null, programFile = null;
    for (const p of programPaths) {
      if (fssync.existsSync(p)) {
        program = await readJsonIfExists(p);
        programFile = p;
        break;
      }
    }
    if (!program) {
      return res.status(404).json({ error: "program not found" });
    }

    // 2) 展示データ（★ today/直下にもフォールバック追加）
    const exhibitionCandidates = [
      path.join(PUBLIC_DIR, "exhibition", "v1", date,  pid, `${race}.json`),
      path.join(PUBLIC_DIR, "exhibition", "v1", "today", pid, `${race}.json`),
      path.join(PUBLIC_DIR, "exhibition", "v1", pid, `${race}.json`),
    ];
    let exhibition = null, exhibitionFile = null;
    for (const p of exhibitionCandidates) {
      if (fssync.existsSync(p)) {
        exhibition = await readJsonIfExists(p);
        exhibitionFile = p;
        break;
      }
    }
    if (!exhibition) {
      return res.status(404).json({ error: "exhibition not found" });
    }

    const progEntries = pickProgramEntryShape(program);
    const exEntries   = pickExhibitionEntryShape(exhibition);

    // 3) エントリーごとにスタッツをスライス
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
        exhibition: ex
          ? {
              rank: ex.exRank ?? null,
              time: ex.exTime ?? null,
              startCourse: ex.startCourse ?? null,
              startTiming: ex.startTiming ?? null,
            }
          : null,
        stats: {
          forCourse: statsForCourse, // selfSummary / winKimariteSelf / loseKimarite / matrixSelf
          exRankStats,               // 展示順位別の成績（該当順位のみ）
        },
      });
    }

    const payload = {
      schemaVersion: "1.0",
      generatedAt: new Date().toISOString(),
      params: { date, pid, race },
      sources: {
        program: programFile ? path.relative(PUBLIC_DIR, programFile) : null,
        exhibition: exhibitionFile ? path.relative(PUBLIC_DIR, exhibitionFile) : null,
        statsDir: "stats/v2/racers/",
      },
      program: { stadium: program?.stadiumName ?? program?.stadium ?? null, raw: program },
      exhibition: { raw: exhibition },
      entries: mergedEntries,
    };

    res.set("cache-control", "public, max-age=30");
    res.json(payload);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e?.message || e) });
  }
});

// -------- 静的配信 --------
app.use(express.static(PUBLIC_DIR, { extensions: ["html"] }));

app.listen(PORT, () => {
  console.log(`server listening on http://localhost:${PORT}`);
});
