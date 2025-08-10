// server.js
// Node v20 / ESM
// 統合API: 出走表 + 展示 + スタッツ を統合して返す & 保存する
//  - GET /api/integrate/v1/:date/:pid/:race
//    例: /api/integrate/v1/20250810/04/1R, /api/integrate/v1/today/04/1R
//  - /ready でヘルスチェック
//  - /public 配下は静的配信

import express from "express";
import path from "node:path";
import fs from "node:fs/promises";
import fssync from "node:fs";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const app  = express();
const PORT = Number(process.env.PORT || 3000);

const PUBLIC_DIR = path.join(__dirname, "public");

// ---------- 小物 ----------
const to2 = (s) => String(s).padStart(2, "0");
const normRace = (r) => {
  const base = String(r).trim().toUpperCase();
  return base.endsWith("R") ? base : `${base}R`;
};
const isNum = (v) => typeof v === "number" && !Number.isNaN(v);

function ymdInJST(d = new Date()) {
  const jst = new Date(d.getTime() + 9 * 60 * 60 * 1000);
  const y = jst.getUTCFullYear();
  const m = String(jst.getUTCMonth() + 1).padStart(2, "0");
  const day = String(jst.getUTCDate()).padStart(2, "0");
  return `${y}${m}${day}`;
}

async function readJsonIfExists(p) {
  try {
    const t = await fs.readFile(p, "utf8");
    return JSON.parse(t);
  } catch {
    return null;
  }
}
async function writeJsonPretty(p, data) {
  await fs.mkdir(path.dirname(p), { recursive: true });
  await fs.writeFile(p, JSON.stringify(data, null, 2), "utf8");
}

function pickProgramEntryShape(program) {
  const list = (program?.entries || program?.boats || []) ?? [];
  return list.map((e) => ({
    lane: Number(e.lane ?? e.course ?? e.c ?? null) || null,
    number: Number(e.number ?? e.racer_number ?? e.id ?? null) || null,
    name: e.name ?? e.racer_name ?? null,
    raw: e,
  }));
}

function pickExhibitionEntryShape(ex) {
  const list = (ex?.entries || []) ?? [];
  return list.map((e) => {
    // previews系 or beforeinfo系 どちらもできるだけ吸収
    const exObj = e.exhibition ?? {};
    const rank = exObj.rank ?? e.exRank ?? null;
    const time = exObj.time ?? e.exTime ?? e.tenjiTime ?? null;
    const sC   = exObj.startCourse ?? e.startCourse ?? null;
    const sT   = exObj.startTiming ?? e.startTiming ?? e.st ?? null;
    return {
      lane: e.lane != null ? Number(e.lane) : null,
      number: e.number != null ? Number(e.number) : null,
      exRank: rank != null ? Number(rank) : null,
      exTime: time != null ? Number(time) : null,
      startCourse: sC != null ? Number(sC) : null,
      startTiming: sT ?? null,
      raw: e,
    };
  });
}

function sliceStatsForCourse(stats, courseN) {
  if (!stats || !courseN) return null;
  const n = Number(courseN);

  // 形揺れ対応: entryCourse が配列 or オブジェクト or すでに単体
  let ec = null;
  const ecVal = stats.entryCourse;

  if (!ecVal) return null;
  if (Array.isArray(ecVal)) {
    ec = ecVal.find((c) => Number(c.course) === n) || null;
  } else if (typeof ecVal === "object") {
    if (isNum(ecVal.course)) {
      ec = Number(ecVal.course) === n ? ecVal : null;
    } else if (ecVal[n]) {
      ec = ecVal[n];
      ec.course = n;
    }
  }

  if (!ec) return null;

  return {
    course: n,
    selfSummary: ec.selfSummary ?? null,
    winKimariteSelf: ec.winKimariteSelf ?? null,
    loseKimarite: ec.loseKimarite ?? null,
    matrixSelf: ec.matrix?.self ?? null,
    avgST: ec.avgST ?? null,
  };
}

function pickExRankStats(stats, exRank) {
  if (!stats || !exRank) return null;
  const arr = stats.exTimeRank || [];
  const item = arr.find((r) => Number(r.rank) === Number(exRank));
  if (!item) return null;
  return {
    rank: Number(item.rank),
    winRate: item.winRate ?? null,
    top2Rate: item.top2Rate ?? null,
    top3Rate: item.top3Rate ?? null,
  };
}

// ---------- ルート ----------
app.get("/ready", (_req, res) => res.json({ ok: true, time: new Date().toISOString() }));
app.get("/", (_req, res) => res.type("text").send("racecard integrate API"));

// ---------- 統合API ----------
app.get("/api/integrate/v1/:date/:pid/:race", async (req, res) => {
  try {
    const dateIn = String(req.params.date || "").trim();
    const date = dateIn.toLowerCase() === "today" ? ymdInJST() : dateIn.replace(/-/g, "");
    const pid  = to2(req.params.pid);
    const race = normRace(req.params.race);

    // 出走表: programs優先、なければ programs-slim、todayフォールバックも試す
    const programCandidates = [
      path.join(PUBLIC_DIR, "programs", "v2", date, pid, `${race}.json`),
      path.join(PUBLIC_DIR, "programs-slim", "v2", date, pid, `${race}.json`),
      path.join(PUBLIC_DIR, "programs", "v2", "today", pid, `${race}.json`),
      path.join(PUBLIC_DIR, "programs-slim", "v2", "today", pid, `${race}.json`),
    ];
    let program = null, programFile = null;
    for (const p of programCandidates) {
      if (fssync.existsSync(p)) { program = await readJsonIfExists(p); programFile = p; break; }
    }
    if (!program) return res.status(404).json({ error: "program not found", tried: programCandidates.map(p=>path.relative(PUBLIC_DIR,p)) });

    // 展示
    const exhibitionFile = path.join(PUBLIC_DIR, "exhibition", "v1", date, pid, `${race}.json`);
    const exhibition = await readJsonIfExists(exhibitionFile);
    if (!exhibition) {
      return res.status(404).json({ error: "exhibition not found", tried: [path.relative(PUBLIC_DIR, exhibitionFile)] });
    }

    const progEntries = pickProgramEntryShape(program);
    const exEntries   = pickExhibitionEntryShape(exhibition);

    // マージ + スタッツ
    const mergedEntries = [];
    for (const pe of progEntries) {
      const ex = exEntries.find((e) => e.number === pe.number) || null;
      const statsPath = path.join(PUBLIC_DIR, "stats", "v2", "racers", `${pe.number}.json`);
      const stats = fssync.existsSync(statsPath) ? await readJsonIfExists(statsPath) : null;

      const courseN = ex?.startCourse ?? null;
      const statsForCourse = courseN ? sliceStatsForCourse(stats, courseN) : null;
      const exRankStats    = ex?.exRank ? pickExRankStats(stats, ex.exRank) : null;

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
        stats: {
          forCourse: statsForCourse,
          exRankStats,
        },
      });
    }

    const payload = {
      schemaVersion: "1.0",
      generatedAt: new Date().toISOString(),
      params: { date, pid, race },
      sources: {
        program: programFile ? path.relative(PUBLIC_DIR, programFile) : null,
        exhibition: path.relative(PUBLIC_DIR, exhibitionFile),
        statsDir: "stats/v2/racers/",
      },
      program: { stadium: program?.stadiumName ?? program?.stadium ?? null, raw: program },
      exhibition: { raw: exhibition },
      entries: mergedEntries,
    };

    // 保存先: public/integrated/v1/<date>/<pid>/<race>.json
    const outFile = path.join(PUBLIC_DIR, "integrated", "v1", date, pid, `${race}.json`);
    await writeJsonPretty(outFile, payload);

    res.set("cache-control", "public, max-age=30");
    res.json({ ...payload, savedAs: path.relative(PUBLIC_DIR, outFile) });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e?.message || e) });
  }
});

// ---------- 静的配信 ----------
app.use(express.static(PUBLIC_DIR, { extensions: ["html"] }));

app.listen(PORT, () => {
  console.log(`server listening on http://localhost:${PORT}`);
});
