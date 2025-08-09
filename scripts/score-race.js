// scripts/score-race.js
// 今日の出走表(JSON) × 選手プロファイル(stats) を合成して
// public/picks/v1/<date>/<stadium>-<rno>.json を出力。
// 依存: Node v20, 既存の fetch-stats.js が生成する
//       public/stats/v1/racers/<regno>.json

import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PUBLIC_DIR = path.resolve(__dirname, "..", "public");
const TODAY_ROOTS = [
  path.join(PUBLIC_DIR, "programs", "v2", "today"),
  path.join(PUBLIC_DIR, "programs-slim", "v2", "today"),
];

const STATS_DIR = path.join(PUBLIC_DIR, "stats", "v1", "racers");
const OUT_ROOT  = path.join(PUBLIC_DIR, "picks", "v1");

// ---------- 小物 ----------
const ensureDir = (p) => fs.mkdir(p, { recursive: true });
const safeNum = (v, d = null) => (Number.isFinite(v) ? v : d);

async function readJson(p) {
  const s = await fs.readFile(p, "utf8");
  return JSON.parse(s);
}
async function readJsonIfExists(p) {
  try { return await readJson(p); } catch { return null; }
}

function pickCourseStats(stats, course) {
  const arr = stats?.courseStats || [];
  return arr.find((x) => x.course === course) || null;
}

function calcAggressiveRate(stats, course) {
  // その選手が「該当コースで“攻めて勝つ”比率」をざっくり推定
  // まくり + まくり差し + 差し を合計 / 1着数（無い場合は総出走で近似）
  const item = (stats?.courseKimarite || []).find((x) => x.course === course);
  if (!item) return null;

  const det = item.detail || {};
  const m = Number(det["まくり"]?.count ?? 0);
  const ms = Number(det["まくり差し"]?.count ?? 0);
  const s = Number(det["差し"]?.count ?? 0);

  // 1着数は headers 3列目想定だったが raw から拾えない場合もあるので合算近似
  const totalFirst =
    Number(det["逃げ"]?.count ?? 0) +
    Number(det["差し"]?.count ?? 0) +
    Number(det["まくり"]?.count ?? 0) +
    Number(det["まくり差し"]?.count ?? 0) +
    Number(det["抜き"]?.count ?? 0) +
    Number(det["恵まれ"]?.count ?? 0);

  if (totalFirst > 0) {
    return safeNum(((m + ms + s) / totalFirst) * 100, null); // %
  }
  // 最低限の後方互換: コース別成績の1着率を攻撃代理として弱め重み
  const cs = pickCourseStats(stats, course);
  if (cs?.top1Rate != null) return Math.max(0, cs.top1Rate * 0.4); // ラフ近似
  return null;
}

function calcNetScore(selfTop1, avgOppAgg) {
  // シンプルなネット指標
  // 自艇の1着率 - 相手の平均攻撃率（％ベース）
  if (selfTop1 == null && avgOppAgg == null) return null;
  const s = (selfTop1 ?? 0) - (avgOppAgg ?? 0);
  return Math.round(s * 10) / 10;
}

function summarizePairs(boats) {
  // 主要ペア（内⇔外）を簡易に列挙
  // 例: (1,2), (1,3), (2,3), (3,4)…などから上位だけ
  const pairs = [];
  for (let i = 0; i < boats.length; i++) {
    for (let j = i + 1; j < boats.length; j++) {
      // ペアスコア: 攻め合い強度（i攻撃 + j攻撃）の平均
      const sc =
        ((boats[i].opponents.avgAggressive ?? 0) +
          (boats[j].opponents.avgAggressive ?? 0)) /
        2;
      pairs.push({
        a: boats[i].boat,
        b: boats[j].boat,
        score: Math.round(sc * 10) / 10,
      });
    }
  }
  // 上位5件
  return pairs.sort((a, b) => b.score - a.score).slice(0, 5);
}

// ---------- 主処理 ----------
async function loadTodayProgramFiles() {
  const files = [];
  for (const root of TODAY_ROOTS) {
    let dirents = [];
    try {
      dirents = await fs.readdir(root, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const d of dirents) {
      if (!d.isDirectory()) continue;
      const dayDir = path.join(root, d.name);
      const fns = await fs.readdir(dayDir).catch(() => []);
      for (const fn of fns) {
        if (fn.endsWith(".json")) files.push(path.join(dayDir, fn));
      }
    }
  }
  return files;
}

function normalizeProgram(json) {
  // いままでのスキーマ差にやさしく
  const boats = json?.boats || json?.program?.boats || [];
  return {
    date: json?.race_date || json?.program?.race_date,
    stadium: json?.race_stadium_number || json?.program?.race_stadium_number,
    rno: json?.race_number || json?.program?.race_number,
    title: json?.race_title || json?.program?.race_title || "",
    boats: boats.map((b) => ({
      boat: b.racer_boat_number ?? b.boat ?? null,        // 1..6
      regno: b.racer_number ?? b.racerNumber ?? b.racer?.number ?? null,
      name: b.racer_name ?? b.racer?.name ?? null,
    })),
  };
}

async function processProgramFile(fullpath) {
  const pj = await readJson(fullpath);
  const p = normalizeProgram(pj);
  if (!p.date || !p.stadium || !p.rno) return null;

  // 各艇のスコアを算出
  const boatsOut = [];
  for (const b of p.boats) {
    if (!b.regno || !b.boat) continue;
    const stats = await readJsonIfExists(path.join(STATS_DIR, `${b.regno}.json`));

    const cs = pickCourseStats(stats, b.boat);
    const selfTop1 = cs?.top1Rate ?? null;

    // 相手の平均“攻め率”
    const oppAggRates = [];
    for (const ob of p.boats) {
      if (ob === b || !ob.regno || !ob.boat) continue;
      const ostats = await readJsonIfExists(path.join(STATS_DIR, `${ob.regno}.json`));
      const ar = calcAggressiveRate(ostats, ob.boat);
      if (ar != null) oppAggRates.push(ar);
    }
    const avgOppAgg =
      oppAggRates.length ? oppAggRates.reduce((a, c) => a + c, 0) / oppAggRates.length : null;

    const net = calcNetScore(selfTop1, avgOppAgg);

    boatsOut.push({
      boat: b.boat,
      regno: b.regno,
      name: b.name,
      self: {
        courseTop1Rate: safeNum(selfTop1, null), // %
      },
      opponents: {
        avgAggressive: safeNum(avgOppAgg, null), // %
        samples: oppAggRates.map((v) => Math.round(v * 10) / 10),
      },
      score: {
        net, // 自艇1着率 − 相手平均攻撃率（％）
      },
    });
  }

  // 並べ替え：ネットスコア降順
  boatsOut.sort((a, b) => (b.score.net ?? -999) - (a.score.net ?? -999));

  // 主要ペア
  const pairs = summarizePairs(boatsOut.slice().sort((a, b) => a.boat - b.boat));

  const outObj = {
    race: {
      date: p.date,
      stadium: p.stadium,
      number: p.rno,
      title: p.title,
    },
    picks: boatsOut,
    pairs,
    meta: {
      generatedAt: new Date().toISOString(),
      hint: "score.net = 自艇1着率 − 相手平均攻撃率（％）。不足時はnull。",
    },
  };

  const outDir = path.join(OUT_ROOT, String(p.date));
  await ensureDir(outDir);
  const outPath = path.join(outDir, `${p.stadium}-${p.rno}.json`);
  await fs.writeFile(outPath, JSON.stringify(outObj, null, 2), "utf8");

  return { outPath };
}

async function main() {
  const files = await loadTodayProgramFiles();
  if (!files.length) {
    console.log("No today programs found.");
    return;
  }
  console.log(`Programs: ${files.length} file(s)`);

  let ok = 0;
  for (const f of files) {
    try {
      const res = await processProgramFile(f);
      if (res?.outPath) {
        console.log(`✅ wrote ${path.relative(PUBLIC_DIR, res.outPath)}`);
        ok++;
      }
    } catch (e) {
      console.warn(`❌ ${path.basename(f)}: ${e.message}`);
    }
  }
  console.log(`done. ${ok}/${files.length} races`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});