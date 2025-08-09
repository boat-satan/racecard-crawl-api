// scripts/merge-exhibition.js
// Node v20 / ESM
import fs from "node:fs/promises";
import path from "node:path";

const ROOT = process.cwd();
const PGM_BASE = path.join(ROOT, "public", "programs", "v2");
const EXH_BASE = path.join(ROOT, "public", "exhibition", "v1");
const OUT_BASE = path.join(ROOT, "public", "programs-merged", "v2");

const DATE  = (process.env.TARGET_DATE || "").replace(/-/g, "");
const PID   = String(process.env.TARGET_PID || "").padStart(2, "0");
const RACEQ = (process.env.TARGET_RACE || "").toUpperCase();
const RACE  = /R$/.test(RACEQ) ? RACEQ : `${RACEQ}R`;

if (!/^\d{8}$/.test(DATE) || !/^\d{2}$/.test(PID) || !/^\d{1,2}R$/.test(RACE)) {
  console.error("Usage: TARGET_DATE=YYYYMMDD TARGET_PID=NN TARGET_RACE=NR node scripts/merge-exhibition.js");
  process.exit(1);
}

async function readJson(p) {
  const txt = await fs.readFile(p, "utf8");
  return JSON.parse(txt);
}

async function ensureDir(p) {
  await fs.mkdir(p, { recursive: true });
}

async function main() {
  const pgmPath = path.join(PGM_BASE, DATE, PID, `${RACE}.json`);
  const exhPath = path.join(EXH_BASE, DATE, PID, `${RACE}.json`);

  const outDir  = path.join(OUT_BASE, DATE, PID);
  await ensureDir(outDir);
  const outPath = path.join(outDir, `${RACE}.json`);

  const pgm = await readJson(pgmPath).catch(() => null);
  const exh = await readJson(exhPath).catch(() => null);

  if (!pgm) {
    console.error(`program not found: ${pgmPath}`);
    process.exit(1);
  }

  const merged = { ...pgm, exhibition: exh || null };
  await fs.writeFile(outPath, JSON.stringify(merged, null, 2), "utf8");
  console.log(`✅ wrote ${path.relative(ROOT, outPath)}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
