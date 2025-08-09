// Node v20 / ESM
// 実行例:
//   node scripts/fetch-exhibition.js
//   TARGET_DATE=20250809 TARGET_PIDS="02,09" node scripts/fetch-exhibition.js --skip-existing
//
// 入力(ENV)
//   TARGET_DATE : YYYYMMDD or "today"（既定: today）
//   TARGET_PIDS : CSV（例 "02,09"。既定: "02"）
//   TARGET_RACES: CSV（例 "1R,2R" or "1,2"。空=1..12）
//
// オプション
//   --skip-existing : 既に同じ出力がある場合は再取得しない
//
// 出力
//   public/exhibition/v1/<date>/<pid>/<nR>.json
//
// 取得元（暫定）
//   https://boatraceopenapi.github.io/previews/v2/<date>/<pid>/<nR>.json
//   ※本番の展示クロール実装に置き換えるまでは previews をソースにする

import fs from "node:fs/promises";
import path from "node:path";

const ROOT = process.cwd();
const OUT_ROOT = path.join(ROOT, "public", "exhibition", "v1");

const isSkipExisting = process.argv.includes("--skip-existing");

// ---------- helpers ----------
const to2 = (s) => String(s).padStart(2, "0");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function yyyymmddTodayJST() {
  const now = new Date();
  // JSTに合わせる（UTC→JST +9h）
  const jst = new Date(now.getTime() + 9 * 60 * 60 * 1000);
  const y = jst.getUTCFullYear();
  const m = to2(jst.getUTCMonth() + 1);
  const d = to2(jst.getUTCDate());
  return `${y}${m}${d}`;
}

function normRaceKey(s) {
  if (s == null || s === "") return null;
  const n = String(s).replace(/[^\d]/g, "");
  if (!n) return null;
  return `${Number(n)}R`;
}

async function ensureDir(p) {
  await fs.mkdir(p, { recursive: true });
}

async function fileExists(p) {
  try { await fs.access(p); return true; } catch { return false; }
}

async function fetchJsonWithRetry(url, { retries = 3, waitMs = 800 } = {}) {
  let lastErr;
  for (let i = 0; i <= retries; i++) {
    try {
      const res = await fetch(url, {
        headers: {
          "user-agent": "Mozilla/5.0 (compatible; racecard-exhibition/1.0)",
          accept: "application/json,text/plain,*/*",
        },
        cache: "no-store",
      });
      if (res.ok) return await res.json();
      lastErr = new Error(`HTTP ${res.status}`);
    } catch (e) {
      lastErr = e;
    }
    if (i < retries) await sleep(waitMs);
  }
  throw lastErr ?? new Error("fetch failed");
}

// ---------- inputs ----------
const DATE_IN = (process.env.TARGET_DATE || "today").trim();
const DATE = /^\d{8}$/.test(DATE_IN) ? DATE_IN : yyyymmddTodayJST();

const PIDS = (process.env.TARGET_PIDS || "02")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean)
  .map((p) => to2(p));

let RACES = (process.env.TARGET_RACES || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean)
  .map(normRaceKey)
  .filter(Boolean);

if (RACES.length === 0) {
  RACES = Array.from({ length: 12 }, (_, i) => `${i + 1}R`);
}

console.log(`exhibition build: date=${DATE} pids=[${PIDS.join(",")}] races=[${RACES.join(",")}] skipExisting=${isSkipExisting}`);

// ---------- main ----------
for (const pid of PIDS) {
  const outDir = path.join(OUT_ROOT, DATE, pid);
  await ensureDir(outDir);

  for (const race of RACES) {
    const outPath = path.join(outDir, `${race}.json`);

    if (isSkipExisting && (await fileExists(outPath))) {
      console.log(`⏭️  skip existing ${path.relative(ROOT, outPath)}`);
      continue;
    }

    const srcUrl = `https://boatraceopenapi.github.io/previews/v2/${DATE}/${pid}/${race}.json`;

    try {
      const json = await fetchJsonWithRetry(srcUrl, { retries: 2, waitMs: 600 });
      await fs.writeFile(outPath, JSON.stringify(json, null, 2), "utf8");
      console.log(`✅ wrote ${path.relative(ROOT, outPath)}`);
    } catch (e) {
      // 404などは普通に起きる（未公開タイミング）。スキップとして扱う
      await fs.writeFile(
        outPath,
        JSON.stringify({ status: "unavailable", source: srcUrl, error: String(e) }, null, 2),
        "utf8"
      );
      console.log(`⚠️ wrote placeholder (unavailable): ${path.relative(ROOT, outPath)}`);
    }

    // 負荷抑制
    await sleep(300);
  }
}

console.log("done exhibitions.");
