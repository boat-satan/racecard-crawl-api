// scripts/fetch-exhibition.js
// Node v20 / ESM
import fs from "node:fs/promises";
import path from "node:path";

const ROOT = process.cwd();
const OUT_BASE = path.join(ROOT, "public", "exhibition", "v1");

// 手動実行用 env:
//   TARGET_DATE=20250809 TARGET_PID=02 TARGET_RACE=1R node scripts/fetch-exhibition.js
// 省略時は "today" ではなく YYYYMMDD 必須にしておく（運用の明確化）
const DATE  = (process.env.TARGET_DATE || "").replace(/-/g, "");
const PID   = String(process.env.TARGET_PID || "").padStart(2, "0");
const RACEQ = (process.env.TARGET_RACE || "").toUpperCase();
const RACE  = /R$/.test(RACEQ) ? RACEQ : `${RACEQ}R`;

// プレビューソースを試すかどうか（当面の暫定）：1 で使用
const USE_PREVIEWS = process.env.USE_PREVIEWS === "1";

if (!/^\d{8}$/.test(DATE) || !/^\d{2}$/.test(PID) || !/^\d{1,2}R$/.test(RACE)) {
  console.error("Usage: TARGET_DATE=YYYYMMDD TARGET_PID=NN TARGET_RACE=NR [USE_PREVIEWS=1] node scripts/fetch-exhibition.js");
  process.exit(1);
}

async function ensureDir(p) {
  await fs.mkdir(p, { recursive: true });
}

async function fetchText(url) {
  const res = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118 Safari/537.36",
      "accept": "application/json,text/plain,*/*"
    }
  });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.text();
}

// 暫定: previews を試す（形は保存しつつ、必要なら将来ここでパースして entries に落とす）
async function tryFetchFromPreviews(date, pid, race) {
  const url = `https://boatraceopenapi.github.io/previews/v2/${date}/${pid}/${race}.json`;
  try {
    const txt = await fetchText(url);
    const raw = JSON.parse(txt);
    return { ok: true, url, raw };
  } catch (e) {
    return { ok: false, url, error: String(e) };
  }
}

function blankPayload() {
  return {
    schemaVersion: "1.0",
    date: DATE,
    stadium: PID,
    race: RACE,
    deadline: null,
    fetchedAt: new Date().toISOString(),
    entries: [],
    source: { type: "manual", url: null }
  };
}

async function main() {
  const outDir = path.join(OUT_BASE, DATE, PID);
  await ensureDir(outDir);
  const outPath = path.join(outDir, `${RACE}.json`);

  let payload = blankPayload();

  if (USE_PREVIEWS) {
    const r = await tryFetchFromPreviews(DATE, PID, RACE);
    if (r.ok) {
      // そのまま raw も添付（将来はここで entries へ正規化）
      payload.source = { type: "preview", url: r.url };
      payload.previewRaw = r.raw; // ★当面は丸ごと保存（後で削る/正規化する）
    }
  }

  await fs.writeFile(outPath, JSON.stringify(payload, null, 2), "utf8");
  console.log(`✅ wrote ${path.relative(ROOT, outPath)}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
