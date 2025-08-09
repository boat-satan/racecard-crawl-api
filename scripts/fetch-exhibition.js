// scripts/fetch-exhibition.js
// 目的: previews を叩いて展示データを保存
// 入力ENV:
//   TARGET_DATE=YYYYMMDD or "today"（必須）
//   TARGET_PID=場コード(01..24)（必須）
//   TARGET_RACE=1R..12R（必須）
//   USE_PREVIEWS=1 のとき previews 経由URLを使う（デフォルト 1）
//
// 保存先: public/exhibition/v1/<date>/<pid>/<race>.json
// デバッグ: public/debug/exh-<date>-<pid>-<race>.txt

import fs from "node:fs/promises";
import path from "node:path";

const to2 = (v) => String(v).padStart(2, "0");
const DATE_RAW = (process.env.TARGET_DATE || "").trim();
const PID_RAW  = (process.env.TARGET_PID  || "").trim();
const RACE_RAW = (process.env.TARGET_RACE || "").trim().toUpperCase();
const USE_PREVIEWS = String(process.env.USE_PREVIEWS ?? "1") === "1";

if (!DATE_RAW || !PID_RAW || !RACE_RAW) {
  console.error("ERROR: TARGET_DATE, TARGET_PID, TARGET_RACE は必須です");
  process.exit(1);
}

const DATE = DATE_RAW.replace(/-/g, "");   // 20250809
const PID  = /^\d+$/.test(PID_RAW) ? to2(PID_RAW) : PID_RAW;
const RACE = /R$/i.test(RACE_RAW) ? RACE_RAW : `${RACE_RAW}R`;

// boatraceopenapi (GitHub Pages) の previews
const BASE = USE_PREVIEWS
  ? "https://boatraceopenapi.github.io/previews/v2"
  : "https://boatraceopenapi.github.io/previews/v2"; // 将来的に切替したければここを分岐

const SRC = `${BASE}/${DATE}/${PID}/${RACE}.json`;

const OUT_DIR = path.join("public", "exhibition", "v1", DATE, PID);
const OUT_FILE = path.join(OUT_DIR, `${RACE}.json`);
const DBG_DIR = path.join("public", "debug");
const DBG_FILE = path.join(DBG_DIR, `exh-${DATE}-${PID}-${RACE}.txt`);

async function ensureDir(p) {
  await fs.mkdir(p, { recursive: true });
}

async function main() {
  console.log("fetch:", SRC);
  const res = await fetch(SRC);
  const status = res.status;
  const text = await res.text();

  await ensureDir(DBG_DIR);
  await fs.writeFile(DBG_FILE, text);

  if (status !== 200) {
    console.error(`ERROR: source fetch ${status}`);
    process.exit(1);
  }

  let json;
  try { json = JSON.parse(text); } catch {
    console.error("ERROR: invalid JSON");
    process.exit(1);
  }

  // 何が来ても保存（将来スキーマ変化に強く）
  const wrapped = {
    schemaVersion: "1.0",
    source: SRC,
    fetchedAt: new Date().toISOString(),
    date: DATE,
    pid: PID,
    race: RACE,
    data: json
  };

  await ensureDir(OUT_DIR);
  await fs.writeFile(OUT_FILE, JSON.stringify(wrapped, null, 2), "utf8");
  console.log("write:", OUT_FILE);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
