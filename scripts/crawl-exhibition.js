// scripts/crawl-exhibition.js
// Node v20 / ESM
// 目的: 展示(試走/周回/展示タイム/展示順位/スタ展 等)を取り込み -> 正規化して保存
//
// ★今は JSON ソース前提（previews 相当）。後で公式サイトスクレイプに差し替え可。
//   入口URLは EX_SRC_ROOT 環境変数で差し替え可能。
//   例) https://boatraceopenapi.github.io/previews/v2/<date>/<pid>/<race>.json
//
// 出力先:
//   public/exhibition/v1/<date>/<pid>/<race>.json
//
// 実行例:
//   node scripts/crawl-exhibition.js              # 今日/02/1R だけテスト
//   TARGET_DATE=20250809 TARGET_PID=02 TARGET_RACE=1 node scripts/crawl-exhibition.js
//   (量産) ALL_RACES=1 node scripts/crawl-exhibition.js

import fs from "node:fs/promises";
import path from "node:path";

const ROOT = process.cwd();
const OUT_ROOT = path.join(ROOT, "public", "exhibition", "v1");

// --------- 入力指定 ----------
const DATE_IN   = (process.env.TARGET_DATE || "today").replace(/-/g, "");
const PID_IN    = process.env.TARGET_PID || "02"; // "02"
const RACE_IN   = process.env.TARGET_RACE || "1"; // "1".."12"
const ALL_RACES = process.env.ALL_RACES === "1";  // 1R〜12R まとめて
const to2 = (s) => String(s).padStart(2, "0");
const PID  = to2(PID_IN);
const RNO  = String(RACE_IN).replace(/[^\d]/g, "");
const RACE = `${RNO}R`;

// ソースルート（差し替え可能）
const EX_SRC_ROOT = process.env.EX_SRC_ROOT || "https://boatraceopenapi.github.io/previews/v2";

// --------- fetch ヘルパ ----------
async function fetchText(url) {
  const res = await fetch(url, {
    headers: { "user-agent": "Mozilla/5.0", accept: "application/json,text/plain" },
  });
  if (!res.ok) throw new Error(`GET ${url} ${res.status}`);
  return await res.text();
}

async function fetchJson(url) {
  const t = await fetchText(url);
  try {
    return JSON.parse(t);
  } catch {
    throw new Error(`Invalid JSON at ${url}`);
  }
}

// --------- 正規化 ----------
// 入ってくるキーの揺れを吸収して統一フォーマットにする。
// わからない項目は raw に残す。
function normExhibitionPayload(src) {
  // src はレース単位の JSON を想定（previews 相当で entries/boats/…を持つ）
  // 代表的なキー名の揺れに対応
  const entries = src.entries || src.boats || src.Exhibition || [];
  const raceId  = src.race || src.race_number || src.Race || null;
  const stadiumCode = src.stadium || src.stadium_number || src.race_stadium_number || null;

  const normEntries = (entries || []).map((e) => {
    const lane  = e.lane ?? e.racer_boat_number ?? e.boat ?? null;
    const number= e.number ?? e.racer_number ?? e.id ?? null;
    // 展示関連（可能性あるキーを広く拾う）
    const exRank   = e.exhibition_rank ?? e.demo_rank ?? e.preview_ex_rank ?? e.exRank ?? null;
    const exTime   = e.exhibition_time ?? e.demo_time ?? e.preview_ex_time ?? e.exTime ?? null; // 例: 6.70
    const sTime    = e.start_timing ?? e.exhibition_start_timing ?? e.sTime ?? null;            // 例: .12 / F.01
    const sCourse  = e.start_course ?? e.exhibition_start_course ?? e.sCourse ?? null;          // 例: 1..6
    const pitOut   = e.pit_out ?? e.pit ?? null; // ピット離れ等のフラグ/文言があるなら

    // 追加でありがちなフィールド
    const lapRank  = e.lap_rank ?? e.turn_rank ?? null; // 周回/ターンの順位等
    const memo     = e.note ?? e.memo ?? null;

    // 触っていない元の断片を保持
    const raw = e;

    return {
      lane: lane ? Number(lane) : null,
      number: number ? Number(number) : null,

      exhibition: {
        rank: exRank != null ? Number(exRank) : null,
        time: exTime != null ? Number(exTime) : null,         // 6.70 など
        startTiming: sTime ?? null,                            // 文字列のまま保持（F/L含む）
        startCourse: sCourse != null ? Number(sCourse) : null, // 1..6
        pitOut: pitOut ?? null,
        lapRank: lapRank != null ? Number(lapRank) : null,
        note: memo ?? null,
      },

      raw,
    };
  });

  return {
    schemaVersion: "1.0",
    date: DATE_IN,
    stadium: stadiumCode ? to2(stadiumCode) : PID,
    race: raceId ? `${String(raceId).replace(/[^\d]/g, "")}R` : RACE,
    updatedAt: new Date().toISOString(),
    entries: normEntries,
    sourceShape: {
      keys: Object.keys(src || {}),
      entryKeys: entries && entries[0] ? Object.keys(entries[0]) : [],
    },
    raw: src, // そのまま残す（将来の解析用）
  };
}

// --------- 保存 ----------
async function saveOne(date, pid, race, payload) {
  const dir = path.join(OUT_ROOT, date, pid);
  await fs.mkdir(dir, { recursive: true });
  const file = path.join(dir, `${race}.json`);
  await fs.writeFile(file, JSON.stringify(payload, null, 2), "utf8");
  console.log("write:", path.relative(ROOT, file));
}

// --------- メイン ----------
async function crawlOne(date, pid, race) {
  const url = `${EX_SRC_ROOT}/${date}/${pid}/${race}.json`;
  const json = await fetchJson(url);
  const payload = normExhibitionPayload(json);
  await saveOne(date, pid, race, payload);
}

async function main() {
  const date = DATE_IN;
  const pid = PID;

  if (ALL_RACES) {
    for (let i = 1; i <= 12; i++) {
      const r = `${i}R`;
      try {
        await crawlOne(date, pid, r);
      } catch (e) {
        console.warn(`skip ${date}/${pid}/${r}: ${e.message}`);
      }
    }
  } else {
    await crawlOne(date, pid, RACE);
  }

  // ヘルス用メタ
  const metaDir = path.join(OUT_ROOT, date);
  await fs.mkdir(metaDir, { recursive: true });
  await fs.writeFile(
    path.join(metaDir, "exhibition-meta.json"),
    JSON.stringify({ status: 200, generatedAt: new Date().toISOString(), pid, allRaces: ALL_RACES }, null, 2),
    "utf8"
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
