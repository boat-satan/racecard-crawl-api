// scripts/merge-exhibition.js
// 目的: public/exhibition/v1/<date>/<pid>/<race>.json を
//       public/programs/v2/<date>/<pid>/<race>.json（フル）にマージ
// ついでに programs-slim 側にも軽量な抜粋を足す（存在すれば）
//
// 入力ENV:
//   TARGET_DATE=YYYYMMDD or "today"（必須）
//   TARGET_PID=場コード(01..24)（必須）
//   TARGET_RACE=1R..12R（必須）
//
// マージ戦略:
//   - full に .exhibition を追加 { source, fetchedAt, entries:[{lane, st, exTime, timeRank, ...}] , raw }
//   - slim にも entries[i].ex を追加できる範囲で追加
//
// ※ previews のスキーマは不定のため、よくあるキーを推測して拾いつつ、raw も保持

import fs from "node:fs/promises";
import path from "node:path";

const to2 = (v) => String(v).padStart(2, "0");
const DATE_RAW = (process.env.TARGET_DATE || "").trim();
const PID_RAW  = (process.env.TARGET_PID  || "").trim();
const RACE_RAW = (process.env.TARGET_RACE || "").trim().toUpperCase();

if (!DATE_RAW || !PID_RAW || !RACE_RAW) {
  console.error("ERROR: TARGET_DATE, TARGET_PID, TARGET_RACE は必須です");
  process.exit(1);
}

const DATE = DATE_RAW.replace(/-/g, "");
const PID  = /^\d+$/.test(PID_RAW) ? to2(PID_RAW) : PID_RAW;
const RACE = /R$/i.test(RACE_RAW) ? RACE_RAW : `${RACE_RAW}R`;

const EXH_FILE  = path.join("public", "exhibition", "v1", DATE, PID, `${RACE}.json`);
const FULL_FILE = path.join("public", "programs", "v2", DATE, PID, `${RACE}.json`);
const SLIM_FILE = path.join("public", "programs-slim", "v2", DATE, PID, `${RACE}.json`);

async function exists(p) {
  try { await fs.access(p); return true; } catch { return false; }
}

// よくある previews の形から抽出を試みる
function normalizeExEntries(raw) {
  // 可能性: raw.data.entries / raw.data.boats / raw.data.lanes / raw.data
  const cand =
    raw?.data?.entries ??
    raw?.data?.boats ??
    raw?.data?.lanes ??
    (Array.isArray(raw?.data) ? raw.data : null);

  if (!Array.isArray(cand)) return [];

  // lane 推定: lane / boat / number / index
  return cand.map((e, i) => {
    // 代表的なキーの候補
    const lane =
      e.lane ?? e.boat ?? e.number ?? e.laneNo ?? (i + 1);

    // ST / 展示タイムなどに該当しそうなキー候補を広めに
    const st =
      e.st ?? e.ST ?? e.start ?? e.start_timing ?? null;

    const exTime =
      e.exTime ?? e.exhibitionTime ?? e.demoTime ?? e.time ?? null;

    const timeRank =
      e.timeRank ?? e.exTimeRank ?? e.rank ?? null;

    // 他にも気づいたら追加してOK
    return {
      lane: Number(lane) || lane,
      st: typeof st === "string" ? st : (st ?? null),
      exTime: typeof exTime === "string" ? exTime : (exTime ?? null),
      timeRank: typeof timeRank === "string" ? timeRank : (timeRank ?? null),
      raw: e
    };
  });
}

async function main() {
  if (!(await exists(EXH_FILE))) {
    console.error(`ERROR: exhibition not found: ${EXH_FILE}`);
    process.exit(1);
  }
  if (!(await exists(FULL_FILE))) {
    console.error(`ERROR: full race file not found: ${FULL_FILE}`);
    process.exit(1);
  }

  const exh = JSON.parse(await fs.readFile(EXH_FILE, "utf8"));
  const full = JSON.parse(await fs.readFile(FULL_FILE, "utf8"));

  const exEntries = normalizeExEntries(exh);

  // full 側に .exhibition を追加/更新
  full.exhibition = {
    source: exh.source ?? null,
    fetchedAt: exh.fetchedAt ?? new Date().toISOString(),
    entries: exEntries,
    raw: exh.data ?? exh
  };

  await fs.writeFile(FULL_FILE, JSON.stringify(full, null, 2), "utf8");
  console.log("updated:", FULL_FILE);

  // slim があれば、entries 順に lane 突合せして軽量付与
  if (await exists(SLIM_FILE)) {
    const slim = JSON.parse(await fs.readFile(SLIM_FILE, "utf8"));
    if (Array.isArray(slim.entries) && slim.entries.length) {
      const byLane = new Map(exEntries.map((x) => [String(x.lane), x]));
      slim.entries = slim.entries.map((en) => {
        const hit = byLane.get(String(en.lane));
        if (!hit) return en;
        return {
          ...en,
          ex: {
            st: hit.st ?? null,
            time: hit.exTime ?? null,
            timeRank: hit.timeRank ?? null
          }
        };
      });
      await fs.writeFile(SLIM_FILE, JSON.stringify(slim, null, 2), "utf8");
      console.log("updated:", SLIM_FILE);
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
