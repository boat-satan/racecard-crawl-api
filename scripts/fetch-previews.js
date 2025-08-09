// scripts/fetch-previews.js
// 目的: previews 風の JSON を自前生成
// 入力: public/programs/v2/<date>/<pid>/<R>.json（出走表）
// 出力: public/previews/v2/<date>/<pid>/<R>.json
// 方針: 出走表を基に公式HTML等から展示/進入をスクレイピングして差し込む
//       セレクタは環境で確認しつつ TODO を埋める

import fs from "node:fs/promises";
import fssync from "node:fs";
import path from "node:path";
import { load } from "cheerio";

const ROOT = process.cwd();
const OUT_ROOT = path.join(ROOT, "public", "previews", "v2");

// ---- ユーティリティ ----
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const to2 = (s) => String(s).padStart(2, "0");
const todayYmd = () => {
  const d = new Date();
  const y = d.getFullYear();
  const m = to2(d.getMonth() + 1);
  const dd = to2(d.getDate());
  return `${y}${m}${dd}`;
};

// 環境変数（任意）
const DATE = (process.env.TARGET_DATE || "today").replace(/-/g, "").toLowerCase();
const PIDS = (process.env.TARGET_PIDS || "").split(",").map(s => s.trim()).filter(Boolean); // 例: "02,09"
const RACES = (process.env.TARGET_RACES || "").split(",").map(s => s.trim().toUpperCase()).filter(Boolean); // 例: "1R,2R"

// 公開の出走表保存場所（このリポの既存構造に合わせる）
const PROG_ROOT = path.join(ROOT, "public", "programs", "v2");

// データ元（ここはあとで公式URLの実装に合わせて拡張）
function buildOfficialDetailUrl({ ymd, pid, rno }) {
  // 例: https://www.boatrace.jp/owpc/pc/race/racelist?jcd=02&hd=20250809
  // 各場・R詳細ページのURLはサイト構造により異なる。ここは実環境で要確認。
  // TODO: レース詳細（展示・進入・展示タイム）へ直接飛べる URL を実装
  return `https://www.boatrace.jp/owpc/pc/race/racelist?jcd=${pid}&hd=${ymd}`;
}

// ---- 展示・進入パース（骨組み） ----
async function fetchHtml(url, { retries = 3, delayMs = 800 } = {}) {
  for (let i = 0; i <= retries; i++) {
    const res = await fetch(url, {
      headers: {
        "user-agent":
          "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118 Safari/537.36",
        accept: "text/html,application/xhtml+xml",
      },
    });
    if (res.ok) return await res.text();
    if (i < retries) await sleep(delayMs);
  }
  throw new Error(`GET ${url} failed`);
}

// 実際の DOM は環境で確認し、セレクタを埋めること
function parseExhibition($, { pid, rno }) {
  // 返す形は previews っぽいミニマム
  // 例：
  // {
  //   pit_order: [1,2,3,4,5,6],   // ピット離れ順（不明なら null）
  //   course_entry: [1,2,3,4,5,6],// 進入コース確定（枠なりなら [1..6]）
  //   st_list: [".16",".15",".13",".21",".19",".25"], // ST（文字列で OK）
  //   ex_times: [6.71,6.75,6.74,6.78,6.80,6.85],      // 展示タイム
  //   updated_at: "ISO8601"
  // }

  // TODO: 実 DOM から抽出
  const course_entry = null; // 例: [1,2,3,4,5,6]
  const st_list = null;      // 例: [".11",".13",".16",".20",".19",".21"]
  const ex_times = null;     // 例: [6.71,6.73,6.74,6.77,6.80,6.83]

  return {
    pit_order: null,
    course_entry,
    st_list,
    ex_times,
    updated_at: new Date().toISOString(),
    source_pid: pid,
    source_race: rno
  };
}

// ---- 出走表 → previews 生成 ----
async function listRaceFiles() {
  const ymd = DATE === "today" ? todayYmd() : DATE;
  const daysDir = path.join(PROG_ROOT, ymd);
  if (!fssync.existsSync(daysDir)) return [];

  const pidDirs = (await fs.readdir(daysDir, { withFileTypes: true }))
    .filter(d => d.isDirectory())
    .map(d => d.name)
    .filter(pid => (PIDS.length ? PIDS.includes(pid) : true));

  const files = [];
  for (const pid of pidDirs) {
    const full = path.join(daysDir, pid);
    const raceFiles = (await fs.readdir(full)).filter(f => /^(?:\d+R|[1-9]R|1[0-2]R)\.json$/.test(f));
    for (const rf of raceFiles) {
      const r = rf.replace(".json","").toUpperCase();
      if (RACES.length && !RACES.includes(r)) continue;
      files.push({ ymd, pid, race: r, path: path.join(full, rf) });
    }
  }
  return files;
}

async function main() {
  const targets = await listRaceFiles();
  if (!targets.length) {
    console.log("no program files found for previews.");
    return;
  }

  let ok = 0, ng = 0;
  for (const t of targets) {
    try {
      const raw = JSON.parse(await fs.readFile(t.path, "utf8"));
      // race 番号（数字だけ）
      const rno = String(raw.race ?? t.race).replace(/[^\d]/g, "");

      // 公式ページを取得して展示情報を抜く
      const url = buildOfficialDetailUrl({ ymd: t.ymd, pid: t.pid, rno });
      const html = await fetchHtml(url);
      const $ = load(html);
      const ex = parseExhibition($, { pid: t.pid, rno });

      // previews 風の最低限スキーマ（必要に応じて拡張）
      const preview = {
        schemaVersion: "preview-1",
        date: t.ymd,
        pid: t.pid,
        race: `${rno}R`,
        generatedAt: new Date().toISOString(),
        // 出走表から最低限のエントリ情報は載せておく（名前や枠）
        entries: (raw.entries || raw.boats || []).map((e) => ({
          lane: e.lane ?? e.racer_boat_number ?? null,
          number: e.number ?? e.racer_number ?? null,
          name: e.name ?? e.racer_name ?? null,
        })),
        exhibition: ex
      };

      // 保存
      const outDir = path.join(OUT_ROOT, t.ymd, t.pid);
      await fs.mkdir(outDir, { recursive: true });
      const outPath = path.join(outDir, `${t.race}.json`);
      await fs.writeFile(outPath, JSON.stringify(preview, null, 2), "utf8");
      console.log(`✅ wrote previews: ${path.relative(ROOT, outPath)}`);
      ok++;

      // マナー
      await sleep(1000);
    } catch (e) {
      console.warn(`❌ ${t.pid} ${t.race}: ${e.message}`);
      ng++;
    }
  }

  // メタ
  const metaDir = path.join(ROOT, "public", "debug");
  await fs.mkdir(metaDir, { recursive: true });
  await fs.writeFile(
    path.join(metaDir, "previews-meta.json"),
    JSON.stringify(
      {
        status: 200,
        generatedAt: new Date().toISOString(),
        success: ok,
        failed: ng
      },
      null,
      2
    ),
    "utf8"
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
