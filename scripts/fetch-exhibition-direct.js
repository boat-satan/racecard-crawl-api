// scripts/fetch-exhibition.js
// Node v20 / ESM / cheerio v1.x
// 保存先: public/exhibitions/v1/<date>/<pid>/<race>.json
//
// 使い方（ローカル or GitHub Actions の "Run workflow" から）:
//   環境変数/inputs:
//     TARGET_DATE  = "YYYYMMDD" or "today"
//     TARGET_PIDS  = "02,09"        ← カンマ区切り（ゼロ埋め2桁でもOK）
//     TARGET_RACES = "1R,2R"  or "1,2"（空なら 1..12）
//   フラグ:
//     --skip-existing  … 出力ファイルが存在する場合スキップ（上書きしない）
//
// 取得ロジック:
//   1) beforeinfo を優先（展示後なら ST を含む）
//   2) 取れなければ racelist をフォールバック（名前/枠のみ）
//
// 備考:
//   - 公式HTML側の見出し依存をやめ、ヘッダ内の「進入/枠/号艇/コース」「ST/スタート」をキーに
//     テーブルを検出。行テキストから F.01 / L.10 / .13 などを正規化して抽出。

import { load } from "cheerio";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

// ----------------------------- 基本設定 -----------------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const OUT_ROOT = path.resolve(__dirname, "..", "public", "exhibitions", "v1");

const UA =
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36";

// ----------------------------- ユーティリティ -----------------------------
const norm = (s) => (s ?? "").replace(/\s+/g, " ").trim();

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function ensureDir(p) {
  await fs.mkdir(p, { recursive: true });
}

async function fileExists(p) {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}

function todayYYYYMMDD() {
  const jst = new Date(Date.now() + 9 * 3600 * 1000);
  const y = jst.getUTCFullYear();
  const m = String(jst.getUTCMonth() + 1).padStart(2, "0");
  const d = String(jst.getUTCDate()).padStart(2, "0");
  return `${y}${m}${d}`;
}

function to2(v) {
  const s = String(v).trim();
  return /^\d+$/.test(s) ? s.padStart(2, "0") : s;
}

function toRaceKey(v) {
  const s = String(v).trim().toUpperCase();
  if (/^\d+$/.test(s)) return `${Number(s)}R`;
  if (/^\d+R$/.test(s)) return s;
  // フォールバック
  const n = s.replace(/[^\d]/g, "");
  return n ? `${Number(n)}R` : s;
}

async function fetchHtml(url, { retries = 2, delayMs = 700 } = {}) {
  for (let i = 0; i <= retries; i++) {
    const res = await fetch(url, { headers: { "user-agent": UA, accept: "text/html" } });
    if (res.ok) return await res.text();
    if (res.status === 404) throw new Error("HTTP 404");
    if (i < retries) await sleep(delayMs);
  }
  throw new Error(`HTTP failed after ${retries + 1} tries`);
}

function buildBeforeInfoUrl(date, pid, raceNum) {
  return `https://www.boatrace.jp/owpc/pc/race/beforeinfo?hd=${date}&jcd=${pid}&rno=${raceNum}`;
}
function buildRaceListUrl(date, pid, raceNum) {
  return `https://www.boatrace.jp/owpc/pc/race/racelist?hd=${date}&jcd=${pid}&rno=${raceNum}`;
}

const pickNumberFromHref = (href) => {
  if (!href) return null;
  const m = href.match(/[?&]dno=(\d{4})\b/);
  return m ? Number(m[1]) : null;
};

const parseSTToken = (txt) => {
  if (!txt) return { st: null, flag: null };
  const flag = /^[FL]/i.test(txt) ? txt[0].toUpperCase() : null;
  // 0.13 / .13 / F.01 / L.10 を拾う
  // まず 0.13 のような明示小数、なければ .13 の孤立小数
  const m = txt.match(/\b(\d?\.\d{2})\b/) || txt.match(/(\.\d{2})/);
  return m ? { st: Number(m[1]), flag } : { st: null, flag };
};

// ----------------------------- パーサー（beforeinfo） -----------------------------
function extractEntriesFromMainTable($) {
  const entries = [];
  $("table").each((_, tbl) => {
    const $tbl = $(tbl);
    const heads = $tbl
      .find("thead th, thead td")
      .map((_, th) => norm($(th).text()))
      .get();
    // レーン/艇/進入 x 名前 のような構成をざっくり検出
    const hasLane = heads.some((h) => /艇|枠|コース|進入|号艇/.test(h));
    const hasName = heads.some((h) => /選手|名/.test(h));
    if (!hasLane || !hasName) return;

    $tbl.find("tbody tr").each((_, tr) => {
      const $tr = $(tr);
      const cells = $tr.find("th,td");
      if (!cells.length) return;

      // lane を最初に見つかった 1..6 で決定
      let lane = null;
      cells.each((i, td) => {
        const v = Number(norm($(td).text()).replace(/[^\d]/g, ""));
        if (!lane && Number.isFinite(v) && v >= 1 && v <= 6) lane = v;
      });
      if (!Number.isFinite(lane)) return;

      // 名前 & 登録番号（profile リンク dno）
      let name = null,
        number = null;
      $tr.find("a[href]").each((_, a) => {
        const href = $(a).attr("href") || "";
        const txt = norm($(a).text());
        const dno = pickNumberFromHref(href);
        if (dno) number = dno;
        if (txt && !name) name = txt;
      });
      if (!name) {
        // フォールバック：行テキストからそれっぽい和名
        const t = norm($tr.text());
        const m = t.match(/[一-龥ぁ-んァ-ン][一-龥ぁ-んァ-ン・\s]{1,10}/);
        if (m) name = norm(m[0]);
      }

      entries.push({
        lane,
        number: number ?? null,
        name: name ?? null,
        st: null,
        stFlag: null,
      });
    });
  });

  // lane 1..6 に揃える（重複があれば最初の行を採用）
  const uniq = [];
  for (let l = 1; l <= 6; l++) {
    const hit = entries.find((e) => e.lane === l);
    if (hit) uniq.push(hit);
  }
  return uniq;
}

function extractStartDisplayST($) {
  const stMap = new Map(); // lane -> {st, flag}

  $("table").each((_, tbl) => {
    const $tbl = $(tbl);
    const heads = $tbl
      .find("thead th, thead td")
      .map((_, th) => norm($(th).text()))
      .get();
    const hasST = heads.some((h) => /Ｓ?T|スタート/.test(h));
    const hasLane = heads.some((h) => /進入|コース|枠|号艇/.test(h));
    if (!hasST || !hasLane) return;

    $tbl.find("tbody tr").each((_, tr) => {
      const texts = $(tr)
        .find("th,td")
        .map((_, td) => norm($(td).text()))
        .get();
      if (!texts.length) return;

      // 最初に見つかった 1..6 を lane とする
      let lane = Number(texts[0]);
      if (!(lane >= 1 && lane <= 6)) {
        const mLane = texts.join(" ").match(/\b([1-6])\b/);
        lane = mLane ? Number(mLane[1]) : NaN;
      }
      if (!(lane >= 1 && lane <= 6)) return;

      const joined = texts.join(" ");
      const { st, flag } = parseSTToken(joined);
      if (st != null) stMap.set(lane, { st, flag });
    });
  });

  return stMap;
}

function parseEntriesBeforeInfo(html) {
  const $ = load(html);
  const entries = extractEntriesFromMainTable($);
  const stMap = extractStartDisplayST($);
  entries.forEach((e) => {
    const hit = stMap.get(e.lane);
    if (hit) {
      e.st = hit.st;
      e.stFlag = hit.flag;
    }
  });
  return entries.sort((a, b) => a.lane - b.lane);
}

// ----------------------------- パーサー（racelist フォールバック） -----------------------------
function parseEntriesFromRaceList(html) {
  const $ = load(html);
  const out = [];
  // シンプルに 1..6 の枠＋選手 a[href*="dno="] を探す
  $("table").each((_, tbl) => {
    const $tbl = $(tbl);
    const heads = $tbl
      .find("thead th, thead td")
      .map((_, th) => norm($(th).text()))
      .get();
    const ok = heads.some((h) => /艇|枠|コース|号艇/.test(h)) && heads.some((h) => /選手|名/.test(h));
    if (!ok) return;

    $tbl.find("tbody tr").each((_, tr) => {
      const $tr = $(tr);
      let lane = null,
        name = null,
        number = null;
      // lane
      $tr.find("th,td").each((_, td) => {
        const v = Number(norm($(td).text()).replace(/[^\d]/g, ""));
        if (!lane && Number.isFinite(v) && v >= 1 && v <= 6) lane = v;
      });
      // name/number
      $tr.find("a[href]").each((_, a) => {
        const href = $(a).attr("href") || "";
        const txt = norm($(a).text());
        const dno = pickNumberFromHref(href);
        if (dno) number = dno;
        if (txt && !name) name = txt;
      });
      if (Number.isFinite(lane)) {
        out.push({ lane, number: number ?? null, name: name ?? null, st: null, stFlag: null });
      }
    });
  });
  const uniq = [];
  for (let l = 1; l <= 6; l++) {
    const hit = out.find((e) => e.lane === l);
    if (hit) uniq.push(hit);
  }
  return uniq.sort((a, b) => a.lane - b.lane);
}

// ----------------------------- 1レース処理 -----------------------------
async function fetchOne({ date, pid, race, skipExisting = false }) {
  const raceNum = Number(String(race).replace(/[^\d]/g, "")); // 1..12
  const outDir = path.join(OUT_ROOT, date, pid);
  const outPath = path.join(outDir, `${race}.json`);

  if (skipExisting && (await fileExists(outPath))) {
    console.log(`⏭️ skip existing: ${path.relative(OUT_ROOT, outPath)}`);
    return;
  }

  await ensureDir(outDir);

  const urlBefore = buildBeforeInfoUrl(date, pid, raceNum);
  const urlList = buildRaceListUrl(date, pid, raceNum);

  let html = null;
  let mode = "beforeinfo";
  try {
    html = await fetchHtml(urlBefore);
  } catch (e) {
    console.warn(`warn beforeinfo ${pid} ${race}: ${e.message} → fallback racelist`);
    mode = "racelist";
    try {
      html = await fetchHtml(urlList);
    } catch (e2) {
      const payload = {
        status: "unavailable",
        date,
        pid,
        race,
        mode,
        source: mode === "beforeinfo" ? urlBefore : urlList,
        error: String(e2),
        generatedAt: new Date().toISOString(),
      };
      await fs.writeFile(outPath, JSON.stringify(payload, null, 2), "utf8");
      console.log(`❌ unavailable wrote: ${path.relative(OUT_ROOT, outPath)}`);
      return;
    }
  }

  let entries =
    mode === "beforeinfo" ? parseEntriesBeforeInfo(html) : parseEntriesFromRaceList(html);

  // beforeinfo で entries が空/不十分なら racelist でも再チャレンジ
  if (mode === "beforeinfo" && (!entries.length || entries.some((e) => !e.name))) {
    try {
      const html2 = await fetchHtml(urlList);
      const listEntries = parseEntriesFromRaceList(html2);
      // 名前/番号の補完
      const map = new Map(listEntries.map((e) => [e.lane, e]));
      entries = entries.map((e) => {
        const m = map.get(e.lane);
        return {
          lane: e.lane,
          number: e.number ?? m?.number ?? null,
          name: e.name ?? m?.name ?? null,
          st: e.st,
          stFlag: e.stFlag,
        };
      });
      // まだ不足なら listEntries に置き換え
      if (!entries.length) entries = listEntries;
    } catch {}
  }

  const payload = {
    date,
    pid,
    race,
    source: mode === "beforeinfo" ? urlBefore : urlList,
    mode,
    generatedAt: new Date().toISOString(),
    entries,
  };

  await fs.writeFile(outPath, JSON.stringify(payload, null, 2), "utf8");
  console.log(`✅ wrote: ${path.relative(OUT_ROOT, outPath)} (mode=${mode})`);
}

// ----------------------------- メイン -----------------------------
function parseCliFlags(argv) {
  return {
    skipExisting: argv.includes("--skip-existing"),
  };
}

async function main() {
  const argv = process.argv.slice(2);
  const { skipExisting } = parseCliFlags(argv);

  const DATE_IN = (process.env.TARGET_DATE || "today").trim();
  const date = DATE_IN === "today" ? todayYYYYMMDD() : DATE_IN;

  const PIDS_IN = (process.env.TARGET_PIDS || "02").trim();
  const pids = PIDS_IN.split(",")
    .map((s) => s.trim())
    .filter(Boolean)
    .map(to2);

  const RACES_IN = (process.env.TARGET_RACES || "").trim();
  const races = RACES_IN
    ? RACES_IN.split(",")
        .map((s) => toRaceKey(s))
        .filter(Boolean)
    : Array.from({ length: 12 }, (_, i) => `${i + 1}R`);

  console.log(`target: date=${date} pids=${pids.join(",")} races=${races.join(",")}`);

  for (const pid of pids) {
    for (const race of races) {
      await fetchOne({ date, pid, race, skipExisting });
      // サイトに優しめに
      await sleep(400);
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
