// scripts/fetch-exhibition.js
// Node v20 / ESM / cheerio v1.x
// 保存: public/exhibitions/v1/<date>/<pid>/<race>.json
//
// 取得順: beforeinfo(名前/枠) → racelist(不足補完) → exhresult(ST)
//
// 実行例:
//   TARGET_DATE=20250809 TARGET_PIDS=02 TARGET_RACES=5 node scripts/fetch-exhibition.js --skip-existing

import { load } from "cheerio";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const OUT_ROOT = path.resolve(__dirname, "..", "public", "exhibitions", "v1");
const UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36";

const norm = (s) => (s ?? "").replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function todayYYYYMMDD() {
  const jst = new Date(Date.now() + 9 * 3600 * 1000);
  const y = jst.getUTCFullYear();
  const m = String(jst.getUTCMonth() + 1).padStart(2, "0");
  const d = String(jst.getUTCDate()).padStart(2, "0");
  return `${y}${m}${d}`;
}
const to2 = (v) => (/^\d+$/.test(String(v).trim()) ? String(v).padStart(2, "0") : String(v).trim().toUpperCase());
const toRaceKey = (v) => {
  const s = String(v).trim().toUpperCase();
  if (/^\d+$/.test(s)) return `${Number(s)}R`;
  if (/^\d+R$/.test(s)) return s;
  const n = s.replace(/[^\d]/g, "");
  return n ? `${Number(n)}R` : s;
};

async function ensureDir(p) { await fs.mkdir(p, { recursive: true }); }
async function fileExists(p) { try { await fs.access(p); return true; } catch { return false; } }

async function fetchHtml(url, { retries = 2, delayMs = 700 } = {}) {
  for (let i = 0; i <= retries; i++) {
    const res = await fetch(url, { headers: { "user-agent": UA, accept: "text/html" } });
    if (res.ok) return await res.text();
    if (res.status === 404) throw new Error("HTTP 404");
    if (i < retries) await sleep(delayMs);
  }
  throw new Error(`HTTP failed after ${retries + 1} tries`);
}

const urls = {
  beforeinfo: (date, pid, rno) => `https://www.boatrace.jp/owpc/pc/race/beforeinfo?hd=${date}&jcd=${pid}&rno=${rno}`,
  racelist:   (date, pid, rno) => `https://www.boatrace.jp/owpc/pc/race/racelist?hd=${date}&jcd=${pid}&rno=${rno}`,
  exhresult:  (date, pid, rno) => `https://www.boatrace.jp/owpc/pc/race/exhresult?hd=${date}&jcd=${pid}&rno=${rno}`,
};

const pickNumberFromHref = (href) => {
  if (!href) return null;
  // プロフィール dno=XXXX を拾う（無いことも多い）
  const m = href.match(/[?&]dno=(\d{4})\b/);
  return m ? Number(m[1]) : null;
};

const parseSTToken = (txt) => {
  if (!txt) return { st: null, flag: null };
  const flag = /^[FL]/i.test(txt) ? txt[0].toUpperCase() : null;
  const m = txt.match(/\b(\d?\.\d{2})\b/) || txt.match(/(\.\d{2})/);
  return m ? { st: Number(m[1]), flag } : { st: null, flag };
};

// ---------- beforeinfo: 枠＆名前（列位置で厳密に） ----------
function parseEntriesFromBeforeInfo(html) {
  const $ = load(html);
  let entries = [];

  $("table").each((_, tbl) => {
    const $tbl = $(tbl);
    const headers = $tbl.find("thead th, thead td").map((_, th) => norm($(th).text())).get();
    if (!headers.length) return;
    const hasName = headers.some((h) => /選手名|選手|氏名/.test(h));
    const hasLane = headers.some((h) => /艇|枠|コース|進入|号艇/.test(h));
    if (!hasLane || !hasName) return;

    // 列インデックス推定
    const laneIdx = headers.findIndex((h) => /艇|枠|コース|進入|号艇/.test(h));
    const nameIdx = headers.findIndex((h) => /選手名|選手|氏名/.test(h));

    $tbl.find("tbody tr").each((_, tr) => {
      const $tr = $(tr);
      const tds = $tr.find("th,td");
      if (!tds.length) return;

      // lane
      let lane = null;
      if (laneIdx >= 0 && tds.eq(laneIdx).length) {
        const v = Number(norm(tds.eq(laneIdx).text()).replace(/[^\d]/g, ""));
        if (Number.isFinite(v) && v >= 1 && v <= 6) lane = v;
      }
      if (!(lane >= 1 && lane <= 6)) return;

      // name（ruby/改行/リンク考慮）
      let name = null;
      if (nameIdx >= 0 && tds.eq(nameIdx).length) {
        const $n = tds.eq(nameIdx);
        const anchorName = norm($n.find("a").text());
        const plain = norm($n.text());
        name = anchorName || plain || null;
        if (name) {
          // フリガナや余計な空白を軽く整理（例: "山田 太郎" 形式に）
          name = name.replace(/\s+/g, " ").replace(/\s*\(.*?\)\s*/g, "").trim();
        }
      }
      // number（リンクがあれば）
      let number = null;
      tds.find("a[href]").each((__, a) => {
        const href = $(a).attr("href") || "";
        const d = pickNumberFromHref(href);
        if (d) number = d;
      });

      entries.push({ lane, number: number ?? null, name: name || null, st: null, stFlag: null });
    });
  });

  // lane 1..6 優先整列＆重複除去
  const uniq = [];
  for (let l = 1; l <= 6; l++) {
    const hit = entries.find((e) => e.lane === l);
    if (hit) uniq.push(hit);
  }
  return uniq;
}

// ---------- racelist: 欠けた名前/番号の補完 ----------
function parseEntriesFromRaceList(html) {
  const $ = load(html);
  const out = [];
  $("table").each((_, tbl) => {
    const $tbl = $(tbl);
    const headers = $tbl.find("thead th, thead td").map((_, th) => norm($(th).text())).get();
    const ok = headers.some((h) => /艇|枠|コース|号艇/.test(h)) && headers.some((h) => /選手|名/.test(h));
    if (!ok) return;

    const laneIdx = headers.findIndex((h) => /艇|枠|コース|進入|号艇/.test(h));
    const nameIdx = headers.findIndex((h) => /選手名|選手|氏名|名/.test(h));

    $tbl.find("tbody tr").each((_, tr) => {
      const $tr = $(tr);
      const tds = $tr.find("th,td");
      if (!tds.length) return;

      let lane = null;
      if (laneIdx >= 0) {
        const v = Number(norm(tds.eq(laneIdx).text()).replace(/[^\d]/g, ""));
        if (Number.isFinite(v) && v >= 1 && v <= 6) lane = v;
      }
      if (!(lane >= 1 && lane <= 6)) return;

      let name = null;
      if (nameIdx >= 0) {
        const $n = tds.eq(nameIdx);
        name = norm($n.find("a").text()) || norm($n.text()) || null;
        if (name) name = name.replace(/\s+/g, " ").replace(/\s*\(.*?\)\s*/g, "").trim();
      }

      let number = null;
      $tr.find("a[href]").each((__, a) => {
        const href = $(a).attr("href") || "";
        const d = pickNumberFromHref(href);
        if (d) number = d;
      });

      out.push({ lane, number: number ?? null, name: name ?? null, st: null, stFlag: null });
    });
  });

  const uniq = [];
  for (let l = 1; l <= 6; l++) {
    const hit = out.find((e) => e.lane === l);
    if (hit) uniq.push(hit);
  }
  return uniq;
}

// ---------- exhresult: ST 取得 ----------
function parseSTFromExhResult(html) {
  const $ = load(html);
  const stMap = new Map(); // lane -> {st, flag}
  $("table").each((_, tbl) => {
    const $tbl = $(tbl);
    const headers = $tbl.find("thead th, thead td").map((_, th) => norm($(th).text())) .get();
    const hasLane = headers.some((h) => /進入|コース|枠|号艇/.test(h));
    const hasST = headers.some((h) => /Ｓ?T|スタート/.test(h));
    if (!hasLane || !hasST) return;

    $tbl.find("tbody tr").each((_, tr) => {
      const $tr = $(tr);
      const texts = $tr.find("th,td").map((__, td) => norm($(td).text())).get();
      if (!texts.length) return;
      // lane (最初の 1..6)
      let lane = null;
      for (const t of texts) {
        const m = t.match(/\b([1-6])\b/);
        if (m) { lane = Number(m[1]); break; }
      }
      if (!(lane >= 1 && lane <= 6)) return;

      const joined = texts.join(" ");
      const { st, flag } = parseSTToken(joined);
      if (st != null) stMap.set(lane, { st, flag });
    });
  });
  return stMap;
}

// ---------- 1レース ----------
async function fetchOne({ date, pid, race, skipExisting = false }) {
  const rno = Number(String(race).replace(/[^\d]/g, ""));
  const outDir = path.join(OUT_ROOT, date, pid);
  const outPath = path.join(outDir, `${race}.json`);

  if (skipExisting && (await fileExists(outPath))) {
    console.log(`⏭️ skip existing: ${path.relative(OUT_ROOT, outPath)}`);
    return;
  }
  await ensureDir(outDir);

  let entries = [];
  let mode = "beforeinfo";
  const urlBefore = urls.beforeinfo(date, pid, rno);
  const urlList = urls.racelist(date, pid, rno);
  const urlExh = urls.exhresult(date, pid, rno);

  // beforeinfo
  try {
    const html = await fetchHtml(urlBefore);
    entries = parseEntriesFromBeforeInfo(html);
  } catch (e) {
    console.warn(`warn beforeinfo: ${e.message}`);
    mode = "racelist";
  }

  // racelist 補完 or 置換
  try {
    const htmlList = await fetchHtml(urlList);
    const listEntries = parseEntriesFromRaceList(htmlList);
    if (!entries.length) {
      entries = listEntries;
    } else {
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
    }
  } catch (e) {
    if (!entries.length) {
      // どっちもダメなら空で保存
      const payload = { date, pid, race, source: urlBefore, mode, generatedAt: new Date().toISOString(), entries: [] };
      await fs.writeFile(outPath, JSON.stringify(payload, null, 2), "utf8");
      console.log(`❌ unavailable wrote: ${path.relative(OUT_ROOT, outPath)}`);
      return;
    }
  }

  // exhresult で ST 取得
  try {
    const htmlExh = await fetchHtml(urlExh);
    const stMap = parseSTFromExhResult(htmlExh);
    if (stMap.size) {
      entries = entries.map((e) => {
        const s = stMap.get(e.lane);
        return s ? { ...e, st: s.st, stFlag: s.flag } : e;
        });
    }
  } catch {
    // ST なしでも可
  }

  const payload = {
    date, pid, race,
    source: mode === "beforeinfo" ? urlBefore : urlList,
    mode,
    generatedAt: new Date().toISOString(),
    entries,
  };
  await fs.writeFile(outPath, JSON.stringify(payload, null, 2), "utf8");
  console.log(`✅ wrote: ${path.relative(OUT_ROOT, outPath)} (mode=${mode})`);
}

// ---------- メイン ----------
function parseCliFlags(argv) {
  return { skipExisting: argv.includes("--skip-existing") };
}

async function main() {
  const { skipExisting } = parseCliFlags(process.argv.slice(2));
  const DATE_IN = (process.env.TARGET_DATE || "today").trim();
  const date = DATE_IN === "today" ? todayYYYYMMDD() : DATE_IN;

  const PIDS_IN = (process.env.TARGET_PIDS || "02").trim();
  const pids = PIDS_IN.split(",").map((s) => s.trim()).filter(Boolean).map(to2);

  const RACES_IN = (process.env.TARGET_RACES || "").trim();
  const races = RACES_IN
    ? RACES_IN.split(",").map((s) => toRaceKey(s)).filter(Boolean)
    : Array.from({ length: 12 }, (_, i) => `${i + 1}R`);

  console.log(`target: date=${date} pids=${pids.join(",")} races=${races.join(",")}`);

  for (const pid of pids) {
    for (const race of races) {
      await fetchOne({ date, pid, race, skipExisting });
      await sleep(350); // 優しめ
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
