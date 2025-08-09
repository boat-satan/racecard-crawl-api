// scripts/fetch-stats.js
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import cheerio from "cheerio";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ROOT = path.resolve(__dirname, "..");
const PUBLIC = path.join(ROOT, "public");
const PROGRAMS_DIR = path.join(PUBLIC, "programs", "v2", "today");
const OUT_DIR = path.join(PUBLIC, "stats", "v1", "racers");

// --- helpers ----------------------------------------------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const ensureDir = (p) => fs.mkdirSync(p, { recursive: true });

const readJson = (p) => JSON.parse(fs.readFileSync(p, "utf8"));

const listRaceJsons = () => {
  // public/programs/v2/today/<jcd>/<rno>.json
  if (!fs.existsSync(PROGRAMS_DIR)) return [];
  const dirs = fs.readdirSync(PROGRAMS_DIR);
  const files = [];
  for (const d of dirs) {
    const sub = path.join(PROGRAMS_DIR, d);
    if (!fs.statSync(sub).isDirectory()) continue;
    for (const f of fs.readdirSync(sub)) {
      if (f.endsWith(".json")) files.push(path.join(sub, f));
    }
  }
  return files;
};

const uniqueRegnosFromPrograms = () => {
  const regnos = new Set();
  for (const fp of listRaceJsons()) {
    try {
      const j = readJson(fp);
      if (Array.isArray(j.boats)) {
        for (const b of j.boats) {
          if (b?.racer_number) regnos.add(Number(b.racer_number));
        }
      } else if (Array.isArray(j.programs)) {
        // スリム形式の保険
        for (const p of j.programs) {
          for (const b of p.boats || []) {
            if (b?.racer_number) regnos.add(Number(b.racer_number));
          }
        }
      }
    } catch {}
  }
  return [...regnos].sort((a, b) => a - b);
};

// 見出し文字を含む要素（h1–h4, .title など）を探し、その直後の table を返す
function findTableByHeading($, keywords = []) {
  const match = (t) =>
    keywords.some((k) => t && t.replace(/\s+/g, "").includes(k));
  // 探索対象の見出しノード
  const nodes = $("h1,h2,h3,h4,section header,div,span")
    .toArray()
    .filter((el) => match($(el).text()));
  for (const el of nodes) {
    const $h = $(el);
    // 直後 or 近傍の table
    let $table = $h.nextAll("table").first();
    if (!$table || !$table.length) {
      // 親の直後
      $table = $h.parent().nextAll("table").first();
    }
    if ($table && $table.length) return $table;
  }
  // 保険：ページ内の table を総当りで caption/thead 文字列を確認
  const tables = $("table").toArray();
  for (const t of tables) {
    const tx = $(t).text().replace(/\s+/g, "");
    if (match(tx)) return $(t);
  }
  return null;
}

function parseNumber(x) {
  if (x == null) return null;
  const s = String(x).replace(/[,%．]/g, (m) => (m === "．" ? "." : ""));
  const n = Number(s.replace(/[^\d.\-]/g, ""));
  return Number.isFinite(n) ? n : null;
}

// テーブル -> 行配列（セル文字列）
function tableToRows($, $table) {
  const rows = [];
  $table.find("tr").each((_, tr) => {
    const cols = [];
    $(tr)
      .find("th,td")
      .each((__, td) => cols.push($(td).text().trim()));
    if (cols.length) rows.push(cols);
  });
  return rows;
}

// 「コース別成績」
function parseCourseStats($) {
  const $table = findTableByHeading($, ["コース別成績", "コース別成績（"]);
  if (!$table) return null;
  const rows = tableToRows($, $table);

  // 期待ヘッダ例：コース / 出走 / 1着 / 2連対率 / 3連対率 / 平均ST など
  const header = rows.shift()?.map((h) => h.replace(/\s+/g, ""));
  if (!header) return null;

  const idx = {
    course: header.findIndex((h) => /コース|枠/.test(h)),
    starts: header.findIndex((h) => /出走/.test(h)),
    win: header.findIndex((h) => /1着|1着率|勝率/.test(h)),
    top2: header.findIndex((h) => /2連対/.test(h)),
    top3: header.findIndex((h) => /3連対/.test(h)),
    avgST: header.findIndex((h) => /平均ST|AvgST|ST/.test(h)),
  };

  const out = {};
  for (const r of rows) {
    const c = parseNumber(r[idx.course]);
    if (!c || c < 1 || c > 6) continue;
    out[c] = {
      starts: parseNumber(r[idx.starts]),
      wins: parseNumber(r[idx.win]),
      top2Rate: parseNumber(r[idx.top2]),
      top3Rate: parseNumber(r[idx.top3]),
      avgST: parseNumber(r[idx.avgST]),
    };
  }
  return out;
}

// 「決まり手（コース別）」
function parseDecisionsByCourse($) {
  const $table = findTableByHeading($, ["決まり手", "決まり手（コース別"]);
  if (!$table) return null;
  const rows = tableToRows($, $table);
  const header = rows.shift()?.map((h) => h.replace(/\s+/g, ""));
  if (!header) return null;

  // 代表的な決まり手列名
  const keys = [
    "逃げ",
    "差し",
    "まくり",
    "まくり差し",
    "捲り差し",
    "抜き",
    "恵まれ",
    "その他",
  ];
  const colIdx = Object.fromEntries(
    keys.map((k) => [k, header.findIndex((h) => h.includes(k))])
  );
  const idxCourse = header.findIndex((h) => /コース|枠/.test(h));

  const out = {};
  for (const r of rows) {
    const c = parseNumber(r[idxCourse]);
    if (!c || c < 1 || c > 6) continue;
    out[c] = {};
    for (const k of keys) {
      const v = colIdx[k] >= 0 ? parseNumber(r[colIdx[k]]) : null;
      if (v != null) out[c][k] = v;
    }
  }
  return out;
}

// 「展示タイム順位別成績」
function parseExTimeRank($) {
  const $table = findTableByHeading($, ["展示タイム順位別成績", "展示タイム順位"]);
  if (!$table) return null;
  const rows = tableToRows($, $table);
  const header = rows.shift()?.map((h) => h.replace(/\s+/g, ""));
  if (!header) return null;

  // 想定：順位 / 出走 / 1着 / 2連対率 / 3連対率
  const idx = {
    rank: header.findIndex((h) => /順位|ランク/.test(h)),
    starts: header.findIndex((h) => /出走/.test(h)),
    win: header.findIndex((h) => /1着/.test(h)),
    top2: header.findIndex((h) => /2連対/.test(h)),
    top3: header.findIndex((h) => /3連対/.test(h)),
  };

  const out = {};
  for (const r of rows) {
    const rk = r[idx.rank]?.replace(/\s+/g, "");
    if (!rk) continue;
    out[rk] = {
      starts: parseNumber(r[idx.starts]),
      wins: parseNumber(r[idx.win]),
      top2Rate: parseNumber(r[idx.top2]),
      top3Rate: parseNumber(r[idx.top3]),
    };
  }
  return out;
}

async function fetchHtml(url) {
  const res = await fetch(url, {
    headers: {
      "user-agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36",
      "accept-language": "ja,en;q=0.8",
    },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return await res.text();
}

async function crawlOne(regno) {
  const url = `https://boatrace-db.net/racer/rcourse/regno/${regno}/`;
  const html = await fetchHtml(url);
  const $ = cheerio.load(html);

  const courseStats = parseCourseStats($);
  const decisionsByCourse = parseDecisionsByCourse($);
  const exTimeRankStats = parseExTimeRank($);

  return {
    regno,
    source: url,
    fetchedAt: new Date().toISOString(),
    courseStats,
    decisionsByCourse,
    exTimeRankStats,
  };
}

// --- main -------------------------------------------------------
async function main() {
  ensureDir(OUT_DIR);

  // 引数 or 環境変数で個別実行も可
  let targets = process.env.REGNOS
    ? process.env.REGNOS.split(",").map((s) => Number(s.trim()))
    : uniqueRegnosFromPrograms();

  if (!targets.length) {
    console.log("No regnos found. Pass REGNOS or ensure programs JSON exists.");
    process.exit(0);
  }

  console.log(`Targets: ${targets.length} racers`);

  // やさしめのレート（サイト負荷配慮）
  for (const [i, regno] of targets.entries()) {
    try {
      const data = await crawlOne(regno);
      const outPath = path.join(OUT_DIR, `${regno}.json`);
      fs.writeFileSync(outPath, JSON.stringify(data, null, 2));
      console.log(`[${i + 1}/${targets.length}] saved: ${outPath}`);
    } catch (e) {
      console.warn(`Failed regno=${regno}: ${e.message}`);
    }
    await sleep(700); // 0.7秒待機
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
