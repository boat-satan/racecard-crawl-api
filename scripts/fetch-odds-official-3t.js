// scripts/fetch-odds-official-3t.js
// 公式サイトの 3連単オッズをスクレイピングして保存
// 出力: public/odds/v1/<date>/<pid>/<race>.json
//
// 使い方:
//   node scripts/fetch-odds-official-3t.js <YYYYMMDD> <pid:01..24> <race:1..12>
//   環境変数: TARGET_DATE / TARGET_PID / TARGET_RACE / SKIP_EXISTING=1
//
// 例:
//   node scripts/fetch-odds-official-3t.js 20250812 01 1
//
// 仕組み:
//   4行×列バンドルで構成されたテーブルから、各バンドルの F(1着) を rowspan セルで取得。
//   バンドル内4行の “小さな数字セル” は T(3着)。{1..6} から F と 4つのTを除いた残り1つが S(2着)。
//   これで (F,S,T) → odds を全復元。

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { load as loadHTML } from "cheerio";

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const log = (...a)=>console.log("[odds3t]", ...a);

const DATE = (process.env.TARGET_DATE || process.argv[2] || "").replace(/-/g,"");
const PID  = (process.env.TARGET_PID  || process.argv[3] || "").padStart(2,"0");
const RACE = String(process.env.TARGET_RACE || process.argv[4] || "").replace(/[^0-9]/g,"");
const SKIP_EXISTING = /^(1|true|yes)$/i.test(String(process.env.SKIP_EXISTING||""));

if (!/^\d{8}$/.test(DATE) || !/^\d{2}$/.test(PID) || !/^(?:[1-9]|1[0-2])$/.test(RACE)) {
  console.error("Usage: node scripts/fetch-odds-official-3t.js <YYYYMMDD> <pid:01..24> <race:1..12>");
  process.exit(1);
}

function officialOdds3tUrl({date, pid, race}) {
  // 公式: /owpc/pc/race/odds3t?rno=<race>&jcd=<pid>&hd=<date>
  return `https://www.boatrace.jp/owpc/pc/race/odds3t?rno=${race}&jcd=${pid}&hd=${date}`;
}

async function fetchText(url) {
  const res = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118 Safari/537.36",
      "accept-language": "ja,en;q=0.8",
    }
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
  return await res.text();
}

const norm = (s)=>String(s||"").replace(/\s+/g," ").trim();
const toNum = (s)=> {
  const m = String(s||"").match(/-?\d+(?:\.\d+)?/);
  return m ? Number(m[0]) : NaN;
};

// テーブル（3連単オッズ）を特定
function findOddsTable($) {
  // 「3連単オッズ」の見出しの直後の table を優先
  let table = null;
  $("*").each((_, el) => {
    const t = norm($(el).text());
    if (/3連単オッズ/.test(t)) {
      const near = $(el).nextAll("div.table1, table").first();
      if (near && near.length) {
        table = near.is("table") ? near : near.find("table").first();
        return false;
      }
    }
    return;
  });
  // フォールバック: 全 table から th/td にオッズっぽい数が並ぶもの
  if (!table) {
    $("table").each((_, t) => {
      const head = norm($(t).find("thead").text());
      const body = norm($(t).find("tbody").text());
      if (/(1|2|3|4|5|6)/.test(head) && /\d+\.\d|\b\d{2,4}\b/.test(body)) {
        table = $(t);
        return false;
      }
    });
  }
  return table;
}

// 4行×列バンドルを走査して (F,S,T,odds) を復元
function parseTrifecta($, $table) {
  const $tbody = $table.find("tbody").first();
  const rows = $tbody.find("tr").toArray();
  const all = [];

  for (let i = 0; i < rows.length; i += 4) {
    const r0 = $(rows[i]), r1 = $(rows[i+1]), r2 = $(rows[i+2]), r3 = $(rows[i+3]);
    if (!r3 || !r2 || !r1) break;

    // row0 で「列バンドル」を検出
    const bundles = []; // [{F, values:[v0..v3], odds:[o0..o3]}]
    {
      const cells0 = r0.find("th,td").toArray();
      let k = 0;
      while (k < cells0.length) {
        const $c = $(cells0[k]);
        let F = null;
        if ($c.attr("rowspan") === "4") {
          F = toNum($c.text());
          k++;
        } else {
          // 安全側: rowspan が付与されてない変化形には対応しない
          // （公式は基本 rowspan=4 なのでスキップ）
          k++;
          continue;
        }
        const v0 = toNum($(cells0[k]).text()); k++;
        const o0 = toNum($(cells0[k]).text()); k++;

        if (Number.isFinite(F) && Number.isFinite(v0) && Number.isFinite(o0)) {
          bundles.push({ F, values: [v0], odds: [o0] });
        }
      }
    }

    // row1..row3 では bundles の順序に合わせて 2セルずつ読む
    const rowsLater = [r1, r2, r3];
    for (let ri = 0; ri < rowsLater.length; ri++) {
      const cells = rowsLater[ri].find("th,td").toArray();
      let k = 0;
      for (let b = 0; b < bundles.length; b++) {
        const v = toNum($(cells[k++]).text());
        const o = toNum($(cells[k++]).text());
        bundles[b].values.push(v);
        bundles[b].odds.push(o);
      }
    }

    // 各バンドルを (F,S,T,odds) に展開
    for (const b of bundles) {
      const F = b.F;
      const vs = b.values;      // 4つ → 3着候補
      const os = b.odds;        // 4つ → オッズ
      const thirdSet = new Set(vs);
      // {1..6} − {F} − {vs} = 残り1つ → 2着
      const remain = [1,2,3,4,5,6].filter(n => n !== F && !thirdSet.has(n));
      if (remain.length !== 1) {
        // 想定外レイアウトはスキップ（壊れた行）
        continue;
      }
      const S = remain[0];
      for (let j = 0; j < vs.length; j++) {
        const T = vs[j];
        const odds = os[j];
        if (Number.isFinite(odds)) {
          all.push({ combo: `${F}-${S}-${T}`, F, S, T, odds });
        }
      }
    }
  }

  // まとめ・ソート（人気順相当：オッズ昇順）
  // 重複（もしあれば）を最後に解消
  const map = new Map();
  for (const e of all) {
    if (!map.has(e.combo) || map.get(e.combo).odds !== e.odds) map.set(e.combo, e);
  }
  return [...map.values()].sort((a,b)=> a.odds - b.odds);
}

async function main() {
  const url = officialOdds3tUrl({ date: DATE, pid: PID, race: RACE });
  const outPath = path.join(__dirname, "..", "public", "odds", "v1", DATE, PID, `${RACE}R.json`);

  if (SKIP_EXISTING && fs.existsSync(outPath)) {
    log("skip existing:", path.relative(process.cwd(), outPath));
    return;
  }

  log("GET", url);
  const html = await fetchText(url);
  const $ = loadHTML(html);

  const $table = findOddsTable($);
  if (!$table || !$table.length) {
    throw new Error("odds table not found");
  }

  const trifecta = parseTrifecta($, $table);
  if (trifecta.length === 0) {
    throw new Error("no trifecta odds parsed");
  }

  const payload = {
    date: DATE, pid: PID, race: `${RACE}R`,
    source: { odds: url },
    generatedAt: new Date().toISOString(),
    trifecta // [{combo:"F-S-T", F,S,T, odds}, ...]  ※オッズ昇順
  };

  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  // .keep を各階層に（任意）
  for (const dir of [
    path.join(__dirname, "..", "public"),
    path.join(__dirname, "..", "public", "odds"),
    path.join(__dirname, "..", "public", "odds", "v1"),
    path.join(__dirname, "..", "public", "odds", "v1", DATE),
    path.join(__dirname, "..", "public", "odds", "v1", DATE, PID),
  ]) {
    try { fs.mkdirSync(dir, { recursive: true }); fs.writeFileSync(path.join(dir, ".keep"), ""); } catch {}
  }

  await fsp.writeFile(outPath, JSON.stringify(payload, null, 2), "utf8");
  log("saved:", path.relative(process.cwd(), outPath));
}

main().catch(e => { console.error(e); process.exit(1); });