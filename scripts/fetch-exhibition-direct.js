// scripts/fetch-exhibition-direct.js
// Node v20 / ESM / cheerio v1.x
// 使い方: node scripts/fetch-exhibition-direct.js 20250809 02 4R
// 出力: public/exhibition/v1/<date>/<pid>/<race>.json

import { load } from "cheerio";
import fs from "node:fs/promises";
import path from "node:path";

const [,, ARG_DATE="today", ARG_PID="02", ARG_RACE="1R"] = process.argv;

function to2(v){ return String(v).padStart(2,"0"); }
function raceKey(r){ return `${String(r).replace(/[^\d]/g,"")}R`; }
function yyyymmdd(dateLike){
  if (dateLike === "today") {
    const d = new Date();
    const y = d.getFullYear(), m = String(d.getMonth()+1).padStart(2,"0"), dd = String(d.getDate()).padStart(2,"0");
    return `${y}${m}${dd}`;
  }
  return String(dateLike).replace(/-/g,"");
}

function urlBeforeInfo(date, pid, raceNo){
  return `https://www.boatrace.jp/owpc/pc/race/beforeinfo?hd=${date}&jcd=${pid}&rno=${raceNo}`;
}
function urlRaceList(date, pid, raceNo){
  return `https://www.boatrace.jp/owpc/pc/race/racelist?hd=${date}&jcd=${pid}&rno=${raceNo}`;
}

async function fetchHtml(url){
  const res = await fetch(url, {
    headers: {
      "user-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118 Safari/537.36",
      "accept":"text/html,application/xhtml+xml"
    }
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return await res.text();
}

function norm(t){ return (t ?? "").replace(/\u00A0/g," ").replace(/\s+/g," ").trim(); }

// プロフィールリンク（/racersearch/profile?...rno=XXXX）から登録番号
function extractRegnoFrom($el){
  const href = $el.attr("href") || "";
  const m = href.match(/[?&]rno=(\d{4})/);
  return m ? Number(m[1]) : null;
}

/** beforeinfo: 6艇の選手（名前/登録番号）＋展示ST */
function parseBeforeInfo(html){
  const $ = load(html);

  // 1) 6艇の行（左大テーブル）から lane / name / regno
  //   - 先頭セルが1..6の行を対象にする
  const entriesMap = new Map(); // lane -> {lane, number, name}
  const candidateTables = $("table");
  candidateTables.each((_, el) => {
    $(el).find("tbody tr").each((__, tr) => {
      const $tr = $(tr);
      const first = norm($tr.find("th,td").first().text());
      if (!/^[1-6]$/.test(first)) return;
      const lane = Number(first);
      // 選手プロフィールリンク
      const $profile = $tr.find('a[href*="/racersearch/profile"]').first();
      let number = null, name = null;
      if ($profile.length){
        number = extractRegnoFrom($profile);
        name = norm($profile.text());
      }
      if (!name) {
        // 日本語文字を含むセルから推測（保険）
        const guess = $tr.find("td").toArray()
          .map(td => norm($(td).text()))
          .find(t => /[^\x00-\x7F]/.test(t) && t.length >= 2);
        if (guess) name = guess.split(/\s/)[0] || null;
      }
      if (lane) entriesMap.set(lane, { lane, number, name });
    });
  });

  // 2) 右サイドの「スタート展示」テーブルからST
  //    見出しに「コース」「並び」「ST」あたりが出る表を探す
  const stByLane = {};
  candidateTables.each((_, el) => {
    const headTxt = norm($(el).find("thead, caption").text());
    const looksLike = /ST/.test(headTxt) || /スタート/.test(headTxt);
    if (!looksLike) return;

    $(el).find("tbody tr").each((__, tr) => {
      const $tr = $(tr);
      // 1列目: コース(=枠番)、2列目: 並び/艇色、3列目: ST
      const cells = $tr.find("th,td");
      if (cells.length < 2) return;
      const c1 = norm($(cells[0]).text());
      const cSt = norm($(cells[cells.length-1]).text()); // 末尾をSTとみなす
      const lane = Number(c1);
      if (Number.isFinite(lane) && lane>=1 && lane<=6) {
        // ST表記: ".13" / "F.03" / "L.10"
        let st = null, flag = null;
        if (/^[FL]/i.test(cSt)) { flag = cSt[0].toUpperCase(); }
        const m = cSt.match(/-?\.?\d+(?:\.\d+)?/);
        if (m) st = Number(m[0]);
        stByLane[lane] = { st, flag }; // flag: 'F'|'L'|null
      }
    });
  });

  // マージして配列へ
  const result = [];
  for (let lane = 1; lane <= 6; lane++){
    if (!entriesMap.has(lane) && !stByLane[lane]) continue;
    const base = entriesMap.get(lane) || { lane };
    const st = stByLane[lane]?.st ?? null;
    const stFlag = stByLane[lane]?.flag ?? null;
    result.push({ ...base, st, stFlag });
  }
  // 6艇揃わない場合も返す（未掲載のときがあるため）
  return result.sort((a,b)=>a.lane - b.lane);
}

/** racelist フォールバック: 名前/登録番号だけ */
function parseRaceList(html){
  const $ = load(html);
  const entries = [];
  $("table").each((_, el) => {
    $(el).find("tbody tr").each((__, tr) => {
      const $tr = $(tr);
      const first = norm($tr.find("th,td").first().text());
      if (!/^[1-6]$/.test(first)) return;
      const lane = Number(first);
      const $profile = $tr.find('a[href*="/racersearch/profile"]').first();
      let number = null, name = null;
      if ($profile.length){
        number = extractRegnoFrom($profile);
        name = norm($profile.text());
      }
      if (lane) entries.push({ lane, number, name });
    });
  });
  return entries.sort((a,b)=>a.lane-b.lane);
}

async function ensureDir(p){ await fs.mkdir(p, { recursive: true }); }

async function main(){
  const date = yyyymmdd(ARG_DATE);
  const pid  = to2(ARG_PID);
  const raceNo = String(ARG_RACE).replace(/[^\d]/g,"");
  const race = raceKey(ARG_RACE);

  let src = urlBeforeInfo(date, pid, raceNo);
  let entries = [];
  let used = "beforeinfo";

  try{
    const html = await fetchHtml(src);
    entries = parseBeforeInfo(html);
  }catch(e){
    // beforeinfo が未公開の場合は racelist にフォールバック
    used = "racelist";
    src = urlRaceList(date, pid, raceNo);
    try{
      const html2 = await fetchHtml(src);
      entries = parseRaceList(html2);
    }catch(e2){
      // どちらもダメ
      entries = [];
    }
  }

  const payload = {
    date, pid, race,
    source: src,
    mode: used,              // "beforeinfo" or "racelist"
    generatedAt: new Date().toISOString(),
    entries                   // [{lane, number, name, st, stFlag}]
  };

  const outDir = path.join("public", "exhibition", "v1", date, pid);
  await ensureDir(outDir);
  await fs.writeFile(path.join(outDir, `${race}.json`), JSON.stringify(payload, null, 2), "utf8");
  console.log(`wrote: public/exhibition/v1/${date}/${pid}/${race}.json (mode=${used}, entries=${entries.length})`);
}

main().catch(e => { console.error(e); process.exit(1); });
