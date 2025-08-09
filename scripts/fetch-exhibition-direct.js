// scripts/fetch-exhibition-direct.js
// Node v20 / ESM / cheerio v1.x

import { load } from "cheerio";
import fs from "node:fs/promises";
import path from "node:path";

// -------- args (flags 無視) --------
const args = process.argv.slice(2).filter(a => !a.startsWith("-"));
const [ DATE_IN = "today", PID_IN = "02", RACE_IN = "1R" ] = args;

function to2(v){ return String(v).padStart(2,"0"); }
function raceKey(r){ return `${String(r).replace(/[^\d]/g,"")}R`; }
function yyyymmdd(x){
  if (x === "today") {
    const d = new Date();
    const y = d.getFullYear(), m = String(d.getMonth()+1).padStart(2,"0"), dd = String(d.getDate()).padStart(2,"0");
    return `${y}${m}${dd}`;
  }
  const s = String(x).replace(/-/g,"");
  // 8桁以外が来たら today にフォールバック
  return /^\d{8}$/.test(s) ? s : yyyymmdd("today");
}

function urlBeforeInfo(date, pid, rno){
  return `https://www.boatrace.jp/owpc/pc/race/beforeinfo?hd=${date}&jcd=${pid}&rno=${rno}`;
}
function urlRaceList(date, pid, rno){
  return `https://www.boatrace.jp/owpc/pc/race/racelist?hd=${date}&jcd=${pid}&rno=${rno}`;
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

const norm = (t) => (t ?? "").replace(/\u00A0/g," ").replace(/\s+/g," ").trim();

function extractRegnoFrom($el){
  const href = $el.attr("href") || "";
  const m = href.match(/[?&]rno=(\d{4})/);
  return m ? Number(m[1]) : null;
}

// beforeinfo から {lane, number, name, st, stFlag}
function parseBeforeInfo(html){
  const $ = load(html);

  const entriesByLane = new Map();
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
      if (!name){
        const guess = $tr.find("td").toArray().map(td => norm($(td).text()))
          .find(t => /[^\x00-\x7F]/.test(t) && t.length>=2);
        if (guess) name = guess.split(/\s/)[0] || null;
      }
      entriesByLane.set(lane, { lane, number, name });
    });
  });

  const stByLane = {};
  $("table").each((_, el) => {
    const head = norm($(el).find("thead, caption").text());
    if (!/ST|スタート/.test(head)) return;
    $(el).find("tbody tr").each((__, tr) => {
      const $tr = $(tr);
      const cells = $tr.find("th,td");
      if (cells.length < 2) return;
      const lane = Number(norm($(cells[0]).text()));
      const stTxt = norm($(cells[cells.length-1]).text());
      if (!Number.isFinite(lane)) return;

      let st = null, stFlag = null;
      if (/^[FL]/i.test(stTxt)) stFlag = stTxt[0].toUpperCase();
      const m = stTxt.match(/-?\.?\d+(?:\.\d+)?/);
      if (m) st = Number(m[0]);

      stByLane[lane] = { st, stFlag };
    });
  });

  const out = [];
  for (let lane=1; lane<=6; lane++){
    const base = entriesByLane.get(lane) || { lane };
    const stPart = stByLane[lane] || {};
    if (!entriesByLane.has(lane) && !stByLane[lane]) continue;
    out.push({ ...base, st: stPart.st ?? null, stFlag: stPart.stFlag ?? null });
  }
  return out.sort((a,b)=>a.lane-b.lane);
}

// racelist フォールバック
function parseRaceList(html){
  const $ = load(html);
  const out = [];
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
      out.push({ lane, number, name });
    });
  });
  return out.sort((a,b)=>a.lane-b.lane);
}

async function ensureDir(p){ await fs.mkdir(p, { recursive: true }); }

async function main(){
  const date = yyyymmdd(DATE_IN);
  const pid  = to2(PID_IN);
  const rno  = String(RACE_IN).replace(/[^\d]/g,"");
  const race = raceKey(RACE_IN);

  let mode = "beforeinfo";
  let src  = urlBeforeInfo(date, pid, rno);
  let entries = [];
  try{
    const html = await fetchHtml(src);
    entries = parseBeforeInfo(html);
  }catch{
    mode = "racelist";
    src  = urlRaceList(date, pid, rno);
    try{
      const html2 = await fetchHtml(src);
      entries = parseRaceList(html2);
    }catch{
      entries = [];
    }
  }

  const payload = { date, pid, race, source: src, mode, generatedAt: new Date().toISOString(), entries };
  const outDir = path.join("public","exhibition","v1", date, pid);
  await ensureDir(outDir);
  await fs.writeFile(path.join(outDir, `${race}.json`), JSON.stringify(payload, null, 2), "utf8");
  console.log(`wrote public/exhibition/v1/${date}/${pid}/${race}.json (mode=${mode}, entries=${entries.length})`);
}

main().catch(e=>{ console.error(e); process.exit(1); });
