// scripts/crawl.js
import fs from "node:fs";
import path from "node:path";
import * as cheerio from "cheerio";

const BASE_OUT = "public/programs-slim/v2";
const UA = "Mozilla/5.0 (compatible; BoatCrawler/1.0; +https://example.com/contact)";
const DELAY_MS = 3000; // サイトの許可に従い 3 秒スリープ

// boatrace-db の pid と場コードは同じ想定
const STADIUMS = {
  "01":"桐生","02":"戸田","03":"江戸川","04":"平和島","05":"多摩川","06":"浜名湖","07":"蒲郡","08":"常滑",
  "09":"津","10":"三国","11":"びわこ","12":"住之江","13":"尼崎","14":"鳴門","15":"丸亀","16":"児島",
  "17":"宮島","18":"徳山","19":"下関","20":"若松","21":"芦屋","22":"福岡","23":"唐津","24":"大村"
};

const sleep = (ms)=> new Promise(r=>setTimeout(r, ms));
const jstDate = (d=new Date())=>{
  const s = new Intl.DateTimeFormat("ja-JP",{timeZone:"Asia/Tokyo",year:"numeric",month:"2-digit",day:"2-digit"}).format(d);
  return s.replaceAll("/","");
};
const ensureDir = (p)=> fs.mkdirSync(p, { recursive:true });

async function fetchText(url) {
  const r = await fetch(url, { headers:{ "user-agent": UA, "accept":"text/html,application/xhtml+xml" }});
  if (!r.ok) throw new Error(`fetch ${r.status}: ${url}`);
  return r.text();
}

function raceDetailUrl(dateStr, pid, rno2) {
  return `https://boatrace-db.net/race/detail/date/${dateStr}/pid/${pid}/rno/${rno2}/`;
}
function toRno2(n){ return String(n).padStart(2, "0"); }

async function crawlVenue(dateStr, pid) {
  const stadiumName = STADIUMS[pid] || pid;
  for (let r=1; r<=12; r++) {
    const rno2 = toRno2(r);
    const url = raceDetailUrl(dateStr, pid, rno2);
    try {
      const html = await fetchText(url);
      const payload = parseRaceDetail(html, {
        date: dateStr,
        stadium: pid,
        stadiumName,
        raceId: `${r}R`,
      });
      if ((payload.entries||[]).length >= 1) {
        writeRaceJson(payload);
        console.log(`ok: ${pid} ${payload.race} (${payload.entries.length} entries)`);
      } else {
        console.warn(`warn: ${pid} ${r}R entries=0 (未掲載かセレクタ未一致)`);
      }
    } catch (e) {
      console.warn(`skip: ${pid} ${r}R -> ${e.message}`);
    }
    await sleep(DELAY_MS);
  }
}

function parseRaceDetail(html, meta) {
  const $ = cheerio.load(html);

  // 例: 〆切時刻（ページにより差があるので緩めに取得）
  const deadline =
    $("time").filter((_,el)=>/[:：]/.test($(el).text())).first().text().trim() ||
    $('td:contains("締切")').next().first().text().trim() ||
    null;

  // ▼▼ テーブル選定：ヘッダに「枠/艇/選手」が含まれる table を拾う（必要なら後で変える）▼▼
  let targetTable = null;
  $("table").each((_, tbl)=>{
    const headText = $(tbl).find("th, thead").text();
    if (/枠|艇|選手/.test(headText)) { targetTable = $(tbl); return false; }
  });

  const entries = [];
  if (targetTable) {
    targetTable.find("tr").each((_, tr)=>{
      const tds = $(tr).find("td");
      if (tds.length < 3) return;

      // 想定: 枠 / 登番 / 選手名 / 支部 / 級
      const lane = Number($(tds[0]).text().replace(/[^\d]/g,""));
      if (!(lane>=1 && lane<=6)) return;

      const number = toNumOrNull($(tds[1]).text());
      const name   = toTextOrNull($(tds[2]).text());
      const branch = toTextOrNull($(tds[3]).text());
      const klass  = toTextOrNull($(tds[4]).text());

      entries.push({ lane, number, name, class: klass, branch });
    });
  }

  return {
    date: meta.date,
    stadium: meta.stadium,
    stadiumName: meta.stadiumName,
    race: meta.raceId,
    deadline,
    entries
  };
}

function writeRaceJson(payload) {
  const outDir = path.join(BASE_OUT, payload.date, payload.stadium);
  ensureDir(outDir);
  fs.writeFileSync(path.join(outDir, `${payload.race}.json`), JSON.stringify(payload, null, 2), "utf8");

  // 場 index.json（軽量）も更新
  const idxPath = path.join(outDir, "index.json");
  let venue = fs.existsSync(idxPath) ? JSON.parse(fs.readFileSync(idxPath, "utf8")) : { stadium: payload.stadium, stadiumName: payload.stadiumName, races: [] };
  const slim = { race: payload.race, deadline: payload.deadline, entries: payload.entries.map(e=>({ lane:e.lane, name:e.name, class:e.class })) };
  const i = venue.races.findIndex(r => r.race === payload.race);
  if (i>=0) venue.races[i] = slim; else venue.races.push(slim);
  fs.writeFileSync(idxPath, JSON.stringify(venue, null, 2), "utf8");
}

function toNumOrNull(s){ const n = parseInt(String(s).replace(/[^\d]/g,""),10); return Number.isFinite(n)? n : null; }
function toTextOrNull(s){ const t = String(s||"").trim().replace(/\s+/g," "); return t || null; }

(async () => {
  const dates = (process.env.TARGET_DATES || "").split(",").map(s=>s.trim()).filter(Boolean);
  const dateList = dates.length ? dates : [ jstDate() ];
  const targets = (process.env.TARGET_STADIUMS || "02").split(",").map(s=>s.trim()).filter(Boolean); // まずは pid=02（戸田）だけ
  for (const d of dateList) {
    for (const pid of targets) {
      await crawlVenue(d, pid);
      await sleep(DELAY_MS);
    }
  }
  console.log("done");
})();
