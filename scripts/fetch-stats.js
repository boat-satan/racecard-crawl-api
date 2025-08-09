// scripts/fetch-stats.js
// Node.js 20+ / GitHub Actions 実行前提
import fs from "node:fs/promises";
import path from "node:path";
import cheerio from "cheerio";

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36";
const OUT_DIR = "public/stats/v1/racer";

const racers = (process.env.RACERS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

if (!racers.length) {
  console.error("使い方: RACERS=4349,4103 node scripts/fetch-stats.js");
  process.exit(1);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchText(url) {
  const r = await fetch(url, { headers: { "User-Agent": UA, "Accept-Language": "ja,en;q=0.8" } });
  if (!r.ok) throw new Error(`fetch ${r.status} ${url}`);
  return await r.text();
}

// --------- parsers (堅めのセレクタ／テキスト併用) ---------
function toInt(s) {
  const n = parseInt(String(s ?? "").replace(/[^\d\-]/g, ""), 10);
  return Number.isFinite(n) ? n : null;
}

function parseCourseTables(html, course) {
  // 返値: { byCourse: { [course]: {starts, win, top2, top3, avgSt|null} }, kimariteByCourse: { [course]: {...}} }
  const $ = cheerio.load(html);
  const byCourse = {};
  const kimariteByCourse = {};

  // 進入時の全艇成績（自艇行だけ）
  // 近い見出しを頼りに次の table を拾う
  const t1 = $("h2,h3,h4").filter((_, el) => $(el).text().includes("進入時の全艇成績")).first().nextAll("table").first();
  if (t1 && t1.length) {
    t1.find("tbody tr").each((_, tr) => {
      const tds = $(tr).find("td").map((i, td) => $(td).text().trim()).get();
      // 例: ['4 コース （自艇）','14','1','0','6', ...] など
      const label = tds[0] || "";
      const isSelf = /自艇/.test(label);
      if (isSelf) {
        byCourse[course] = {
          starts: toInt(tds[1]),
          win: toInt(tds[2]),
          top2: toInt(tds[3]),
          top3: toInt(tds[4]),
          avgSt: null // 同表に平均STが無いことが多い
        };
      }
    });
  }

  // 進入時の全艇決まり手（自艇行）
  const t2 = $("h2,h3,h4").filter((_, el) => $(el).text().includes("進入時の全艇決まり手")).first().nextAll("table").first();
  if (t2 && t2.length) {
    t2.find("tbody tr").each((_, tr) => {
      const tds = $(tr).find("td").map((i, td) => $(td).text().trim()).get();
      const label = tds[0] || "";
      const isSelf = /自艇/.test(label);
      if (isSelf) {
        kimariteByCourse[course] = {
          starts: toInt(tds[1]),
          nige: toInt(tds[2]), // 1コース時に意味が立つが統一で保持
          sashi: toInt(tds[3]),
          makuri: toInt(tds[4]),
          makurizashi: toInt(tds[5]),
          nuki: toInt(tds[6]),
          other: toInt(tds[7])
        };
      }
    });
  }

  return { byCourse, kimariteByCourse };
}

function parseDemoRank(html) {
  const $ = cheerio.load(html);
  const out = { byRank: {} };
  const t = $("h2,h3,h4").filter((_, el) => $(el).text().includes("展示タイム順位別成績")).first().nextAll("table").first();
  if (t && t.length) {
    t.find("tbody tr").each((_, tr) => {
      const tds = $(tr).find("td").map((i, td) => $(td).text().trim()).get();
      // 例: ['1 位','33','7','7','7', ...]
      const rank = (tds[0] || "").replace(/[^\d]/g, "");
      if (!rank) return;
      out.byRank[rank] = {
        starts: toInt(tds[1]),
        win: toInt(tds[2]),
        top2: toInt(tds[3]),
        top3: toInt(tds[4])
      };
    });
  }
  return out;
}

// --------- main ---------
for (const regno of racers) {
  const baseOut = path.join(OUT_DIR, regno);
  await fs.mkdir(baseOut, { recursive: true });

  // コース別: 1..6 を回す
  const byCourse = {};
  const kimariteByCourse = {};

  for (let c = 1; c <= 6; c++) {
    const url = `https://boatrace-db.net/racer/rcourse/regno/${regno}/course/${c}/`;
    try {
      const html = await fetchText(url);
      const { byCourse: bc, kimariteByCourse: kc } = parseCourseTables(html, String(c));
      Object.assign(byCourse, bc);
      Object.assign(kimariteByCourse, kc);
    } catch (e) {
      console.error(`[${regno}] course ${c} fetch error:`, e.message);
    }
    // 約束の3秒間隔
    await sleep(3000);
  }

  // 展示タイム順位別
  let demo = { byRank: {} };
  try {
    const url = `https://boatrace-db.net/racer/rdemo/regno/${regno}/`;
    const html = await fetchText(url);
    demo = parseDemoRank(html);
  } catch (e) {
    console.error(`[${regno}] demo fetch error:`, e.message);
  }
  await sleep(3000);

  const now = new Date().toISOString();

  await fs.writeFile(
    path.join(baseOut, "course.json"),
    JSON.stringify({ schemaVersion: "1.0", racer: Number(regno), updatedAt: now, byCourse }, null, 2)
  );

  await fs.writeFile(
    path.join(baseOut, "kimarite.json"),
    JSON.stringify({ schemaVersion: "1.0", racer: Number(regno), updatedAt: now, byCourse: kimariteByCourse }, null, 2)
  );

  await fs.writeFile(
    path.join(baseOut, "extime-rank.json"),
    JSON.stringify({ schemaVersion: "1.0", racer: Number(regno), updatedAt: now, window: { months: 6 }, ...demo }, null, 2)
  );

  console.log("done:", regno);
}

console.log("all done.");
