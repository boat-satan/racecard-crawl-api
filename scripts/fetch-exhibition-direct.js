// scripts/fetch-exhibition-direct.js
import fs from "node:fs/promises";
import path from "node:path";
import fetch from "node-fetch";
import { load } from "cheerio";

/**
 * beforeinfo ページのURL生成
 */
function buildUrl(date, pid, raceNo) {
  return `https://www.boatrace.jp/owpc/pc/race/beforeinfo?hd=${date}&jcd=${pid}&rno=${raceNo}`;
}

/**
 * デバッグ用にHTMLを保存
 */
async function dumpHtmlForDebug({ html, date, pid, race }) {
  try {
    const outDir = path.join("public", "debug", "exhibition-html");
    await fs.mkdir(outDir, { recursive: true });
    const file = path.join(outDir, `${date}-${pid}-${race}.html`);
    await fs.writeFile(file, html, "utf8");
    console.log(`debug html saved: ${file}`);
  } catch (e) {
    console.warn("debug html save failed:", e.message);
  }
}

/**
 * beforeinfoページから展示情報を抽出
 */
async function fetchExhibition(date, pid, raceKey) {
  const raceNo = raceKey.replace(/\D/g, "");
  const url = buildUrl(date, pid, raceNo);

  console.log(`Fetching exhibition: ${url}`);
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }

  const html = await res.text();
  const $ = load(html);

  const entries = [];

  // セレクタは beforeinfo の「展示タイム・進入・ST」テーブルに合わせる
  $(".is-tableFixed__3rdadd tr").each((i, el) => {
    const tds = $(el).find("td");
    if (tds.length < 5) return;

    const lane = $(tds[0]).text().trim();
    const name = $(tds[1]).text().trim();
    const exTime = $(tds[4]).text().trim();
    const st = $(tds[3]).text().trim();

    if (lane && name) {
      entries.push({
        lane,
        name,
        st,
        exTime
      });
    }
  });

  const result = {
    date,
    pid,
    race: raceKey,
    source: url,
    generatedAt: new Date().toISOString(),
    entries
  };

  if (entries.length === 0) {
    await dumpHtmlForDebug({ html, date, pid, race: raceKey });
  }

  return result;
}

/**
 * メイン処理
 */
async function main() {
  const date = process.env.TARGET_DATE || "today";
  const pid = process.env.TARGET_PIDS || "02";
  const races = (process.env.TARGET_RACES || "").split(",").filter(Boolean);

  const targetDate = date === "today" ? new Date().toISOString().slice(0, 10).replace(/-/g, "") : date;

  for (const race of races) {
    try {
      const data = await fetchExhibition(targetDate, pid, race);
      const outDir = path.join("public", "exhibition", "v1", targetDate, pid);
      await fs.mkdir(outDir, { recursive: true });
      const file = path.join(outDir, `${race}.json`);
      await fs.writeFile(file, JSON.stringify(data, null, 2));
      console.log(`saved: ${file}`);
    } catch (e) {
      console.error(`Failed to fetch ${race}:`, e.message);
    }
  }
}

main();
