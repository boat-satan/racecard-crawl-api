// scripts/fetch-exhibition.js
// 目的: 展示(仮)データを同一リポジトリに保存
// 出力: public/exhibition/v1/<YYYYMMDD>/<PID>/<R>.json
// まずは boatraceopenapi の previews を暫定ソースに使用（後で差し替えやすい設計）

import fs from "node:fs/promises";
import path from "node:path";

const ROOT = process.cwd();
const OUT_ROOT = path.join(ROOT, "public", "exhibition", "v1");

// ---- env / inputs ----
// DATE=YYYYMMDD  or "today"
const DATE = (process.env.TARGET_DATE || "today").replace(/-/g, "");
// PIDS="02,09"（空なら全場は今は未実装：まずはカンマ列で運用）
const PIDS = (process.env.TARGET_PIDS || "02").split(",").map(s => s.trim()).filter(Boolean);
// RACES="1R,2R" or "1,2" or 空（空=1..12）
const RIN = (process.env.TARGET_RACES || "").split(",").map(s => s.trim()).filter(Boolean);
const RACES = RIN.length ? RIN.map(r => /R$/i.test(r)? r.toUpperCase() : `${r}R`) : Array.from({length:12}, (_,i)=>`${i+1}R`);

// ---- source (暫定) ----
const SOURCE_BASE = "https://boatraceopenapi.github.io/previews/v2";
// URL: `${SOURCE_BASE}/${YYYYMMDD}/${PID}/${RACE}.json`

async function fetchJson(url, {retries = 3, delay = 800} = {}) {
  for (let i=0;i<=retries;i++){
    const res = await fetch(url, {headers: {accept: "application/json"}});
    if (res.ok) return await res.json().catch(()=>null);
    if (i<retries) await new Promise(r=>setTimeout(r, delay));
  }
  throw new Error(`GET ${url} failed`);
}

async function main() {
  let ok = 0, miss = 0;

  for (const pid of PIDS) {
    for (const race of RACES) {
      const url = `${SOURCE_BASE}/${DATE}/${pid}/${race}.json`;
      try {
        const json = await fetchJson(url, {retries: 2});
        if (!json) throw new Error("invalid json");

        // 保存先
        const outDir = path.join(OUT_ROOT, DATE, pid);
        await fs.mkdir(outDir, {recursive: true});

        // そのまま保存（今は素直に格納。後でスキーマ整形する場合はここで加工）
        const outPath = path.join(outDir, `${race}.json`);
        await fs.writeFile(outPath, JSON.stringify(json, null, 2), "utf8");
        console.log(`✅ exhibition: ${path.relative(ROOT, outPath)}`);
        ok++;
      } catch (e) {
        console.warn(`⚠️  skip: ${url} :: ${e.message}`);
        miss++;
      }
      // 負荷をさげる
      await new Promise(r=>setTimeout(r, 400));
    }
  }

  // ヘルス
  const metaDir = path.join(ROOT, "public", "debug");
  await fs.mkdir(metaDir, {recursive:true});
  await fs.writeFile(
    path.join(metaDir, "exhibition-meta.json"),
    JSON.stringify({status:200, date:DATE, pids:PIDS, races:RACES, ok, miss, generatedAt:new Date().toISOString()}, null, 2),
    "utf8"
  );
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
