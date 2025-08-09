// 展示 -> 出走表(= programs) 統合スクリプト
// 対象: public/exhibition/v1/<date>/<pid>/<race>.json
// マージ先候補(優先順):
//   1) public/programs/v2/today/<pid>/<race>.json
//   2) public/programs/v2/<date>/<pid>/<race>.json
//   3) public/startlist/v1/<date>/<pid>/<race>.json (フォールバック)
//
// 付加: entry.exhibition = { lane, weight, tenjiTime, tilt, st, stFlag }
// 進入反映: APPLY_COURSE_FROM_EXHIBITION = true なら course/lane を上書き

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";

const APPLY_COURSE_FROM_EXHIBITION = true;

function log(...a){ console.log("[merge-exh]", ...a); }
async function readJSON(p){ return JSON.parse(await fsp.readFile(p, "utf-8")); }
async function writeJSON(p,o){ await fsp.mkdir(path.dirname(p),{recursive:true}); await fsp.writeFile(p, JSON.stringify(o,null,2)+"\n"); }
function listArg(raw){ return (raw||"").split(",").map(s=>s.trim()).filter(Boolean); }

function indexExhibition(exh){
  const byNumber=new Map(), byLane=new Map();
  for(const e of exh.entries||[]){
    const num=(e.number||"").trim();
    if(num) byNumber.set(num,e);
    if(Number.isInteger(e.lane)&&e.lane>=1&&e.lane<=6) byLane.set(e.lane,e);
  }
  return {byNumber, byLane};
}

function mergeOne(start, exh, idx){
  if(!Array.isArray(start.entries)) return {changed:false, start};
  let changed=false;

  for(const entry of start.entries){
    const num=(entry.number||"").trim();
    const laneFallback = Number.isInteger(entry.lane) ? entry.lane :
                         Number.isInteger(entry.course) ? entry.course : undefined;

    const src = (num && idx.byNumber.get(num)) || (laneFallback && idx.byLane.get(laneFallback));
    if(!src) continue;

    const newExh = {
      lane: src.lane,
      weight: src.weight || "",
      tenjiTime: src.tenjiTime || "",
      tilt: src.tilt || "",
      st: src.st || "",
      stFlag: src.stFlag || "",
    };

    const prev = JSON.stringify(entry.exhibition||{});
    const next = JSON.stringify(newExh);
    if(prev!==next){ entry.exhibition=newExh; changed=true; }

    if(APPLY_COURSE_FROM_EXHIBITION && Number.isInteger(src.lane)){
      if(entry.course!==src.lane){ entry.course=src.lane; changed=true; }
      if(entry.lane!==undefined && entry.lane!==src.lane){ entry.lane=src.lane; changed=true; }
    }
  }

  const meta = start.meta||{};
  const mergedMeta = {
    ...meta,
    lastMergedExhibitionAt: new Date().toISOString(),
    mergedFrom: {
      date: exh.date, pid: exh.pid, race: exh.race,
      source: exh.source, generatedAt: exh.generatedAt,
    },
  };
  if(JSON.stringify(meta)!==JSON.stringify(mergedMeta)){ start.meta=mergedMeta; changed=true; }

  return {changed, start};
}

function basenameRace(filePath){
  // ".../7R.json" -> "7R.json"
  return path.basename(filePath);
}

function deriveProgramTargets(exhJsonPath, exhJson){
  // 展示ファイル名の "7R.json" をそのまま使う
  const raceFile = basenameRace(exhJsonPath);
  const { date, pid } = exhJson; // pidは "02" など

  return [
    path.posix.join("public","programs","v2","today", pid, raceFile),         // 1st
    path.posix.join("public","programs","v2", date,  pid, raceFile),          // 2nd
    // 旧構成フォールバック
    path.posix.join("public","startlist","v1", date, pid, raceFile),          // 3rd
  ];
}

async function pickExisting(paths){
  for(const p of paths){
    if(fs.existsSync(p)) return p;
  }
  return null;
}

async function processOne(exhPath){
  if(!fs.existsSync(exhPath)){ log("skip (no exhibition):", exhPath); return; }

  const exh = await readJSON(exhPath);
  const targets = deriveProgramTargets(exhPath, exh);
  const startPath = await pickExisting(targets);

  if(!startPath){ log("skip (no startlist/programs found):", targets.join(" | ")); return; }

  const start = await readJSON(startPath);
  const idx = indexExhibition(exh);
  const {changed, start:merged} = mergeOne(start, exh, idx);
  if(!changed){ log("no changes:", startPath); return; }

  await writeJSON(startPath, merged);
  log("merged ->", startPath);
}

async function main(){
  const files = listArg(process.argv.slice(2).join(",") || process.env.EXHIBITION_FILES);
  if(files.length===0){ console.error("No exhibition files given."); return; }
  for(const f of files){
    try{ await processOne(f); }
    catch(e){ console.error("Failed to merge:", f, String(e)); }
  }
}
main().catch(e=>{ console.error(e); process.exit(1); });
