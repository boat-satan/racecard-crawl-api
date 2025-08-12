// scripts/predict.mjs
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

import envAdjust from "../src/predict/environmentAdjust.mjs";
import stPred from "../src/predict/predictedST.mjs";
import realClass from "../src/predict/realClass.mjs";
import slitAndAttack from "../src/predict/slitAndAttack.mjs";
import venueAdjust from "../src/predict/venueAdjust.mjs";
import upsetIndex from "../src/predict/upsetIndex.mjs";
import simulateRace from "../src/predict/simulateRace.mjs";
import bets from "../src/predict/bets.mjs";

// ---- JSONC reader (comments / trailing commas OK)
function readJsonLoose(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  const noComments = raw.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|\s)\/\/.*$/gm, "");
  const noTrailing = noComments.replace(/,\s*([\]}])/g, "$1");
  return JSON.parse(noTrailing);
}

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// ---- scenarios load (array or {scenarios:[...]})
const scenariosPath = path.join(__dirname, "../src/predict/scenarios.json");
let scenariosList = [];
try {
  const raw = readJsonLoose(scenariosPath);
  scenariosList = Array.isArray(raw) ? raw : (Array.isArray(raw?.scenarios) ? raw.scenarios : []);
} catch (e) {
  console.error("[ERROR] failed to load scenarios.json:", e?.message);
  scenariosList = [];
}
if (!Array.isArray(scenariosList)) {
  console.error("[ERROR] scenariosList is not an array. typeof:", typeof scenariosList);
  scenariosList = [];
}

// ---- input resolve
function to2(s){ return String(s).padStart(2, "0"); }
function resolveInputPath() {
  const argPath = process.argv[2];
  if (argPath) return path.resolve(argPath);
  const DATE = (process.env.DATE || "today").replace(/-/g, "");
  const PID  = to2(process.env.PID || "04");
  const RACE = ((process.env.RACE || "1R").toUpperCase().replace(/[^\d]/g, "") + "R");
  return path.join(__dirname, "..", "public", "integrated", "v1", DATE, PID, `${RACE}.json`);
}
const raceDataPath = resolveInputPath();
if (!fs.existsSync(raceDataPath)) {
  console.error(`[predict] integrated json not found: ${raceDataPath}`);
  process.exit(1);
}
const raceData = JSON.parse(fs.readFileSync(raceDataPath, "utf-8"));

const date = (raceData.date || (process.env.DATE || "today")).replace(/-/g, "");
const pid  = to2(raceData.pid || process.env.PID || "04");
const race = (raceData.race || process.env.RACE || "1R").toUpperCase().replace(/[^\d]/g, "") + "R";

// ---- preprocessing chain
let adjusted = envAdjust(raceData);
adjusted = stPred(adjusted);
adjusted = slitAndAttack(adjusted);
adjusted = realClass(adjusted);
adjusted = venueAdjust(adjusted);
adjusted = upsetIndex(adjusted);

// ---- make sure entries exists (for older simulateRace implementations)
function buildEntriesFallback(a) {
  if (Array.isArray(a?.entries)) return a.entries;
  if (Array.isArray(a?.ranking)) return a.ranking.map(r => ({ lane: Number(r.lane)||0, ...r }));
  if (Array.isArray(a?.slitOrder) && a.slitOrder.length) {
    return a.slitOrder.map(s => ({ lane: Number(s.lane)||0 }));
  }
  // fallback 1..6
  return Array.from({length:6}, (_,i)=>({ lane: i+1 }));
}
const adjustedForSim = { ...adjusted, entries: buildEntriesFallback(adjusted) };

// ---- scenario match
function matchScenario(sc, data) {
  if (sc?.requires?.attackType && !sc.requires.attackType.includes(data.attackType)) return false;
  if (sc?.requires?.head && !sc.requires.head.includes(data.head)) return false;
  return true;
}
console.log(`[predict] scenarios loaded: ${scenariosList.length}`);

const matchedScenarios = scenariosList
  .filter(sc => matchScenario(sc, adjusted))
  .map(sc => ({ ...sc, weight: (sc.baseWeight ?? 1) * (adjusted.venueWeight ?? 1) * (adjusted.upsetWeight ?? 1) }));

const baseList = Array.isArray(scenariosList) ? scenariosList : [];
const effectiveScenarios = matchedScenarios.length ? matchedScenarios : baseList.slice(0,3).map(sc => ({...sc, weight:1}));
console.log(`[predict] matched: ${matchedScenarios.length}, effective: ${effectiveScenarios.length}`);

// ---- normalize start order from slitOrder when scenario omits it
function normalizeStartOrder(oneMark, slitOrder=[]) {
  const so = Array.isArray(oneMark?.startOrder) ? oneMark.startOrder : [];
  return so.length ? so : (Array.isArray(slitOrder) ? slitOrder.map((s,i)=>({lane:s.lane, pos:i+1})) : []);
}

// ---- simulateRace compatibility wrapper
function runSimulate(sc) {
  const startOrder = normalizeStartOrder(sc.oneMark, adjusted.slitOrder || []);
  const linked = Array.isArray(sc.oneMark?.linkedPairs) ? sc.oneMark.linkedPairs.length : 0;
  const tpool  = Array.isArray(sc.oneMark?.thirdPool) ? sc.oneMark.thirdPool.length : 0;
  console.log(`[DEBUG] simulate ${sc.id} w=${sc.weight} startOrder=${startOrder.map(x=>x.lane).join("-")} linked=${linked} thirdPool=${tpool}`);

  let out;
  try {
    // 署名が (adjusted, oneMark) の場合
    if (simulateRace.length >= 2) {
      out = simulateRace(adjustedForSim, sc.oneMark);
    } else {
      // 署名が (startOrder, perfMap/laps...) の場合（第二引数は使われない想定）
      out = simulateRace(startOrder, sc.oneMark);
    }
  } catch (e) {
    console.error(`Error:  simulateRace failed for scenario: ${sc.id}`, e?.message);
    return {};
  }

  // 返り値が「確率マップ」ならそのまま
  if (out && !Array.isArray(out) && typeof out === "object") return out;

  // 返り値が「最終ポジション配列」なら 3連単キーに変換
  if (Array.isArray(out)) {
    const top3 = [...out].sort((a,b)=>a.pos-b.pos).slice(0,3);
    if (top3.length === 3) return { [`${top3[0].lane}-${top3[1].lane}-${top3[2].lane}`]: 1 };
    return {};
  }

  return {};
}

const scenarioResults = effectiveScenarios.map(sc => {
  const finalProb = runSimulate(sc);
  const keys = Object.keys(finalProb);
  console.log(`[DEBUG] result ${sc.id}: keys=${keys.length} sample=${keys[0] || "-"}`);
  return { id: sc.id, notes: sc.notes, oneMark: sc.oneMark, weight: sc.weight, finalProb };
});

// ---- aggregate & normalize
let aggregated = {};
let totalWeight = 0;
for (const sr of scenarioResults) {
  totalWeight += sr.weight || 0;
  for (const [k, p] of Object.entries(sr.finalProb || {})) {
    aggregated[k] = (aggregated[k] || 0) + (Number(p)||0) * (sr.weight||0);
  }
}
if (totalWeight > 0) for (const k of Object.keys(aggregated)) aggregated[k] /= totalWeight;
console.log(`[DEBUG] aggregated keys=${Object.keys(aggregated).length} (normalized by weight=${totalWeight.toFixed(3)})`);

// ---- bets (with compact fallback)
function buildCompactFromMain(main = []) {
  const byHead = new Map();
  for (const t of main) {
    if (!Array.isArray(t) || t.length < 3) continue;
    const [h,s,t3] = t.map(Number);
    if (!byHead.has(h)) byHead.set(h,{S:new Set(),T:new Set()});
    const g = byHead.get(h);
    if (s!==h) g.S.add(s);
    if (t3!==s && t3!==h) g.T.add(t3);
  }
  const chunks = [];
  for (const [h,g] of byHead.entries()) {
    const S=[...g.S].sort((a,b)=>a-b).join("");
    const T=[...g.T].sort((a,b)=>a-b).join("");
    if (S && T) chunks.push(`${h}-${S}-${T}`);
  }
  return chunks.join(", ");
}

let betResult;
try { betResult = bets(aggregated, 18); }
catch(e) { console.error("[ERROR] bets() failed:", e?.message); betResult = {compact:"", main:[], ana:[], markdown:"_no bets_"}; }

if ((!betResult?.compact || betResult.compact.length===0) && betResult?.main?.length>0) {
  const fb = buildCompactFromMain(betResult.main);
  if (fb) {
    betResult.compact = fb;
    betResult.markdown = fb;
    console.log("[INFO] generated fallback compact from main:", fb);
  }
}
if (!betResult?.markdown) betResult.markdown = betResult.compact || "_no bets_";

// ---- output files
const outDir = path.join("public", "predictions", date, pid, race);
fs.mkdirSync(outDir, { recursive: true });

const predictionData = {
  meta: { date, pid, race, sourceFile: path.relative(process.cwd(), raceDataPath), generatedAt: new Date().toISOString() },
  weather: adjusted.weather || null,
  ranking: adjusted.ranking || null,
  slitOrder: adjusted.slitOrder || null,
  attackType: adjusted.attackType || null,
  scenarios: scenarioResults.map(s => ({ id: s.id, notes: s.notes, oneMark: s.oneMark, weight: s.weight })),
  probs: aggregated,
  bets: { compact: betResult.compact, main: betResult.main, ana: betResult.ana }
};
fs.writeFileSync(path.join(outDir, "race.json"), JSON.stringify(predictionData, null, 2), "utf-8");

const lines = [];
lines.push(`# 予測 ${date} pid=${pid} race=${race}`);
if (adjusted?.weather) {
  const w = adjusted.weather;
  lines.push(`**気象**: ${w.weather ?? ""} / 気温${w.temperature ?? "-"}℃ 風${w.windSpeed ?? "-"}m ${w.windDirection ?? ""} 波高${w.waveHeight ?? "-"}m${w.stabilizer ? "（安定板）" : ""}`);
}
if (adjusted?.attackType) lines.push(`**想定攻め手**: ${adjusted.attackType}`);
lines.push("");
lines.push("## 買い目（compact）");
lines.push(betResult.markdown);
lines.push("");
lines.push("## 参考（上位評価）");
if (Array.isArray(adjusted.ranking)) {
  adjusted.ranking.slice(0, 6).forEach((r, i) => lines.push(`${i+1}. 枠${r.lane} 登番${r.number} ${r.name} score=${(r.score ?? 0).toFixed(2)}`));
}
fs.writeFileSync(path.join(outDir, "race.md"), lines.join("\n"), "utf-8");

console.log(`[predict] wrote:
- ${path.join(outDir, "race.json")}
- ${path.join(outDir, "race.md")}
`);