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

// ---- JSONC reader
function readJsonLoose(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  const noComments = raw.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|\s)\/\/.*$/gm, "");
  const noTrailing = noComments.replace(/,\s*([\]}])/g, "$1");
  return JSON.parse(noTrailing);
}

const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// ---- scenarios load
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

// ---- preprocessing
let adjusted = envAdjust(raceData);
adjusted = stPred(adjusted);
adjusted = slitAndAttack(adjusted);
adjusted = realClass(adjusted);
adjusted = venueAdjust(adjusted);
adjusted = upsetIndex(adjusted);

// entries フォールバック（simulateRace が entries 参照するため）
function buildEntriesFallback(a) {
  if (Array.isArray(a?.entries)) return a.entries;
  if (Array.isArray(a?.ranking)) return a.ranking.map(r => ({ lane: Number(r.lane)||0, ...r }));
  if (Array.isArray(a?.slitOrder) && a.slitOrder.length) {
    return a.slitOrder.map(s => ({ lane: Number(s.lane)||0 }));
  }
  return Array.from({length:6}, (_,i)=>({ lane: i+1 }));
}
adjusted = { ...adjusted, entries: buildEntriesFallback(adjusted) };

// ---- scenario match（A〜D全部対象）
/**
 * ルール:
 *  - attackType: 指定があれば必ず一致（データ無ければ不一致）
 *  - bigDelayLane: 指定があれば必ず一致（データ無ければ不一致）→ Dは検出時のみ通す
 *  - それ以外（head/inReliability/wind/各種フラグ）は「data側に値がある時だけ」厳格一致。無ければ無視
 */
function matchScenario(sc, data) {
  const req = sc?.requires || {};

  // 必須一致系
  if (req.attackType) {
    if (!data.attackType || !req.attackType.includes(data.attackType)) return false;
  }
  if (req.bigDelayLane !== undefined) {
    if (data.bigDelayLane === undefined) return false;
    if (req.bigDelayLane !== data.bigDelayLane) return false;
  }

  // 任意一致系（データがあるときだけ判定）
  if (req.head) {
    if (data.head && !req.head.includes(data.head)) return false;
  }
  if (req.inReliability) {
    if (data.inReliability && req.inReliability !== data.inReliability) return false;
  }
  if (req.wind) {
    const g = data.weather?.windDirectionGroup;
    if (g && req.wind !== g) return false;
  }

  const optKeys = [
    "dashAdvantage","outerNobi","twoCoursePassive","threeCourseAggressive",
    "outerFollow","slitLeader","slitPusher","inAvgST"
  ];
  for (const k of optKeys) {
    if (req[k] !== undefined && data[k] !== undefined && req[k] !== data[k]) return false;
  }

  return true;
}

console.log(`[predict] scenarios loaded: ${scenariosList.length}`);
const matchedScenarios = scenariosList
  .filter(sc => matchScenario(sc, adjusted))
  .map(sc => ({ ...sc, weight: (sc.baseWeight ?? 1) * (adjusted.venueWeight ?? 1) * (adjusted.upsetWeight ?? 1) }));

// タイプ別マッチ件数ログ
const typeCount = { A:0, B:0, C:0, D:0 };
for (const sc of matchedScenarios) {
  const t = String(sc.id || "").charAt(0);
  if (typeCount[t] !== undefined) typeCount[t]++;
}
console.log(`[match] A:${typeCount.A} B:${typeCount.B} C:${typeCount.C} D:${typeCount.D} total:${matchedScenarios.length}`);

const effectiveScenarios = matchedScenarios.length ? matchedScenarios : scenariosList.map(sc => ({
  ...sc, weight: sc.baseWeight ?? 1
}));
console.log(`[predict] matched: ${matchedScenarios.length}, effective(use): ${effectiveScenarios.length}`);

// ---- simulate each scenario -> prob map（分布のまま取得）
function runSimulate(sc) {
  try {
    const out = simulateRace(adjusted, sc.oneMark);
    // out は { "a-b-c": prob } の想定。無ければ空オブジェクト。
    return (out && typeof out === "object" && !Array.isArray(out)) ? out : {};
  } catch (e) {
    console.error(`Error: simulateRace failed for scenario: ${sc.id}`, e?.message);
    return {};
  }
}

const scenarioResults = effectiveScenarios.map(sc => {
  const finalProb = runSimulate(sc);
  const keys = Object.keys(finalProb);
  console.log(`[DEBUG] scenario ${sc.id} w=${(sc.weight||0).toFixed(3)} keys=${keys.length}`);
  return { id: sc.id, weight: sc.weight || 1, finalProb };
});

// ---- aggregate ALL outcomes（全出目保持）
let aggregated = {};
let totalWeight = 0;
for (const sr of scenarioResults) {
  totalWeight += sr.weight || 0;
  for (const [k, p] of Object.entries(sr.finalProb || {})) {
    const v = Number(p);
    if (!Number.isFinite(v) || v <= 0) continue;
    aggregated[k] = (aggregated[k] || 0) + v * (sr.weight || 0);
  }
}
if (totalWeight > 0) {
  for (const k of Object.keys(aggregated)) aggregated[k] /= totalWeight;
}

// 正規化（数値誤差を丸め）
const sumAgg = Object.values(aggregated).reduce((s,v)=>s+v,0);
if (sumAgg > 0) for (const k of Object.keys(aggregated)) aggregated[k] /= sumAgg;

console.log(`[DEBUG] aggregated outcomes: ${Object.keys(aggregated).length} (after normalize)`);

// ---- bets: 抽出は bets 側に任せる（probs は全出目のまま）
let betResult;
try {
  betResult = bets(aggregated, 18);
} catch (e) {
  console.error("[ERROR] bets() failed:", e?.message);
  betResult = { compact: "", main: [], ana: [], markdown: "_no bets_" };
}

// compact フォールバック
if ((!betResult?.compact || betResult.compact.length===0) && betResult?.main?.length) {
  const fb = buildCompactFromMain(betResult.main);
  betResult.compact = fb;
  betResult.markdown = fb || "_no bets_";
}

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

// ---- output
const outDir = path.join("public", "predictions", date, pid, race);
fs.mkdirSync(outDir, { recursive: true });

const predictionData = {
  meta: {
    date, pid, race,
    sourceFile: path.relative(process.cwd(), raceDataPath),
    generatedAt: new Date().toISOString()
  },
  weather: adjusted.weather || null,
  ranking: adjusted.ranking || null,
  slitOrder: adjusted.slitOrder || null,
  attackType: adjusted.attackType || null,
  scenarios: scenarioResults.map(s => ({ id: s.id, weight: s.weight })),
  probs: aggregated, // ← 全出目そのまま
  bets: { compact: betResult.compact, main: betResult.main, ana: betResult.ana }
};
fs.writeFileSync(path.join(outDir, "race.json"), JSON.stringify(predictionData, null, 2), "utf-8");

// race.md
const lines = [];
lines.push(`# 予測 ${date} pid=${pid} race=${race}`);
if (adjusted?.weather) {
  const w = adjusted.weather;
  lines.push(`**気象**: ${w.weather ?? ""} / 気温${w.temperature ?? "-"}℃ 風${w.windSpeed ?? "-"}m ${w.windDirection ?? ""} 波高${w.waveHeight ?? "-"}m${w.stabilizer ? "（安定板）" : ""}`);
}
if (adjusted?.attackType) lines.push(`**想定攻め手**: ${adjusted.attackType}`);
lines.push("");
lines.push("## 買い目（compact）");
lines.push(betResult.markdown || betResult.compact || "_no bets_");
lines.push("");
lines.push("## デバッグ");
lines.push(`- scenarios used: ${effectiveScenarios.length}`);
lines.push(`- outcomes: ${Object.keys(aggregated).length}`);
fs.writeFileSync(path.join(outDir, "race.md"), lines.join("\n"), "utf-8");

console.log(`[predict] wrote:
- ${path.join(outDir, "race.json")}
- ${path.join(outDir, "race.md")}
`);