// scripts/predict.mjs
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

// ===== 依存モジュール =====
import envAdjust from "../src/predict/environmentAdjust.mjs";
import stPred from "../src/predict/predictedST.mjs";
import realClass from "../src/predict/realClass.mjs";
import slitAndAttack from "../src/predict/slitAndAttack.mjs";
import venueAdjust from "../src/predict/venueAdjust.mjs";
import upsetIndex from "../src/predict/upsetIndex.mjs";
import simulateRace from "../src/predict/simulateRace.mjs";
import bets from "../src/predict/bets.mjs";

// ---------- JSONCローダ（コメント/末尾カンマ許容） ----------
function readJsonLoose(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  const noComments = raw
    .replace(/\/\*[\s\S]*?\*\//g, "")   // /* ... */
    .replace(/(^|\s)\/\/.*$/gm, "");    // // ...
  const noTrailingCommas = noComments.replace(/,\s*([\]}])/g, "$1");
  return JSON.parse(noTrailingCommas);
}

// ---------- 1Mシナリオ正規化（simulateRace 向け） ----------
function splitToken(tok) {
  // "2/3" or "(2|3)" or "1" を [2,3] / [2,3] / [1] に
  if (Array.isArray(tok)) return tok.map(n => Number(n));
  const s = String(tok).trim();
  if (!s) return [];
  if (s.includes("/")) return s.split("/").map(n => Number(n));
  const m = s.match(/^\(([^)]+)\)$/); // (2|3)
  if (m) return m[1].split("|").map(n => Number(n));
  return [Number(s)];
}
function normalizeOneMark(oneMark) {
  const om = oneMark || {};
  const orderIn = om.order || [];
  const startOrder = orderIn.map(splitToken);

  const linkedPairs = Array.isArray(om.linkedPairs)
    ? om.linkedPairs.map(p => (Array.isArray(p) ? p.map(n => Number(n)) : splitToken(p)))
    : [];

  const thirdPool = Array.isArray(om.thirdPool)
    ? om.thirdPool.map(n => Number(n))
    : [];

  return {
    startOrder,              // Array<Array<number>>
    linkedPairs,             // Array<[number, number]>
    thirdPool,               // Array<number>
    thirdBias: om.thirdBias || null
  };
}

// ===== シナリオリスト読込 =====
const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

const scenariosPath = path.join(__dirname, "../src/predict/scenarios.json");

let scenariosListRaw;
let scenariosList;
try {
  scenariosListRaw = readJsonLoose(scenariosPath);
  if (Array.isArray(scenariosListRaw)) {
    scenariosList = scenariosListRaw;
  } else if (scenariosListRaw && Array.isArray(scenariosListRaw.scenarios)) {
    scenariosList = scenariosListRaw.scenarios;
  } else {
    scenariosList = [];
  }
} catch (e) {
  console.error("[ERROR] failed to load scenarios.json:", e?.message);
  scenariosList = [];
}

if (!Array.isArray(scenariosList)) {
  console.error("[ERROR] scenariosList is not an array. typeof:", typeof scenariosList);
  console.error('[HINT] scenarios.json は配列、または {"scenarios":[...]} の形にしてください。');
  scenariosList = [];
}

// ===== 入力の解決（CLI引数 or 環境変数）=====
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
  console.error("Usage: npm run predict <path/to/integrated.json>");
  process.exit(1);
}
const raceData = JSON.parse(fs.readFileSync(raceDataPath, "utf8"));

// 日付 / pid / race は統合JSONから取得（安全に）
const date = (raceData.date || (process.env.DATE || "today")).replace(/-/g, "");
const pid  = to2(raceData.pid || process.env.PID || "04");
const race = (raceData.race || process.env.RACE || "1R").toUpperCase().replace(/[^\d]/g, "") + "R";

// ===== 1) 前処理・補正チェーン =====
let adjusted = envAdjust(raceData);
adjusted = stPred(adjusted);
adjusted = slitAndAttack(adjusted);
adjusted = realClass(adjusted);
adjusted = venueAdjust(adjusted);
adjusted = upsetIndex(adjusted);

// ===== 2) シナリオマッチ（1Mまでの展開）=====
function matchScenario(sc, data) {
  if (sc?.requires?.attackType) {
    if (!sc.requires.attackType.includes(data.attackType)) return false;
  }
  if (sc?.requires?.head) {
    if (!sc.requires.head.includes(data.head)) return false;
  }
  return true;
}

const matchedScenarios = (scenariosList || []).filter(sc => matchScenario(sc, adjusted))
  .map(sc => ({
    ...sc,
    weight: (sc.baseWeight ?? 1) * (adjusted.venueWeight ?? 1) * (adjusted.upsetWeight ?? 1)
  }));

const baseList = Array.isArray(scenariosList) ? scenariosList : [];
const effectiveScenarios = matchedScenarios.length
  ? matchedScenarios
  : baseList.slice(0, 3).map(sc => ({ ...sc, weight: 1 }));

console.log(`[predict] scenarios loaded: ${Array.isArray(scenariosList) ? scenariosList.length : 0}`);
console.log(`[predict] matched: ${matchedScenarios.length}, effective: ${effectiveScenarios.length}`);

// ===== 3) 1M後〜ゴールの道中展開シミュレーション（ログ強化） =====
const scenarioResults = effectiveScenarios.map(sc => {
  const oneMark = normalizeOneMark(sc.oneMark);
  let finalProb = {};
  try {
    // 入力の軽い要約を出す
    const shape = oneMark.startOrder.map(a => `[${a.join("")}]`).join("-");
    console.log(`[DEBUG] simulate ${sc.id} w=${sc.weight ?? 1} startOrder=${shape} linked=${(oneMark.linkedPairs||[]).length} thirdPool=${(oneMark.thirdPool||[]).length}`);
    finalProb = simulateRace(adjusted, oneMark) || {};
    const keys = Object.keys(finalProb);
    console.log(`[DEBUG] result ${sc.id}: keys=${keys.length}${keys.length ? ` sample=${keys.slice(0, 3).join(",")}` : ""}`);
  } catch (e) {
    console.error(`Error: simulateRace failed for scenario: ${sc?.id} ${e?.message || e}`);
    finalProb = {};
  }
  return {
    id: sc.id,
    notes: sc.notes,
    oneMark,
    weight: sc.weight,
    finalProb
  };
});

// ===== 4) シナリオ結果の重み付け合算（正規化）=====
let aggregated = {};
let totalWeight = 0;

for (const sr of scenarioResults) {
  totalWeight += sr.weight || 0;
  for (const [ticket, prob] of Object.entries(sr.finalProb || {})) {
    aggregated[ticket] = (aggregated[ticket] || 0) + (Number(prob) || 0) * (sr.weight || 0);
  }
}

const aggKeys = Object.keys(aggregated);
if (totalWeight > 0 && aggKeys.length > 0) {
  for (const k of aggKeys) aggregated[k] /= totalWeight;
  console.log(`[DEBUG] aggregated keys=${aggKeys.length} (normalized by weight=${totalWeight.toFixed(3)})`);
} else {
  console.warn(`[WARN] aggregated empty. totalWeight=${totalWeight}, scenario non-empty counts=${scenarioResults.filter(r=>Object.keys(r.finalProb||{}).length>0).length}/${scenarioResults.length}`);
}

// ===== 5) 買い目生成（18点固定 / compact表記含む）=====
let betResult;
try {
  betResult = bets(aggregated, 18);
} catch (e) {
  console.error("[ERROR] bets() failed:", e?.message);
  betResult = { compact: "", main: [], ana: [], markdown: "_no bets_" };
}
if ((!betResult.main || betResult.main.length === 0) &&
    (!betResult.compact || betResult.compact.length === 0)) {
  console.warn("[WARN] bets came back empty. probs keys:", Object.keys(aggregated));
}

// ===== 6) race.md / race.json を所定フォルダに保存 =====
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
  scenarios: scenarioResults.map(s => ({
    id: s.id, notes: s.notes, oneMark: s.oneMark, weight: s.weight
  })),
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
lines.push(betResult.markdown || "_no bets_");
lines.push("");
lines.push("## 参考（上位評価）");
if (Array.isArray(adjusted.ranking)) {
  adjusted.ranking.slice(0, 6).forEach((r, i) => {
    lines.push(`${i + 1}. 枠${r.lane} 登番${r.number} ${r.name} score=${(r.score ?? 0).toFixed(2)}`);
  });
}
fs.writeFileSync(path.join(outDir, "race.md"), lines.join("\n"), "utf-8");

console.log(`[predict] wrote:
- ${path.join(outDir, "race.json")}
- ${path.join(outDir, "race.md")}
`);