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
  const noComments = raw.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|\s)\/\/.*$/gm, "");
  const noTrailingCommas = noComments.replace(/,\s*([\]}])/g, "$1");
  return JSON.parse(noTrailingCommas);
}

// ===== シナリオリスト読込 =====
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const scenariosPath = path.join(__dirname, "../src/predict/scenarios.json");
const scenariosList = readJsonLoose(scenariosPath);

// ===== 入力解決（CLI優先 → 環境変数）=====
const to2 = (s) => String(s).padStart(2, "0");
function resolveInputPath() {
  const argPath = process.argv[2];
  if (argPath) return path.resolve(argPath);
  const DATE = (process.env.DATE || "today").replace(/-/g, "");
  const PID = to2(process.env.PID || "04");
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

// 日付 / pid / race
const date = (raceData.date || (process.env.DATE || "today")).replace(/-/g, "");
const pid = to2(raceData.pid || process.env.PID || "04");
const race = (raceData.race || process.env.RACE || "1R").toUpperCase().replace(/[^\d]/g, "") + "R";

// ===== 1) 前処理チェーン =====
let adjusted = envAdjust(raceData);
adjusted = stPred(adjusted);
adjusted = slitAndAttack(adjusted);
adjusted = realClass(adjusted);
adjusted = venueAdjust(adjusted);
adjusted = upsetIndex(adjusted);

// ===== 2) シナリオマッチ（1Mまで）=====
function matchScenario(sc, data) {
  if (sc?.requires?.attackType && !sc.requires.attackType.includes(data.attackType)) return false;
  if (sc?.requires?.head && !sc.requires.head.includes(data.head)) return false;
  return true;
}

const matchedScenarios = (Array.isArray(scenariosList) ? scenariosList : [])
  .filter((sc) => matchScenario(sc, adjusted))
  .map((sc) => ({
    ...sc,
    weight: (sc.baseWeight ?? 1) * (adjusted.venueWeight ?? 1) * (adjusted.upsetWeight ?? 1),
  }));

const effectiveScenarios =
  matchedScenarios.length ? matchedScenarios : scenariosList.slice(0, 3).map((sc) => ({ ...sc, weight: 1 }));

// ===== 3) 道中展開シミュレーション =====
const scenarioResults = effectiveScenarios.map((sc) => {
  const finalProb = simulateRace(adjusted, sc.oneMark);
  return { id: sc.id, notes: sc.notes, oneMark: sc.oneMark, weight: sc.weight, finalProb };
});

// ===== 4) 重み付け合算・正規化 =====
let aggregated = {};
let totalWeight = 0;
for (const sr of scenarioResults) {
  totalWeight += sr.weight;
  for (const [ticket, prob] of Object.entries(sr.finalProb || {})) {
    aggregated[ticket] = (aggregated[ticket] || 0) + prob * sr.weight;
  }
}
if (totalWeight > 0) {
  for (const k of Object.keys(aggregated)) aggregated[k] /= totalWeight;
}

// ===== フォールバック：確率が空なら暫定で組む =====
function buildFallbackProbs(ad) {
  const probs = {};
  const slit = Array.isArray(ad.slitOrder) ? ad.slitOrder : [];
  const lanes = slit.length
    ? slit.map((x) => Number(x.lane))
    : (Array.isArray(ad.ranking) ? ad.ranking : []).map((r) => Number(r.lane));
  if (!lanes.length) return probs;

  const heads = lanes.slice(0, 3);
  for (const h of heads) {
    const seconds = lanes.filter((l) => l !== h).slice(0, 3);
    const thirds = lanes.filter((l) => l !== h).slice(0, 4);
    for (let i = 0; i < seconds.length; i++) {
      for (let j = 0; j < thirds.length; j++) {
        const s = seconds[i],
          t = thirds[j];
        if (s === t) continue;
        const key = `${h}-${s}-${t}`;
        const wHead = 1.0 - heads.indexOf(h) * 0.1;
        const wSec = 1.0 - i * 0.1;
        probs[key] = (probs[key] || 0) + wHead * wSec;
      }
    }
  }
  const sum = Object.values(probs).reduce((a, b) => a + b, 0);
  if (sum > 0) for (const k of Object.keys(probs)) probs[k] /= sum;
  return probs;
}
if (!Object.keys(aggregated).length) {
  aggregated = buildFallbackProbs(adjusted);
}

// ===== 5) 買い目生成（18点固定 / compact表記）=====
const betResult = bets(aggregated, 18);

// ===== 6) 出力 =====
const outDir = path.join("public", "predictions", date, pid, race);
fs.mkdirSync(outDir, { recursive: true });

const predictionData = {
  meta: {
    date,
    pid,
    race,
    sourceFile: path.relative(process.cwd(), raceDataPath),
    generatedAt: new Date().toISOString(),
  },
  weather: adjusted.weather || null,
  ranking: adjusted.ranking || null,
  slitOrder: adjusted.slitOrder || null,
  attackType: adjusted.attackType || null,
  scenarios: scenarioResults.map((s) => ({
    id: s.id,
    notes: s.notes,
    oneMark: s.oneMark,
    weight: s.weight,
  })),
  probs: aggregated,
  bets: { compact: betResult.compact, main: betResult.main, ana: betResult.ana },
};
fs.writeFileSync(path.join(outDir, "race.json"), JSON.stringify(predictionData, null, 2), "utf-8");

// md（compact表記込み）
const lines = [];
lines.push(`# 予測 ${date} pid=${pid} race=${race}`);
if (adjusted?.weather) {
  const w = adjusted.weather;
  lines.push(
    `**気象**: ${w.weather ?? ""} / 気温${w.temperature ?? "-"}℃ 風${w.windSpeed ?? "-"}m ${w.windDirection ?? ""} 波高${
      w.waveHeight ?? "-"
    }m${w.stabilizer ? "（安定板）" : ""}`
  );
}
if (adjusted?.attackType) lines.push(`**想定攻め手**: ${adjusted.attackType}`);
lines.push("");
lines.push("## 買い目（compact）");
lines.push(betResult.markdown || "");
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