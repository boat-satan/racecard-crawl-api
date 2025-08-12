// scripts/predict.mjs
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

// 各モジュール
import envAdjust from "../src/predict/environmentAdjust.mjs";
import stPred from "../src/predict/predictedST.mjs";
import realClass from "../src/predict/realClass.mjs";
import slitAndAttack from "../src/predict/slitAndAttack.mjs";
import venueAdjust from "../src/predict/venueAdjust.mjs";
import upsetIndex from "../src/predict/upsetIndex.mjs";
import simulateRace from "../src/predict/simulateRace.mjs";
import bets from "../src/predict/bets.mjs";

// シナリオリスト
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const scenariosPath = path.join(__dirname, "../src/predict/scenarios.json");
const scenariosList = JSON.parse(fs.readFileSync(scenariosPath, "utf8"));

// 入力データ
const raceDataPath = process.argv[2];
if (!raceDataPath) {
  console.error("Usage: npm run predict <raceData.json>");
  process.exit(1);
}
const raceData = JSON.parse(fs.readFileSync(raceDataPath, "utf8"));

// ===== 1. 前処理 =====
let adjustedData = envAdjust(raceData);
adjustedData = stPred(adjustedData);
adjustedData = slitAndAttack(adjustedData);
adjustedData = realClass(adjustedData);
adjustedData = venueAdjust(adjustedData);
adjustedData = upsetIndex(adjustedData);

// ===== 2. シナリオ判定（1Mまで） =====
function matchScenario(scenario, data) {
  // 条件に合うシナリオだけ抽出
  if (scenario.requires.attackType &&
      !scenario.requires.attackType.includes(data.attackType)) return false;
  if (scenario.requires.head &&
      !scenario.requires.head.includes(data.head)) return false;
  // 他条件も必要に応じて
  return true;
}

const matchedScenarios = scenariosList
  .filter(sc => matchScenario(sc, adjustedData))
  .map(sc => ({
    ...sc,
    weight: sc.baseWeight * adjustedData.venueWeight * adjustedData.upsetWeight
  }));

// ===== 3. 道中展開シミュレーション（1M後〜ゴール） =====
const scenarioResults = matchedScenarios.map(sc => {
  const finalProb = simulateRace(adjustedData, sc.oneMark);
  return {
    id: sc.id,
    notes: sc.notes,
    weight: sc.weight,
    finalProb
  };
});

// ===== 4. シナリオごとの結果を重み付け合算 =====
let totalProb = {};
let totalWeight = 0;

scenarioResults.forEach(sr => {
  totalWeight += sr.weight;
  for (const [ticket, prob] of Object.entries(sr.finalProb)) {
    if (!totalProb[ticket]) totalProb[ticket] = 0;
    totalProb[ticket] += prob * sr.weight;
  }
});

// 正規化（確率合計が1になるように）
for (const t in totalProb) {
  totalProb[t] /= totalWeight;
}

// ===== 5. 買い目生成（18点固定 + compact表記） =====
const betResult = bets(totalProb, 18);

// ===== 6. 出力 =====
console.log("# 最終予想");
console.log(betResult.markdown); // compact表記含む