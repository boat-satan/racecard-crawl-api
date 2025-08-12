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

// ===== シナリオリスト読込 =====
const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);
const scenariosPath = path.join(__dirname, "../src/predict/scenarios.json");
const scenariosList = JSON.parse(fs.readFileSync(scenariosPath, "utf8"));

// ===== 入力の解決（CLI引数 or 環境変数）=====
function to2(s){ return String(s).padStart(2, "0"); }

function resolveInputPath() {
  // 1) CLI 引数優先
  const argPath = process.argv[2];
  if (argPath) return path.resolve(argPath);

  // 2) 環境変数から推定
  const DATE = (process.env.DATE || "today").replace(/-/g, "");
  const PID  = to2(process.env.PID || "04");
  const RACE = ((process.env.RACE || "1R").toUpperCase().replace(/[^\d]/g, "") + "R");

  const p = path.join(
    __dirname,
    "..",
    "public",
    "integrated",
    "v1",
    DATE,
    PID,
    `${RACE}.json`
  );
  return p;
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
let adjusted = envAdjust(raceData);     // 気象・水面補正（波/水温/気温/安定板 など）
adjusted = stPred(adjusted);            // 予測ST（今回の新ロジックに基づく）
adjusted = slitAndAttack(adjusted);     // スリット順と attackType 判定
adjusted = realClass(adjusted);         // 実質級別の再評価
adjusted = venueAdjust(adjusted);       // 場別特性補正（平和島/戸田/他…）
adjusted = upsetIndex(adjusted);        // 波乱指数（イン壁/伸び/風/外枠良機 ほか）

// ===== 2) シナリオマッチ（1Mまでの展開）=====
function matchScenario(sc, data) {
  // attackType 条件
  if (sc?.requires?.attackType) {
    if (!sc.requires.attackType.includes(data.attackType)) return false;
  }
  // “head（想定先頭）”等の任意条件も必要に応じて
  if (sc?.requires?.head) {
    if (!sc.requires.head.includes(data.head)) return false;
  }
  return true;
}

const matchedScenarios = scenariosList
  .filter(sc => matchScenario(sc, adjusted))
  .map(sc => ({
    ...sc,
    // シナリオ重み：ベース * 場補正 * 波乱/安定補正 など
    weight: (sc.baseWeight ?? 1) * (adjusted.venueWeight ?? 1) * (adjusted.upsetWeight ?? 1)
  }));

// マッチ0件回避（最低1本は通す）
const effectiveScenarios = matchedScenarios.length ? matchedScenarios : scenariosList.slice(0, 3).map(sc => ({...sc, weight: 1}));

// ===== 3) 1M後〜ゴールの道中展開シミュレーション =====
const scenarioResults = effectiveScenarios.map(sc => {
  // simulateRace は (adjusted, sc.oneMark) を受け取り、組番 "1-2-3": 確率 … の形を返す想定
  const finalProb = simulateRace(adjusted, sc.oneMark);
  return {
    id: sc.id,
    notes: sc.notes,
    oneMark: sc.oneMark,
    weight: sc.weight,
    finalProb
  };
});

// ===== 4) シナリオ結果の重み付け合算（正規化）=====
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

// ===== 5) 買い目生成（18点固定 / compact表記含む）=====
const betResult = bets(aggregated, 18);

// ===== 6) race.md / race.json を所定フォルダに保存 =====
const outDir = path.join("public", "predictions", date, pid, race);
fs.mkdirSync(outDir, { recursive: true });

// race.json（解析一式）
const predictionData = {
  meta: {
    date,
    pid,
    race,
    sourceFile: path.relative(process.cwd(), raceDataPath),
    generatedAt: new Date().toISOString()
  },
  weather: adjusted.weather || null,
  ranking: adjusted.ranking || null,    // スコアボードがあれば
  slitOrder: adjusted.slitOrder || null,
  attackType: adjusted.attackType || null,
  scenarios: scenarioResults.map(s => ({
    id: s.id,
    notes: s.notes,
    oneMark: s.oneMark,
    weight: s.weight
  })),
  probs: aggregated,
  bets: {
    compact: betResult.compact,   // 例: "1-23=234, 4=35-35 ..."
    main: betResult.main,
    ana: betResult.ana
  }
};
fs.writeFileSync(
  path.join(outDir, "race.json"),
  JSON.stringify(predictionData, null, 2),
  "utf-8"
);

// race.md（見やすい要約 + compact表記）
const lines = [];
lines.push(`# 予測 ${date} pid=${pid} race=${race}`);
if (adjusted?.weather) {
  const w = adjusted.weather;
  lines.push(`**気象**: ${w.weather ?? ""} / 気温${w.temperature ?? "-"}℃ 風${w.windSpeed ?? "-"}m ${w.windDirection ?? ""} 波高${w.waveHeight ?? "-"}m${w.stabilizer ? "（安定板）" : ""}`);
}
if (adjusted?.attackType) lines.push(`**想定攻め手**: ${adjusted.attackType}`);
lines.push("");
lines.push("## 買い目（compact）");
lines.push(betResult.markdown); // 内部で compact 表記も含めている想定
lines.push("");
lines.push("## 参考（上位評価）");
if (Array.isArray(adjusted.ranking)) {
  adjusted.ranking.slice(0, 6).forEach((r, i) => {
    lines.push(`${i + 1}. 枠${r.lane} 登番${r.number} ${r.name} score=${(r.score ?? 0).toFixed(2)}`);
  });
}

fs.writeFileSync(path.join(outDir, "race.md"), lines.join("\n"), "utf-8");

// 標準出力は最小限（CIログ見やすく）
console.log(`[predict] wrote:
- ${path.join(outDir, "race.json")}
- ${path.join(outDir, "race.md")}
`);