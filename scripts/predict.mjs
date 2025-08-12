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

// ---------- JSONCローダ（最小追加） ----------
function readJsonLoose(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  const noComments = raw
    .replace(/\/\*[\s\S]*?\*\//g, "")       // /* ... */ コメント除去
    .replace(/(^|\s)\/\/.*$/gm, "");        // // コメント除去
  const noTrailingCommas = noComments.replace(/,\s*([\]}])/g, "$1"); // 末尾カンマ除去
  return JSON.parse(noTrailingCommas);
}

// ===== シナリオリスト読込 =====
const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);
const scenariosPath = path.join(__dirname, "../src/predict/scenarios.json");
let scenariosList = [];
try {
  scenariosList = readJsonLoose(scenariosPath);
} catch (e) {
  console.warn(`[predict] scenarios.json の読み込みに失敗: ${e.message}`);
  scenariosList = [];
}

// ===== 入力の解決（CLI引数 or 環境変数）=====
const to2 = (s) => String(s).padStart(2, "0");

function resolveInputPath() {
  const argPath = process.argv[2];
  if (argPath) return path.resolve(argPath);

  const DATE = (process.env.DATE || "today").replace(/-/g, "");
  const PID  = to2(process.env.PID || "04");
  const RACE = ((process.env.RACE || "1R").toUpperCase().replace(/[^\d]/g, "") + "R");

  return path.join(__dirname, "..", "public", "integrated", "v1", DATE, PID, `${RACE}.json`);
}

const raceDataPath = resolveInputPath();

// ===== データが無い場合でも成功扱いで空ファイルを出力 =====
if (!fs.existsSync(raceDataPath)) {
  const date = (process.env.DATE || "today").replace(/-/g, "");
  const pid  = to2(process.env.PID || "00");
  const race = (process.env.RACE || "1R").toUpperCase().replace(/[^\d]/g, "") + "R";

  const outDir = path.join("public", "predictions", date, pid, race);
  fs.mkdirSync(outDir, { recursive: true });

  const msg = `[predict] integrated json not found: ${raceDataPath}`;
  console.warn(msg);

  const emptyJson = {
    meta: { date, pid, race, sourceFile: raceDataPath, generatedAt: new Date().toISOString() },
    error: "no integrated data",
  };
  fs.writeFileSync(path.join(outDir, "race.json"), JSON.stringify(emptyJson, null, 2), "utf-8");
  fs.writeFileSync(
    path.join(outDir, "race.md"),
    `# 予測失敗\n\nデータが見つかりませんでした。\n\n- path: \`${raceDataPath}\`\n`,
    "utf-8"
  );

  console.log(`[predict] wrote (no data):
- ${path.join(outDir, "race.json")}
- ${path.join(outDir, "race.md")}
`);
  process.exit(0); // 成功扱いで終了
}

// ===== 入力読込 =====
const raceData = JSON.parse(fs.readFileSync(raceDataPath, "utf8"));

// 日付 / pid / race は統合JSONから取得（安全に）
const date = (raceData.date || (process.env.DATE || "today")).replace(/-/g, "");
const pid  = to2(raceData.pid || process.env.PID || "04");
const race = (raceData.race || process.env.RACE || "1R").toUpperCase().replace(/[^\d]/g, "") + "R";

// ===== 1) 前処理・補正チェーン =====
let adjusted = envAdjust(raceData);     // 気象・水面補正
adjusted = stPred(adjusted);            // 予測ST
adjusted = slitAndAttack(adjusted);     // スリット順 & 攻め手
adjusted = realClass(adjusted);         // 実質級別
adjusted = venueAdjust(adjusted);       // 場別補正
adjusted = upsetIndex(adjusted);        // 波乱指数

// ===== 2) シナリオマッチ（1Mまでの展開）=====
function matchScenario(sc, data) {
  if (sc?.requires?.attackType && !sc.requires.attackType.includes(data.attackType)) return false;
  if (sc?.requires?.head && !sc.requires.head.includes(data.head)) return false;
  return true;
}

const matchedScenarios = (scenariosList || [])
  .filter((sc) => matchScenario(sc, adjusted))
  .map((sc) => ({
    ...sc,
    weight: (sc.baseWeight ?? 1) * (adjusted.venueWeight ?? 1) * (adjusted.upsetWeight ?? 1),
  }));

const effectiveScenarios =
  matchedScenarios.length ? matchedScenarios : (scenariosList || []).slice(0, 3).map((sc) => ({ ...sc, weight: 1 }));

// ===== 3) 1M後〜ゴールの道中展開シミュレーション =====
const scenarioResults = effectiveScenarios.map((sc) => {
  const finalProb = simulateRace(adjusted, sc.oneMark);
  return { id: sc.id, notes: sc.notes, oneMark: sc.oneMark, weight: sc.weight, finalProb };
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

// race.md
const lines = [];
lines.push(`# 予測 ${date} pid=${pid} race=${race}`);
if (adjusted?.weather) {
  const w = adjusted.weather;
  lines.push(
    `**気象**: ${w.weather ?? ""} / 気温${w.temperature ?? "-"}℃ 風${w.windSpeed ?? "-"}m ${w.windDirection ?? ""} 波高${w.waveHeight ?? "-"}m${w.stabilizer ? "（安定板）" : ""}`
  );
}
if (adjusted?.attackType) lines.push(`**想定攻め手**: ${adjusted.attackType}`);
lines.push("");
lines.push("## 買い目（compact）");
lines.push(betResult.markdown);
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