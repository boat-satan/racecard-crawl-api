// scripts/predict.mjs
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

// ========== 依存モジュール ==========
import envAdjust from "../src/predict/environmentAdjust.mjs";
import stPred from "../src/predict/predictedST.mjs";
import realClass from "../src/predict/realClass.mjs";
import slitAndAttack from "../src/predict/slitAndAttack.mjs";
import venueAdjust from "../src/predict/venueAdjust.mjs";
import upsetIndex from "../src/predict/upsetIndex.mjs";
import simulateRace from "../src/predict/simulateRace.mjs";
import bets from "../src/predict/bets.mjs";

// ========== ユーティリティ ==========
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const to2 = (s) => String(s ?? "").padStart(2, "0");
const asArray = (v) =>
  Array.isArray(v) ? v : (v && Array.isArray(v.scenarios) ? v.scenarios : []);

// JSONC（コメント/末尾カンマ）を許す緩いローダ
function readJsonLoose(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  const noComments = raw.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|\s)\/\/.*$/gm, "");
  const noTrailingCommas = noComments.replace(/,\s*([\]}])/g, "$1");
  return JSON.parse(noTrailingCommas);
}

// 入力パス解決（CLI引数 > 環境変数）
function resolveInputPath() {
  const arg = process.argv[2];
  if (arg) return path.resolve(arg);

  const DATE = (process.env.DATE || "today").replace(/-/g, "");
  const PID = to2(process.env.PID || "04");
  const RACE = (String(process.env.RACE || "1R").toUpperCase().replace(/[^\d]/g, "") || "1") + "R";

  return path.join(__dirname, "..", "public", "integrated", "v1", DATE, PID, `${RACE}.json`);
}

// ========== データ読込 ==========
const raceDataPath = resolveInputPath();
if (!fs.existsSync(raceDataPath)) {
  console.error(`[predict] integrated json not found: ${raceDataPath}`);
  console.error("Usage: npm run predict <path/to/integrated.json>");
  process.exit(1);
}
const raceData = JSON.parse(fs.readFileSync(raceDataPath, "utf8"));

const date = (raceData.date || (process.env.DATE || "today")).replace(/-/g, "");
const pid = to2(raceData.pid || process.env.PID || "04");
const race = ((raceData.race || process.env.RACE || "1R").toUpperCase().replace(/[^\d]/g, "") || "1") + "R";

// シナリオ読込（JSONC可）
const scenariosPath = path.join(__dirname, "../src/predict/scenarios.json");
let scenariosListRaw = [];
try {
  scenariosListRaw = readJsonLoose(scenariosPath);
} catch (e) {
  console.warn(`[predict] scenarios.json read failed (${e?.message}). continue with empty list.`);
}
const scenariosList = asArray(scenariosListRaw); // 常に配列へ

// ========== 1) 前処理チェーン ==========
let adjusted = envAdjust(raceData) ?? raceData;
adjusted = stPred(adjusted) ?? adjusted;
adjusted = slitAndAttack(adjusted) ?? adjusted;
adjusted = realClass(adjusted) ?? adjusted;
adjusted = venueAdjust(adjusted) ?? adjusted;
adjusted = upsetIndex(adjusted) ?? adjusted;

// ========== 2) シナリオマッチ（1Mまでの展開） ==========
function matchScenario(sc, data) {
  if (!sc) return false;
  // attackType 条件
  if (sc?.requires?.attackType) {
    if (!Array.isArray(sc.requires.attackType)) return false;
    if (!sc.requires.attackType.includes(data?.attackType)) return false;
  }
  // 先頭想定など拡張条件
  if (sc?.requires?.head) {
    if (!Array.isArray(sc.requires.head)) return false;
    if (!sc.requires.head.includes(data?.head)) return false;
  }
  return true;
}

const matchedScenarios = scenariosList
  .filter((sc) => matchScenario(sc, adjusted))
  .map((sc) => ({
    ...sc,
    weight: (sc?.baseWeight ?? 1) * (adjusted?.venueWeight ?? 1) * (adjusted?.upsetWeight ?? 1),
  }));

// マッチ0件はフォールバック（先頭3件を重み1で）
const effectiveScenarios =
  matchedScenarios.length > 0
    ? matchedScenarios
    : scenariosList.slice(0, 3).map((sc) => ({ ...sc, weight: 1 }));

// ========== 3) 1M後〜ゴールのシミュレーション ==========
const scenarioResults = effectiveScenarios.map((sc) => {
  // simulateRace は (adjusted, sc.oneMark) -> { "1-2-3": prob, ... } を想定
  let finalProb = {};
  try {
    finalProb = simulateRace(adjusted, sc?.oneMark) || {};
  } catch (e) {
    console.warn(`[predict] simulateRace failed on scenario ${sc?.id ?? "?"}: ${e?.message}`);
  }
  return {
    id: sc?.id ?? null,
    notes: sc?.notes ?? "",
    oneMark: sc?.oneMark ?? null,
    weight: Number.isFinite(sc?.weight) ? sc.weight : 1,
    finalProb,
  };
});

// ========== 4) 重み付き合算 & 正規化 ==========
const aggregated = {};
let totalWeight = 0;

for (const sr of scenarioResults) {
  const w = Number.isFinite(sr?.weight) ? sr.weight : 1;
  totalWeight += w;
  const probObj = sr?.finalProb && typeof sr.finalProb === "object" ? sr.finalProb : {};
  for (const [ticket, p] of Object.entries(probObj)) {
    const numP = Number(p);
    if (!Number.isFinite(numP) || numP <= 0) continue;
    aggregated[ticket] = (aggregated[ticket] || 0) + numP * w;
  }
}
if (totalWeight > 0) {
  for (const k of Object.keys(aggregated)) {
    aggregated[k] = aggregated[k] / totalWeight;
  }
}

// ========== 5) 買い目（18点固定 / compact表記含む） ==========
let betResult = { compact: "", main: [], ana: [], markdown: "" };
try {
  betResult = bets(aggregated, 18) || betResult;
} catch (e) {
  console.warn(`[predict] bets() failed: ${e?.message}`);
}

// ========== 6) 出力（public/predictions/YYYYMMDD/PP/RR） ==========
const outDir = path.join("public", "predictions", date, pid, race);
fs.mkdirSync(outDir, { recursive: true });

// race.json
const predictionData = {
  meta: {
    date,
    pid,
    race,
    sourceFile: path.relative(process.cwd(), raceDataPath),
    generatedAt: new Date().toISOString(),
  },
  weather: adjusted?.weather ?? null,
  ranking: Array.isArray(adjusted?.ranking) ? adjusted.ranking : null,
  slitOrder: Array.isArray(adjusted?.slitOrder) ? adjusted.slitOrder : null,
  attackType: adjusted?.attackType ?? null,
  scenarios: scenarioResults.map((s) => ({
    id: s.id,
    notes: s.notes,
    oneMark: s.oneMark,
    weight: s.weight,
  })),
  probs: aggregated,
  bets: {
    compact: betResult.compact ?? "",
    main: Array.isArray(betResult.main) ? betResult.main : [],
    ana: Array.isArray(betResult.ana) ? betResult.ana : [],
  },
};
fs.writeFileSync(path.join(outDir, "race.json"), JSON.stringify(predictionData, null, 2), "utf-8");

// race.md
const lines = [];
lines.push(`# 予測 ${date} pid=${pid} race=${race}`);
if (adjusted?.weather) {
  const w = adjusted.weather;
  lines.push(
    `**気象**: ${w.weather ?? ""} / 気温${w.temperature ?? "-"}℃ 風${w.windSpeed ?? "-"}m ${w.windDirection ?? ""} ` +
      `波高${w.waveHeight ?? "-"}m${w.stabilizer ? "（安定板）" : ""}`
  );
}
if (adjusted?.attackType) lines.push(`**想定攻め手**: ${adjusted.attackType}`);
lines.push("");
lines.push("## 買い目（compact）");
lines.push(betResult.markdown || betResult.compact || "(生成なし)");
lines.push("");
lines.push("## 参考（上位評価）");
if (Array.isArray(adjusted?.ranking)) {
  adjusted.ranking.slice(0, 6).forEach((r, i) => {
    const sc = Number.isFinite(r?.score) ? Number(r.score).toFixed(2) : "-";
    lines.push(`${i + 1}. 枠${r?.lane ?? "-"} 登番${r?.number ?? "-"} ${r?.name ?? ""} score=${sc}`);
  });
}
fs.writeFileSync(path.join(outDir, "race.md"), lines.join("\n"), "utf-8");

// CI で見やすい最終ログ
console.log(`[predict] wrote:
- ${path.join(outDir, "race.json")}
- ${path.join(outDir, "race.md")}
`);