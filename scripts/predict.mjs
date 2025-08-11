// scripts/predict.mjs
// Node v20 / ESM 最小骨組み：統合JSONを読み → 簡易スコア → 予測JSON/MDを保存

import fs from "node:fs/promises";
import fssync from "node:fs";
import path from "node:path";

const ROOT = process.cwd();
const to2 = (s) => String(s).padStart(2, "0");
const nowISO = () => new Date().toISOString();

function pickArg(name, fallback){
  const val = process.env[name] ?? getCliArg(`--${name.toLowerCase()}`);
  return (val ?? fallback);
}
function getCliArg(flag){
  const i = process.argv.indexOf(flag);
  return i > -1 ? process.argv[i+1] : undefined;
}

const DATE = (pickArg("DATE","today") || "").replace(/-/g,"");
const PID  = to2(pickArg("PID","04"));         // 既定: 平和島
const RACE = (pickArg("RACE","1R") || "").toUpperCase().replace(/[^\d]/g,"") + "R";

const INTEGRATED = path.join(ROOT,"public","integrated","v1", DATE, PID, `${RACE}.json`);
const OUTDIR     = path.join(ROOT,"public","predictions", DATE, PID);
const OUTJSON    = path.join(OUTDIR, `${RACE}.json`);
const OUTMD      = path.join(OUTDIR, `${RACE}.md`);
const HEURIS     = path.join(ROOT,"rules","heuristics.json");

async function ensureDir(p){ await fs.mkdir(p,{recursive:true}); }
async function readJson(p){ return JSON.parse(await fs.readFile(p,"utf8")); }
function exists(p){ return fssync.existsSync(p); }

function pct(n){ return Number.isFinite(n) ? Math.round(n*10)/10 : null; }
function safeNum(n, d=0){ const v = Number(n); return Number.isFinite(v) ? v : d; }

function baseWinFromStats(entry){
  const m = entry?.stats?.entryCourse?.matrixSelf;
  if (m?.winRate != null) return safeNum(m.winRate, 0);
  const ss = entry?.stats?.entryCourse?.selfSummary;
  if (ss && ss.starts > 0) return (ss.firstCount/ss.starts)*100;
  return 0;
}

// ざっくり重み（あとで heuristics.json から読み替え）
const DEFAULT_WEIGHTS = {
  abs_win_w:        0.55,   // 絶対(自己)勝率
  avgst_boost_w:    0.20,   // 平均STの良さ補正
  motor_boat_w:     0.15,   // 機力 (motor/boat 連対率)
  tenji_w:          0.10,   // 展示 (ST/タイム)
  wind_in_weak:     0.90,   // 追/向でのイン弱化/強化の係数(場補正で乗算)
  dash_favor:       1.05,   // ダッシュ有利係数
  heiwajima_bias:   1.02    // 平和島わずかにイン弱＋中枠加点 (例)
};

function directionToCardinal(dirStr){
  // 「西」「南西」などから大まか方位に正規化
  if (!dirStr) return null;
  const s = String(dirStr);
  if (s.includes("北") && s.includes("東")) return "NE";
  if (s.includes("北") && s.includes("西")) return "NW";
  if (s.includes("南") && s.includes("東")) return "SE";
  if (s.includes("南") && s.includes("西")) return "SW";
  if (s.includes("北")) return "N";
  if (s.includes("南")) return "S";
  if (s.includes("東")) return "E";
  if (s.includes("西")) return "W";
  return null;
}

function scoreEntry(e, heur){
  const w = heur?.weights ?? DEFAULT_WEIGHTS;

  // 1) 絶対(自己)勝率
  const absWin = baseWinFromStats(e); // 0..100

  // 2) 平均ST（小さいほど良い → 0.05〜0.20 を 100..0 に線形マップ）
  const avgST = safeNum(e?.stats?.entryCourse?.avgST, null);
  let stScore = 0;
  if (avgST != null){
    const min=0.05, max=0.20;
    const cl = Math.max(min, Math.min(max, avgST));
    stScore = (1 - (cl - min)/(max - min)) * 100; // 0..100
  }

  // 3) 機力（motor/boat 2連率を素直に平均）
  const rc = e?.racecard || {};
  const motor2 = safeNum(rc.motorTop2, 0);
  const boat2  = safeNum(rc.boatTop2, 0);
  const mech   = (motor2 + boat2)/2; // 0..100

  // 4) 展示（ST速い・展示タイム短いほど加点：ごく弱く）
  const exST = String(e?.exhibition?.st ?? "").replace("F","").trim();
  const exSTn = Number(exST.startsWith(".") ? `0${exST}` : exST);
  let exScore = 0;
  if (Number.isFinite(exSTn)){
    const min=0.00, max=0.20;
    const cl = Math.max(min, Math.min(max, exSTn));
    exScore += (1 - (cl - min)/(max - min)) * 50; // 0..50
  }
  const tenjiTime = Number(e?.exhibition?.tenjiTime ?? null);
  if (Number.isFinite(tenjiTime)){
    // 6.50〜6.95を良→悪で 50→0
    const min=6.50, max=6.95;
    const cl = Math.max(min, Math.min(max, tenjiTime));
    exScore += (1 - (cl - min)/(max - min)) * 50; // 0..100 合算
  }

  // 重み合成
  let score = 0;
  score += absWin * w.abs_win_w;
  score += stScore * w.avgst_boost_w;
  score += mech   * w.motor_boat_w;
  score += exScore* w.tenji_w;

  return { score, breakdown: { absWin, stScore:pct(stScore), mech:pct(mech), exScore:pct(exScore) } };
}

function applyTrackWeatherAdjust(entries, meta, heur){
  const w = heur?.weights ?? DEFAULT_WEIGHTS;
  const placePid = String(meta?.pid || "");
  const weather  = meta?.weather || {};
  const windSp   = Number(weather?.windSpeed ?? 0);
  const windDir  = directionToCardinal(weather?.windDirection);

  // 例：平和島(04) 追/向5mでイン弱/ダッシュやや有利
  const isHeiwajima = (placePid === "04");
  const dashFavor = (windSp >= 4 ? w.dash_favor : 1.0);
  const inWeak    = (windSp >= 4 ? w.wind_in_weak : 1.0);

  return entries.map(row => {
    let mul = 1.0;
    if (isHeiwajima) mul *= w.heiwajima_bias;

    // ざっくり：内枠(1-2)は強風でわずかに弱化、ダッシュ側(4-6)は強化
    if (windSp >= 4) {
      if (row.lane === 1 || row.lane === 2) mul *= inWeak;     // < 1
      if (row.lane >= 4)               mul *= dashFavor;  // > 1
    }

    // 風向きで追 or 向（仮：W/西→追い、E/東→向い と想定、場ごとにあとで厳密化）
    if (windDir === "W" || windDir === "SW" || windDir === "NW") {
      // 追い風：ダッシュほんのり
      if (row.lane >= 4) mul *= 1.02;
    } else if (windDir === "E" || windDir === "SE" || windDir === "NE") {
      // 向い風：インほんのり
      if (row.lane <= 2) mul *= 1.02;
    }

    return { ...row, scoreAdj: row.score * mul, adjMul: Number(mul.toFixed(3)) };
  });
}

async function main(){
  if (!exists(INTEGRATED)) {
    console.error(`Integrated JSON not found: ${path.relative(ROOT, INTEGRATED)}`);
    process.exit(0); // 失敗ではなくスキップ扱い
  }
  const integ = await readJson(INTEGRATED);
  const heur  = exists(HEURIS) ? await readJson(HEURIS) : { weights: DEFAULT_WEIGHTS, notes: [] };

  const rows = (integ.entries || []).map(e => {
    const { score, breakdown } = scoreEntry(e, heur);
    return {
      number: e.number,
      name: e.racecard?.name ?? e.exhibition?.name ?? "",
      lane: e.lane,
      startCourse: e.startCourse,
      score: Number(score.toFixed(3)),
      breakdown,
      rc: e.racecard ?? null,
      ex: e.exhibition ?? null,
      stats: e.stats ?? null
    };
  });

  // 天候・場補正
  const adjusted = applyTrackWeatherAdjust(rows, { pid: integ.pid, weather: integ.weather }, heur);

  // ソート
  adjusted.sort((a,b) => b.scoreAdj - a.scoreAdj);

  // 簡易の買い目（最小骨組み）：上位3艇の組み合わせを数点。詳細ルールは後で拡張。
  const top = adjusted.slice(0,3).map(r => r.lane);
  const trifecta = [
    [top[0], top[1], top[2]],
    [top[0], top[2], top[1]],
    [top[1], top[0], top[2]],
  ].filter(a => a.every(Boolean));

  const out = {
    schemaVersion: "0.1",
    generatedAt: nowISO(),
    meta: { date: integ.date, pid: integ.pid, race: integ.race, source: path.relative(ROOT, INTEGRATED) },
    weather: integ.weather ?? null,
    ranking: adjusted.map(r => ({
      lane: r.lane, number: r.number, name: r.name,
      score: r.score, scoreAdj: Number(r.scoreAdj.toFixed(3)), adjMul: r.adjMul,
      breakdown: r.breakdown
    })),
    picks: {
      trifectaPrimary: trifecta,
      trifectaAlt: []
    },
    rationale: [
      "最小版：自己勝率/平均ST/機力/展示を重み合成し、場・風で微調整。",
      "買い目は上位3艇の並び（雛形）。ルールと点数はあとで heuristics.json に移し替え。"
    ]
  };

  await ensureDir(OUTDIR);
  await fs.writeFile(OUTJSON, JSON.stringify(out, null, 2), "utf8");

  const lines = [];
  lines.push(`# 予測 ${integ.date} pid=${integ.pid} race=${integ.race}`);
  if (integ.weather){
    const w = integ.weather;
    lines.push(`**気象**: ${w.weather ?? "-"} / 気温${w.temperature ?? "-"}℃ 風${w.windSpeed ?? "-"}m ${w.windDirection ?? ""} 波高${w.waveHeight ?? "-"}m`);
  }
  lines.push(`\n## ランキング`);
  adjusted.forEach((r,i) => {
    lines.push(`${i+1}. 枠${r.lane} 登番${r.number} ${r.name}  score=${r.scoreAdj.toFixed(1)}  (基本:${r.score.toFixed(1)}×${r.adjMul})`);
  });
  lines.push(`\n## 買い目(雛形)`);
  out.picks.trifectaPrimary.forEach(arr => lines.push(`3連単 ${arr.join("-")}`));

  await fs.writeFile(OUTMD, lines.join("\n"), "utf8");

  console.log("wrote:", path.relative(ROOT, OUTJSON));
  console.log("wrote:", path.relative(ROOT, OUTMD));
}

main().catch(e => { console.error(e); process.exit(1); });
