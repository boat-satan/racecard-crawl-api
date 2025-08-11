// scripts/predict.mjs
// Node v20 / ESM
// 入力: public/integrated/v1/<DATE>/<PID>/<RACE>.json
// 出力: out/prediction.json, public/predictions/<DATE>/<PID>/<RACE>.md

import fs from "node:fs/promises";
import path from "node:path";

const ROOT = process.cwd();
const to2 = (s) => String(s).padStart(2, "0");
const DATE = (process.env.DATE || "today").replace(/-/g, "");
const PID  = to2(process.env.PID || "04");
const RACE = ((process.env.RACE || "1R").toUpperCase().replace(/[^\d]/g, "")) + "R";

function clamp(x, lo, hi){ return Math.max(lo, Math.min(hi, x)); }
function mean2(a,b){
  const an = (a!=null && !Number.isNaN(a)) ? Number(a) : null;
  const bn = (b!=null && !Number.isNaN(b)) ? Number(b) : null;
  if (an!=null && bn!=null) return (an+bn)/2;
  if (an!=null) return an;
  if (bn!=null) return bn;
  return 0.20; // デフォ
}

async function readJson(p){ return JSON.parse(await fs.readFile(p, "utf8")); }
async function ensureDir(p){ await fs.mkdir(p, { recursive: true }); }

// ---------- 展示ST（新ロジック） ----------
function parseTenjiST(raw){
  if (raw == null) return { st: null, fFlag: false, fMag: null };
  const s = String(raw).trim();
  if (/^F\.\d{2}$/i.test(s)) {
    return { st: null, fFlag: true, fMag: Number("0."+s.slice(2)) };
  }
  const m = s.match(/(\d+\.\d{2}|\.\d{2})/);
  if (!m) return { st: null, fFlag: false, fMag: null };
  const n = m[0].startsWith(".") ? Number("0"+m[0]) : Number(m[0]);
  return { st: n, fFlag: false, fMag: null };
}

/**
 * 予測ST（展示は控えめ寄せ + 荒天/安定板で展示寄せ半減 + Fは気配ボーナス）
 * 最終クリップ: 0.06〜0.35
 */
function calcPredictedST(entryAvgST, rcAvgST, tenjiSTRaw, exRank, opts = {}){
  const { windSpeed = 0, stabilizer = false, isDash = false } = opts;

  const base = mean2(entryAvgST, rcAvgST);
  const { st: tenjiSTVal, fFlag } = parseTenjiST(tenjiSTRaw);

  let w = 0.20; // 展示寄せは基本2割
  if (exRank === 1) w += 0.10;
  else if (exRank === 2) w += 0.05;

  const severe = (Number(windSpeed) >= 7) || !!stabilizer;
  if (severe) w *= 0.5;
  w = clamp(w, 0.05, 0.35);

  let blend = base;
  if (tenjiSTVal != null) {
    let t = tenjiSTVal;
    if (base - t > 0.03) t = base - 0.03; // 速すぎ寄せ制限
    if (t - base > 0.05) t = base + 0.05; // 遅すぎ寄せ制限
    blend = base*(1 - w) + t*w;
  }

  if (fFlag) {
    blend -= 0.010;          // “攻め気配”としてボーナス
    if (isDash) blend -= 0.005;
  }

  return clamp(Number(blend.toFixed(3)), 0.06, 0.35);
}

// ---------- 実質級別（安全版：利用可能な指標だけで算出） ----------
function calcRealClassSafe(p){
  // 使えるものだけ使う（欠損多い場面に強い）
  let score = 0;

  // モーター2連率
  const m2 = Number(p.motor2 ?? 0);
  if (m2 >= 45) score += 2;
  else if (m2 >= 35) score += 1;
  else if (m2 < 30) score -= 1;

  // 平均ST（小さいほど加点）
  const ast = Number(p.avgST ?? 0);
  if (ast && ast <= 0.16) score += 1;
  else if (ast && ast >= 0.21) score -= 1;

  // コース別1着率
  const c1w = Number(p.course1Win ?? 0);
  if (c1w >= 40) score += 1;
  else if (c1w > 0 && c1w < 20) score -= 1;

  // 年齢ざっくり補正
  const age = Number(p.age ?? 0);
  if (age && age <= 30) score += 1;
  if (age && age >= 55) score -= 1;

  if (score >= 3) return "A2";
  if (score >= 1) return "B1上位";
  if (score >= -1) return "B1中位";
  if (score >= -3) return "B1下位";
  return "B2";
}

// ---------- 展示タイム順位 ----------
function toNumOrNull(s){
  if (s == null) return null;
  const m = String(s).match(/(\d+\.\d{2}|\.\d{2})/);
  if (!m) return null;
  return Number(m[0].startsWith(".") ? "0"+m[0] : m[0]);
}
function assignExRanks(entries){
  const arr = entries.map((e,i)=>({
    i,
    t: toNumOrNull(e?.exhibition?.tenjiTime),
  }));
  const ranked = arr
    .filter(x => x.t != null)
    .sort((a,b)=> a.t - b.t)
    .map((x, idx)=> ({ ...x, rank: idx+1 }));
  const exRankMap = new Map(ranked.map(x => [x.i, x.rank]));
  return entries.map((e,i)=> ({ ...e, exRank: exRankMap.get(i) ?? null }));
}

// ---------- コース別 1/2/3着率（stats.selfSummary から） ----------
function pickCourseRates(entry){
  const ss = entry?.stats?.entryCourse?.selfSummary;
  if (!ss) return { winRate: 0, top2Rate: 0, top3Rate: 0, starts: 0, first: 0, second: 0, third: 0 };
  const starts = Number(ss.starts || 0);
  const first  = Number(ss.firstCount || 0);
  const second = Number(ss.secondCount || 0);
  const third  = Number(ss.thirdCount || 0);
  const winRate  = starts ? (first / starts) * 100 : 0;
  const top2Rate = starts ? ((first+second) / starts) * 100 : 0;
  const top3Rate = starts ? ((first+second+third) / starts) * 100 : 0;
  return {
    winRate: Math.round(winRate*10)/10,
    top2Rate: Math.round(top2Rate*10)/10,
    top3Rate: Math.round(top3Rate*10)/10,
    starts, first, second, third
  };
}

// ---------- 簡易 波乱指数 ----------
function calcUpsetScoreSimple(e, weather){
  let s = 0;
  const lane = Number(e.startCourse ?? e.lane);
  const dash = lane >= 4;
  const exRank = e.exRank ?? 99;
  const motor2 = Number(e?.racecard?.motorTop2 ?? e?.racecard?.motor2 ?? 0);
  const courseRates = e.courseRates?.winRate ?? 0;

  if (dash && exRank <= 2) s += 20;
  if (motor2 >= 40 && lane >= 5) s += 10;
  if (courseRates < 20 && lane === 1) s += 10; // イン弱め

  const ws = Number(weather?.windSpeed ?? 0);
  if (ws >= 6 && dash) s += 10;

  return clamp(s, 0, 100);
}

// ---------- スリット順 & 攻め手 ----------
function predictSlitOrder(boats){
  return boats
    .map(b => {
      const dashBonus = b.isDash ? -0.015 : 0;
      return { lane: b.lane, adjustedST: Number((b.predictedST + dashBonus).toFixed(3)) };
    })
    .sort((a,b)=> a.adjustedST - b.adjustedST);
}
function decideAttackType(lead, boats){
  if (!lead) return "none";
  const lane = lead.lane;
  const byLane = Object.fromEntries(boats.map(b => [b.lane, b.adjustedST]));
  if (lane === 4 && lead.adjustedST <= 0.14) return "fullMakuri";
  if (lane === 3 && byLane[2]!=null && byLane[2] > lead.adjustedST + 0.01) return "makuriZashi";
  if (lane === 2 && boats[0]?.lane === 4) return "sashi";
  return "none";
}

// ---------- 簡易 道中シミュレーション ----------
function simulateRace(startOrder, perf, laps = 3){
  let positions = [...startOrder]; // [{lane,pos}]
  for (let lap=1; lap<=laps; lap++){
    positions = positions.map(p => {
      const st = perf[p.lane] || { turnSkill:0.5, straightSkill:0.5 };
      const turnGain = (st.turnSkill - 0.5) * 0.2;
      const straGain = (st.straightSkill - 0.5) * 0.2;
      const rnd = Math.random() * 0.05;
      return { ...p, score: turnGain + straGain + rnd };
    }).sort((a,b)=> b.score - a.score)
      .map((p,idx)=> ({ ...p, pos: idx+1 }));
  }
  return positions;
}

// ---------- シナリオ→買い目 ----------
function generateBets(scenarios, mainCount=18, anaCount=2){
  const sorted = [...scenarios].sort((a,b)=> b.prob - a.prob);
  let main = [];
  for (const sc of sorted){
    for (const f of sc.first){
      for (const s of sc.second){
        if (s === f) continue;
        for (const t of sc.third){
          if (t===f || t===s) continue;
          main.push([f,s,t]);
        }
      }
    }
    if (main.length >= mainCount) break;
  }
  main = main.slice(0, mainCount);

  let ana = [];
  for (const sc of [...sorted].reverse()){
    if (sc.prob < 0.10){
      for (const f of sc.first){
        for (const s of sc.second){
          for (const t of sc.third){
            if (f!==s && s!==t && f!==t) ana.push([f,s,t]);
          }
        }
      }
    }
    if (ana.length >= anaCount) break;
  }
  ana = ana.slice(0, anaCount);
  return { main, ana };
}

// ---------- メイン ----------
async function main(){
  const inPath = path.join(ROOT, "public", "integrated", "v1", DATE, PID, `${RACE}.json`);
  const data = await readJson(inPath);

  const weather = data?.weather || {};
  const venueName = data?.meta?.venueName || "";

  // 展示順位の付与
  const entriesEx = assignExRanks(data.entries || []);

  // per-boat feature抽出
  const enriched = entriesEx.map(e => {
    const lane = Number(e.startCourse ?? e.lane);
    const isDash = lane >= 4;

    const entryAvg = e?.stats?.entryCourse?.avgST ?? null;
    const rcAvg    = e?.racecard?.avgST ?? null;
    const tenjiRaw = e?.exhibition?.st ?? null;
    const exRank   = e.exRank ?? null;

    const predictedST = calcPredictedST(
      entryAvg, rcAvg, tenjiRaw, exRank,
      { windSpeed: weather.windSpeed ?? 0, stabilizer: weather.stabilizer ?? false, isDash }
    );

    const cr = pickCourseRates(e);
    const realClass = calcRealClassSafe({
      motor2: e?.racecard?.motorTop2 ?? e?.racecard?.motor2,
      avgST: rcAvg,
      course1Win: lane===1 ? cr.winRate : undefined,
      age: e?.racecard?.age
    });

    // 簡易スコア（説明: 1着率・モーター・展示順位・予測STの混合）
    const motor2 = Number(e?.racecard?.motorTop2 ?? e?.racecard?.motor2 ?? 0);
    const exTerm = exRank ? (7 - exRank) * 0.3 : 0;       // 1位=+1.8, 2位=+1.5...
    const stTerm = (0.25 - predictedST) * 30;             // 0.15で +3, 0.20で +1.5
    const baseScore = (cr.winRate * 0.2) + (motor2 * 0.1) + exTerm + stTerm;

    const upset = calcUpsetScoreSimple({ ...e, startCourse: lane, courseRates: cr }, weather);

    return {
      number: Number(e.number ?? e?.racecard?.number),
      name:   e?.racecard?.name ?? e?.exhibition?.name ?? "",
      lane,
      startCourse: lane,
      realClass,
      natWin: e?.racecard?.natWin ?? e?.racecard?.natTop1 ?? null,
      locWin: e?.racecard?.locWin ?? e?.racecard?.locTop1 ?? null,
      motor2: motor2,
      age: e?.racecard?.age ?? null,
      avgST: rcAvg ?? null,
      courseRates: cr,
      predictedST,
      upset,
      exRank,
      tilt: e?.exhibition?.tilt ?? null,
      score: Number(baseScore.toFixed(2)),
      tenjiTime: e?.exhibition?.tenjiTime ?? null,
      tenjiST: e?.exhibition?.st ?? null
    };
  });

  // ランキング
  const ranking = [...enriched].sort((a,b)=> b.score - a.score);

  // スリット順（予測ST + ダッシュ補正）
  const slitOrder = predictSlitOrder(
    enriched.map(r => ({ lane: r.lane, predictedST: r.predictedST, isDash: r.lane>=4 }))
  );

  const attackType = decideAttackType(slitOrder[0], slitOrder);

  // 簡易 道中性能（級別/展示から適当に割当）
  const perf = {};
  for (const r of enriched){
    let turn = 0.5, stra = 0.5;
    if (r.realClass.startsWith("A")) { turn += 0.08; stra += 0.06; }
    if (r.exRank === 1) stra += 0.05;
    if (r.predictedST <= 0.14) stra += 0.02;
    perf[r.lane] = {
      turnSkill: clamp(turn, 0.3, 0.8),
      straightSkill: clamp(stra, 0.3, 0.8)
    };
  }

  // スタート直後の順位（スリット順を仮の初期順に）
  const startOrder = slitOrder.map((s,idx)=> ({ lane: s.lane, pos: idx+1 }));
  const simulate = simulateRace(startOrder, perf, 3);

  // 簡易シナリオ生成（攻め手別）
  const scen = [];
  if (attackType === "fullMakuri"){
    // 4頭シナリオ
    scen.push({ first:[4], second:[3,5,2,1], third:[3,5,2,1,6], prob:0.35 });
    // 3頭（差し/まくり差し）
    scen.push({ first:[3], second:[4,5], third:[2,1,6], prob:0.15 });
  }else if (attackType === "makuriZashi"){
    scen.push({ first:[3], second:[4,1,2], third:[4,1,2,5,6], prob:0.35 });
    scen.push({ first:[1], second:[3,2], third:[3,2,5,6], prob:0.15 });
  }else if (attackType === "sashi"){
    scen.push({ first:[2], second:[1,3], third:[1,3,4,5], prob:0.35 });
    scen.push({ first:[1], second:[2,3], third:[2,3,4,5], prob:0.15 });
  }else{
    // デフォ：インもしくは外伸び拮抗
    scen.push({ first:[ranking[0]?.lane ?? 1], second:[1,2,3,4,5,6].filter(x=>x!==(ranking[0]?.lane ?? 1)).slice(0,3), third:[1,2,3,4,5,6], prob:0.30 });
    scen.push({ first:[(ranking[1]?.lane ?? 2)], second:[(ranking[0]?.lane ?? 1),(ranking[2]?.lane ?? 3)], third:[1,2,3,4,5,6], prob:0.15 });
  }

  const bets = generateBets(scen, 18, 2);

  // 出力 JSON
  const outJson = {
    meta: {
      date: DATE, pid: PID, venueName, race: RACE,
      file: path.relative(ROOT, inPath),
      generatedAt: new Date().toISOString()
    },
    weather,
    ranking,
    slitOrder,
    attackType,
    simulate,
    scenarios: scen,
    bets
  };

  await ensureDir(path.join(ROOT, "out"));
  await fs.writeFile(path.join(ROOT, "out", "prediction.json"), JSON.stringify(outJson, null, 2), "utf8");

  // 簡易Markdown
  const mdLines = [];
  mdLines.push(`# 予測 ${DATE} pid=${PID} race=${RACE}`);
  const wtxt = [
    weather?.weather ?? "—",
    weather?.temperature!=null ? `気温${weather.temperature}℃` : null,
    weather?.windSpeed!=null ? `風${weather.windSpeed}m` : null,
    weather?.windDirection ?? null,
    weather?.waveHeight!=null ? `波高${Number(weather.waveHeight)*100}cm` : null,
    weather?.stabilizer ? `安定板` : null
  ].filter(Boolean).join(" / ");
  mdLines.push(`**気象**: ${wtxt || "—"}`);
  mdLines.push("");
  mdLines.push("## ランキング");
  ranking.forEach((r,i)=>{
    mdLines.push(`${i+1}. 枠${r.lane} 登番${r.number} ${r.name}  score=${r.score}  (予測ST:${r.predictedST}${r.exRank?` / 展示${r.exRank}位`:``})`);
  });
  mdLines.push("");
  mdLines.push("## 買い目(雛形)");
  [...bets.main.slice(0,6), ...bets.ana].forEach(t=>{
    mdLines.push(`3連単 ${t[0]}-${t[1]}-${t[2]}`);
  });
  mdLines.push("");

  const mdDir = path.join(ROOT, "public", "predictions", DATE, PID);
  await ensureDir(mdDir);
  await fs.writeFile(path.join(mdDir, `${RACE}.md`), mdLines.join("\n"), "utf8");

  console.log("wrote: out/prediction.json");
  console.log(`wrote: ${path.relative(ROOT, path.join(mdDir, `${RACE}.md`))}`);
}

main().catch(e => { console.error(e); process.exit(1); });
