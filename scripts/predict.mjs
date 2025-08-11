// scripts/predict.mjs
// Node v20 / ESM
// 入力: public/integrated/v1/<date>/<pid>/<race>.json（統合データ）
// 出力: out/prediction.json（中間値フル） / public/predictions/<date>/<pid>/<race>/1R.md（要約）
// 環境変数: DATE(YYYYMMDD|today) / PID(01..24) / RACE(1R..12R) / SLACK_WEBHOOK_URL(optional)

import fs from "node:fs/promises";
import fssync from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

const ROOT = process.cwd();
const to2 = (s) => String(s).padStart(2, "0");
const clamp = (x, a, b) => Math.max(a, Math.min(b, x));
const rnd = (x, d=2) => Math.round(x * (10**d)) / (10**d);

// --------- 引数/環境変数 ----------
function pickArg(name, fallback){
  const v = process.env[name];
  return v == null || v === "" ? fallback : v;
}
let DATE = (pickArg("DATE","today")||"").replace(/-/g,"");
let PID  = to2(pickArg("PID","04"));
let RACE = (pickArg("RACE","1R")||"").toUpperCase().replace(/[^\d]/g,"") + "R";

// --------- 便利系 ----------
async function ensureDir(p){ await fs.mkdir(p,{recursive:true}); }
const readJson = async (p) => JSON.parse(await fs.readFile(p,"utf8"));
const exists = (p) => fssync.existsSync(p);
function tryParseST(stStr){
  if (!stStr) return null;
  const s = String(stStr).trim();
  if (s.startsWith("F")) {
    const n = Number("0." + s.replace(/[F\.]/g,""));
    return { st: n, f: true };
  }
  if (s.startsWith(".")) return { st: Number("0"+s), f: false };
  const n = Number(s);
  return isFinite(n) ? { st: n, f: false } : null;
}
function asNumber(x){ const n = Number(x); return isFinite(n) ? n : null; }

// --------- 1) 統合データの読み込み ----------
async function loadIntegrated(){
  const p1 = path.join(ROOT,"public","integrated","v1", DATE, PID, `${RACE}.json`);
  const p2 = path.join(ROOT,"public","integrated","v1","today", PID, `${RACE}.json`);
  if (exists(p1)) return { json: await readJson(p1), path: p1 };
  if (DATE === "today" && exists(p2)) return { json: await readJson(p2), path: p2 };
  throw new Error(`integrated not found: ${DATE}/${PID}/${RACE}`);
}

// --------- 2) 計算関数（あなたがくれた断片を統合） ----------

/** 実質級別算出 */
function calcRealClass(p) {
  let score = 0;
  const natWin = asNumber(p.natWin) ?? 0;
  const locWin = asNumber(p.locWin) ?? 0;
  const motor2 = asNumber(p.motor2) ?? 0;
  const age    = asNumber(p.age) ?? 0;
  const avgST  = asNumber(p.avgST) ?? null;
  const c1Win  = asNumber(p.course1Win) ?? null;

  if (natWin >= 7.0) score += 6;
  else if (natWin >= 6.0) score += 5;
  else if (natWin >= 5.0) score += 4;
  else if (natWin >= 4.0) score += 3;
  else if (natWin >= 3.0) score += 2;
  else score += 1;

  if (locWin >= natWin * 0.95) score += 1;
  if (locWin >= natWin) score += 1;

  if (motor2 >= 45) score += 2;
  else if (motor2 >= 35) score += 1;
  else if (motor2 < 30) score -= 1;

  if (age <= 30) score += 1;
  if (age >= 55) score -= 1;

  if (avgST && avgST <= 0.16) score += 1;
  else if (avgST && avgST >= 0.21) score -= 1;

  if (c1Win != null && c1Win >= 40) score += 1;
  if (c1Win != null && c1Win < 20) score -= 1;

  if (score >= 9) return { label: "A1", score };
  if (score >= 7) return { label: "A2", score };
  if (score >= 5) return { label: "B1上位", score };
  if (score >= 3) return { label: "B1中位", score };
  if (score >= 1) return { label: "B1下位", score };
  return { label: "B2", score };
}

/** コース別成績（1着率・2連対率） */
function calcCourseRates(firstCount, secondCount, starts) {
  if (!starts) return { winRate: 0, top2Rate: 0 };
  const winRate = (firstCount / starts) * 100;
  const top2Rate = ((firstCount + secondCount) / starts) * 100;
  return { winRate: rnd(winRate,1), top2Rate: rnd(top2Rate,1) };
}

/** 展示順位別過去成績の参照（存在しなければ0%） */
function getExhibitionRankStats(rank, statsMap) {
  return statsMap?.[rank] ?? { win: 0, top2: 0 };
}

/** 予想ST補正 */
function calcPredictedST(avgST, tenjiST, opts = {}) {
  const { attackBonus=false, fCount=0, motorNobi=false } = opts;
  let baseST;
  if (avgST && avgST > 0) baseST = (avgST + tenjiST) / 2;
  else baseST = tenjiST + 0.02;
  if (attackBonus) baseST -= 0.03;
  if (motorNobi)   baseST -= 0.01;
  if (fCount > 0)  baseST += 0.02;
  baseST = clamp(baseST, 0.10, 0.30);
  return rnd(baseST, 2);
}

/** 波乱指数 */
function calcUpsetScore(p, opts = {}) {
  const { isInCourse=false, windSpeed=0, windDir="", dashCourse=false } = opts;
  let score = 0;
  if (isInCourse) {
    if (p.course1Win != null && p.course1Win < 50) score += 15;
    if (p.avgST != null && p.avgST > 0.19) score += 10;
    if ((p.loseMakuri ?? 0) > 3) score += 10;
  }
  if (dashCourse && (p.avgST ?? 9) <= 0.16) score += 20;
  if (p.motorNobi) score += 10;
  if ((p.kimariteMakuri ?? 0) >= 3) score += 15;
  if ((p.exTimeRank ?? 9) <= 2) score += 10;
  if ((p.lane ?? 7) >= 5 && (p.motor2 ?? 0) >= 40) score += 10;
  if (windSpeed >= 2 && /(西|向)/.test(String(windDir)) && dashCourse) score += 10;
  return Math.min(100, score);
}

/** スリット順予測 */
function predictSlitOrder(boats) {
  return boats
    .map(b => {
      const dashBonus = b.dash ? -0.015 : 0;
      return { lane: b.lane, adjustedST: (b.predictedST ?? 0.2) + dashBonus };
    })
    .sort((a, b) => a.adjustedST - b.adjustedST);
}

/** 攻め手判定 */
function decideAttackType(lead, slitOrder) {
  if (!lead) return "none";
  const lane = lead.lane;
  const byLane = Object.fromEntries(slitOrder.map((b)=>[b.lane,b]));
  if (lane === 4 && lead.adjustedST <= 0.14) return "fullMakuri";
  if (lane === 3 && byLane[2] && byLane[2].adjustedST > lead.adjustedST + 0.01) return "makuriZashi";
  if (lane === 2 && slitOrder[0]?.lane === 4) return "sashi";
  return "none";
}

/** 道中展開シミュレーション（簡易） */
function simulateRace(startOrder, perf, laps = 3) {
  let positions = [...startOrder];
  for (let lap=1; lap<=laps; lap++){
    positions = positions.map(p => {
      const st = perf[p.lane] ?? {turnSkill:.5, straightSkill:.5};
      const turnGain = (st.turnSkill - 0.5) * 0.2;
      const straightGain = (st.straightSkill - 0.5) * 0.2;
      const noise = (crypto.randomInt(0,5))/100; // 0.00〜0.04
      return { ...p, score: turnGain + straightGain + noise };
    });
    positions.sort((a,b)=> b.score - a.score);
    positions = positions.map((p,i)=> ({ lane:p.lane, pos:i+1 }));
  }
  return positions;
}

/** フォーメーション生成 */
function generateBets(scenarios, mainCount=18, anaCount=2){
  const sorted = [...scenarios].sort((a,b)=> b.prob - a.prob);

  let mainBets = [];
  for (const sc of sorted){
    sc.first.forEach(f=>{
      sc.second.forEach(s=>{
        if (s===f) return;
        sc.third.forEach(t=>{
          if (t===f || t===s) return;
          mainBets.push([f,s,t]);
        });
      });
    });
    if (mainBets.length >= mainCount) break;
  }
  mainBets = mainBets.slice(0, mainCount);

  let anaBets = [];
  for (const sc of [...sorted].reverse()){
    if (sc.prob < 0.1){
      sc.first.forEach(f=>{
        sc.second.forEach(s=>{
          sc.third.forEach(t=> anaBets.push([f,s,t]));
        });
      });
    }
    if (anaBets.length >= anaCount) break;
  }
  anaBets = anaBets.slice(0, anaCount);

  return { main: mainBets, ana: anaBets };
}

// --------- 3) メイン処理 ----------
async function main(){
  const { json, path: srcPath } = await loadIntegrated();
  const weather = json.weather ?? {};
  const entries = (json.entries ?? []).map(e => ({...e}));

  // 展示タイム順位（小さいほど上位）
  const tenjiTimes = entries.map(e => ({
    lane: e.lane, t: asNumber(e.exhibition?.tenjiTime) ?? 99
  })).sort((a,b)=> a.t - b.t);
  const tenjiRankByLane = Object.fromEntries(tenjiTimes.map((x,i)=>[x.lane, i+1]));

  // 各艇の特徴抽出＆各段階計算
  const boats = entries.map((e) => {
    const rc = e.racecard ?? {};
    const stAvg = asNumber(rc.avgST);
    const stTenjiParsed = tryParseST(e.exhibition?.st ?? null);
    const stTenji = stTenjiParsed?.st ?? null;
    const fOnStart = stTenjiParsed?.f ? 1 : 0;
    const self = e.stats?.entryCourse?.selfSummary;
    const starts = asNumber(self?.starts) ?? 0;
    const firstC = asNumber(self?.firstCount) ?? 0;
    const secondC= asNumber(self?.secondCount) ?? 0;
    const courseRates = calcCourseRates(firstC, secondC, starts);

    const natWin = asNumber(rc.natTop1); // 近似: 全国勝率の代理
    const locWin = asNumber(rc.locTop1) ?? natWin ?? 0;
    const motor2 = asNumber(rc.motorTop2);
    const age    = asNumber(rc.age);
    const c1Win  = e.startCourse === 1 ? courseRates.winRate : null;

    const realClass = calcRealClass({ natWin, locWin, motor2, age, avgST: stAvg, course1Win: c1Win });

    // 伸び寄り推定（展示タイム順位が良い or motor2良）
    const motorNobi = (tenjiRankByLane[e.lane] ?? 7) <= 2 || (motor2 ?? 0) >= 45;

    // 予想ST
    const predictedST = calcPredictedST(
      stAvg ?? 0.18,
      stTenji ?? 0.18,
      {
        attackBonus: e.startCourse >= 4, // 角想定
        fCount: (rc.flyingCount ?? 0) + fOnStart,
        motorNobi
      }
    );

    // 波乱指数
    const upset = calcUpsetScore({
      course1Win: c1Win, avgST: stAvg, loseMakuri: 0, // 欠損は0扱い
      motorNobi, kimariteMakuri: 0, exTimeRank: tenjiRankByLane[e.lane],
      lane: e.lane, motor2
    }, {
      isInCourse: e.startCourse === 1,
      windSpeed: asNumber(weather.windSpeed) ?? 0,
      windDir: String(weather.windDirection ?? ""),
      dashCourse: e.startCourse >= 4
    });

    // 道中性能（簡易）
    const perf = {
      turnSkill: clamp(((motor2??0)/60) + ((tenjiRankByLane[e.lane]??4) <=2 ? 0.1:0) , 0.3, 0.8),
      straightSkill: clamp(((motor2??0)/70) + (motorNobi?0.1:0), 0.3, 0.8)
    };

    // 総合スコア（並び用の簡易指標）
    const baseScore = (realClass.score*2) + (motor2??0)/10 + ( (6-(tenjiRankByLane[e.lane]??6))*1.2 );

    return {
      lane: e.lane,
      number: e.number,
      name: rc.name ?? e.exhibition?.name ?? "",
      startCourse: e.startCourse,
      dash: e.startCourse >= 4,
      tenjiTime: asNumber(e.exhibition?.tenjiTime),
      tenjiRank: tenjiRankByLane[e.lane],
      avgST: stAvg, tenjiST: stTenji, predictedST,
      realClass, courseRates,
      motor2, age, natWin, locWin,
      upsetScore: upset,
      perf,
      baseScore: rnd(baseScore,1)
    };
  });

  // スリット順→攻め手→スタート直後の順
  const slitOrder = predictSlitOrder(boats);
  const attackType = decideAttackType(slitOrder[0], slitOrder);
  const startOrder = slitOrder.map((b,i)=> ({ lane: b.lane, pos: i+1 }));

  // 道中シミュレーション
  const perfMap = Object.fromEntries(boats.map(b=> [b.lane, b.perf]));
  const goalOrder = simulateRace(startOrder, perfMap, 3);

  // シナリオ（超簡易の重み付け）
  const lead = slitOrder[0]?.lane;
  const scenarios = [];
  if (attackType === "fullMakuri"){
    scenarios.push({ first:[lead], second:[3,5,2,1].filter(x=>x!==lead), third:[1,2,3,5,6].filter(x=>x!==lead), prob:0.35 });
  }else if (attackType === "makuriZashi"){
    scenarios.push({ first:[3], second:[4,5,1,2], third:[1,2,4,5,6], prob:0.28 });
  }else if (attackType === "sashi"){
    scenarios.push({ first:[2], second:[3,4,1], third:[3,4,5,1,6], prob:0.23 });
  }else{
    scenarios.push({ first:[1], second:[2,3], third:[4,5,6], prob:0.22 });
  }
  // バックアップ数本
  scenarios.push({ first:[goalOrder[0]?.lane ?? 1], second:[goalOrder[1]?.lane ?? 2], third:[goalOrder[2]?.lane ?? 3], prob:0.18 });
  scenarios.push({ first:[slitOrder[0]?.lane ?? 1], second:[slitOrder[1]?.lane ?? 2], third:[1,2,3,4,5,6], prob:0.12 });

  const bets = generateBets(scenarios, 18, 2);

  // 人向けランキング（baseScore降順）
  const ranking = [...boats].sort((a,b)=> b.baseScore - a.baseScore)
    .map((b,i)=> ({
      rank: i+1,
      lane: b.lane,
      number: b.number,
      name: b.name,
      score: b.baseScore,
      detail: {
        class: b.realClass.label,
        motor2: b.motor2, tenjiRank: b.tenjiRank,
        predictedST: b.predictedST
      }
    }));

  // -------- 出力（JSON + Markdown） --------
  const outDir = path.join(ROOT,"out");
  await ensureDir(outDir);
  const jsonOut = {
    params: { DATE, PID, RACE },
    source: path.relative(ROOT, srcPath),
    weather,
    boats,
    slitOrder,
    attackType,
    startOrder,
    goalOrder,
    scenarios,
    bets
  };
  await fs.writeFile(path.join(outDir,"prediction.json"), JSON.stringify(jsonOut,null,2), "utf8");

  const pubDir = path.join(ROOT,"public","predictions", DATE, PID, RACE);
  await ensureDir(pubDir);
  const md = [
    `# 予測 ${DATE} pid=${PID} race=${RACE}`,
    ``,
    `**気象**: ${weather.weather ?? "-"} / 気温${weather.temperature ?? "-"}℃ 風${weather.windSpeed ?? "-"}m ${weather.windDirection ?? "-"} 波高${weather.waveHeight ?? "-"}m`,
    ``,
    `## ランキング`,
    ...ranking.map(r => `- 枠${r.lane} 登番${r.number} ${r.name}  score=${r.score}（${r.detail.class} / 予想ST:${r.detail.predictedST} / 展示順位:${r.detail.tenjiRank} / ﾓｰﾀｰ2連:${r.detail.motor2 ?? "-"}）`),
    ``,
    `## スタート展開`,
    `- スリット順: ${slitOrder.map(x=>x.lane).join(" > ")} / 攻め手: **${attackType}**`,
    `- 道中（簡易）ゴール順: ${goalOrder.map(x=>x.lane).join(" > ")}`,
    ``,
    `## シナリオ要約`,
    ...scenarios.map(s => `- P=${rnd(s.prob*100,1)}%: 1着[${s.first.join(",")}] 2着[${s.second.join(",")}] 3着[${s.third.join(",")}]`),
    ``,
    `## 買い目（本命〜中穴18点＋穴2点＝計20点）`,
    `**本命〜中穴**`,
    ...bets.main.map(a=> `- 3連単 ${a[0]}-${a[1]}-${a[2]}`),
    ``,
    `**穴目**`,
    ...bets.ana.map(a=> `- 3連単 ${a[0]}-${a[1]}-${a[2]}`),
    ``
  ].join("\n");
  await fs.writeFile(path.join(pubDir,"1R.md"), md, "utf8");

  // Slack 通知（任意）
  const hook = process.env.SLACK_WEBHOOK_URL;
  if (hook){
    try{
      const payload = {
        text: `予測完了 ${DATE} pid=${PID} race=${RACE}\nトップ: 枠${ranking[0]?.lane} ${ranking[0]?.name} / 攻め手:${attackType}\nスリット:${slitOrder.map(x=>x.lane).join(">")} / ゴール:${goalOrder.map(x=>x.lane).join(">")}`
      };
      const res = await fetch(hook, {
        method:"POST",
        headers: { "Content-Type":"application/json" },
        body: JSON.stringify(payload)
      });
      if (!res.ok) console.error("Slack webhook failed:", res.status, await res.text());
    }catch(err){ console.error("Slack webhook error:", err?.message); }
  }

  console.log("wrote:", path.relative(ROOT, path.join(outDir,"prediction.json")));
  console.log("wrote:", path.relative(ROOT, path.join(pubDir,"1R.md")));
}

main().catch(e => { console.error(e); process.exit(1); });
