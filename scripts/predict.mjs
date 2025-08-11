// scripts/predict.mjs
// Node v20 / ESM
// 入力: public/integrated/v1/<date>/<pid>/<race>.json
// 出力: out/prediction.json（機械可読） / public/predictions/<date>/<pid>/<race>.md（人間向け）
//
// 環境変数:
//   DATE: YYYYMMDD or "today"
//   PID:  "01".."24"
//   RACE: "1R".."12R"

import fs from "node:fs/promises";
import fssync from "node:fs";
import path from "node:path";

const ROOT = process.cwd();
const to2 = (s) => String(s).padStart(2, "0");
const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
const round1 = (x) => Math.round(x * 10) / 10;
const round2 = (x) => Math.round(x * 100) / 100;

// -------- 引数/環境 --------
const DATE = (process.env.DATE || "today").replace(/-/g, "");
const PID  = to2(process.env.PID || "04");
const RACE = (process.env.RACE || "1R").toUpperCase().replace(/[^\d]/g,"") + "R";

// ---------- I/O ----------
async function ensureDir(p){ await fs.mkdir(p, { recursive: true }); }
async function readJSON(p){ return JSON.parse(await fs.readFile(p,"utf8")); }
function exists(p){ return fssync.existsSync(p); }

// ---------- 実用ユーティリティ ----------
function safeNum(n, d=null){ const x=Number(n); return Number.isFinite(x) ? x : d; }
function pick(obj, ...ks){ const r={}; for(const k of ks){ if(obj?.[k] !== undefined) r[k]=obj[k]; } return r; }

// =========================================
// 1) 計算ユーティリティ（ユーザー提示の関数群）
// =========================================

/**
 * 実質級別算出
 * @param {Object} p
 *   natWin: 全国勝率（例 6.20）
 *   locWin: 当地勝率
 *   motor2: モーター2連対率(%)
 *   age: 年齢
 *   avgST: 平均ST(秒)
 *   course1Win: 進入コース1での1着率（%）
 */
function calcRealClass(p) {
  let score = 0;

  // 基礎点：全国勝率
  if (p.natWin >= 7.0) score += 6;
  else if (p.natWin >= 6.0) score += 5;
  else if (p.natWin >= 5.0) score += 4;
  else if (p.natWin >= 4.0) score += 3;
  else if (p.natWin >= 3.0) score += 2;
  else score += 1;

  // 当地勝率ボーナス
  if (p.locWin >= p.natWin * 0.95) score += 1;
  if (p.locWin >= p.natWin) score += 1;

  // モーター2連率補正
  if (p.motor2 >= 45) score += 2;
  else if (p.motor2 >= 35) score += 1;
  else if (p.motor2 < 30) score -= 1;

  // 年齢補正
  if (p.age <= 30) score += 1;
  if (p.age >= 55) score -= 1;

  // 平均ST補正
  if (p.avgST && p.avgST <= 0.16) score += 1;
  else if (p.avgST && p.avgST >= 0.21) score -= 1;

  // コース別1着率補正
  if (p.course1Win && p.course1Win >= 40) score += 1;
  if (p.course1Win && p.course1Win < 20) score -= 1;

  if (score >= 9) return "A1";
  if (score >= 7) return "A2";
  if (score >= 5) return "B1上位";
  if (score >= 3) return "B1中位";
  if (score >= 1) return "B1下位";
  return "B2";
}

/**
 * コース別成績（1着率・2連対率）
 */
function calcCourseRates(firstCount, secondCount, starts) {
  const s = Number(starts) || 0;
  if (s === 0) return { winRate: 0, top2Rate: 0 };
  const winRate  = (Number(firstCount)||0) / s * 100;
  const top2Rate = ((Number(firstCount)||0) + (Number(secondCount)||0)) / s * 100;
  return { winRate: round1(winRate), top2Rate: round1(top2Rate) };
}

/**
 * 展示順位別成績評価
 * stats: {1:{win,top2},2:{...},...}
 */
function getExhibitionRankStats(rank, stats) {
  return stats?.[rank] ? { winRate: stats[rank].win, top2Rate: stats[rank].top2 } : { winRate: 0, top2Rate: 0 };
}

/**
 * 予想ST補正計算
 */
function calcPredictedST(avgST, tenjiST, opts = {}) {
  const {
    attackBonus = false,
    fCount = 0,
    motorNobi = false
  } = opts;

  let baseST;
  const a = safeNum(avgST);
  const t = safeNum(tenjiST);
  if (a && a > 0) {
    baseST = (a + t) / 2;
  } else {
    baseST = (t || 0.16) + 0.02;
  }

  if (attackBonus) baseST -= 0.03;
  if (motorNobi)   baseST -= 0.01;
  if (fCount > 0)  baseST += 0.02;

  baseST = clamp(baseST, 0.10, 0.30);
  return round2(baseST);
}

/**
 * 波乱指数
 */
function calcUpsetScore(p, opts = {}) {
  const {
    isInCourse = false,
    windSpeed = 0,
    windDir = "",
    dashCourse = false
  } = opts;

  let score = 0;

  if (isInCourse) {
    if ((p.course1Win ?? 100) < 50) score += 15;
    if ((p.avgST ?? 0.2) > 0.19) score += 10;
    if ((p.loseMakuri ?? 0) > 3) score += 10;
  }

  if (dashCourse && (p.avgST ?? 0.2) <= 0.16) score += 20;
  if (p.motorNobi) score += 10;
  if ((p.kimariteMakuri ?? 0) >= 3) score += 15;

  if ((p.exTimeRank ?? 9) <= 2) score += 10;

  if ((p.lane ?? 0) >= 5 && (p.motor2 ?? 0) >= 40) score += 10;

  if ((windSpeed ?? 0) >= 2 && /西|向/.test(windDir || "") && dashCourse) score += 10;

  return Math.min(100, score);
}

/**
 * スリット順予測（ダッシュ助走補正）
 */
function predictSlitOrder(boats) {
  return boats
    .map(b => {
      const dashBonus = b.dash ? -0.015 : 0;
      return { lane: b.lane, adjustedST: (b.predictedST ?? 0.2) + dashBonus };
    })
    .sort((a, b) => a.adjustedST - b.adjustedST);
}

/**
 * 攻め手判定
 */
function decideAttackType(lead, boats) {
  const lane = lead.lane;
  const byLane = Object.fromEntries(boats.map(b => [b.lane, b]));
  if (lane === 4 && lead.adjustedST <= 0.14) return "fullMakuri";     // 4カド全速
  if (lane === 3 && byLane[2] && byLane[2].adjustedST > lead.adjustedST + 0.01) return "makuriZashi"; // 3のま差し
  if (lane === 2 && boats[0]?.lane === 4) return "sashi";             // 4が先行で2が差し
  return "none";
}

/**
 * 簡易道中シミュレーション
 */
function simulateRace(startOrder, perf, laps = 3) {
  let positions = [...startOrder];
  for (let lap = 1; lap <= laps; lap++) {
    positions = positions.map(p => {
      const stats = perf[p.lane] || { turnSkill: 0.5, straightSkill: 0.5 };
      const turnGain     = (stats.turnSkill - 0.5) * 0.2;
      const straightGain = (stats.straightSkill - 0.5) * 0.2;
      p.score = turnGain + straightGain + Math.random() * 0.02;
      return p;
    });
    positions.sort((a, b) => b.score - a.score);
    positions = positions.map((p, i) => ({ ...p, pos: i + 1 }));
  }
  return positions;
}

/**
 * シナリオから舟券生成
 */
function generateBets(scenarios, mainCount = 18, anaCount = 2) {
  const sorted = [...scenarios].sort((a, b) => b.prob - a.prob);

  let mainBets = [];
  for (const sc of sorted) {
    sc.first.forEach(f => {
      sc.second.forEach(s => {
        if (s === f) return;
        sc.third.forEach(t => {
          if (t === f || t === s) return;
          mainBets.push([f, s, t]);
        });
      });
    });
    if (mainBets.length >= mainCount) break;
  }
  mainBets = mainBets.slice(0, mainCount);

  let anaBets = [];
  for (const sc of [...sorted].reverse()) {
    if (sc.prob < 0.1) {
      sc.first.forEach(f => {
        sc.second.forEach(s => {
          sc.third.forEach(t => {
            if (f !== s && f !== t && s !== t) anaBets.push([f, s, t]);
          });
        });
      });
    }
    if (anaBets.length >= anaCount) break;
  }
  anaBets = anaBets.slice(0, anaCount);

  return { main: mainBets, ana: anaBets };
}

// =========================================
// 2) 場別特性補正テーブル（簡易）
// =========================================

const VENUE = {
  "01":"桐生","02":"戸田","03":"江戸川","04":"平和島","05":"多摩川","06":"浜名湖",
  "07":"蒲郡","08":"常滑","09":"津","10":"三国","11":"びわこ","12":"住之江",
  "13":"尼崎","14":"鳴門","15":"丸亀","16":"児島","17":"宮島","18":"徳山",
  "19":"下関","20":"若松","21":"芦屋","22":"福岡","23":"唐津","24":"大村"
};

// 風向の読み → 東/西/南/北 を含むかでざっくり
const EAST = /東/; const WEST = /西/; const SOUTH = /南/; const NORTH = /北/;

// 波高・風での閾値（任意調整）
const WAVE_LOW = 0.03;   // 3cm未満を静水面寄り
const WAVE_HIGH = 0.07;  // 7cm以上で荒れ
const WIND_MID = 4;
const WIND_HIGH = 7;

// 場別：コース系の係数（ベース）
const VENUE_BASE = {
  "02": { // 戸田
    inNerf: 0.94, dashAgg: 1.02, tightTurn: 1.05, prefer23Sashi: 1.03
  },
  "04": { // 平和島
    inNerf: 0.96, dashAgg: 1.03, tightTurn: 1.03, prefer23: 1.02, motorWeight: 1.05
  },
  "03": { // 江戸川（荒れやすい）
    inNerf: 0.92, dashAgg: 1.05, waveBoost: 1.06
  },
  "24": { // 大村（イン有利）
    inBuff: 1.04, dashAgg: 0.98
  },
  "12": { // 住之江（静水面寄り・ST勝負）
    stWeight: 1.05, waveBoost: 0.97
  }
  // 他場はデフォルト
};

function applyVenueWeatherAdjust({ baseScore, lane, startCourse, st, exRank, weather, pid, loseKimarite1, loseKimarite2 }) {
  let score = baseScore;
  const v = VENUE_BASE[pid] || {};
  const ws = safeNum(weather?.windSpeed, 0);
  const wd = weather?.windDirection || "";
  const wave = safeNum(weather?.waveHeight, 0);
  const stabilizer = !!weather?.stabilizer;

  // イン/ダッシュ係数
  const isDash = startCourse >= 4;
  if (v.inBuff && startCourse === 1) score *= v.inBuff;
  if (v.inNerf && startCourse === 1) score *= v.inNerf;
  if (v.dashAgg && isDash) score *= v.dashAgg;

  // ターン巧者/狭水面想定
  if (v.tightTurn && (startCourse === 2 || startCourse === 3 || startCourse === 4)) {
    if ((exRank ?? 4) <= 3) score *= v.tightTurn; // 展示上位＝操舵よしと仮定
  }

  // モーター重視場
  if (v.motorWeight && exRank === 1) score *= v.motorWeight;

  // 風向補正（向風=西寄りでダッシュ補正）
  if (ws >= WIND_MID && WEST.test(wd) && isDash) score *= 1.02;
  if (ws >= WIND_HIGH && WEST.test(wd) && isDash) score *= 1.03;

  // 波高補正（荒れ→内減、静→内微増）
  if (wave >= WAVE_HIGH) {
    if (startCourse === 1) score *= 0.95;
    if (isDash) score *= 1.02;
  } else if (wave <= WAVE_LOW) {
    if (startCourse === 1) score *= 1.02;
  }

  // 安定板（基本は握りにくい＝外減少、ただし場/風次第で逆もあり）
  if (stabilizer) {
    if (isDash) score *= 0.98;
    else score *= 1.01;
  }

  // 失敗型（決まり手負け傾向の転用）
  // 1コース「まくられ多い」→外攻め成功補正
  if (startCourse >= 4 && (loseKimarite1?.["まくり"] ?? 0) >= 4) score *= 1.02;
  // 1コース「差され多い」→2の差し補正
  if (startCourse === 2 && (loseKimarite1?.["差し"] ?? 0) >= 4) score *= 1.02;
  // 2コース「逃がし傾向」→1の信頼微増
  if (startCourse === 1 && (loseKimarite2?.["差し"] ?? 0) === 0 && (loseKimarite2?.["まくり"] ?? 0) === 0) {
    score *= 1.01;
  }

  return score;
}

// =========================================
// 3) メイン処理
// =========================================

function findIntegratedPath() {
  const p = path.join(ROOT, "public", "integrated", "v1", DATE, PID, `${RACE}.json`);
  if (exists(p)) return p;
  const pToday = path.join(ROOT, "public", "integrated", "v1", "today", PID, `${RACE}.json`);
  if (exists(pToday)) return pToday;
  throw new Error(`integrated json not found: ${DATE}/${PID}/${RACE}`);
}

function courseRateFromStatsEntry(entryStats) {
  // stats.entryCourse.selfSummary {starts,firstCount,secondCount}
  const ss = entryStats?.entryCourse?.selfSummary;
  if (!ss) return { winRate: 0, top2Rate: 0, starts: 0, first: 0, second: 0 };
  const { winRate, top2Rate } = calcCourseRates(ss.firstCount, ss.secondCount, ss.starts);
  return { winRate, top2Rate, starts: ss.starts, first: ss.firstCount, second: ss.secondCount };
}

function takeLoseKimariteFrom(stats, courseN=1){
  // そのコースでの loseKimarite を返す（なければ推定0）
  const lk = stats?.entryCourse?.loseKimarite;
  if (lk) return lk;
  // ない場合は全体 rows から推定…（既に slice 済み想定なので nullでOK）
  return null;
}

function decideDash(startCourse){ return startCourse >= 4; }

function predictedPerfFrom(entry, exRank) {
  // 簡易：展示上位をターン力/直線力へ割当
  const baseTurn = 0.5 + (exRank ? (3 - exRank) * 0.04 : 0); // rank1→+0.08, rank2→+0.04
  const baseStr  = 0.5 + ((entry.racecard?.motorTop2 ?? 30) - 30) / 100 * 0.2; // モーター2連対率基準
  return {
    turnSkill: clamp(baseTurn, 0.3, 0.8),
    straightSkill: clamp(baseStr, 0.3, 0.8)
  };
}

function tiltAffects(tiltStr){
  const t = Number(String(tiltStr||"").replace(/[^\-0-9.]/g,""));
  if (!Number.isFinite(t)) return 1.0;
  if (t > 0)  return 1.01;   // プラスチルトわずかに直線方向
  if (t < 0)  return 0.99;   // マイナスは安定寄りで微減
  return 1.0;
}

(async function main(){
  const file = findIntegratedPath();
  const data = await readJSON(file);

  const date = data.date || DATE;
  const pid  = data.pid  || PID;
  const race = data.race || RACE;
  const venueName = VENUE[pid] || `pid=${pid}`;

  const weather = data.weather || {};
  const ws  = safeNum(weather.windSpeed, 0);
  const wd  = weather.windDirection || "";
  const wt  = safeNum(weather.temperature, null);
  const wtr = safeNum(weather.waterTemperature, null);
  const wh  = safeNum(weather.waveHeight, null);
  const stb = !!weather.stabilizer;

  const entries = Array.isArray(data.entries) ? data.entries : [];

  // 展示タイム順位（簡易）を lane→rank で推定
  const tenjiList = entries.map(e => ({ lane: e.lane, val: safeNum(e.exhibition?.tenjiTime, null) })).filter(x => x.val != null);
  const exRankMap = {};
  if (tenjiList.length === 6) {
    tenjiList.sort((a,b) => a.val - b.val).forEach((x,i)=>{ exRankMap[x.lane] = i+1; });
  }

  // 1・2コースの loseKimarite を全体補正用に抽出
  const e1 = entries.find(e => e.startCourse === 1);
  const e2 = entries.find(e => e.startCourse === 2);
  const loseK1 = e1?.stats ? takeLoseKimariteFrom(e1.stats, 1) : null;
  const loseK2 = e2?.stats ? takeLoseKimariteFrom(e2.stats, 2) : null;

  // 各艇評価
  const boatsEval = entries.map((e) => {
    const rc = e.racecard || {};
    const st = e.stats    || {};
    const ex = e.exhibition || {};

    const natWin = safeNum(rc.natTop1, null);  // 全国勝率（例 6.12）
    const locWin = safeNum(rc.locTop1, natWin);
    const motor2 = safeNum(rc.motorTop2, null);
    const age    = safeNum(rc.age, null);
    const avgST  = safeNum(rc.avgST, null);
    const lane   = safeNum(e.lane, null);
    const startCourse = safeNum(e.startCourse, lane);

    // 進入コース別 自己成績
    const cr = courseRateFromStatsEntry(st);

    // 実質級別
    const realClass = calcRealClass({
      natWin, locWin, motor2, age, avgST,
      course1Win: (startCourse === 1 ? cr.winRate : null)
    });

    // 予想ST
    const fCount = safeNum(rc.flyingCount, 0);
    const tenjiST = ex?.st ? Number(String(ex.st).replace(/[^\d.]/g,""))/100 : 0.15; // ".08" → 0.08
    const attackBonus = decideDash(startCourse);
    // 伸び判定：展示タイム上位 or モーター良
    const motorNobi = (exRankMap[lane] && exRankMap[lane] <= 2) || (motor2 >= 45);
    const predictedST = calcPredictedST(avgST, tenjiST, { attackBonus, fCount, motorNobi });

    // 波乱指数（参考）
    const upset = calcUpsetScore({
      course1Win: cr.winRate, avgST, loseMakuri: (st.entryCourse?.matrixSelf?.loseMakuri ?? 0),
      motorNobi, kimariteMakuri: safeNum(st.entryCourse?.winKimariteSelf?.["まくり"], 0),
      exTimeRank: exRankMap[lane] || 9, lane, motor2
    }, { isInCourse: startCourse===1, windSpeed: ws, windDir: wd, dashCourse: decideDash(startCourse) });

    // ベーススコア（素点）
    let base = 10;
    // 勝率・当地
    if (natWin != null) base += (natWin - 4.5) * 1.2;   // 4.5を基準
    if (locWin != null) base += (locWin - (natWin ?? locWin)) * 0.6;
    // モーター
    if (motor2 != null) base += (motor2 - 35) * 0.1;
    // コース適性
    base += (cr.winRate - 15) * 0.08 + (cr.top2Rate - 30) * 0.04;
    // ST
    if (predictedST) base += (0.18 - predictedST) * 50 * 0.12; // 0.01 早いごとに +0.6点程度
    // 展示タイム順位
    if (exRankMap[lane]) base += (3 - exRankMap[lane]) * 0.7; // 1位:+1.4, 2位:+0.7
    // チルト
    base *= tiltAffects(ex.tilt);

    // 風・波・場特性・失敗型転用の総合補正
    const score = applyVenueWeatherAdjust({
      baseScore: Math.max(1, base),
      lane, startCourse,
      st: predictedST,
      exRank: exRankMap[lane],
      weather, pid,
      loseKimarite1: loseK1,
      loseKimarite2: loseK2
    });

    return {
      number: e.number,
      name: rc.name || e.exhibition?.name || String(e.number),
      lane, startCourse,
      realClass,
      natWin, locWin, motor2, age, avgST,
      courseRates: cr,
      predictedST,
      upset,
      exRank: exRankMap[lane] || null,
      tilt: ex.tilt ?? null,
      score: round2(score)
    };
  });

  // スリット順 → 攻め手推定
  const slitOrder = predictSlitOrder(
    boatsEval.map(b => ({ lane: b.lane, predictedST: b.predictedST, dash: decideDash(b.startCourse) }))
  );
  const attackType = decideAttackType(slitOrder[0], slitOrder);

  // 簡易シナリオ生成（攻め手種別に応じたフォーカス）
  let scenarios = [];
  if (attackType === "fullMakuri") {
    // 4→（3,5,2,1）→（3,5,2,1,6）
    scenarios.push({ first: [4], second: [3,5,2,1], third: [3,5,2,1,6], prob: 0.35 });
    scenarios.push({ first: [3], second: [4,5], third: [2,1,6], prob: 0.15 });
  } else if (attackType === "makuriZashi") {
    scenarios.push({ first: [3], second: [4,2], third: [4,2,1,5,6], prob: 0.32 });
    scenarios.push({ first: [1], second: [3,2], third: [3,2,4,5], prob: 0.18 });
  } else if (attackType === "sashi") {
    scenarios.push({ first: [2], second: [1,3,4], third: [1,3,4,5], prob: 0.28 });
    scenarios.push({ first: [1], second: [2,3], third: [2,3,4,5], prob: 0.22 });
  } else {
    // なし → 穏当寄り
    scenarios.push({ first: [1], second: [2,3,4], third: [2,3,4,5], prob: 0.30 });
    scenarios.push({ first: [3,2], second: [1,4], third: [1,4,5,6], prob: 0.20 });
  }

  // 道中性能（簡易）を作ってシミュレーション（参考：順位のブレを作る）
  const perf = {};
  for (const b of boatsEval) {
    const p = predictedPerfFrom({ racecard: { motorTop2: b.motor2 } }, b.exRank);
    perf[b.lane] = p;
  }
  const startOrder = slitOrder.map((s, i) => ({ lane: s.lane, pos: i + 1 }));
  const simResult = simulateRace(startOrder, perf, 2); // 2周ぶんの変動

  // スコアでランキング
  const ranking = [...boatsEval].sort((a,b)=> b.score - a.score);

  // 舟券生成
  const { main, ana } = generateBets(scenarios, 18, 2);

  // ========= 出力 =========
  const md = [
    `# 予測 ${date} pid=${pid}（${venueName}） ${race}`,
    `**気象**: ${weather.weather ?? "-"} / 気温${wt ?? "-"}℃ 風${ws ?? "-"}m ${wd || "-"} 波高${wh ?? "-"}m ${stb ? "（安定板）" : ""}`.trim(),
    "",
    "## ランキング",
    ...ranking.map((r,i)=>`${i+1}. 枠${r.lane} 登番${r.number} ${r.name}  score=${r.score}  (ST予想:${r.predictedST})  実質:${r.realClass}`),
    "",
    "## スリット順予測（補正後ST昇順）",
    slitOrder.map(s=>`枠${s.lane}: ${round2(s.adjustedST)}`).join(" / "),
    "",
    `## 攻め手推定: ${attackType}`,
    "",
    "## シミュレーション（参考・終盤順位）",
    simResult.map(p=>`P${p.pos}: 枠${p.lane}`).join(" / "),
    "",
    "## 買い目（本命〜中穴 18点）",
    main.map(x=>`3連単 ${x[0]}-${x[1]}-${x[2]}`).join("\n"),
    "",
    "## 穴目（2点）",
    ana.map(x=>`3連単 ${x[0]}-${x[1]}-${x[2]}`).join("\n"),
    ""
  ].join("\n");

  const outDir = path.join(ROOT, "out");
  await ensureDir(outDir);
  await fs.writeFile(path.join(outDir, "prediction.json"), JSON.stringify({
    meta: { date, pid, venueName, race, file, generatedAt: new Date().toISOString() },
    weather: { ...weather },
    ranking,
    slitOrder,
    attackType,
    simulate: simResult,
    scenarios,
    bets: { main, ana }
  }, null, 2), "utf8");

  const pubMd = path.join(ROOT, "public", "predictions", date, pid);
  await ensureDir(pubMd);
  await fs.writeFile(path.join(pubMd, `${race}.md`), md, "utf8");

  console.log(`wrote: out/prediction.json`);
  console.log(`wrote: public/predictions/${date}/${pid}/${race}.md`);
})().catch(e=>{ console.error(e); process.exit(1); });
