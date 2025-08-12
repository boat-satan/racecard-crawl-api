// 実質級別（絶対評価）を算出するモジュール
// 入力は integrated JSON の各エントリから整えた素朴なオブジェクトを想定。
// 数値は欠損に強く（null/undefinedを許容）、安全に評価します。

/**
 * @typedef {Object} RealClassInput
 * @property {number|null} natWin        全国勝率（例: 6.12）
 * @property {number|null} locWin        当地勝率（例: 5.80）なければ null
 * @property {number|null} motor2        モーター2連対率（%）
 * @property {number|null} age           年齢
 * @property {number|null} avgST         平均ST（秒）
 * @property {number|null} courseWinRate 進入コースの1着率（%）※ stats.entryCourse.selfSummary から算出して渡すと良い
 */

/**
 * 実質級別スコアを計算（高いほど強い）
 * 目安：A1≒9点以上 / A2≒7点以上 / B1上位≒5点以上 / B1中位≒3点以上 / B1下位≒1点以上 / B2未満
 * @param {RealClassInput} p
 * @returns {{score:number, label:string, breakdown:Record<string, number>}}
 */
export function calcRealClass(p) {
  const get = (v, d = null) => (v === undefined || v === null ? d : v);

  const natWin        = Number(get(p.natWin, 0)) || 0;
  const locWinRaw     = get(p.locWin, null);
  const locWin        = locWinRaw === null ? natWin * 0.92 : Number(locWinRaw) || 0; // 当地0.00は全国×0.92で代替
  const motor2        = Number(get(p.motor2, 0)) || 0;
  const age           = Number(get(p.age, 0)) || 0;
  const avgST         = Number(get(p.avgST, 0)) || 0;
  const courseWinRate = Number(get(p.courseWinRate, 0)) || 0;

  const breakdown = {};

  // 1) 全国勝率（主軸）
  //   7.0+ : +6 / 6.0+ : +5 / 5.0+ : +4 / 4.0+ : +3 / 3.0+ : +2 / else : +1
  let baseNat = 1;
  if (natWin >= 7.0) baseNat = 6;
  else if (natWin >= 6.0) baseNat = 5;
  else if (natWin >= 5.0) baseNat = 4;
  else if (natWin >= 4.0) baseNat = 3;
  else if (natWin >= 3.0) baseNat = 2;
  breakdown.natWin = baseNat;

  // 2) 当地勝率ボーナス（全国との比較）
  //   loc >= nat : +2 / loc >= 0.95*nat : +1
  let locBonus = 0;
  if (locWin >= natWin) locBonus += 2;
  else if (locWin >= natWin * 0.95) locBonus += 1;
  breakdown.locWin = locBonus;

  // 3) モーター2連率
  //   45+ : +2 / 35+ : +1 / <30 : -1
  let motor = 0;
  if (motor2 >= 45) motor += 2;
  else if (motor2 >= 35) motor += 1;
  else if (motor2 < 30) motor -= 1;
  breakdown.motor2 = motor;

  // 4) 年齢補正（若手+1 / 55+ -1）
  let ageAdj = 0;
  if (age > 0 && age <= 30) ageAdj += 1;
  if (age >= 55) ageAdj -= 1;
  breakdown.age = ageAdj;

  // 5) ST補正（<=0.16: +1 / >=0.21: -1）
  let stAdj = 0;
  if (avgST > 0) {
    if (avgST <= 0.16) stAdj += 1;
    else if (avgST >= 0.21) stAdj -= 1;
  }
  breakdown.avgST = stAdj;

  // 6) コース別1着率（40%以上 +1 / 20%未満 -1）
  let c1Adj = 0;
  if (courseWinRate >= 40) c1Adj += 1;
  else if (courseWinRate > 0 && courseWinRate < 20) c1Adj -= 1;
  breakdown.courseWinRate = c1Adj;

  const score = baseNat + locBonus + motor + ageAdj + stAdj + c1Adj;
  const label = labelFromScore(score);

  return { score, label, breakdown };
}

/**
 * スコア→ラベル
 * @param {number} score
 * @returns {"A1"|"A2"|"B1上位"|"B1中位"|"B1下位"|"B2"}
 */
export function labelFromScore(score) {
  if (score >= 9) return "A1";
  if (score >= 7) return "A2";
  if (score >= 5) return "B1上位";
  if (score >= 3) return "B1中位";
  if (score >= 1) return "B1下位";
  return "B2";
}

/**
 * integrated entry から RealClassInput を作る補助
 * @param {Object} entry - integrated JSON の entries[i]
 * @returns {RealClassInput}
 */
export function mapEntryToRealClassInput(entry) {
  // racecardの全国/当地 勝率（natTop1=勝率? 仕様に合わせてプロジェクト側で調整してOK）
  const natWin = safeNum(entry.racecard?.natTop1);
  const locWin = safeNum(entry.racecard?.locTop1);

  // モーター2連対率
  const motor2 = safeNum(entry.racecard?.motorTop2);

  // 年齢
  const age = safeNum(entry.racecard?.age);

  // 平均ST（racecardのavgST、なければ stats.entryCourse.avgST）
  const avgST = safeNum(entry.racecard?.avgST ?? entry.stats?.entryCourse?.avgST);

  // コース別1着率（stats.entryCourse.selfSummary から算出）
  let courseWinRate = 0;
  const ss = entry.stats?.entryCourse?.selfSummary;
  if (ss?.starts > 0) {
    courseWinRate = (Number(ss.firstCount || 0) / Number(ss.starts)) * 100;
  }

  return { natWin, locWin, motor2, age, avgST, courseWinRate };
}

function safeNum(v, def = null) {
  if (v === undefined || v === null) return def;
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}

// 例: function realClass(data){...}
export default realClass;