// 実質級別算出（A1/A2/B1上位/B1中位/B1下位/B2）
// 既存フィールドが無い場合は安全側でスコア低めに振る

/** 単体選手の実質級別を判定 */
export function calcRealClass(p = {}) {
  let score = 0;

  // 基礎点：全国勝率
  const nat = Number(p.natWin ?? 0);
  if (nat >= 7.0) score += 6;
  else if (nat >= 6.0) score += 5;
  else if (nat >= 5.0) score += 4;
  else if (nat >= 4.0) score += 3;
  else if (nat >= 3.0) score += 2;
  else score += 1;

  // 当地勝率ボーナス
  const loc = Number(p.locWin ?? 0);
  if (loc >= nat * 0.95) score += 1;
  if (loc >= nat) score += 1;

  // モーター2連率
  const m2 = Number(p.motor2 ?? 0);
  if (m2 >= 45) score += 2;
  else if (m2 >= 35) score += 1;
  else if (m2 > 0 && m2 < 30) score -= 1;

  // 年齢
  const age = Number(p.age ?? 0);
  if (age > 0 && age <= 30) score += 1;
  if (age >= 55) score -= 1;

  // 平均ST
  const st = Number(p.avgST ?? 0);
  if (st && st <= 0.16) score += 1;
  else if (st && st >= 0.21) score -= 1;

  // 1コース1着率（持っていれば）
  const c1 = Number(p.course1Win ?? p.course?.one?.winRate ?? 0);
  if (c1 >= 40) score += 1;
  if (c1 > 0 && c1 < 20) score -= 1;

  // 判定
  if (score >= 9) return "A1";
  if (score >= 7) return "A2";
  if (score >= 5) return "B1上位";
  if (score >= 3) return "B1中位";
  if (score >= 1) return "B1下位";
  return "B2";
}

/**
 * デフォルト：レースデータ全体の ranking に realClass を埋め込む
 * （構造が違っても壊さないようにクローンしてから書き込み）
 */
function realClass(race) {
  if (!race || !Array.isArray(race.ranking)) return race;
  const cloned = JSON.parse(JSON.stringify(race));
  cloned.ranking = cloned.ranking.map(p => ({
    ...p,
    realClass: calcRealClass(p),
  }));
  return cloned;
}

export default realClass;