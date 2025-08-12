// 予測ST（Start Timing）計算モジュール
// 取り決め：
// - ベース = (平均ST + 進入コース平均ST) / 2 （どちらか欠損ならある方、両方欠損は 0.18）
// - 展示STの取り込み（速い展示ほど重み大）
// - 展示Fは「攻め意識」として -0.005
// - 調整：attackBonus -0.02 / motorNobi -0.005 / fCount>0 +0.02
// - 最終は 0.06〜0.35 にクランプし小数2桁

/** 文字列STを数値へ（"F.05" → {value:0.05,isF:true}） */
export function parseST(st) {
  if (st == null) return { value: null, isF: false };
  if (typeof st === "number") return { value: Number.isFinite(st) ? st : null, isF: false };
  const s = String(st).trim();
  const isF = s.toUpperCase().startsWith("F");
  const m = s.match(/(\d+)?\.(\d{1,2})/);
  if (!m) return { value: null, isF };
  const v = Number(`0.${m[2].padEnd(2, "0")}`);
  return { value: Number.isFinite(v) ? v : null, isF };
}
function clamp(x, lo, hi) { return Math.max(lo, Math.min(hi, x)); }
function round2(x) { return Math.round(x * 100) / 100; }

/** 展示STの重み（速いほど重く） */
function weightForTenji(tenji) {
  if (tenji == null) return 0;
  if (tenji <= 0.10) return 0.60;
  if (tenji <= 0.12) return 0.50;
  if (tenji <= 0.15) return 0.35;
  return 0.20;
}

/** 単体選手の予測ST */
export function calcPredictedST({
  avgST,
  courseAvgST,
  tenjiST,
  fCount = 0,
  attackBonus = false,
  motorNobi = false,
} = {}) {
  // ベース
  const hasAvg = typeof avgST === "number" && isFinite(avgST) && avgST > 0;
  const hasCourse = typeof courseAvgST === "number" && isFinite(courseAvgST) && courseAvgST > 0;
  let base = 0.18;
  if (hasAvg && hasCourse) base = (avgST + courseAvgST) / 2;
  else if (hasAvg) base = avgST;
  else if (hasCourse) base = courseAvgST;

  // 展示取り込み
  const { value: tenjiVal, isF: isTenjiF } = parseST(tenjiST);
  const w = weightForTenji(tenjiVal);
  let st = base;
  if (tenjiVal != null) st = base * (1 - w) + tenjiVal * w;

  // 展示Fの“攻め意識”
  if (isTenjiF && tenjiVal != null) st -= 0.005;

  // 補正
  if (attackBonus) st -= 0.02;
  if (motorNobi)   st -= 0.005;
  if (fCount > 0)  st += 0.02;

  // 仕上げ
  st = clamp(st, 0.06, 0.35);
  return round2(st);
}

/**
 * デフォルト：レースデータ全体へ適用して返す
 * 既存構造の違いに耐えるよう、可能性のあるフィールド名を幅広く見る
 */
function predictedST(race) {
  if (!race || !Array.isArray(race.ranking)) return race;
  const cloned = JSON.parse(JSON.stringify(race));

  cloned.ranking = cloned.ranking.map(p => {
    // フィールド名のゆらぎ吸収
    const avgST =
      p.avgST ??
      p.averageST ??
      null;

    const courseAvgST =
      p.courseAvgST ??
      p.entryCourseAvgST ??
      p.course?.avgST ??
      p.courseStats?.avgST ??
      null;

    const tenjiST =
      p.tenjiST ??
      p.exST ??
      p.exhibitionST ??
      p.tenji?.st ??
      null;

    const fCount = p.fCount ?? p.f ?? 0;

    // 攻め・伸びのヒント（なければ軽い推定）
    const attackBonus =
      (p.attackBonus === true) ||
      (p.lane === 4) || // 4カド簡易
      false;

    const motorNobi = !!(p.motorNobi || p.nobi || false);

    const predicted = calcPredictedST({
      avgST,
      courseAvgST,
      tenjiST,
      fCount,
      attackBonus,
      motorNobi,
    });

    return { ...p, predictedST: predicted };
  });

  return cloned;
}

export default predictedST;