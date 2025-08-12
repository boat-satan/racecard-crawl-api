// 予測ST（Start Timing）計算モジュール
// 仕様（今回の取り決め）
// - ベース値： (平均ST + 進入コース平均ST) / 2
//   * どちらか欠損ならある方のみ、両方欠損なら 0.18 を仮置き
// - 展示STの取り込み（重み付け）
//   * tenjiST が速いほど重みを高く：
//       <=0.10: 0.60, <=0.12: 0.50, <=0.15: 0.35, それ以外: 0.20
//   * 展示STが "F.xx" なら数値xxはそのまま使い、最後に“攻め意識”として -0.005 を適用（少しだけ速く）
// - 調整項目：
//   * attackBonus（カド/積極）： -0.02
//   * motorNobi（伸び寄り）：    -0.005
//   * fCount>0（本番F持ち）：     +0.02（慎重）
// - 最終値は 0.06〜0.35 にクランプし、小数2桁に丸める

/**
 * 文字列STを数値へ変換する。例: ".06" -> 0.06, "F.05" -> {value:0.05,isF:true}
 * @param {string|number|null|undefined} st
 * @returns {{value:number|null,isF:boolean}}
 */
export function parseST(st) {
  if (st == null) return { value: null, isF: false };
  if (typeof st === "number") {
    return { value: isFinite(st) ? st : null, isF: false };
  }
  const s = String(st).trim();
  const isF = s.toUpperCase().startsWith("F");
  // ".06" / "0.06" / "F.05" などから小数部を拾う
  const m = s.match(/(\d+)?\.(\d{1,2})/);
  if (!m) return { value: null, isF };
  const v = Number(`0.${m[2].padEnd(2, "0")}`);
  return { value: isFinite(v) ? v : null, isF };
}

/**
 * 指定値を [lo, hi] に丸める
 * @param {number} x
 * @param {number} lo
 * @param {number} hi
 */
function clamp(x, lo, hi) {
  return Math.max(lo, Math.min(hi, x));
}

/**
 * 小数2桁に丸め
 * @param {number} x
 */
function round2(x) {
  return Math.round(x * 100) / 100;
}

/**
 * 展示STの重みを決める
 * @param {number|null} tenji
 * @returns {number} weight 0..1
 */
function weightForTenji(tenji) {
  if (tenji == null) return 0;
  if (tenji <= 0.10) return 0.60;
  if (tenji <= 0.12) return 0.50;
  if (tenji <= 0.15) return 0.35;
  return 0.20;
}

/**
 * 予測STのメイン関数
 * @param {Object} params
 * @param {number|null|undefined} params.avgST            - 平均ST（出走表）
 * @param {number|null|undefined} params.courseAvgST      - 進入コース平均ST（stats.entryCourse.avgST）
 * @param {string|number|null|undefined} params.tenjiST   - 展示ST文字列（例: ".06", "F.05"）
 * @param {number} [params.fCount=0]                       - 本番F持ち数
 * @param {boolean} [params.attackBonus=false]             - カド/積極タイプ
 * @param {boolean} [params.motorNobi=false]               - モーター伸び寄りフラグ
 * @returns {number} predicted ST（0.06..0.35）
 */
export function calcPredictedST({
  avgST,
  courseAvgST,
  tenjiST,
  fCount = 0,
  attackBonus = false,
  motorNobi = false,
} = {}) {
  // ベース値の決定
  const hasAvg = typeof avgST === "number" && isFinite(avgST) && avgST > 0;
  const hasCourse = typeof courseAvgST === "number" && isFinite(courseAvgST) && courseAvgST > 0;
  let base;
  if (hasAvg && hasCourse) {
    base = (avgST + courseAvgST) / 2;
  } else if (hasAvg) {
    base = avgST;
  } else if (hasCourse) {
    base = courseAvgST;
  } else {
    base = 0.18; // 情報なしの仮置き
  }

  // 展示ST取り込み
  const { value: tenjiVal, isF: isTenjiF } = parseST(tenjiST);
  const w = weightForTenji(tenjiVal);
  let predicted = base;
  if (tenjiVal != null) {
    predicted = base * (1 - w) + tenjiVal * w;
  }

  // 攻め意識（展示F）の微調整
  if (isTenjiF && tenjiVal != null) {
    predicted -= 0.005;
  }

  // 調整項目
  if (attackBonus) predicted -= 0.02;
  if (motorNobi)   predicted -= 0.005;
  if (fCount > 0)  predicted += 0.02;

  // 最終丸め・クランプ
  predicted = clamp(predicted, 0.06, 0.35);
  return round2(predicted);
}
