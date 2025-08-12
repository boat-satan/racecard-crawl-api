// lib/calcPredictedST.mjs
// 予測STの新ロジック：
// - ベースは「全体平均ST」と「進入コース平均ST」の平均（片方なければある方、両方なければ0.18）
// - 展示STは“雰囲気”として重み付け（速いほど比重高、遅いほど軽め）
// - F展示は攻め意識として微速化ボーナス
// - モーター伸び寄り・攻めボーナス・F持ち慎重化などの小調整
// - 最終クリップ 0.06〜0.35

/**
 * 文字列の展示ST（例: "F.05", ".09"）を数値へ
 * @param {string|number|null|undefined} v
 * @returns {{value: number|null, isF: boolean}}
 */
export function parseTenjiST(v) {
  if (v == null) return { value: null, isF: false };
  if (typeof v === 'number') return { value: v, isF: false };
  const s = String(v).trim().toUpperCase();
  const isF = s.startsWith('F');
  const m = s.match(/(\d+)?\.(\d{1,2})/);
  if (!m) return { value: null, isF };
  const frac = m[2];
  const val = Number(`0.${frac.padEnd(2, '0')}`);
  return { value: val, isF };
}

/**
 * 0〜1の範囲で数値をクリップ
 */
function clamp(x, lo, hi) {
  return Math.max(lo, Math.min(hi, x));
}

/**
 * 与えられた数値の平均（null/undefined/NaN は除外）
 */
function mean(...vals) {
  const a = vals.filter(v => typeof v === 'number' && Number.isFinite(v));
  if (a.length === 0) return null;
  return a.reduce((p, c) => p + c, 0) / a.length;
}

/**
 * 予測STを計算
 * @param {Object} params
 * @param {number|null} params.avgST 全体平均ST（例: 0.18）
 * @param {number|null} params.courseAvgST 進入コース平均ST（例: 0.16）※ stats.entryCourse.avgST
 * @param {string|number|null} [params.tenjiST] 展示ST（".09" や "F.05" も可）
 * @param {Object} [params.opts]
 * @param {boolean} [params.opts.attackBonus=false] 攻めボーナス（カド/積極タイプ等）
 * @param {number}  [params.opts.fCount=0] F持ち数（多いほど慎重）
 * @param {boolean} [params.opts.motorNobi=false] モーター伸び寄り
 * @returns {{
 *   predictedST:number,
 *   breakdown:{
 *     base:number,
 *     tenji:number|null,
 *     tenjiWeight:number,
 *     combinedBeforeAdj:number,
 *     adjustments:{
 *       exF:number,
 *       attackBonus:number,
 *       motorNobi:number,
 *       fCount:number
 *     }
 *   }
 * }}
 */
export default function calcPredictedST({
  avgST = null,
  courseAvgST = null,
  tenjiST = null,
  opts = {}
} = {}) {
  const {
    attackBonus = false,
    fCount = 0,
    motorNobi = false
  } = opts;

  // 1) ベース：全体平均STとコース平均STの平均
  let base = mean(avgST, courseAvgST);
  if (base == null) base = 0.18; // フォールバック

  // 2) 展示STの重みづけ（速いほど比重高、遅いほど軽め）
  const { value: tenjiVal, isF: exFflag } = parseTenjiST(tenjiST);
  let tenjiWeight = 0;
  if (typeof tenjiVal === 'number') {
    if (tenjiVal <= 0.10) tenjiWeight = 0.35;        // 超速
    else if (tenjiVal <= 0.13) tenjiWeight = 0.25;   // 速い
    else if (tenjiVal <= 0.16) tenjiWeight = 0.12;   // 普通〜やや速
    else tenjiWeight = 0.05;                          // 遅めは“雰囲気”だけ
  }

  const combined = (tenjiVal == null)
    ? base
    : base * (1 - tenjiWeight) + tenjiVal * tenjiWeight;

  // 3) 微調整
  let adj = 0;

  // 展示Fは攻め意識：ほんの少し速く（過大評価は避ける）
  const exFAdj = exFflag ? -0.012 : 0;
  adj += exFAdj;

  // モーター伸び寄り：わずかに速く
  const nobiAdj = motorNobi ? -0.008 : 0;
  adj += nobiAdj;

  // 攻めボーナス（例：4カド、攻めタイプ等）
  const atkAdj = attackBonus ? -0.015 : 0;
  adj += atkAdj;

  // F持ちは慎重になりがち：1本で+0.015、最大+0.03まで
  const fAdj = fCount > 0 ? Math.min(0.015 * fCount, 0.03) : 0;
  adj += fAdj;

  let predicted = combined + adj;

  // 4) 最終クリップ
  predicted = clamp(predicted, 0.06, 0.35);

  return {
    predictedST: Number(predicted.toFixed(2)),
    breakdown: {
      base: Number(base.toFixed(3)),
      tenji: tenjiVal == null ? null : Number(tenjiVal.toFixed(3)),
      tenjiWeight: Number(tenjiWeight.toFixed(3)),
      combinedBeforeAdj: Number(combined.toFixed(3)),
      adjustments: {
        exF: Number(exFAdj.toFixed(3)),
        attackBonus: Number(atkAdj.toFixed(3)),
        motorNobi: Number(nobiAdj.toFixed(3)),
        fCount: Number(fAdj.toFixed(3))
      }
    }
  };
}

/* ===== 使い方の例 =====
import calcPredictedST, { parseTenjiST } from './lib/calcPredictedST.mjs';

const res = calcPredictedST({
  avgST: 0.18,
  courseAvgST: 0.16,           // stats.entryCourse.avgST
  tenjiST: 'F.05',             // 展示FでもOK
  opts: {
    attackBonus: true,         // 例: 4カド
    fCount: 1,                 // F1
    motorNobi: true            // 伸び寄り
  }
});
console.log(res);
// {
//   predictedST: 0.12,
//   breakdown: { base: 0.17, tenji: 0.05, tenjiWeight: 0.35, ... }
// }
*/
