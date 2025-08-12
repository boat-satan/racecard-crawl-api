/**
 * スリット順予測 & 攻め手判定
 * - entries: 統合JSONの entries 配列（各艇の startCourse, lane, predictedST などを含む想定）
 * - env: { windSpeed, windDirection } を与えると微補正
 *
 * export:
 *   predictSlitOrder(entries, opts) -> [{ lane, adjustedST, startCourse }]
 *   decideAttackType(slitOrder, context) -> "fullMakuri" | "makuri" | "makuriZashi" | "sashi" | "none"
 */

const MIN_ST = 0.06;
const MAX_ST = 0.35;

/** 風向がダッシュ有利っぽいか（だいたい追い/横追い） */
function isDashFriendlyWind(windDir = "") {
  // ざっくり：西/北西/南西を「ダッシュ有利寄り」に
  return /西|北西|南西/.test(String(windDir));
}

/**
 * スリット順予測
 * @param {Array} entries - integrated entries[]
 * @param {Object} opts   - { windSpeed, windDirection }
 * @returns {Array<{lane:number, startCourse:number, adjustedST:number}>}
 */
export function predictSlitOrder(entries, opts = {}) {
  const { windSpeed = 0, windDirection = "" } = opts;

  return entries
    .map((e) => {
      const lane = Number(e.lane ?? e.racecard?.lane);
      const sc = Number(e.startCourse ?? lane);
      // predictedST は scripts/predict.mjs 側で各艇へ付与済み前提
      let st = Number(e.predictedST ?? e.racecard?.avgST ?? 0.18);

      // 物理範囲でクランプ
      st = Math.min(MAX_ST, Math.max(MIN_ST, st));

      // ダッシュ補正（助走距離分 0.010〜0.020 速い扱い）
      const isDash = sc >= 4;
      let dashBonus = isDash ? -0.015 : 0;

      // 風補正：ダッシュ有利風 × 風速が中以上なら さらに微加速
      if (isDash && isDashFriendlyWind(windDirection) && windSpeed >= 4) {
        dashBonus -= 0.005; // 合計で -0.02 程度まで
      }

      return {
        lane,
        startCourse: sc,
        adjustedST: clamp2(st + dashBonus, MIN_ST, MAX_ST),
      };
    })
    .sort((a, b) => a.adjustedST - b.adjustedST);
}

/**
 * 攻め手判定（最先着の艇と前後差でざっくり分類）
 * @param {Array} slitOrder - predictSlitOrder の結果（昇順）
 * @param {Object} ctx - { innerWallWeak:boolean } など将来用
 * @returns {string}
 */
export function decideAttackType(slitOrder, ctx = {}) {
  if (!Array.isArray(slitOrder) || slitOrder.length === 0) return "none";

  const lead = slitOrder[0]; // 最速
  const second = slitOrder[1] ?? null;

  const leadLane = lead.lane;
  const leadST = lead.adjustedST;
  const gap = second ? second.adjustedST - leadST : 0.02;

  // 閾値：0.01以上ぶっちぎると「攻め手成立」寄り
  const BIG_EDGE = 0.012;
  const SMALL_EDGE = 0.007;

  // 4カド快速はまず「まくり」起点で判定
  if (lead.startCourse === 4 && gap >= SMALL_EDGE) {
    if (gap >= BIG_EDGE) return "fullMakuri";
    return "makuri";
  }

  // 3コース先行で、2コースが遅れなら「まくり差し」寄り
  if (lead.startCourse === 3 && gap >= SMALL_EDGE) {
    const two = slitOrder.find((x) => x.startCourse === 2);
    if (two && two.adjustedST - leadST >= SMALL_EDGE) return "makuriZashi";
  }

  // 2コース先行で1がやや遅れ → 差し
  if (lead.startCourse === 2 && gap >= SMALL_EDGE) {
    const one = slitOrder.find((x) => x.startCourse === 1);
    if (one && one.adjustedST - leadST >= SMALL_EDGE) return "sashi";
  }

  return "none";
}

/** helpers */
function clamp2(v, min, max) {
  const x = Math.min(max, Math.max(min, v));
  return Math.round(x * 1000) / 1000; // 0.001精度で丸め
}