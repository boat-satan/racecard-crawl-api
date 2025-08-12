/**
 * スリット順予測 & 攻め手判定
 * - entries: 統合JSONの entries/ ranking 配列（各艇の startCourse, lane, predictedST など）
 * - env: { windSpeed, windDirection } を与えると微補正
 *
 * export:
 *   predictSlitOrder(entries, opts) -> [{ lane, adjustedST, startCourse }]
 *   decideAttackType(slitOrder, context) -> "fullMakuri" | "makuri" | "makuriZashi" | "sashi" | "none"
 *   default slitAndAttack(race) -> race+{slitOrder, attackType}
 */

const MIN_ST = 0.06;
const MAX_ST = 0.35;

/** 風向がダッシュ有利っぽいか（だいたい追い/横追い） */
function isDashFriendlyWind(windDir = "") {
  return /西|北西|南西/.test(String(windDir));
}

/**
 * スリット順予測
 * @param {Array} entries - integrated entries[] / ranking[]
 * @param {Object} opts   - { windSpeed, windDirection }
 * @returns {Array<{lane:number, startCourse:number, adjustedST:number}>}
 */
export function predictSlitOrder(entries, opts = {}) {
  const { windSpeed = 0, windDirection = "" } = opts;

  return (entries || [])
    .map((e) => {
      const lane = Number(e.lane ?? e.racecard?.lane ?? e.startCourse ?? 0);
      const sc   = Number(e.startCourse ?? lane);
      let st     = Number(e.predictedST ?? e.avgST ?? e.racecard?.avgST ?? 0.18);

      // クランプ
      st = Math.min(MAX_ST, Math.max(MIN_ST, st));

      // ダッシュ補正
      const isDash = sc >= 4;
      let dashBonus = isDash ? -0.015 : 0;

      // 風補正
      if (isDash && isDashFriendlyWind(windDirection) && windSpeed >= 4) {
        dashBonus -= 0.005;
      }

      return {
        lane,
        startCourse: sc,
        adjustedST: clamp3(st + dashBonus, MIN_ST, MAX_ST),
      };
    })
    .sort((a, b) => a.adjustedST - b.adjustedST);
}

/**
 * 攻め手判定
 * @param {Array} slitOrder - predictSlitOrder の結果（昇順）
 * @returns {string}
 */
export function decideAttackType(slitOrder) {
  if (!Array.isArray(slitOrder) || slitOrder.length === 0) return "none";

  const lead   = slitOrder[0];
  const second = slitOrder[1] ?? null;

  const leadST = lead.adjustedST;
  const gap    = second ? second.adjustedST - leadST : 0.02;

  const BIG_EDGE = 0.012;
  const SMALL_EDGE = 0.007;

  if (lead.startCourse === 4 && gap >= SMALL_EDGE) {
    return gap >= BIG_EDGE ? "fullMakuri" : "makuri";
  }
  if (lead.startCourse === 3 && gap >= SMALL_EDGE) {
    const two = slitOrder.find((x) => x.startCourse === 2);
    if (two && two.adjustedST - leadST >= SMALL_EDGE) return "makuriZashi";
  }
  if (lead.startCourse === 2 && gap >= SMALL_EDGE) {
    const one = slitOrder.find((x) => x.startCourse === 1);
    if (one && one.adjustedST - leadST >= SMALL_EDGE) return "sashi";
  }
  return "none";
}

/** helpers */
function clamp3(v, min, max) {
  const x = Math.min(max, Math.max(min, v));
  return Math.round(x * 1000) / 1000;
}

/**
 * 追加：デフォルトエクスポート本体
 * レースオブジェクトを受け取り、slitOrder と attackType を付与して返す
 */
function slitAndAttack(race) {
  if (!race) return race;
  const weather = race.weather || {};
  const entries = Array.isArray(race.ranking) ? race.ranking
                 : Array.isArray(race.entries) ? race.entries
                 : [];

  const slitOrder = predictSlitOrder(entries, {
    windSpeed: weather.windSpeed ?? 0,
    windDirection: weather.windDirection ?? "",
  });
  const attackType = decideAttackType(slitOrder);

  return { ...race, slitOrder, attackType };
}

export default slitAndAttack;