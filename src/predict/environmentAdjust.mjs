/**
 * 環境補正モジュール
 * - 予測STの環境補正
 * - スコア（強さ指数）の環境補正
 * - 波乱スコアの環境補正
 *
 * ざっくり方針（全場共通の安全サイド初期版）：
 *   風：強風(>=6m)でダッシュ有利 / インは慎重 → ST遅れ＆スコア微減
 *   波：高波(>=0.06m)で全体に慎重、特に外伸び/握り弱体。安定板があれば緩和
 *   チルト：正チルトは直線寄り＝ダッシュ・伸び側を微強化、マイナスは安定＝イン側微強化
 *   クリップ：STは常に 0.06〜0.35 に収める
 */

export const ENV_DEFAULTS = {
  strongWind: 6,    // 6m以上で強風扱い
  highWave: 0.06,   // 6cm以上で高波扱い（m単位）
  clipST: { min: 0.06, max: 0.35 }
};

/**
 * 予測STを環境で微調整
 * @param {number} predST - 事前計算済みST（秒）
 * @param {Object} ctx
 * @param {number} ctx.windSpeed
 * @param {number} ctx.waveHeight       - m（例: 0.03）
 * @param {boolean} ctx.stabilizer
 * @param {boolean} ctx.isDash          - コース5-6や角受けのダッシュ想定なら true
 * @param {boolean} ctx.isIn            - イン(1)なら true
 * @param {number|string|null} ctx.tilt - 例 "0.5" / "-0.5" / 0
 * @param {Object} [cfg]                - ENV_DEFAULTS を上書き可能
 * @returns {number} 調整後ST（0.06〜0.35）
 */
export function envAdjustForST(predST, ctx = {}, cfg = ENV_DEFAULTS) {
  const wind = toNum(ctx.windSpeed, 0);
  const wave = toNum(ctx.waveHeight, 0);
  const tilt = toNum(ctx.tilt, 0);
  const stab = !!ctx.stabilizer;
  const isDash = !!ctx.isDash;
  const isIn = !!ctx.isIn;

  let st = toNum(predST, 0.18);

  // 強風：ダッシュ有利（助走で-0.005）、インは慎重（+0.005）
  if (wind >= cfg.strongWind) {
    if (isDash) st -= 0.005;
    if (isIn)   st += 0.005;
    st += 0.005; // 全体的にもやや慎重
  }

  // 高波：全体的に+0.015、安定板ありなら+0.005に緩和
  if (wave >= cfg.highWave) {
    st += stab ? 0.005 : 0.015;
  }

  // チルト：+0.5は直線寄り→ダッシュ-0.003、-0.5は安定→イン-0.003
  if (tilt >= 0.5 && isDash) st -= 0.003;
  if (tilt <= -0.5 && isIn)  st += 0.003; // マイナスは慎重でわずかに遅れ

  // クリップ
  st = clamp(st, cfg.clipST.min, cfg.clipST.max);
  return round3(st);
}

/**
 * スコア（強さ指数）を環境で倍率調整
 * @param {number} baseScore - 事前計算済みスコア
 * @param {Object} ctx       - envAdjustForST と同じ
 * @param {Object} [cfg]
 * @returns {number} 調整後スコア
 */
export function envAdjustForScore(baseScore, ctx = {}, cfg = ENV_DEFAULTS) {
  const wind = toNum(ctx.windSpeed, 0);
  const wave = toNum(ctx.waveHeight, 0);
  const tilt = toNum(ctx.tilt, 0);
  const stab = !!ctx.stabilizer;
  const isDash = !!ctx.isDash;
  const isIn = !!ctx.isIn;

  let mul = 1.0;

  // 強風：ダッシュ+5%、イン-5%、全体-2%（操作難）
  if (wind >= cfg.strongWind) {
    if (isDash) mul *= 1.05;
    if (isIn)   mul *= 0.95;
    mul *= 0.98;
  }

  // 高波：外握り弱体＆全体-3%、安定板で-1%に緩和
  if (wave >= cfg.highWave) {
    mul *= stab ? 0.99 : 0.97;
    if (!isIn && !stab) mul *= 0.99; // 外はさらにわずかに不利
  }

  // チルト：+0.5はダッシュ+2%、-0.5はイン+2%
  if (tilt >= 0.5 && isDash) mul *= 1.02;
  if (tilt <= -0.5 && isIn)  mul *= 1.02;

  return baseScore * mul;
}

/**
 * 波乱スコアの環境補正
 * @param {number} upset - 0〜100
 * @param {Object} ctx   - 風・波・ダッシュ等
 * @param {Object} [cfg]
 * @returns {number} 0〜100
 */
export function envAdjustForUpset(upset, ctx = {}, cfg = ENV_DEFAULTS) {
  const wind = toNum(ctx.windSpeed, 0);
  const wave = toNum(ctx.waveHeight, 0);
  const isDash = !!ctx.isDash;
  const isIn = !!ctx.isIn;

  let u = toNum(upset, 0);

  // 強風：波乱増。ダッシュなら +8、インは +4、その他 +6
  if (wind >= cfg.strongWind) {
    if (isDash) u += 8;
    else if (isIn) u += 4;
    else u += 6;
  }

  // 高波：+5（安定板がない前提で）。安定板ありなら +2
  if (wave >= cfg.highWave) {
    u += ctx.stabilizer ? 2 : 5;
  }

  return clamp(u, 0, 100);
}

// ========== helpers ==========
function toNum(v, def = 0) {
  if (v === null || v === undefined || v === "") return def;
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}
function clamp(x, lo, hi) { return Math.max(lo, Math.min(hi, x)); }
function round3(x) { return Math.round(x * 1000) / 1000; }