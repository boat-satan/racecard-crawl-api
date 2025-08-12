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
  strongWind: 6,
  highWave: 0.06,
  clipST: { min: 0.06, max: 0.35 }
};

/** ---------- 個別補正関数 ---------- */
export function envAdjustForST(predST, ctx = {}, cfg = ENV_DEFAULTS) {
  const wind = toNum(ctx.windSpeed, 0);
  const wave = toNum(ctx.waveHeight, 0);
  const tilt = toNum(ctx.tilt, 0);
  const stab = !!ctx.stabilizer;
  const isDash = !!ctx.isDash;
  const isIn = !!ctx.isIn;

  let st = toNum(predST, 0.18);

  if (wind >= cfg.strongWind) {
    if (isDash) st -= 0.005;
    if (isIn)   st += 0.005;
    st += 0.005;
  }
  if (wave >= cfg.highWave) {
    st += stab ? 0.005 : 0.015;
  }
  if (tilt >= 0.5 && isDash) st -= 0.003;
  if (tilt <= -0.5 && isIn)  st += 0.003;

  st = clamp(st, cfg.clipST.min, cfg.clipST.max);
  return round3(st);
}

export function envAdjustForScore(baseScore, ctx = {}, cfg = ENV_DEFAULTS) {
  const wind = toNum(ctx.windSpeed, 0);
  const wave = toNum(ctx.waveHeight, 0);
  const tilt = toNum(ctx.tilt, 0);
  const stab = !!ctx.stabilizer;
  const isDash = !!ctx.isDash;
  const isIn = !!ctx.isIn;

  let mul = 1.0;

  if (wind >= cfg.strongWind) {
    if (isDash) mul *= 1.05;
    if (isIn)   mul *= 0.95;
    mul *= 0.98;
  }
  if (wave >= cfg.highWave) {
    mul *= stab ? 0.99 : 0.97;
    if (!isIn && !stab) mul *= 0.99;
  }
  if (tilt >= 0.5 && isDash) mul *= 1.02;
  if (tilt <= -0.5 && isIn)  mul *= 1.02;

  return baseScore * mul;
}

export function envAdjustForUpset(upset, ctx = {}, cfg = ENV_DEFAULTS) {
  const wind = toNum(ctx.windSpeed, 0);
  const wave = toNum(ctx.waveHeight, 0);
  const isDash = !!ctx.isDash;
  const isIn = !!ctx.isIn;

  let u = toNum(upset, 0);

  if (wind >= cfg.strongWind) {
    if (isDash) u += 8;
    else if (isIn) u += 4;
    else u += 6;
  }
  if (wave >= cfg.highWave) {
    u += ctx.stabilizer ? 2 : 5;
  }

  return clamp(u, 0, 100);
}

/** ---------- helpers ---------- */
function toNum(v, def = 0) {
  if (v === null || v === undefined || v === "") return def;
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}
function clamp(x, lo, hi) { return Math.max(lo, Math.min(hi, x)); }
function round3(x) { return Math.round(x * 1000) / 1000; }

/** ---------- デフォルトエクスポート本体（重複定義なし） ---------- */
/**
 * レース全体にざっくり環境補正を適用して返す
 * （最小変更：既存フィールドがあれば軽く補正、無ければそのまま）
 */
export default function envAdjust(race) {
  if (!race) return race;

  const weather = race.weather || {};
  const common = {
    windSpeed: weather.windSpeed,
    waveHeight: weather.waveHeight,
    stabilizer: !!weather.stabilizer
  };

  const cloned = JSON.parse(JSON.stringify(race));
  if (Array.isArray(cloned.ranking)) {
    cloned.ranking = cloned.ranking.map(p => {
      const isIn = p.lane === 1;
      const isDash = p.lane >= 4; // 進入不明時の簡易推定
      const ctx = { ...common, isDash, isIn, tilt: p.tilt };

      const baseST    = p.predictedST ?? p.avgST;
      const baseScore = p.score;
      const baseUpset = p.upset;

      const adj = {
        ...(baseST    != null ? { predictedST: envAdjustForST(baseST, ctx) } : {}),
        ...(baseScore != null ? { score:      envAdjustForScore(baseScore, ctx) } : {}),
        ...(baseUpset != null ? { upset:      envAdjustForUpset(baseUpset, ctx) } : {}),
      };

      return { ...p, ...adj };
    });
  }

  return cloned;
}