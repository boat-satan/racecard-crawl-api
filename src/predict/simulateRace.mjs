/**
 * 道中展開シミュレーション
 * - startOrder: [{ lane, pos }] 先頭pos=1
 * - perfMap: { [lane]: { turn:0..1, straight:0..1, stability:0..1 } }
 * - laps: 周回数（通常3）
 * - seed: 乱数シード（同じ入力で再現したい時用）
 *
 * export:
 *   buildPerfMap(entries, ctx) -> perfMap
 *   simulateRace(startOrder, perfMap, laps=3, seed=0) -> [{ lane, pos, score }]
 */

function clamp01(v) { return Math.max(0, Math.min(1, v)); }

// シード付き擬似乱数（xorshift32）
function makePRNG(seed = 1) {
  let x = (seed >>> 0) || 1;
  return () => {
    x ^= x << 13; x ^= x >>> 17; x ^= x << 5;
    // 0..1
    return ((x >>> 0) / 0xFFFFFFFF);
  };
}

/**
 * 各艇の道中性能を entries から推定して生成
 * - 直線: モーター2連率、展示タイム順位、チルト(+)
 * - ターン: 実質級別、コース3連対率、展示ST順位（回転の良さの代替）
 * - 安定: 風・波の影響を受けにくさ（級別と年齢、スタビ有無）
 */
export function buildPerfMap(entries, ctx = {}) {
  const {
    windSpeed = 0,
    waveHeight = 0,
    stabilizer = false,
  } = ctx;

  // 環境係数：荒れるほど「安定」の重みを効かせる
  const envTough = clamp01((windSpeed / 8) * 0.5 + (waveHeight / 0.15) * 0.5); // 0..1

  const map = {};

  for (const e of entries) {
    const lane = Number(e.lane);

    // ---- 生データ抽出（無ければ穏当なデフォルト）
    const exRank = Number(e.exhibition?.exRank ?? e.stats?.exTimeRank?.[0]?.rank ?? 3); // 1位が良い
    const motor2 = Number(e.racecard?.motorTop2 ?? e.racecard?.motor2 ?? e.motor2 ?? 30); // 2連率%
    const tilt = parseFloat(String(e.exhibition?.tilt ?? "0").replace("°","")) || 0;
    const natTop3 = Number(e.racecard?.natTop3 ?? 40);
    const courseTop3 = Number(e.stats?.entryCourse?.matrixSelf?.top3Rate ?? 40);
    const realClass = String(e.realClass ?? "B1中位");

    // 実質級別→係数
    const classFactor = (
      realClass.startsWith("A1") ? 1.00 :
      realClass.startsWith("A2") ? 0.90 :
      realClass.includes("B1上位") ? 0.82 :
      realClass.includes("B1中位") ? 0.75 :
      realClass.includes("B1下位") ? 0.70 : 0.65
    );

    // 直線力 0..1
    let straight =
      0.55 * norm(motor2, 20, 55) +     // モーター素性
      0.25 * norm(7 - exRank, 0, 6) +   // 展示タイム上位
      0.10 * (tilt > 0 ? 1 : tilt < 0 ? 0.3 : 0.6) +
      0.10 * classFactor;
    straight = clamp01(straight);

    // ターン力 0..1
    let turn =
      0.45 * norm(natTop3, 20, 70) +      // 選手の地力
      0.35 * norm(courseTop3, 20, 70) +   // そのコースの3連対率
      0.10 * norm(7 - exRank, 0, 6) +     // 回り足の代替
      0.10 * classFactor;
    turn = clamp01(turn);

    // 安定性 0..1（荒れ場面で効く）
    let stability =
      0.50 * classFactor +
      0.25 * (stabilizer ? 1 : 0.6) +
      0.25 * (tilt >= 0 ? 0.8 : 0.6);
    stability = clamp01(stability * (0.7 + 0.3 * (1 - envTough)));

    map[lane] = { turn, straight, stability };
  }

  return map;
}

/**
 * レースシミュレーション
 * - 各周回で turn/straight/stability からゲインを計算し、スコア高い順に入れ替える
 */
export function simulateRace(startOrder, perfMap, laps = 3, seed = 0) {
  const rand = makePRNG(seed);
  // 深いコピー
  let positions = startOrder.map(p => ({ lane: p.lane, pos: p.pos, score: 0 }));

  for (let lap = 1; lap <= laps; lap++) {
    // ターン区間
    positions = positions.map(p => {
      const perf = perfMap[p.lane] || { turn: 0.5, straight: 0.5, stability: 0.5 };
      const base = (perf.turn - 0.5) * 0.22;    // ±0.11
      const noise = (rand() - 0.5) * (0.10 * (1 - perf.stability)); // 荒れやすいほどブレる
      return { ...p, score: base + noise };
    }).sort((a, b) => b.score - a.score)
      .map((p, i) => ({ ...p, pos: i + 1 }));

    // 直線区間
    positions = positions.map(p => {
      const perf = perfMap[p.lane] || { turn: 0.5, straight: 0.5, stability: 0.5 };
      const base = (perf.straight - 0.5) * 0.18; // ±0.09
      const noise = (rand() - 0.5) * (0.08 * (1 - perf.stability));
      return { ...p, score: p.score + base + noise };
    }).sort((a, b) => b.score - a.score)
      .map((p, i) => ({ ...p, pos: i + 1 }));
  }

  return positions;
}

/** 線形正規化: v ∈ [min,max] -> 0..1 */
function norm(v, min, max) {
  const d = max - min;
  if (d <= 0) return 0.5;
  return clamp01((v - min) / d);
}

export default simulateRace;