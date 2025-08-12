/**
 * 道中展開シミュレーション（最小変更・全出目分布を返す版）
 * 既存互換:
 *   default export simulateRace(adjusted, oneMark) -> { "1-2-3": 0.123, ... }
 *
 * 仕様ポイント
 *  - oneMark.startOrder の "(a|b)" / "a/b" は毎試行ランダム具体化（スリット順を重み付け）
 *  - linkedPairs は 1-2 並び成立時にカウント重みブースト（確率の偏りだけを与える）
 *  - 3着候補の制限は一旦無し（thirdPool は現状未使用）
 */

function clamp01(v) { return Math.max(0, Math.min(1, v)); }

// xorshift32
function makePRNG(seed = 1) {
  let x = (seed >>> 0) || 1;
  return () => {
    x ^= x << 13; x ^= x >>> 17; x ^= x << 5;
    return ((x >>> 0) / 0xFFFFFFFF);
  };
}

/* ===== 性能マップ ===== */
export function buildPerfMap(entries, ctx = {}) {
  const { windSpeed = 0, waveHeight = 0, stabilizer = false } = ctx;
  const envTough = clamp01((windSpeed / 8) * 0.5 + (waveHeight / 0.15) * 0.5); // 0..1
  const map = {};

  for (const e of entries || []) {
    const lane = Number(e.lane ?? e.racecard?.lane);
    if (!Number.isFinite(lane)) continue;

    const exRank     = Number(e.exhibition?.exRank ?? e.stats?.exTimeRank?.[0]?.rank ?? 3);
    const motor2     = Number(e.racecard?.motorTop2 ?? e.racecard?.motor2 ?? e.motor2 ?? 30);
    const tilt       = parseFloat(String(e.exhibition?.tilt ?? "0").replace("°","")) || 0;
    const natTop3    = Number(e.racecard?.natTop3 ?? 40);
    const courseTop3 = Number(e.stats?.entryCourse?.matrixSelf?.top3Rate ?? 40);
    const realClass  = String(e.realClass ?? "B1中位");

    const classFactor =
      realClass.startsWith("A1") ? 1.00 :
      realClass.startsWith("A2") ? 0.90 :
      realClass.includes("B1上位") ? 0.82 :
      realClass.includes("B1中位") ? 0.75 :
      realClass.includes("B1下位") ? 0.70 : 0.65;

    let straight =
      0.55 * norm(motor2, 20, 55) +
      0.25 * norm(7 - exRank, 0, 6) +
      0.10 * (tilt > 0 ? 1 : tilt < 0 ? 0.3 : 0.6) +
      0.10 * classFactor;
    straight = clamp01(straight);

    let turn =
      0.45 * norm(natTop3, 20, 70) +
      0.35 * norm(courseTop3, 20, 70) +
      0.10 * norm(7 - exRank, 0, 6) +
      0.10 * classFactor;
    turn = clamp01(turn);

    let stability =
      0.50 * classFactor +
      0.25 * (stabilizer ? 1 : 0.6) +
      0.25 * (tilt >= 0 ? 0.8 : 0.6);
    stability = clamp01(stability * (0.7 + 0.3 * (1 - envTough)));

    map[lane] = { turn, straight, stability };
  }
  return map;
}

/* ===== 1本シミュ ===== */
export function simCore(startOrder, perfMap, laps = 3, seed = 0) {
  const rand = makePRNG(seed);
  let positions = (startOrder || []).map(p => ({ lane: p.lane, pos: p.pos, score: 0 }));

  for (let lap = 1; lap <= laps; lap++) {
    // ターン区間
    positions = positions.map(p => {
      const perf = perfMap[p.lane] || { turn: 0.5, straight: 0.5, stability: 0.5 };
      const base = (perf.turn - 0.5) * 0.22;
      const noise = (rand() - 0.5) * (0.10 * (1 - perf.stability));
      return { ...p, score: base + noise };
    }).sort((a, b) => b.score - a.score)
      .map((p, i) => ({ ...p, pos: i + 1 }));

    // 直線区間
    positions = positions.map(p => {
      const perf = perfMap[p.lane] || { turn: 0.5, straight: 0.5, stability: 0.5 };
      const base = (perf.straight - 0.5) * 0.18;
      const noise = (rand() - 0.5) * (0.08 * (1 - perf.stability));
      return { ...p, score: p.score + base + noise };
    }).sort((a, b) => b.score - a.score)
      .map((p, i) => ({ ...p, pos: i + 1 }));
  }
  return positions;
}

/* ===== oneMark 正規化 ===== */
function splitToken(tok) {
  if (Array.isArray(tok)) return tok.map(n => Number(n));
  const s = String(tok ?? "").trim();
  if (!s) return [];
  if (s.includes("/")) return s.split("/").map(n => Number(n));
  const m = s.match(/^\(([^)]+)\)$/);
  if (m) return m[1].split("|").map(n => Number(n));
  return [Number(s)];
}

function normalizeOneMark(oneMark) {
  const om = oneMark || {};
  const rawOrder = om.order ?? om.startOrder ?? [];
  const startOrderTokens = Array.isArray(rawOrder) ? rawOrder.map(splitToken) : [];

  const linkedPairs = Array.isArray(om.linkedPairs)
    ? om.linkedPairs.map(p => (Array.isArray(p) ? p.map(n => Number(n)) : splitToken(p)))
    : [];

  const thirdPool = Array.isArray(om.thirdPool) ? om.thirdPool.map(n => Number(n)) : [];

  return { startOrderTokens, linkedPairs, thirdPool, thirdBias: om.thirdBias || null };
}

function getSlitOrderFallback(adjusted) {
  const so = Array.isArray(adjusted?.slitOrder) ? adjusted.slitOrder : [];
  return so.slice().sort((a, b) => (a.adjustedST ?? 9) - (b.adjustedST ?? 9)).map(x => Number(x.lane));
}

function weightedPick(cands, slitPref, used, rand) {
  const pool = cands.filter(n => !used.has(n));
  if (pool.length === 0) return null;
  if (pool.length === 1) return pool[0];
  const weights = pool.map(n => {
    const idx = slitPref.indexOf(n);
    return (idx >= 0 ? (slitPref.length - idx) : 1) + 1e-6;
  });
  const sum = weights.reduce((s, w) => s + w, 0);
  let r = rand() * sum;
  for (let i = 0; i < pool.length; i++) {
    r -= weights[i];
    if (r <= 0) return pool[i];
  }
  return pool[pool.length - 1];
}

function materializeStartOrderRandom(tokens, adjusted, rand) {
  const used = new Set();
  const slitPref = getSlitOrderFallback(adjusted);
  const lanes = [];
  for (const cands of tokens) {
    const pick = weightedPick(cands, slitPref, used, rand);
    if (pick == null) continue;
    used.add(pick);
    lanes.push(pick);
  }
  for (const l of slitPref) {
    if (lanes.length >= 6) break;
    if (!used.has(l)) {
      used.add(l);
      lanes.push(l);
    }
  }
  return lanes.slice(0, 6).map((lane, i) => ({ lane, pos: i + 1 }));
}

/* ===== 既存互換 default：分布を返す ===== */
export default function simulateRace(adjusted, oneMark) {
  const { startOrderTokens, linkedPairs } = normalizeOneMark(oneMark || {});

  const entries = adjusted?.entries || adjusted?.ranking || [];
  const weather = adjusted?.weather || {};
  const perfMap = buildPerfMap(entries, {
    windSpeed: weather.windSpeed,
    waveHeight: weather.waveHeight,
    stabilizer: !!weather.stabilizer,
  });

  const TRIALS = 2000; // 分布を厚く取る
  const counts = new Map();

  for (let i = 0; i < TRIALS; i++) {
    const rand = makePRNG(10007 + i);
    const startOrder = materializeStartOrderRandom(
      startOrderTokens.length ? startOrderTokens : [[1],[2],[3],[4],[5],[6]],
      adjusted,
      rand
    );

    const res = simCore(startOrder, perfMap, 3, 20011 + i);
    const ordered = res.slice().sort((a, b) => a.pos - b.pos);
    const a = ordered[0]?.lane, b = ordered[1]?.lane, c = ordered[2]?.lane;
    if (!a || !b || !c) continue;

    let w = 1;
    if (Array.isArray(linkedPairs) && linkedPairs.length) {
      for (const [x, y] of linkedPairs) {
        if (a === Number(x) && b === Number(y)) { w *= 1.35; break; }
      }
    }
    const key = `${a}-${b}-${c}`;
    counts.set(key, (counts.get(key) || 0) + w);
  }

  const total = [...counts.values()].reduce((s, v) => s + v, 0);
  const probs = {};
  if (total > 0) for (const [k, v] of counts.entries()) probs[k] = v / total;
  return probs;
}

function norm(v, min, max) {
  const d = max - min;
  if (d <= 0) return 0.5;
  return clamp01((v - min) / d);
}