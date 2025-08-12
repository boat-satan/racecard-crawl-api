// src/predict/upsetIndex.mjs

/**
 * 波乱指数（0..100）を計算して返す（安全版）
 * - entries が配列でない場合でも落ちない
 * - 結果は race オブジェクトに merge して返す
 */

function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }
function nz(n, d = 0) { const v = Number(n); return Number.isFinite(v) ? v : d; }

// --- フィールド取り出しヘルパ ---
function getLose(entry, name) {
  return nz(entry?.stats?.entryCourse?.loseKimarite?.[name], 0);
}
function getExRank(entry) {
  // 展示タイム順位 or exTimeRank[0].rank を優先（小さいほど良）
  return nz(
    entry?.exhibition?.exTimeRank ??
    entry?.exRank ??
    entry?.stats?.exTimeRank?.[0]?.rank, 4
  );
}
function getPredST(entry) {
  // predictedST 優先、無ければ展示STの数値化、無ければ 0.18
  const stStr = entry?.exhibition?.st;
  if (typeof entry?.predictedST === "number") return entry.predictedST;
  if (typeof stStr === "string") {
    const s = stStr.replace(/F/gi, "").trim();
    const m = s.match(/(\d+)?\.(\d{1,2})/);
    if (m) return Number(`0.${m[2].padEnd(2, "0")}`);
  }
  return 0.18;
}

// --- メイン計算（個艇ごと） ---
export function calcUpsetIndex(entries, weather = {}) {
  const {
    windSpeed = 0, windDirection = "", waveHeight = 0, stabilizer = false
  } = weather;

  // 荒天係数（0..1）
  const env = clamp(
    (windSpeed / 8) * 0.6 + (waveHeight / 0.15) * 0.4,
    0, 1
  );

  // 1・2コースの壁力
  const lane1 = entries.find?.(e => Number(e.lane) === 1) || null;
  const lane2 = entries.find?.(e => Number(e.lane) === 2) || null;
  const inLoseMakuri = lane1 ? getLose(lane1, "まくり") : 0;
  const inLoseSashi  = lane1 ? getLose(lane1, "差し") : 0;
  const lane2Top2 = nz(lane2?.stats?.entryCourse?.matrixSelf?.top2Rate, 40);

  const list = [];

  for (const e of entries) {
    const lane = Number(e.lane);
    const reasons = [];
    let score = 0;

    // --- イン崩れ要因
    if (lane !== 1) {
      if (inLoseMakuri >= 3) { score += 8; reasons.push("1のまくられ負け傾向"); }
      if (inLoseSashi  >= 3) { score += 6; reasons.push("1の差され負け傾向"); }
      if (lane2Top2 >= 55)  { score -= 4; reasons.push("2が壁でイン保護"); }
      else if (lane2Top2 <= 40) { score += 4; reasons.push("2の壁弱い"); }
    }

    // --- 展示・モーター
    const exRank = getExRank(e);                         // 1〜6
    const motor2 = nz(e?.racecard?.motorTop2 ?? e?.motorTop2 ?? e?.motor2, 30);
    if (exRank <= 2)            { score += 8; reasons.push("展示上位"); }
    if (motor2 >= 40 && lane>=4){ score += 7; reasons.push("外の好モーター"); }
    if (motor2 < 30)            { score -= 3; reasons.push("モーター不安"); }

    // --- 予想ST・ダッシュ
    const pst = getPredST(e);
    const isDash = lane >= 4;
    if (pst <= 0.13) { score += isDash ? 10 : 6; reasons.push("予想ST速い"); }
    else if (pst >= 0.20) { score -= 4; reasons.push("予想ST遅め"); }

    // --- チルト
    const tilt = parseFloat(String(e?.exhibition?.tilt ?? "0").replace("°","")) || 0;
    if (tilt > 0) { score += 4; reasons.push("チルト+で伸び"); }
    if (tilt < 0) { score -= 3; reasons.push("チルト-で安定寄り"); }

    // --- 風向（例：向かい風をざっくり検知）
    const isHead = /向|西/.test(windDirection);
    if (env > 0.2) {
      if (isHead && isDash) { score += 6; reasons.push("向かい風でダッシュ利"); }
      if (!stabilizer && env > 0.5) { score += 5; reasons.push("荒天・非スタビ"); }
    }

    // 仕上げ
    score = clamp(Math.round(score + env * 10), 0, 100);
    list.push({ lane, score, reasons });
  }

  const mostUpset = list.length ? [...list].sort((a,b)=>b.score-a.score)[0] : null;
  return { upsetList: list, mostUpset };
}

// --- レース全体に適用（デフォルトエクスポート） ---
export default function upsetIndex(race) {
  // entries の安全取得（ranking 優先、無ければ entries、無ければ空）
  const entries =
    Array.isArray(race?.ranking) ? race.ranking :
    Array.isArray(race?.entries) ? race.entries : [];

  // データが無ければそのまま返す（重み=1）
  if (entries.length === 0) {
    return { ...race, upsetWeight: 1 };
  }

  const weather = race?.weather ?? {};
  const { upsetList, mostUpset } = calcUpsetIndex(entries, weather);

  // アップセット重み：0.9〜1.1 に収める軽めの係数
  const weight = 0.9 + (mostUpset ? (mostUpset.score / 100) * 0.2 : 0);

  return {
    ...race,
    upsetList,
    mostUpset,
    upsetWeight: Number.isFinite(weight) ? weight : 1
  };
}