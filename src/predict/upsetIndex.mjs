/**
 * 波乱指数（0..100）を計算して返す
 * 使う主な要素：
 *  - 1コース信頼度（loseKimarite：まくられ・差され負けの多さ）
 *  - 2コースの「逃がし／差し」傾向（loseKimarite と course成績）
 *  - 展示タイム順位・展示ST順位（攻め足/気配）
 *  - モーター素性（2連率）と外枠のモーター穴
 *  - 風・波（荒れるほど加点）、スタビ有無
 *  - 予想ST（速ければ仕掛け成功余地↑）
 *  - チルト（+は伸び寄り加点、-は安定性寄り減点）
 */

function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }
function nz(n, d = 0) { const v = Number(n); return Number.isFinite(v) ? v : d; }
function getLose(entry, name) {
  return nz(entry?.stats?.entryCourse?.loseKimarite?.[name], 0);
}
function getExRank(entry) {
  // 展示タイム順位 or exTimeRank[0].rank を優先利用（小さいほど良い）
  return nz(entry?.exhibition?.exTimeRank ?? entry?.exRank ??
           entry?.stats?.exTimeRank?.[0]?.rank, 4);
}
function getPredST(entry) {
  return nz(entry?.predictedST ?? entry?.exhibition?.st?.replace('F','') , 0.18);
}

export function calcUpsetIndex(entries, weather = {}) {
  const {
    windSpeed = 0,          // m/s
    windDirection = "",     // 文字列（例: "西", "南西"）
    waveHeight = 0,         // m
    stabilizer = false
  } = weather;

  // 荒天係数（0..1）
  const env = clamp(
    (windSpeed / 8) * 0.6 + (waveHeight / 0.15) * 0.4,
    0, 1
  );

  // 1コースと2コースの「壁力」参考
  const lane1 = entries.find(e => Number(e.lane) === 1);
  const lane2 = entries.find(e => Number(e.lane) === 2);
  const inLoseMakuri = getLose(lane1, "まくり");
  const inLoseSashi  = getLose(lane1, "差し");
  const lane2Top2 = nz(lane2?.stats?.entryCourse?.matrixSelf?.top2Rate, 40);

  const list = [];

  for (const e of entries) {
    const lane = Number(e.lane);
    const reasons = [];
    let score = 0;

    // --- イン崩れ要因（他コースの波乱材料）
    if (lane !== 1) {
      if (inLoseMakuri >= 3) { score += 8; reasons.push("1のまくられ負け傾向"); }
      if (inLoseSashi  >= 3) { score += 6; reasons.push("1の差され負け傾向"); }
      // 2コースが「逃がし」タイプだと波乱はやや減
      if (lane2Top2 >= 55) { score -= 4; reasons.push("2が壁でイン保護"); }
      else if (lane2Top2 <= 40) { score += 4; reasons.push("2の壁弱い"); }
    }

    // --- 展示・モーター気配
    const exRank = getExRank(e);              // 1〜6
    const motor2 = nz(e?.racecard?.motorTop2 ?? e?.motorTop2 ?? e?.motor2, 30);
    if (exRank <= 2) { score += 8; reasons.push("展示上位"); }
    if (motor2 >= 40 && lane >= 4) { score += 7; reasons.push("外の好モーター"); }
    if (motor2 < 30) { score -= 3; reasons.push("モーター不安"); }

    // --- 予想ST・ダッシュ補正（速ければ荒れ方向）
    const pst = getPredST(e);
    const isDash = lane >= 4; // 簡易：4〜6をダッシュとみなす
    if (pst <= 0.13) { score += isDash ? 10 : 6; reasons.push("予想ST速い"); }
    else if (pst >= 0.20) { score -= 4; reasons.push("予想ST遅め"); }

    // --- チルト傾向
    const tilt = parseFloat(String(e?.exhibition?.tilt ?? "0").replace("°","")) || 0;
    if (tilt > 0) { score += 4; reasons.push("チルト+で伸び"); }
    if (tilt < 0) { score -= 3; reasons.push("チルト-で安定寄り"); }

    // --- 風向き（向かい風×ダッシュ優位をやや加点）
    const isHead = /西|向/.test(windDirection); // 例
    if (env > 0.2) {
      if (isHead && isDash) { score += 6; reasons.push("向かい風でダッシュ利"); }
      if (!stabilizer && env > 0.5) { score += 5; reasons.push("荒天・非スタビ"); }
    }

    // 正規化＆上限下限
    score = clamp(Math.round((score + env * 10)), 0, 100);

    list.push({ lane, score, reasons });
  }

  // 最大の艇をメモ
  const max = [...list].sort((a,b)=>b.score-a.score)[0] ?? null;
  return { upsetList: list, mostUpset: max };
}