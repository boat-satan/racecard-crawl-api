// scripts/lib/extract-stats.js
// Node v20 / ESM
// 目的: fetch-stats の出力(JSON)から、(n)コース進入時の要約と展示順位別データだけを安全に抜き出す

/**
 * 数字っぽいものを 1..6 の範囲に正規化（それ以外は null）
 */
export function normalizeCourseNumber(n) {
  const num = Number(String(n).replace(/[^\d]/g, ""));
  return Number.isFinite(num) && num >= 1 && num <= 6 ? num : null;
}

/**
 * entryCourse から course===n の要素を返す（見つからなければ null）
 */
export function findEntryCourse(stats, n) {
  const course = normalizeCourseNumber(n);
  if (!stats || !Array.isArray(stats.entryCourse) || !course) return null;
  return stats.entryCourse.find((c) => Number(c?.course) === course) || null;
}

/**
 * (n)コース進入時の要約を作る
 * 返すもの:
 * - course: コース番号
 * - avgST: (n)コース時の平均ST
 * - selfSummary: 出走数/1-3着数のサマリ（自艇行）
 * - winKimariteSelf: 勝ち決まり手（自艇行の横列）
 * - loseKimarite: 負け決まり手（他艇合算）
 * - matrixSelf: マトリクスの自艇行（winRate/top2Rate/top3Rate 等込み）
 */
export function selectCourseStats(stats, n) {
  const row = findEntryCourse(stats, n);
  if (!row) {
    return {
      course: normalizeCourseNumber(n),
      avgST: null,
      selfSummary: null,
      winKimariteSelf: null,
      loseKimarite: null,
      matrixSelf: null,
    };
  }
  return {
    course: Number(row.course),
    avgST: row.avgST ?? null,
    selfSummary: row.selfSummary ?? row.matrix?.self ?? null,
    winKimariteSelf: row.winKimariteSelf ?? null,
    loseKimarite: row.loseKimarite ?? null,
    matrixSelf: row.matrix?.self ?? null,
  };
}

/**
 * 展示順位別スタッツを配列/マップの両方で返す
 * - list: exTimeRank の生配列（rank, winRate, top2Rate, top3Rate）
 * - byRank: { 1: {...}, 2: {...}, ... } の形（欠損は含めない）
 */
export function selectExRankStats(stats) {
  const list = Array.isArray(stats?.exTimeRank) ? stats.exTimeRank.map((r) => ({
    rank: Number(r.rank),
    winRate: r.winRate ?? null,
    top2Rate: r.top2Rate ?? null,
    top3Rate: r.top3Rate ?? null,
    raw: r.raw ?? null,
  })) : [];

  const byRank = {};
  for (const item of list) {
    if (Number.isFinite(item.rank)) byRank[item.rank] = item;
  }
  return { list, byRank };
}

/**
 * 1レーサー分の「(n)コース要約 + 展示順位別」をまとめて取得
 * - stats: fetch-stats で保存した 1 選手の JSON
 * - n: 展示データの startCourse（1..6）
 */
export function buildRacerStatsForCourse(stats, n) {
  const courseStats = selectCourseStats(stats, n);
  const exRankStats = selectExRankStats(stats);
  return {
    course: courseStats.course,
    avgST: courseStats.avgST,
    selfSummary: courseStats.selfSummary,
    winKimariteSelf: courseStats.winKimariteSelf,
    loseKimarite: courseStats.loseKimarite,
    matrixSelf: courseStats.matrixSelf,
    exTimeRank: exRankStats.list,
    exTimeRankByRank: exRankStats.byRank,
    // 参照元の最小限メタ
    _source: {
      schemaVersion: stats?.schemaVersion ?? null,
      regno: stats?.regno ?? null,
      fetchedAt: stats?.fetchedAt ?? null,
    },
  };
}
