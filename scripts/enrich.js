// scripts/enrich.js
import fs from "node:fs/promises";
import path from "node:path";

const ROOT = process.cwd();
const SRC = path.join(ROOT, "public/debug/src-today.txt");  // さっきの生データ
const OUT_FULL_DIR = path.join(ROOT, "public/programs/v2/today");
const OUT_SLIM_DIR = path.join(ROOT, "public/programs-slim/v2/today");

// 既存の「slim」フォーマットの最低限（必要なら調整して）
const toSlim = (p) => {
  const pid = String(p.race_stadium_number).padStart(2, "0");
  const race = `${String(p.race_number).padStart(2, "0")}R`.replace(/^0/,''); // 1R..12R 表記に合わせるなら調整
  const entries = p.boats.map((b) => ({
    lane: b.racer_boat_number ?? null,
    number: b.racer_number ?? null,
    name: b.racer_name ?? null,
    class: b.racer_class_number ?? null,
    branch: b.racer_branch_number ?? null,
    age: b.racer_age ?? null,
    // ここは後で強化予定（コース別成績など）
  }));
  return {
    schemaVersion: "1.0",
    date: p.race_date.replaceAll("-", ""),
    pid,
    race: `${p.race_number}R`,
    deadline: p.race_closed_at ?? null,
    title: p.race_title ?? null,
    distance: p.race_distance ?? null,
    entries
  };
};

async function main() {
  const raw = await fs.readFile(SRC, "utf8");
  const data = JSON.parse(raw);               // { programs: [...] } 形式
  const programs = data.programs ?? [];

  // 出力先フォルダ作成
  await Promise.all([
    fs.mkdir(OUT_FULL_DIR, { recursive: true }),
    fs.mkdir(OUT_SLIM_DIR, { recursive: true }),
  ]);

  for (const p of programs) {
    const pid = String(p.race_stadium_number).padStart(2, "0");
    const raceKey = `${String(p.race_number)}R`;

    const fullDir = path.join(OUT_FULL_DIR, pid);
    const slimDir = path.join(OUT_SLIM_DIR, pid);
    await fs.mkdir(fullDir, { recursive: true });
    await fs.mkdir(slimDir, { recursive: true });

    // フル（元データそのまま寄せる）
    await fs.writeFile(
      path.join(fullDir, `${raceKey}.json`),
      JSON.stringify(p, null, 2)
    );

    // スリム（API互換）
    const slim = toSlim(p);
    await fs.writeFile(
      path.join(slimDir, `${raceKey}.json`),
      JSON.stringify(slim, null, 2)
    );
  }

  // メタ（成功ヘルス用）
  const meta = { status: 200, count: programs.length, generatedAt: new Date().toISOString() };
  await fs.writeFile(path.join(ROOT, "public/debug/meta-today.json"), JSON.stringify(meta, null, 2));

  console.log(`done: programs=${programs.length}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
