import fs from "node:fs/promises";
import path from "node:path";

// ======== 入力パラメータ ========
const dateArg = process.argv[2] || "today";
const pidsArg = process.argv[3] || "";
const racesArg = process.argv[4] || "";

console.log(`[build-previews] date=${dateArg}, pids=${pidsArg}, races=${racesArg}`);

// TODO: ここに実際のデータ取得処理を入れる（previewsや公式HTMLスクレイピングなど）
// 仮テスト用のダミーデータ
const date = dateArg === "today"
  ? new Date().toISOString().slice(0, 10).replace(/-/g, "")
  : dateArg;

const OUTPUT_BASE = `public/exhibition/v1/${date}`;
await fs.mkdir(OUTPUT_BASE, { recursive: true });

let filesWritten = [];

// ここはテスト用ダミー — 実際は取得データループで書く
const sampleJson = { hello: "world", date, pid: "02", race: "1R" };
const filePath = path.join(OUTPUT_BASE, `02-1R.json`);
await fs.writeFile(filePath, JSON.stringify(sampleJson, null, 2), "utf8");
filesWritten.push({ pid: "02", race: "1R", path: filePath });

// ======== メタ情報書き出し ========
const META_DIR = "public/debug";
await fs.mkdir(META_DIR, { recursive: true });
await fs.writeFile(
  path.join(META_DIR, "exhibition-meta.json"),
  JSON.stringify({
    status: 200,
    generatedAt: new Date().toISOString(),
    baseDir: OUTPUT_BASE,
    pids: Array.from(new Set(filesWritten.map(f => f.pid))).sort(),
    count: filesWritten.length,
    sample: filesWritten.slice(0, 5),
  }, null, 2),
  "utf8"
);

console.log(`exhibition write count = ${filesWritten.length}`);
filesWritten.forEach(f => console.log("wrote:", f.path));
