// scripts/collect-racers.js
import fs from "node:fs/promises";
import path from "node:path";

const DATE = (process.env.TARGET_DATE || "today").replace(/-/g, "");
const base = path.join("public", "programs-slim", "v2", DATE);

async function walk(dir, acc = []) {
  try {
    const items = await fs.readdir(dir, { withFileTypes: true });
    for (const it of items) {
      const p = path.join(dir, it.name);
      if (it.isDirectory()) await walk(p, acc);
      else if (it.isFile() && it.name.endsWith(".json") && it.name !== "index.json") acc.push(p);
    }
  } catch {}
  return acc;
}

const files = await walk(base);
const set = new Set();
for (const f of files) {
  try {
    const j = JSON.parse(await fs.readFile(f, "utf8"));
    for (const e of j.entries || []) {
      if (e?.number) set.add(String(e.number));
    }
  } catch {}
}

const racersCsv = Array.from(set).join(",");
process.env.RACERS = racersCsv;
console.log("RACERS:", racersCsv);
