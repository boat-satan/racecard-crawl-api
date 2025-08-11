// scripts/update-rules.mjs
import fs from "node:fs/promises";
import fssync from "node:fs";
import path from "node:path";

const ROOT = process.cwd();
const HEURIS = path.join(ROOT,"rules","heuristics.json");

const NOTE = process.env.NOTE || process.argv.slice(2).join(" ");
if (!NOTE) {
  console.error("NOTE is empty.");
  process.exit(0);
}

async function main(){
  let j = { weights:{}, notes:[] };
  if (fssync.existsSync(HEURIS)) {
    j = JSON.parse(await fs.readFile(HEURIS,"utf8"));
  }
  j.notes = j.notes || [];
  j.notes.push(`${new Date().toISOString()}  ${NOTE}`);
  await fs.writeFile(HEURIS, JSON.stringify(j,null,2), "utf8");
  console.log("updated:", path.relative(ROOT, HEURIS));
}
main().catch(e => { console.error(e); process.exit(1); });
