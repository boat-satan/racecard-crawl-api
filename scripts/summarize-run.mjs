// scripts/summarize-run.mjs
import fs from "node:fs/promises";
import path from "node:path";

const date = (process.env.DATE || "").replace(/-/g,"");
const pid  = (process.env.PID  || "").padStart(2,"0");
const race = (process.env.RACE || "1R").toUpperCase().replace(/[^\d]/g,"") + "R";

const ROOT = process.cwd();
const OUTJSON = path.join(ROOT,"public","predictions", date, pid, `${race}.json`);

const out = await fs.readFile(OUTJSON,"utf8").then(JSON.parse);
const top = out?.ranking?.slice(0,3) ?? [];
const summary = {
  meta: out.meta,
  top3: top.map(x => ({ lane:x.lane, number:x.number, name:x.name, score:x.scoreAdj }))
};
console.log(JSON.stringify(summary,null,2));
