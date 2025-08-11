// scripts/autotune-rules.mjs
// Node v20 / ESM
// 役割: 予想ログと結果ログから、ST予測/展示/風補正などの重みを微調整して rules/rules.json を更新提案
// 期待するログの配置:
//   logs/predict/v1/YYYYMMDD/PID/RR.json
//   logs/feedback/v1/YYYYMMDD/PID/RR.json
// 期待するルールの配置:
//   rules/rules.json

import fs from "node:fs/promises";
import fssync from "node:fs";
import path from "node:path";

const ROOT = process.cwd();
const DAYS = Number(process.env.DAYS || "3");
const DRY_RUN = String(process.env.DRY_RUN || "true").toLowerCase() === "true";

const RULES_PATH = path.join(ROOT, "rules", "rules.json");
const PRED_DIR = path.join(ROOT, "logs", "predict", "v1");
const FB_DIR   = path.join(ROOT, "logs", "feedback", "v1");

// ---------- helpers ----------
const exists = (p) => fssync.existsSync(p);
const readJson = async (p) => JSON.parse(await fs.readFile(p, "utf8"));
const writeJson = async (p, j) => {
  await fs.mkdir(path.dirname(p), { recursive: true });
  await fs.writeFile(p, JSON.stringify(j, null, 2), "utf8");
};

function* lastNDates(n) {
  const now = new Date();
  for (let i = 0; i < n; i++) {
    const d = new Date(now.getTime() - i * 86400000);
    const y = String(d.getFullYear());
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const day = String(d.getDate()).padStart(2, "0");
    yield `${y}${m}${day}`;
  }
}

// ログの最小フォーマット例：
// predict: { date,pid,race, features:{stRecent[], stExhibition[], motorIndex[]}, pred:{ st:[6], scenario: "..."} }
// feedback: { date,pid,race, result:{ st:[6], order:[6] }, weather:{ windSpeed, windDirection } }
function safeArr(a, len = 6) {
  if (!Array.isArray(a)) return Array(len).fill(null);
  const out = Array(len).fill(null);
  for (let i = 0; i < len; i++) out[i] = a[i] ?? null;
  return out;
}

function corr(x, y) {
  const xs = x.filter((v, i) => v != null && y[i] != null);
  const ys = y.filter((v, i) => x[i] != null && y[i] != null);
  const n = xs.length;
  if (n < 3) return 0;
  const mean = (a) => a.reduce((s, v) => s + v, 0) / a.length;
  const mx = mean(xs), my = mean(ys);
  let num = 0, dx = 0, dy = 0;
  for (let i = 0; i < n; i++) {
    const vx = xs[i] - mx, vy = ys[i] - my;
    num += vx * vy; dx += vx * vx; dy += vy * vy;
  }
  const den = Math.sqrt(dx * dy);
  return den === 0 ? 0 : num / den;
}

// ---------- main ----------
async function loadRules() {
  if (!exists(RULES_PATH)) {
    return {
      schemaVersion: 1,
      weights: {
        st_recent_weight: 0.6,
        st_exhibition_weight: 0.3,
        st_motor_weight: 0.1
      },
      adjustments: {
        exhibition_fast_bonus: 0.01,   // 展示最速ボーナス（ST短縮）
        exhibition_F_bonus: 0.005,     // 展示F表記を攻め意識として微加点
        wind_head_penalty_per_mps: 0.002 // 向かい風1m/sあたりのST遅延仮定
      },
      notes: []
    };
  }
  return readJson(RULES_PATH);
}

async function collectPairs() {
  const pairs = [];
  for (const date of lastNDates(DAYS)) {
    const datePredDir = path.join(PRED_DIR, date);
    if (!exists(datePredDir)) continue;
    const pids = fssync.readdirSync(datePredDir);
    for (const pid of pids) {
      const ridDir = path.join(datePredDir, pid);
      const races = fssync.readdirSync(ridDir).filter(f => f.endsWith(".json"));
      for (const file of races) {
        const race = file.replace(/\.json$/,"");
        const pPath = path.join(PRED_DIR, date, pid, `${race}.json`);
        const fPath = path.join(FB_DIR, date, pid, `${race}.json`);
        if (!exists(pPath) || !exists(fPath)) continue;
        try{
          const pred = await readJson(pPath);
          const fb   = await readJson(fPath);
          pairs.push({pred, fb});
        }catch{}
      }
    }
  }
  return pairs;
}

function propose(rules, pairs) {
  const notes = [];
  if (pairs.length === 0) {
    notes.push("データ不足のため変更なし");
    return { rules, notes, changed:false };
  }

  // 1) ST誤差から weights を微調整
  // 誤差 = |predST - actualST|
  const deltasRecent = [];
  const deltasExh    = [];
  const errors       = [];

  for (const {pred, fb} of pairs) {
    const stPred = safeArr(pred?.pred?.st);
    const stAct  = safeArr(fb?.result?.st);
    const stRecent = safeArr(pred?.features?.stRecent);
    const stExh    = safeArr(pred?.features?.stExhibition);

    for (let i = 0; i < 6; i++) {
      if (stPred[i] == null || stAct[i] == null) continue;
      const err = Math.abs(Number(stPred[i]) - Number(stAct[i]));
      errors.push(err);

      // “どっちの情報が当たってたか” をラフに見るために
      if (stRecent[i] != null) deltasRecent.push(Math.abs(stRecent[i] - stAct[i]));
      if (stExh[i] != null)    deltasExh.push(Math.abs(stExh[i] - stAct[i]));
    }
  }

  const avg = (a) => a.length ? a.reduce((s,v)=>s+v,0)/a.length : null;
  const mae = avg(errors);
  const maeRecent = avg(deltasRecent);
  const maeExh    = avg(deltasExh);

  notes.push(`全体ST MAE: ${mae?.toFixed(3) ?? "n/a"}`);
  if (maeRecent!=null) notes.push(`Recent基準の誤差: ${maeRecent.toFixed(3)}`);
  if (maeExh!=null)    notes.push(`Exhibition基準の誤差: ${maeExh.toFixed(3)}`);

  // recent と exhibition のどちらが“近かったか”で重みを微調整（±0.02の範囲）
  let changed = false;
  const step = 0.02;

  if (maeRecent!=null && maeExh!=null) {
    if (maeExh + 0.005 < maeRecent) {
      // 展示の方が近い → exhibition を上げ recent を下げる
      const w1 = rules.weights.st_exhibition_weight + step;
      const w2 = rules.weights.st_recent_weight - step;
      const w3 = rules.weights.st_motor_weight;
      const s = w1 + w2 + w3;
      rules.weights.st_exhibition_weight = +(w1/s).toFixed(3);
      rules.weights.st_recent_weight     = +(w2/s).toFixed(3);
      rules.weights.st_motor_weight      = +(w3/s).toFixed(3);
      notes.push(`展示寄りに重み調整 (+exh, -recent)`);
      changed = true;
    } else if (maeRecent + 0.005 < maeExh) {
      const w1 = rules.weights.st_exhibition_weight - step;
      const w2 = rules.weights.st_recent_weight + step;
      const w3 = rules.weights.st_motor_weight;
      const s = w1 + w2 + w3;
      rules.weights.st_exhibition_weight = +(w1/s).toFixed(3);
      rules.weights.st_recent_weight     = +(w2/s).toFixed(3);
      rules.weights.st_motor_weight      = +(w3/s).toFixed(3);
      notes.push(`Recent寄りに重み調整 (-exh, +recent)`);
      changed = true;
    }
  }

  // 2) 風影響の妥当性ざっくりチェック（向かい風が強い日にSTが遅れがちなら penalty を微増）
  const wind = [];
  const stLag = [];
  for (const {pred, fb} of pairs) {
    const ws = Number(fb?.weather?.windSpeed ?? 0);
    const stPred = safeArr(pred?.pred?.st);
    const stAct  = safeArr(fb?.result?.st);
    for (let i=0;i<6;i++){
      if (stPred[i]==null || stAct[i]==null) continue;
      wind.push(ws);
      stLag.push(stAct[i]-stPred[i]); // +だと実測が遅れ
    }
  }
  if (wind.length >= 10) {
    const c = corr(wind, stLag);
    notes.push(`風速とST遅れの相関: ${c.toFixed(3)}`);
    if (c > 0.2 && rules.adjustments.wind_head_penalty_per_mps < 0.005) {
      rules.adjustments.wind_head_penalty_per_mps = +(rules.adjustments.wind_head_penalty_per_mps + 0.0005).toFixed(4);
      notes.push(`向かい風ペナルティを微増 (+0.0005)`);
      changed = true;
    } else if (c < 0 && rules.adjustments.wind_head_penalty_per_mps > 0.0005) {
      rules.adjustments.wind_head_penalty_per_mps = +(rules.adjustments.wind_head_penalty_per_mps - 0.0005).toFixed(4);
      notes.push(`向かい風ペナルティを微減 (-0.0005)`);
      changed = true;
    }
  }

  rules.notes = [
    ...(Array.isArray(rules.notes) ? rules.notes.slice(-50) : []),
    { at: new Date().toISOString(), mae, maeRecent, maeExh, changes: notes }
  ];

  return { rules, notes, changed };
}

async function main(){
  const rules = await loadRules();
  const pairs = await collectPairs();
  const { rules: updated, notes, changed } = propose(structuredClone(rules), pairs);

  const outNote = `rules/autotune-notes/${new Date().toISOString().replace(/[:.]/g,"-")}.md`;
  await fs.mkdir(path.dirname(path.join(ROOT, outNote)), { recursive: true });
  await fs.writeFile(path.join(ROOT, outNote),
`# Autotune Notes
- DAYS: ${DAYS}
- DRY_RUN: ${DRY_RUN}
- pairs: ${pairs.length}

${notes.map(n=>`- ${n}`).join("\n")}
`, "utf8");

  if (!changed) {
    console.log("no changes. exit.");
    return;
  }

  if (DRY_RUN) {
    const draft = path.join(ROOT, "rules", "rules.draft.json");
    await writeJson(draft, updated);
    console.log(`DRY_RUN=true -> wrote draft: ${path.relative(ROOT, draft)}`);
  } else {
    await writeJson(RULES_PATH, updated);
    console.log(`rules.json updated.`);
  }
}

main().catch(e => { console.error(e); process.exit(1); });
