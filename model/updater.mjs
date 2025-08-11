// node model/updater.mjs 20250811    ← 日付指定（未指定なら直近7日サマリ）
import fs from "node:fs/promises";
import fss from "node:fs";
import path from "node:path";

const ROOT = process.cwd();
const rulesPath = path.join(ROOT, "model", "rules.json");
const lockPath  = path.join(ROOT, "model", "rules.lock.json");
const changelog = path.join(ROOT, "model", "rules.changelog.md");

function jread(p, d=null){ return fss.existsSync(p) ? JSON.parse(fss.readFileSync(p,"utf8")) : (d ?? {}); }
function clip(x,a,b){ return Math.max(a, Math.min(b, x)); }
function sign(x){ return x<0?-1:(x>0?1:0); }

function* listLines(dir, yyyyMMdd) {
  if (!fss.existsSync(dir)) return;
  if (yyyyMMdd) {
    const p = path.join(dir, `${yyyyMMdd}.jsonl`);
    if (fss.existsSync(p)) yield* fss.readFileSync(p, "utf8").trim().split(/\n+/);
    return;
  }
  for (const f of fss.readdirSync(dir)) {
    if (!/\.jsonl$/.test(f)) continue;
    yield* fss.readFileSync(path.join(dir,f), "utf8").trim().split(/\n+/);
  }
}

function brierPerBoat(pred, finish) {
  // 1着確率のBrier（0/1ターゲット）
  const firstLane = finish[0]; // 1着の枠
  return pred.per_boat.map(b => {
    const y = (b.lane === firstLane) ? 1 : 0;
    const p = (b.p1 ?? 0); // 1着確率を使う
    return { lane:b.lane, err:(p - y) ** 2, feat:b.features || {} };
  });
}

function aggregate(grps) {
  const out = {};
  for (const g of grps) {
    for (const k of Object.keys(g.feat)) {
      const v = g.feat[k];
      if (v == null || v !== v) continue;
      out[k] = out[k] || { n:0, sumErr:0, sumVal:0 };
      out[k].n += 1;
      out[k].sumErr += g.err;
      out[k].sumVal += (typeof v === "number" ? v : 0);
    }
  }
  return out;
}

function proposeDelta(agg, rules) {
  const deltas = {};
  const { lr=0.02, clip:clipStep=0.05, min_support=30 } = rules.learning || {};
  for (const [k, s] of Object.entries(agg)) {
    if (s.n < min_support) continue;
    // 単純勾配符号法：誤差と特徴量の相関符号で微調整
    const corrSign = sign(s.sumVal * s.sumErr - (s.sumVal * s.sumVal)/s.n); // 粗い近似
    if (corrSign === 0) continue;
    deltas[k] = clip(corrSign * lr, -clipStep, clipStep);
  }
  return deltas;
}

function applyVenueAware(deltas, rules) {
  // コース別/パラメタ別に存在するものだけ反映（未知キーは無視）
  const w = rules.weights;
  const out = {};
  for (const [k, dv] of Object.entries(deltas)) {
    if (k.startsWith("course_bias.")) {
      const c = k.split(".")[1];
      if (w.course_bias?.[c] != null) {
        out["course_bias."+c] = dv;
      }
      continue;
    }
    if (w[k] != null) out[k] = dv;
  }
  return out;
}

function patchWeights(weights, deltas, bounds) {
  const w = JSON.parse(JSON.stringify(weights));
  const { min=-0.6, max=0.6 } = bounds || {};
  for (const [k, dv] of Object.entries(deltas)) {
    if (k.startsWith("course_bias.")) {
      const c = k.split(".")[1];
      w.course_bias[c] = clip((w.course_bias[c] ?? 0) + dv, min, max);
    } else {
      w[k] = clip((w[k] ?? 0) + dv, min, max);
    }
  }
  return w;
}

async function main() {
  const targetDate = process.argv[2];
  const rules = jread(rulesPath);
  const lock  = jread(lockPath, { history:[] });

  // 予想×結果を突き合わせ
  const diffs = [];
  for (const line of listLines(path.join(ROOT,"data","predictions"), targetDate)) {
    const pred = JSON.parse(line);
    const rid = `${pred.date}-${pred.pid}-${pred.race}`;
    const resLine = [...listLines(path.join(ROOT,"data","results"), pred.date)]
      .map(JSON.parse).find(r => r.date===pred.date && r.pid===pred.pid && r.race===pred.race);
    if (!resLine) continue;

    const per = brierPerBoat(pred, resLine.finish);
    const agg = aggregate(per);

    // 特徴キー名を “weights に対応するキー” へ正規化例
    const mapped = {};
    for (const [k,s] of Object.entries(agg)) {
      if (k === "ex_rank") mapped["ex_time_rank_bonus"] = s;
      else if (k === "start_adv_0p01") mapped["start_adv_per_0p01"] = s;
      else mapped[k] = s;
    }

    const deltaRaw = proposeDelta(mapped, rules);
    const delta = applyVenueAware(deltaRaw, rules);
    if (Object.keys(delta).length === 0) continue;

    diffs.push({ rid, delta, support: Object.values(mapped)[0]?.n || 0 });
  }

  if (!diffs.length) {
    console.log("no-sufficient-data");
    process.exit(0);
  }

  // まとめて平均し、微調整案を作る
  const merged = {};
  for (const d of diffs) {
    for (const [k,v] of Object.entries(d.delta)) {
      merged[k] = (merged[k] || 0) + v;
    }
  }
  for (const k of Object.keys(merged)) merged[k] /= diffs.length;

  const newWeights = patchWeights(rules.weights, merged, rules.bounds);
  const now = new Date().toISOString();

  // 変更がほぼゼロならスキップ
  const any = Object.keys(merged).length > 0;
  if (!any) { console.log("no-delta"); process.exit(0); }

  // lock & changelog 更新
  lock.history.push({ at: now, merged, sample: diffs.slice(0,5) });
  await fs.writeFile(lockPath, JSON.stringify(lock, null, 2));
  const lines = [
    `## ${now}`,
    `- merged: ${JSON.stringify(merged)}`,
    diffs.length ? `- samples: ${diffs.length} races` : "- samples: 0",
    ""
  ].join("\n");
  await fs.appendFile(changelog, lines, "utf8");

  // 提案は rules.json を直接書き換えせず、隣に保存（ワークフローでPR作る）
  await fs.writeFile(path.join(ROOT,"model","rules.proposed.json"),
    JSON.stringify({ ...rules, weights: newWeights }, null, 2));

  console.log("proposed-delta", merged);
}
main().catch(e => { console.error(e); process.exit(1); });
