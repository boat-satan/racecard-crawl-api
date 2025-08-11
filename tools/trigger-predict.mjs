#!/usr/bin/env node
// Node v20 ESM
// 使い方:
//   GITHUB_TOKEN=xxxxx node tools/trigger-predict.mjs --place 戸田 --race 1R --date today --owner boat-satan --repo racecard-crawl-api --wait
//   GITHUB_TOKEN=xxxxx node tools/trigger-predict.mjs "戸田1R予想して"

import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";

const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
if (!GITHUB_TOKEN) {
  console.error("ERROR: GITHUB_TOKEN が未設定です（workflow 権限が必要）");
  process.exit(1);
}

function jstDateYYYYMMDD(d = new Date()){
  // JSTでYYYYMMDD
  const dt = new Date(d.toLocaleString("en-US", { timeZone: "Asia/Tokyo" }));
  const y = dt.getFullYear();
  const m = String(dt.getMonth()+1).padStart(2,"0");
  const day = String(dt.getDate()).padStart(2,"0");
  return `${y}${m}${day}`;
}

const PLACE2PID = {
  "桐生":"01","戸田":"02","江戸川":"03","平和島":"04","多摩川":"05","浜名湖":"06",
  "蒲郡":"07","常滑":"08","津":"09","三国":"10","びわこ":"11","住之江":"12",
  "尼崎":"13","鳴門":"14","丸亀":"15","児島":"16","宮島":"17","徳山":"18",
  "下関":"19","若松":"20","芦屋":"21","福岡":"22","唐津":"23","大村":"24",
};

function parseArgs(argv){
  // 自然文でも拾う（例: "戸田1R予想して"）
  const joined = argv.join(" ");
  const out = { owner:"boat-satan", repo:"racecard-crawl-api", wait:false };

  // flags
  for (let i=0;i<argv.length;i++){
    const a = argv[i];
    const nx = argv[i+1];
    if (a === "--owner") out.owner = nx;
    if (a === "--repo") out.repo = nx;
    if (a === "--date") out.date = nx;
    if (a === "--pid") out.pid = nx;
    if (a === "--place") out.place = nx;
    if (a === "--race") out.race = nx;
    if (a === "--wait") out.wait = true;
  }

  // 自然文から place / race 推定
  if (!out.place){
    const hit = Object.keys(PLACE2PID).find(p => joined.includes(p));
    if (hit) out.place = hit;
  }
  if (!out.race){
    const m = joined.match(/(\d{1,2})\s*R/i) || joined.match(/(\d{1,2})/);
    if (m) out.race = `${parseInt(m[1],10)}R`;
  }

  // 正規化
  if (!out.pid && out.place) out.pid = PLACE2PID[out.place];
  if (!out.date || out.date.toLowerCase() === "today") out.date = jstDateYYYYMMDD();
  if (out.race) out.race = String(out.race).toUpperCase().replace(/[^\d]/g,"")+"R";

  if (!out.pid || !out.race) {
    console.error("使い方例: node tools/trigger-predict.mjs --place 戸田 --race 1R [--date today] [--wait]");
    process.exit(1);
  }
  return out;
}

async function gh(path, init = {}){
  const res = await fetch(`https://api.github.com${path}`, {
    ...init,
    headers: {
      "authorization": `Bearer ${GITHUB_TOKEN}`,
      "accept": "application/vnd.github+json",
      "x-github-api-version": "2022-11-28",
      ...(init.headers||{})
    }
  });
  if (!res.ok) {
    const t = await res.text();
    throw new Error(`GitHub API ${res.status} ${res.statusText}: ${t}`);
  }
  return res;
}

async function dispatchWorkflow({owner, repo, date, pid, race}){
  const ref = "main";
  await gh(`/repos/${owner}/${repo}/actions/workflows/predict.yml/dispatches`, {
    method: "POST",
    body: JSON.stringify({ ref, inputs: { date, pid, race } })
  });
}

async function waitForRun({owner, repo, startedAt}){
  // predict ワークフローの最新 run を掴む
  const deadline = Date.now() + 10*60*1000; // 最大10分待つ
  while (Date.now() < deadline){
    const res = await gh(`/repos/${owner}/${repo}/actions/runs?event=workflow_dispatch&per_page=20`);
    const json = await res.json();
    const run = (json.workflow_runs||[]).find(r =>
      r.name === "predict" && new Date(r.created_at).getTime() >= startedAt
    );
    if (run){
      // 結果待機
      const runId = run.id;
      while (true){
        const r2 = await gh(`/repos/${owner}/${repo}/actions/runs/${runId}`);
        const j2 = await r2.json();
        if (j2.status === "completed") return j2;
        await new Promise(s => setTimeout(s, 5000));
      }
    }
    await new Promise(s => setTimeout(s, 3000));
  }
  throw new Error("run が見つからない/完了しない");
}

async function downloadArtifactJSON({owner, repo, runId, name="predict-output"}){
  const r = await gh(`/repos/${owner}/${repo}/actions/runs/${runId}/artifacts`);
  const j = await r.json();
  const art = (j.artifacts||[]).find(a => a.name === name);
  if (!art) return null;

  const zipRes = await gh(`/repos/${owner}/${repo}/actions/artifacts/${art.id}/zip`);
  const buf = Buffer.from(await zipRes.arrayBuffer());
  const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "predict-"));
  const zipPath = path.join(tmp, "a.zip");
  await fs.writeFile(zipPath, buf);

  // 解凍（GitHub ホスト上なら unzip、ローカルなら OS 既定を想定）
  // 依存を増やしたくないので、Nodeだけで中身を拾う簡易処理（prediction.json だけ想定）
  // → 超簡易: 'unzip -p' があれば利用
  try{
    const { execSync } = await import("node:child_process");
    const out = execSync(`unzip -p "${zipPath}"`, { encoding:"utf8" });
    return JSON.parse(out);
  }catch{
    console.warn("zip 展開が失敗。アーティファクト UI から直接参照してね。");
    return null;
  }
}

async function main(){
  const args = parseArgs(process.argv.slice(2));
  const { owner, repo, date, pid, race, wait } = args;

  console.log(`Dispatch predict: ${owner}/${repo} date=${date} pid=${pid} race=${race}`);
  const ts = Date.now();
  await dispatchWorkflow(args);
  console.log("→ 起動しました。");

  if (!wait){
    console.log(`Actions ページ: https://github.com/${owner}/${repo}/actions`);
    return;
  }

  console.log("→ 完了を待機中...");
  const run = await waitForRun({owner, repo, startedAt: ts});
  console.log(`status=${run.status} conclusion=${run.conclusion} run_id=${run.id}`);
  const json = await downloadArtifactJSON({owner, repo, runId: run.id});
  if (json) {
    console.log("=== prediction.json ===");
    console.log(JSON.stringify(json, null, 2));
  } else {
    console.log(`アーティファクト: https://github.com/${owner}/${repo}/actions/runs/${run.id}`);
  }
}

main().catch(e => { console.error(e); process.exit(1); });
