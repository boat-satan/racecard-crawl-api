// src/predict/bets.mjs
// どちらの入力でもOK：
//  A) scenarios: [{first:[..], second:[..], third:[..], prob:0.xx}, ...]
//  B) probs: { "1-2-3": 0.12, "1-3-2": 0.08, ... }

function uniqTickets(arr){
  const set = new Set(), out=[];
  for(const t of arr){
    const k = `${t[0]}-${t[1]}-${t[2]}`;
    if(!set.has(k)){ set.add(k); out.push(t); }
  }
  return out;
}

function parseFromProbs(probsObj={}, limit=80){
  // "1-2-3":p → [[1,2,3,p], ...] へ
  return Object.entries(probsObj)
    .map(([k,p])=>{
      const m = k.match(/^(\d)-(\d)-(\d)$/);
      if(!m) return null;
      return [Number(m[1]), Number(m[2]), Number(m[3]), Number(p)||0];
    })
    .filter(Boolean)
    .sort((a,b)=>b[3]-a[3])
    .slice(0, limit)
    .map(([a,b,c])=>[a,b,c]);
}

function aggregateByHeadFromTickets(tickets){
  // 頭ごとに2着/3着の重み集計
  const byHead = new Map();
  for(const [h,s,t] of tickets){
    if(!byHead.has(h)) byHead.set(h,{sec:new Map(), tri:new Map(), win:0});
    const b = byHead.get(h);
    b.win += 1;
    b.sec.set(s, (b.sec.get(s)||0)+1);
    b.tri.set(t, (b.tri.get(t)||0)+1);
  }
  return [...byHead.entries()].sort((a,b)=>b[1].win-a[1].win);
}

function pickTop(map, k){
  return [...map.entries()].sort((a,b)=>b[1]-a[1]).slice(0,k).map(([n])=>Number(n));
}

function expandA(h,S,T){
  const out=[];
  for(const s of S) for(const t of T){ if(s!==t) out.push([h,s,t]); }
  return out;
}

// ===== コンパクト表記の作成（簡易） =====
function toA(h,S,T){ return `${h}-${S.join("")}-${T.join("")}`; }

export default function bets(input, target=18){
  let tickets = [];

  if (Array.isArray(input)) {
    // 旧：シナリオ配列
    const scenarios = input.slice().sort((a,b)=>(b.prob||0)-(a.prob||0));
    for(const sc of scenarios){
      for(const f of sc.first||[]) for(const s of sc.second||[]) for(const t of sc.third||[]){
        if(f===s || s===t || f===t) continue;
        tickets.push([f,s,t]);
      }
      if(tickets.length >= target*3) break;
    }
  } else if (input && typeof input === "object") {
    // 新：確率Map
    tickets = parseFromProbs(input, target*6);
  }

  tickets = uniqTickets(tickets);
  if (tickets.length === 0) {
    return { compact:"", main:[], ana:[], markdown:"(no bets)" };
  }

  // 頭集計 → 強い頭から 1〜2本の compact に要約して18点へ
  const heads = aggregateByHeadFromTickets(tickets);
  const strings = [];
  let out = [];

  for(const [h, bucket] of heads){
    if(out.length >= target) break;
    const S = pickTop(bucket.sec, 3);   // 2着 2〜3艇
    const T = pickTop(bucket.tri, 4);   // 3着 3〜4艇
    if (S.length < 2 || T.length < 3) continue;

    const exp = uniqTickets(expandA(h,S,T));
    strings.push(toA(h,S,T));
    out = uniqTickets([...out, ...exp]);
  }

  // 点数調整
  if(out.length > target) out = out.slice(0, target);
  if(out.length < target){
    for(const t of tickets){
      out.push(t);
      out = uniqTickets(out);
      if(out.length >= target) break;
    }
  }

  const compact = strings.join(", ");
  const main = out;        // とりあえず全て本命〜中穴側として返す
  const ana  = [];         // 穴目は後日拡張

  const md = compact
    ? `**買い目（本命〜中穴 18点）**\n${compact}`
    : "**買い目**\n(生成なし)";

  return { compact, main, ana, markdown: md };
}