// src/predict/bets.mjs
// コンパクト表記で 3連単を 18点に揃える（A/B/C方式）
// 入力は scenarios = [{ first:[..], second:[..], third:[..], prob }, ...]
// 返り値: { strings, tickets, compact, markdown }

function uniqTickets(arr) {
  const key = t => `${t[0]}-${t[1]}-${t[2]}`;
  const seen = new Set();
  const out = [];
  for (const t of arr) {
    const k = key(t);
    if (!seen.has(k)) { seen.add(k); out.push(t); }
  }
  return out;
}

function expandA(head, S, T) {
  const out = [];
  for (const s of S) for (const t of T) {
    if (t === s) continue;
    out.push([head, s, t]);
  }
  return out;
}

// 2=3ボックス（S 内入替）＋ 3着側追加候補（T\S）
function expandB(head, S, T) {
  const out = [];
  for (let i = 0; i < S.length; i++) {
    for (let j = 0; j < S.length; j++) {
      if (i === j) continue;
      out.push([head, S[i], S[j]]);
    }
  }
  const extra = T.filter(x => !S.includes(x));
  for (const s of S) for (const t of extra) {
    if (t === s) continue;
    out.push([head, s, t]);
  }
  return out;
}

// 2着は S、3着は T（同一不可）…Cは“3着側優先の2=3緩和”の簡易解釈
function expandC(head, S, T) {
  const out = [];
  for (const s of S) for (const t of T) {
    if (t === s) continue;
    out.push([head, s, t]);
  }
  return out;
}

function fmtSet(nums) { return nums.join(""); }
function toA(head, S, T) { return `${head}-${fmtSet(S)}-${fmtSet(T)}`; }
function toB(head, S, T) { return `${head}=${fmtSet(S)}-${fmtSet(T)}`; }
function toC(head, S, T) { return `${head}-${fmtSet(S)}=${fmtSet(T)}`; }

function pickTop(mapWeighted, k) {
  return [...mapWeighted.entries()]
    .sort((a,b)=>b[1]-a[1])
    .slice(0, k)
    .map(([n])=>Number(n));
}

function aggregateByHead(scenarios) {
  // 頭ごとに second/third の重みを集計
  const byHead = new Map(); // h -> { winW, secW:Map, triW:Map }
  for (const sc of scenarios) {
    const prob = sc.prob ?? 0;
    for (const h of (sc.first || [])) {
      if (!byHead.has(h)) byHead.set(h, { winW:0, secW:new Map(), triW:new Map() });
      const bucket = byHead.get(h);
      bucket.winW += prob;
      for (const s of (sc.second || [])) {
        bucket.secW.set(s, (bucket.secW.get(s)||0) + prob);
      }
      for (const t of (sc.third || [])) {
        bucket.triW.set(t, (bucket.triW.get(t)||0) + prob);
      }
    }
  }
  return [...byHead.entries()].sort((a,b)=>b[1].winW - a[1].winW);
}

/**
 * 中核: 18点に揃うまで、頭の強い順に A/B/C を自動探索し compact を作る
 */
export function buildCompactBets(scenarios, target = 18) {
  const headList = aggregateByHead(scenarios);
  const strings = [];
  let tickets = [];

  const secSizes = [2,3,4];
  const triSizes = [3,4,5,6];

  for (let hi = 0; hi < headList.length && tickets.length < target; hi++) {
    const [head, bucket] = headList[hi];
    const Sfull = pickTop(bucket.secW, 5);
    const Tfull = pickTop(bucket.triW, 6);

    let best = null; // { type, S, T, str, exp, rank }

    const need = Math.max(0, target - tickets.length);

    for (const sK of secSizes) {
      const S = Sfull.slice(0, Math.min(sK, Sfull.length));
      if (S.length < 2) continue;

      for (const tK of triSizes) {
        const T = Tfull.slice(0, Math.min(tK, Tfull.length));
        if (T.length < 2) continue;

        const cand = [
          { type:"A", S, T, str:toA(head,S,T), exp:expandA(head,S,T) },
          { type:"B", S, T, str:toB(head,S,T), exp:expandB(head,S,T) },
          { type:"C", S, T, str:toC(head,S,T), exp:expandC(head,S,T) },
        ];

        for (const c of cand) {
          const unique = uniqTickets(c.exp);
          const diff = Math.abs(need - unique.length);
          const over = unique.length - need; // >0 ならオーバー
          const rank = (diff === 0 ? 0 : diff) + (over > 0 ? 0.5 : 0); // ピタリ最優先、過剰は軽く減点
          if (!best || rank < best.rank) best = { ...c, exp:unique, rank };
        }
      }
    }

    if (best && best.exp.length) {
      strings.push(best.str);
      tickets = uniqTickets([...tickets, ...best.exp]);
    }
  }

  // 18点ぴったり調整
  if (tickets.length > target) {
    tickets = tickets.slice(0, target);
  } else if (tickets.length < target) {
    const extra = [];
    for (const sc of [...scenarios].sort((a,b)=> (b.prob??0)-(a.prob??0))) {
      for (const f of sc.first||[]) for (const s of sc.second||[]) for (const t of sc.third||[]) {
        if (f===s || s===t || f===t) continue;
        extra.push([f,s,t]);
      }
      if (tickets.length + extra.length >= target) break;
    }
    tickets = uniqTickets([...tickets, ...extra]).slice(0, target);
  }

  if (strings.length === 0 && tickets.length) {
    const head = tickets[0][0];
    const S = [...new Set(tickets.filter(t=>t[0]===head).map(t=>t[1]))].slice(0,3);
    const T = [...new Set(tickets.filter(t=>t[0]===head).map(t=>t[2]))].slice(0,4);
    strings.push(toA(head, S, T));
  }

  return { strings, tickets };
}

// ---- 表示用ユーティリティ ----
function ticketsToMarkdown(tickets) {
  if (!tickets?.length) return "_(なし)_";
  const lines = tickets.map(t => `3連単 ${t[0]}-${t[1]}-${t[2]}`);
  return lines.join(" ");
}

/**
 * 外部公開API:
 *   scenarios を受け取り、{ strings, tickets, compact, markdown } を返す
 *   - compact: 「1-23-234 / 4=35-135 …」の1行
 *   - markdown: 見出し＋行で貼れるテキスト
 */
export function generateBetsFromScenarios(scenarios, target = 18) {
  const { strings, tickets } = buildCompactBets(scenarios, target);
  const compact = strings.join(" / ");
  const md = [
    "### 買い目（18点・コンパクト表記）",
    compact || "_(なし)_",
    "",
    "### 展開済み（3連単）",
    ticketsToMarkdown(tickets),
  ].join("\n");
  return { strings, tickets, compact, markdown: md };
}

// default: predict.mjs からはこれを呼ぶ想定
export default generateBetsFromScenarios;