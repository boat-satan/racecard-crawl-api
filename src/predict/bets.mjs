/**
 * コンパクト表記の買い目生成
 * 入力: scenarios = [{ first:[..], second:[..], third:[..], prob:0.XX }, ...]
 * 出力: 
 *  - strings: ["1-23-234", "4=35-135" ...]  // 18点に揃うよう自動で組成
 *  - tickets: [[1,2,3], [1,3,2], ...]       // 実展開した3連単（重複排除）
 *
 * 表記の意味（本モジュール定義）：
 *  - A: "h-AB-CDE"  …… 1着= h, 2着∈{A,B}, 3着∈{C,D,E}（同一艇の重複不可）
 *        展開数 = Σ_{s∈S} ( |T| - [s∈T] )
 *  - B: "h=AB-CDE" …… 1着= h, 2・3着は {A,B} の入れ替え + 3着に {C,D,E} の“追加候補”
 *        展開数 = m*(m-1) + m*|T\{A,B}|
 *  - C: "h-AB=CDE" …… 1着= h, 2着は {A,B}, ただし 2着と3着は {C,D,E} 側とも入れ替え可
 *        展開数 = |{A,B}| * (|{C,D,E}| - 1)  // 実質Aに近いが 3着側の集合を優先
 *
 * ※Bは「2=3の入れ替えを含む」慣用表記。Cは“3着側優先で2=3ボックスも吸収”の簡易解釈。
 */

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
  for (const s of S) {
    for (const t of T) {
      if (t === s) continue;
      out.push([head, s, t]);
    }
  }
  return out;
}

function expandB(head, S, T) {
  // 2=3 の入れ替え（S内ボックス） + 3着追加候補（T \ S）
  const out = [];
  for (let i = 0; i < S.length; i++) {
    for (let j = 0; j < S.length; j++) {
      if (i === j) continue;
      out.push([head, S[i], S[j]]);
    }
  }
  const extra = T.filter(x => !S.includes(x));
  for (const s of S) {
    for (const t of extra) {
      if (t === s) continue;
      out.push([head, s, t]);
    }
  }
  return out;
}

function expandC(head, S, T) {
  // 2着はSから、3着はTから（同一禁止）。T優先の2=3緩和イメージ
  const out = [];
  for (const s of S) {
    for (const t of T) {
      if (t === s) continue;
      out.push([head, s, t]);
    }
  }
  return out;
}

function fmtSet(nums) { return nums.join(""); }
function toA(head, S, T) { return `${head}-${fmtSet(S)}-${fmtSet(T)}`; }
function toB(head, S, T) { return `${head}=${fmtSet(S)}-${fmtSet(T)}`; }
function toC(head, S, T) { return `${head}-${fmtSet(S)}=${fmtSet(T)}`; }

function pickTop(listWeighted, k) {
  // listWeighted: Map<num, weight>
  return [...listWeighted.entries()]
    .sort((a,b)=>b[1]-a[1])
    .slice(0, k)
    .map(([n])=>Number(n));
}

function aggregateByHead(scenarios) {
  // 頭ごとに second/third の出現重みを集計
  const byHead = new Map(); // h -> { winW, secW:Map, triW:Map }
  for (const sc of scenarios) {
    for (const h of sc.first || []) {
      if (!byHead.has(h)) byHead.set(h, { winW:0, secW:new Map(), triW:new Map() });
      const bucket = byHead.get(h);
      bucket.winW += sc.prob || 0;

      for (const s of sc.second || []) {
        bucket.secW.set(s, (bucket.secW.get(s) || 0) + (sc.prob || 0));
      }
      for (const t of sc.third || []) {
        bucket.triW.set(t, (bucket.triW.get(t) || 0) + (sc.prob || 0));
      }
    }
  }
  return [...byHead.entries()]
    .sort((a,b)=>b[1].winW - a[1].winW); // 頭の強さ順
}

/**
 * 18点に揃うまで、頭の強い順に compact 表記を選ぶ。
 * まずは最有力の頭で S/T のサイズと表記タイプ(A/B/C)を自動探索してピタリ目を狙う。
 * 足りなければ次点の頭で数を補填、超えたら末尾をカット。
 */
export function buildCompactBets(scenarios, target = 18) {
  const headList = aggregateByHead(scenarios);
  const strings = [];
  let tickets = [];

  // 探索パラメータ（秒着候補2〜4、三着候補3〜6）
  const secSizes = [2,3,4];
  const triSizes = [3,4,5,6];

  for (let hi = 0; hi < headList.length && tickets.length < target; hi++) {
    const [head, bucket] = headList[hi];
    const Sfull = pickTop(bucket.secW, 5);
    const Tfull = pickTop(bucket.triW, 6);

    let best = null; // {type, S, T, str, expTickets}

    // 3タイプ×サイズのグリッド探索で target - 現在枚数 に最も近い構成を選択
    const need = Math.max(0, target - tickets.length);

    for (const sK of secSizes) {
      const S = Sfull.slice(0, Math.min(sK, Sfull.length));
      if (S.length < 2) continue;

      for (const tK of triSizes) {
        const T = Tfull.slice(0, Math.min(tK, Tfull.length));
        if (T.length < 2) continue;

        const cand = [];

        const A = expandA(head, S, T);
        cand.push({ type:"A", S, T, str:toA(head,S,T), exp:A });

        const B = expandB(head, S, T);
        cand.push({ type:"B", S, T, str:toB(head,S,T), exp:B });

        const C = expandC(head, S, T);
        cand.push({ type:"C", S, T, str:toC(head,S,T), exp:C });

        for (const c of cand) {
          const unique = uniqTickets(c.exp);
          const diff = Math.abs(need - unique.length);
          const scoreFit = (need === 0) ? 999 : (need - unique.length); // <=0 はオーバー
          // 1) ピタリ優先 2) 近い方 3) 票数が少し多い場合は避ける
          const rank = (diff === 0 ? 0 : Math.abs(scoreFit)) + (scoreFit < 0 ? 0.5 : 0);
          if (!best || rank < best.rank) best = { ...c, exp:unique, rank, diff };
        }
      }
    }

    if (best && best.exp.length) {
      strings.push(best.str);
      tickets = uniqTickets([...tickets, ...best.exp]);
    }
  }

  // 18点ぴったりに調整（超過ならカット、不足なら scenarios 直展開で補完）
  if (tickets.length > target) {
    tickets = tickets.slice(0, target);
  } else if (tickets.length < target) {
    // 既存ticketsに無いものを scenarios から明示列挙で補完
    const extra = [];
    for (const sc of scenarios.sort((a,b)=>b.prob-a.prob)) {
      for (const f of sc.first||[]) for (const s of sc.second||[]) for (const t of sc.third||[]) {
        if (f===s || s===t || f===t) continue;
        extra.push([f,s,t]);
      }
      if (tickets.length + extra.length >= target) break;
    }
    tickets = uniqTickets([...tickets, ...extra]).slice(0, target);
  }

  // strings が空（極端ケース）のときは最後の救済として単純 1-23-234 を1つ
  if (strings.length === 0 && tickets.length) {
    const head = tickets[0][0];
    const S = [...new Set(tickets.filter(t=>t[0]===head).map(t=>t[1]))].slice(0,3);
    const T = [...new Set(tickets.filter(t=>t[0]===head).map(t=>t[2]))].slice(0,4);
    strings.push(toA(head, S, T));
  }

  return { strings, tickets };
}