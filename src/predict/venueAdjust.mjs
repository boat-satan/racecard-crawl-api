/**
 * 場別特性補正
 * - ST補正：その場で出やすい ST 傾向を微調整
 * - スコア補正：イン/差し/まくり寄りなどを倍率で反映
 * - シナリオ重み補正：決まり手傾向をシナリオ確率に寄与
 */

const VENUE_PROFILES = {
  "01": { name:"桐生",  bias:{ inScore:1.03, dashScore:0.99, sashi:1.03, makuri:0.98, mZashi:1.01 }, stBias:{ in:-0.002, dash:-0.001 } },
  "02": { name:"戸田",  bias:{ inScore:0.98, dashScore:1.02, sashi:1.05, makuri:0.97, mZashi:1.02 }, stBias:{ in:+0.002, dash:-0.002 } },
  "03": { name:"江戸川",bias:{ inScore:0.97, dashScore:1.03, sashi:1.05, makuri:0.96, mZashi:1.02 }, stBias:{ in:+0.003, dash:0 } },
  "04": { name:"平和島",bias:{ inScore:0.99, dashScore:1.02, sashi:1.03, makuri:1.01, mZashi:1.03 }, stBias:{ in:+0.001, dash:-0.002 } },
  "05": { name:"多摩川",bias:{ inScore:1.01, dashScore:1.02, sashi:1.00, makuri:1.03, mZashi:1.02 }, stBias:{ in:-0.001, dash:-0.002 } },
  "06": { name:"浜名湖",bias:{ inScore:1.00, dashScore:1.02, sashi:1.01, makuri:1.02, mZashi:1.01 }, stBias:{ in:0, dash:-0.002 } },
  "07": { name:"蒲郡",  bias:{ inScore:1.03, dashScore:1.01, sashi:1.01, makuri:1.02, mZashi:1.00 }, stBias:{ in:-0.002, dash:-0.001 } },
  "08": { name:"常滑",  bias:{ inScore:1.01, dashScore:1.00, sashi:1.00, makuri:1.01, mZashi:1.00 }, stBias:{ in:-0.001, dash:-0.001 } },
  "09": { name:"津",    bias:{ inScore:1.02, dashScore:1.01, sashi:1.00, makuri:1.02, mZashi:1.00 }, stBias:{ in:-0.002, dash:-0.001 } },
  "10": { name:"三国",  bias:{ inScore:1.00, dashScore:1.02, sashi:1.03, makuri:1.00, mZashi:1.02 }, stBias:{ in:+0.001, dash:-0.001 } },
  "11": { name:"びわこ",bias:{ inScore:0.99, dashScore:1.02, sashi:1.04, makuri:0.99, mZashi:1.02 }, stBias:{ in:+0.002, dash:-0.001 } },
  "12": { name:"住之江",bias:{ inScore:1.04, dashScore:0.99, sashi:1.00, makuri:1.01, mZashi:0.99 }, stBias:{ in:-0.003, dash:-0.001 } },
  "13": { name:"尼崎",  bias:{ inScore:1.02, dashScore:1.00, sashi:1.01, makuri:1.00, mZashi:1.00 }, stBias:{ in:-0.001, dash:0 } },
  "14": { name:"鳴門",  bias:{ inScore:1.00, dashScore:1.03, sashi:1.00, makuri:1.03, mZashi:1.03 }, stBias:{ in:0, dash:-0.003 } },
  "15": { name:"丸亀",  bias:{ inScore:1.03, dashScore:1.00, sashi:1.00, makuri:1.01, mZashi:0.99 }, stBias:{ in:-0.002, dash:-0.001 } },
  "16": { name:"児島",  bias:{ inScore:1.00, dashScore:1.02, sashi:1.03, makuri:1.01, mZashi:1.02 }, stBias:{ in:+0.001, dash:-0.001 } },
  "17": { name:"宮島",  bias:{ inScore:1.00, dashScore:1.02, sashi:1.01, makuri:1.02, mZashi:1.01 }, stBias:{ in:0, dash:-0.002 } },
  "18": { name:"徳山",  bias:{ inScore:1.04, dashScore:0.99, sashi:0.99, makuri:1.00, mZashi:0.98 }, stBias:{ in:-0.003, dash:0 } },
  "19": { name:"下関",  bias:{ inScore:1.03, dashScore:1.00, sashi:1.00, makuri:1.00, mZashi:1.00 }, stBias:{ in:-0.002, dash:-0.001 } },
  "20": { name:"若松",  bias:{ inScore:1.01, dashScore:1.02, sashi:1.01, makuri:1.02, mZashi:1.01 }, stBias:{ in:-0.001, dash:-0.002 } },
  "21": { name:"芦屋",  bias:{ inScore:1.02, dashScore:1.00, sashi:1.02, makuri:0.99, mZashi:1.00 }, stBias:{ in:-0.001, dash:0 } },
  "22": { name:"福岡",  bias:{ inScore:1.01, dashScore:1.01, sashi:1.02, makuri:1.01, mZashi:1.02 }, stBias:{ in:0, dash:-0.001 } },
  "23": { name:"唐津",  bias:{ inScore:0.99, dashScore:1.02, sashi:1.03, makuri:1.01, mZashi:1.02 }, stBias:{ in:+0.001, dash:-0.001 } },
  "24": { name:"大村",  bias:{ inScore:1.06, dashScore:0.98, sashi:0.98, makuri:0.99, mZashi:0.97 }, stBias:{ in:-0.004, dash:0 } }
};

export function getVenueProfile(pid) {
  return VENUE_PROFILES[String(pid).padStart(2, "0")] ?? {
    name: "未知場",
    bias: { inScore: 1.0, dashScore: 1.0, sashi: 1.0, makuri: 1.0, mZashi: 1.0 },
    stBias: { in: 0, dash: 0 }
  };
}

/** ST補正（場特性） */
export function venueAdjustST(baseST, ctx = {}, laneCtx = {}) {
  const prof = getVenueProfile(ctx.pid);
  let st = num(baseST, 0.18);
  if (laneCtx.isIn)   st += prof.stBias.in  || 0;
  if (laneCtx.isDash) st += prof.stBias.dash|| 0;
  return round3(st); // クリップは上流/下流で実施
}

/** スコア補正（場特性） */
export function venueAdjustScore(baseScore, ctx = {}, laneCtx = {}, attackType = null) {
  const prof = getVenueProfile(ctx.pid);
  let s = num(baseScore, 0);
  if (laneCtx.isIn)   s *= prof.bias.inScore   ?? 1.0;
  if (laneCtx.isDash) s *= prof.bias.dashScore ?? 1.0;
  if (attackType === "sashi")       s *= prof.bias.sashi ?? 1.0;
  if (attackType === "makuri")      s *= prof.bias.makuri ?? 1.0;
  if (attackType === "makuriZashi") s *= prof.bias.mZashi ?? 1.0;
  return s;
}

/** シナリオ確率補正（場特性） */
export function venueAdjustScenarioProb(baseProb, ctx = {}, { attackType, lanes = [] } = {}) {
  const prof = getVenueProfile(ctx.pid);
  let p = num(baseProb, 0);
  switch (attackType) {
    case "inNige":     p *= prof.bias.inScore ?? 1.0; break;
    case "sashi":      p *= prof.bias.sashi   ?? 1.0; break;
    case "makuri":     p *= prof.bias.makuri  ?? 1.0; break;
    case "makuriZashi":p *= prof.bias.mZashi  ?? 1.0; break;
    default: break;
  }
  if (lanes.every(l => l >= 4)) p *= prof.bias.dashScore ?? 1.0;
  return p;
}

// ---------- helpers ----------
const num    = (v, d=0) => (Number.isFinite(+v) ? +v : d);
const round3 = (x) => Math.round(x * 1000) / 1000;

/**
 * 追加：デフォルトエクスポート本体
 * レースオブジェクト全体に場補正を適用して返す
 */
function venueAdjust(race) {
  if (!race) return race;

  const pid = race?.meta?.pid || race?.pid || race?.venueId || "";
  const attackType = race.attackType || null;

  const out = JSON.parse(JSON.stringify(race));
  if (Array.isArray(out.ranking)) {
    out.ranking = out.ranking.map(p => {
      const lane = Number(p.lane ?? p.startCourse ?? 0);
      const startCourse = Number(p.startCourse ?? lane);
      const laneCtx = {
        lane,
        startCourse,
        isIn: lane === 1,
        isDash: startCourse >= 4,
        tilt: p.tilt
      };
      const ctx = { pid };

      const st = p.predictedST != null
        ? venueAdjustST(p.predictedST, ctx, laneCtx)
        : p.predictedST;
      const score = p.score != null
        ? venueAdjustScore(p.score, ctx, laneCtx, attackType)
        : p.score;

      return { ...p, ...(st!=null?{predictedST:st}:{}) , ...(score!=null?{score}:{}) };
    });
  }

  return out;
}

export default venueAdjust;