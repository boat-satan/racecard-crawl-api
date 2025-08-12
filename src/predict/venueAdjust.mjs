/**
 * 場別特性補正
 * - ST補正：その場で出やすい ST 傾向を微調整
 * - スコア補正：イン/差し/まくり寄りなどを倍率で反映
 * - シナリオ重み補正：決まり手傾向をシナリオ確率に寄与
 *
 * 使い方：
 *  const ctx = { pid:"04", windSpeed, waveHeight };
 *  const laneCtx = { lane, isIn: lane===1, isDash: lane>=4, startCourse, tilt };
 *  st = venueAdjustST(baseST, ctx, laneCtx);
 *  score = venueAdjustScore(baseScore, ctx, laneCtx);
 *  prob = venueAdjustScenarioProb(baseProb, ctx, { attackType, lanes:[...] });
 */

// --- 場プロファイル（初期版・安全サイド） ---
// weight系は倍率（1.00基準で±数％）
// stBias は秒の微調整（±0.002〜0.01程度）
const VENUE_PROFILES = {
  // 01 桐生：ナイター追い風でインやや強め・差しも決まる
  "01": { name: "桐生",
    bias: {
      inScore: 1.03, dashScore: 0.99, sashi: 1.03, makuri: 0.98, mZashi: 1.01
    },
    stBias: { in: -0.002, dash: -0.001 } // わずかに速め
  },

  // 02 戸田：狭水面・まくりは難度高、差し寄り・イン信頼は全国比やや低め
  "02": { name: "戸田",
    bias: {
      inScore: 0.98, dashScore: 1.02, sashi: 1.05, makuri: 0.97, mZashi: 1.02
    },
    stBias: { in: +0.002, dash: -0.002 }
  },

  // 03 江戸川：風影響大・波で握りづらい→差し寄り、イン過信禁物
  "03": { name: "江戸川",
    bias: {
      inScore: 0.97, dashScore: 1.03, sashi: 1.05, makuri: 0.96, mZashi: 1.02
    },
    stBias: { in: +0.003, dash: 0 }
  },

  // 04 平和島：狭水面・2,3優遇、ダッシュの攻めは通る時は通る
  "04": { name: "平和島",
    bias: {
      inScore: 0.99, dashScore: 1.02, sashi: 1.03, makuri: 1.01, mZashi: 1.03
    },
    stBias: { in: +0.001, dash: -0.002 }
  },

  // 05 多摩川：直線系伸び活きやすい、スピード戦寄り
  "05": { name: "多摩川",
    bias: {
      inScore: 1.01, dashScore: 1.02, sashi: 1.00, makuri: 1.03, mZashi: 1.02
    },
    stBias: { in: -0.001, dash: -0.002 }
  },

  // 06 浜名湖：広め・スピード戦、向かい風でダッシュ有利化
  "06": { name: "浜名湖",
    bias: {
      inScore: 1.00, dashScore: 1.02, sashi: 1.01, makuri: 1.02, mZashi: 1.01
    },
    stBias: { in: 0, dash: -0.002 }
  },

  // 07 蒲郡：ナイター・イン強めだが角の一撃も
  "07": { name: "蒲郡",
    bias: {
      inScore: 1.03, dashScore: 1.01, sashi: 1.01, makuri: 1.02, mZashi: 1.00
    },
    stBias: { in: -0.002, dash: -0.001 }
  },

  // 08 常滑：平均的。風で傾くことあり
  "08": { name: "常滑",
    bias: {
      inScore: 1.01, dashScore: 1.00, sashi: 1.00, makuri: 1.01, mZashi: 1.00
    },
    stBias: { in: -0.001, dash: -0.001 }
  },

  // 09 津：追い風でイン強・向かい風で外
  "09": { name: "津",
    bias: {
      inScore: 1.02, dashScore: 1.01, sashi: 1.00, makuri: 1.02, mZashi: 1.00
    },
    stBias: { in: -0.002, dash: -0.001 }
  },

  // 10 三国：向かい風でダッシュ強め・差し有効
  "10": { name: "三国",
    bias: {
      inScore: 1.00, dashScore: 1.02, sashi: 1.03, makuri: 1.00, mZashi: 1.02
    },
    stBias: { in: +0.001, dash: -0.001 }
  },

  // 11 びわこ：風の影響大。差し寄り、外伸び注意
  "11": { name: "びわこ",
    bias: {
      inScore: 0.99, dashScore: 1.02, sashi: 1.04, makuri: 0.99, mZashi: 1.02
    },
    stBias: { in: +0.002, dash: -0.001 }
  },

  // 12 住之江：ナイター・イン強い
  "12": { name: "住之江",
    bias: {
      inScore: 1.04, dashScore: 0.99, sashi: 1.00, makuri: 1.01, mZashi: 0.99
    },
    stBias: { in: -0.003, dash: -0.001 }
  },

  // 13 尼崎：平均〜ややイン、風でブレ
  "13": { name: "尼崎",
    bias: {
      inScore: 1.02, dashScore: 1.00, sashi: 1.01, makuri: 1.00, mZashi: 1.00
    },
    stBias: { in: -0.001, dash: 0 }
  },

  // 14 鳴門：ダッシュ強め、まくり差しも成立
  "14": { name: "鳴門",
    bias: {
      inScore: 1.00, dashScore: 1.03, sashi: 1.00, makuri: 1.03, mZashi: 1.03
    },
    stBias: { in: 0, dash: -0.003 }
  },

  // 15 丸亀：ナイター・イン強め
  "15": { name: "丸亀",
    bias: {
      inScore: 1.03, dashScore: 1.00, sashi: 1.00, makuri: 1.01, mZashi: 0.99
    },
    stBias: { in: -0.002, dash: -0.001 }
  },

  // 16 児島：差し寄り、外の一撃も
  "16": { name: "児島",
    bias: {
      inScore: 1.00, dashScore: 1.02, sashi: 1.03, makuri: 1.01, mZashi: 1.02
    },
    stBias: { in: +0.001, dash: -0.001 }
  },

  // 17 宮島：スピード戦、ダッシュ寄り
  "17": { name: "宮島",
    bias: {
      inScore: 1.00, dashScore: 1.02, sashi: 1.01, makuri: 1.02, mZashi: 1.01
    },
    stBias: { in: 0, dash: -0.002 }
  },

  // 18 徳山：イン強い
  "18": { name: "徳山",
    bias: {
      inScore: 1.04, dashScore: 0.99, sashi: 0.99, makuri: 1.00, mZashi: 0.98
    },
    stBias: { in: -0.003, dash: 0 }
  },

  // 19 下関：ナイター・イン強め
  "19": { name: "下関",
    bias: {
      inScore: 1.03, dashScore: 1.00, sashi: 1.00, makuri: 1.00, mZashi: 1.00
    },
    stBias: { in: -0.002, dash: -0.001 }
  },

  // 20 若松：ナイター・ダッシュ一撃注意
  "20": { name: "若松",
    bias: {
      inScore: 1.01, dashScore: 1.02, sashi: 1.01, makuri: 1.02, mZashi: 1.01
    },
    stBias: { in: -0.001, dash: -0.002 }
  },

  // 21 芦屋：インやや強、差し寄り
  "21": { name: "芦屋",
    bias: {
      inScore: 1.02, dashScore: 1.00, sashi: 1.02, makuri: 0.99, mZashi: 1.00
    },
    stBias: { in: -0.001, dash: 0 }
  },

  // 22 福岡：広め・差しもまくり差しも
  "22": { name: "福岡",
    bias: {
      inScore: 1.01, dashScore: 1.01, sashi: 1.02, makuri: 1.01, mZashi: 1.02
    },
    stBias: { in: 0, dash: -0.001 }
  },

  // 23 唐津：風で外有利化、差しも決まる
  "23": { name: "唐津",
    bias: {
      inScore: 0.99, dashScore: 1.02, sashi: 1.03, makuri: 1.01, mZashi: 1.02
    },
    stBias: { in: +0.001, dash: -0.001 }
  },

  // 24 大村：イン最強クラス
  "24": { name: "大村",
    bias: {
      inScore: 1.06, dashScore: 0.98, sashi: 0.98, makuri: 0.99, mZashi: 0.97
    },
    stBias: { in: -0.004, dash: 0 }
  }
};

export function getVenueProfile(pid) {
  return VENUE_PROFILES[String(pid).padStart(2, "0")] ?? {
    name: "未知場",
    bias: { inScore: 1.0, dashScore: 1.0, sashi: 1.0, makuri: 1.0, mZashi: 1.0 },
    stBias: { in: 0, dash: 0 }
  };
}

/**
 * ST補正（場特性）
 */
export function venueAdjustST(baseST, ctx = {}, laneCtx = {}) {
  const prof = getVenueProfile(ctx.pid);
  let st = num(baseST, 0.18);

  if (laneCtx.isIn) st += prof.stBias.in || 0;
  if (laneCtx.isDash) st += prof.stBias.dash || 0;

  // クリップは環境側で最終実施する前提（ここでは微調整だけ）
  return round3(st);
}

/**
 * スコア補正（場特性）
 * - イン/ダッシュで倍率
 * - 攻め手タイプ（差し/まくり/まくり差し）で倍率（必要に応じて呼ぶ）
 */
export function venueAdjustScore(baseScore, ctx = {}, laneCtx = {}, attackType = null) {
  const prof = getVenueProfile(ctx.pid);
  let s = num(baseScore, 0);

  if (laneCtx.isIn) s *= prof.bias.inScore ?? 1.0;
  if (laneCtx.isDash) s *= prof.bias.dashScore ?? 1.0;

  // 攻め手タイプ別の寄与（null のときは無視）
  if (attackType === "sashi")        s *= prof.bias.sashi ?? 1.0;
  if (attackType === "makuri")       s *= prof.bias.makuri ?? 1.0;
  if (attackType === "makuriZashi")  s *= prof.bias.mZashi ?? 1.0;

  return s;
}

/**
 * シナリオ確率補正（場特性）
 * attackType: "inNige" | "sashi" | "makuri" | "makuriZashi" | など
 * lanes: そのシナリオに強く関与する枠の配列（例: [1,2]）
 */
export function venueAdjustScenarioProb(baseProb, ctx = {}, { attackType, lanes = [] } = {}) {
  const prof = getVenueProfile(ctx.pid);
  let p = num(baseProb, 0);

  switch (attackType) {
    case "inNige":
      // イン寄与
      p *= prof.bias.inScore ?? 1.0;
      break;
    case "sashi":
      p *= prof.bias.sashi ?? 1.0;
      break;
    case "makuri":
      p *= prof.bias.makuri ?? 1.0;
      break;
    case "makuriZashi":
      p *= prof.bias.mZashi ?? 1.0;
      break;
    default:
      // 何もしない
      break;
  }

  // 角(4)や外(5,6)しか関係しないシナリオは dashScore も少し乗せる
  if (lanes.every(l => l >= 4)) p *= prof.bias.dashScore ?? 1.0;

  return p;
}

// ------ helpers ------
const num = (v, d=0) => (Number.isFinite(+v) ? +v : d);
const round3 = (x) => Math.round(x*1000)/1000;