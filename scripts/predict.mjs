// ===== 入力の解決（CLI引数 or 環境変数 + フォールバック）=====
function to2(s){ return String(s).padStart(2, "0"); }

// 日付ディレクトリ（YYYYMMDD）を新しい順で列挙
function listDateDirsDesc(baseDir) {
  if (!fs.existsSync(baseDir)) return [];
  return fs.readdirSync(baseDir, { withFileTypes: true })
    .filter(d => d.isDirectory() && /^\d{8}$/.test(d.name))
    .map(d => d.name)
    .sort((a, b) => b.localeCompare(a)); // 文字列でOK（YYYYMMDD）
}

function resolveInputPath() {
  // 1) CLI 引数優先
  const argPath = process.argv[2];
  if (argPath) return path.resolve(argPath);

  // 2) env から推定（DATE が today/未指定なら最新日付を自動採用）
  const baseDir = path.join(__dirname, "..", "public", "integrated", "v1");
  const pid  = to2(process.env.PID || "04");
  const race = ((process.env.RACE || "1R").toUpperCase().replace(/[^\d]/g, "") + "R");

  const rawDate = (process.env.DATE || "today").replace(/-/g, "");
  const datesDesc = listDateDirsDesc(baseDir);

  // 探索候補の日付リストを用意
  let candidates;
  if (rawDate !== "today" && /^\d{8}$/.test(rawDate)) {
    // 指定日付を最優先 → 見つからなければ他の日にフォールバック
    candidates = [rawDate, ...datesDesc.filter(d => d !== rawDate)];
  } else {
    // today/未指定なら最新から順に
    candidates = datesDesc;
  }

  for (const d of candidates) {
    const p = path.join(baseDir, d, pid, `${race}.json`);
    if (fs.existsSync(p)) {
      if (rawDate !== d && rawDate !== "today") {
        console.warn(`[predict] fallback: DATE=${rawDate} が見つからず最新 ${d} を使用します`);
      } else if (rawDate === "today") {
        console.warn(`[predict] DATE=today -> 最新 ${d} を使用します`);
      }
      return p;
    }
  }

  // どれも無ければ丁寧にエラー
  const tried = candidates.slice(0, 5).join(", ") + (candidates.length > 5 ? ", ..." : "");
  throw new Error(
    `[predict] integrated json not found under ${baseDir}/<DATE>/${pid}/${race}. ` +
    `tried dates: [${tried}]`
  );
}