// Mobile Signal Bot V1 — Cloudflare Worker
// Public market data: MEXC Futures contract API
// Push notifications: ntfy
//
// Required Worker secret:
// NTFY_TOPIC
//
// Optional environment variables:
// MIN_SCORE (default 7)
// CRON_MINUTES is configured in wrangler.jsonc.
//
// KV binding:
// SIGNAL_STATE

const CRYPTO_BASES = [
  "BTC","ETH","BNB","SOL","XRP","DOGE","ADA","AVAX","LINK","DOT",
  "SUI","LTC","BCH","UNI","NEAR","APT","FIL","ATOM","ARB","OP",
  "INJ","SEI","TIA","PEPE","WIF","ZEC","JELLYJELLY","TAO","RENDER","ICP"
];

const COMMODITIES = [
  { base: "GOLD(XAU)", label: "GOLD" },
  { base: "SILVER(XAG)", label: "SILVER" },
  { base: "OIL(BRENT)", label: "BRENT" }
];

const MEXC = "https://contract.mexc.com";
const DEFAULT_MIN_SCORE = 7;

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === "/") {
      return new Response(JSON.stringify({
        name: "Mobile Signal Bot V1",
        status: "online",
        message:message: "Scanner runs on Cloudflare Cron. Signals are sent to ntfy.",
        watchlist: { crypto: CRYPTO_BASES, commodities: COMMODITIES.map(x => x.label) }
      }, null, 2), { headers: { "content-type": "application/json" }});
    }
   if (url.pathname === "/test") {
  if (!env.NTFY_TOPIC) {
    return new Response("Missing NTFY_TOPIC secret", { status: 500 });
  }

  await sendNtfy(
    env,
    "🧪 Mobile Signal Bot V1 test\nntfy alerts are connected."
  );

  return new Response("ntfy test notification sent.");
}
    }
    if (url.pathname === "/scan") {
      const result = await runScan(env);
      return new Response(JSON.stringify(result, null, 2), {
        headers: { "content-type": "application/json" }
      });
    }
    return new Response("Not found", { status: 404 });
  },

  async scheduled(controller, env, ctx) {
    ctx.waitUntil(runScan(env));
  }
};

async function runScan(env) {
  const minScore = Number(env.MIN_SCORE || DEFAULT_MIN_SCORE);

  const contractsResp = await fetch(`${MEXC}/api/v1/contract/detail`);
  if (!contractsResp.ok) throw new Error(`MEXC contract detail HTTP ${contractsResp.status}`);
  const contractsJson = await contractsResp.json();
  const contracts = contractsJson?.data || [];

  const wanted = [];
  for (const base of CRYPTO_BASES) {
    const c = contracts.find(x =>
      x.settleCoin === "USDT" &&
      x.baseCoin === base &&
      x.symbol?.endsWith("_USDT")
    );
    if (c) wanted.push({ symbol: c.symbol, label: base, category: "CRYPTO" });
  }
  for (const item of COMMODITIES) {
    const c = contracts.find(x =>
      x.settleCoin === "USDT" &&
      x.baseCoin === item.base &&
      x.symbol?.endsWith("_USDT")
    );
    if (c) wanted.push({ symbol: c.symbol, label: item.label, category: "COMMODITY" });
  }

  const missing = [
    ...CRYPTO_BASES.filter(base => !wanted.some(x => x.label === base)),
    ...COMMODITIES.filter(x => !wanted.some(y => y.label === x.label)).map(x => x.label)
  ];

  // Scan concurrently. MEXC's public endpoint is rate-limited; a short pause
  // between batches keeps requests conservative.
  const results = [];
  for (let i = 0; i < wanted.length; i += 10) {
    const batch = wanted.slice(i, i + 10);
    const batchResults = await Promise.all(batch.map(x => scanSymbol(x, env, minScore)));
    results.push(...batchResults);
    if (i + 10 < wanted.length) await sleep(250);
  }

  const alerts = results.filter(x => x.signal === "BUY" || x.signal === "SELL");
  const sent = [];

  for (const s of alerts) {
    const key = `state:${s.symbol}`;
    const previous = env.SIGNAL_STATE ? await env.SIGNAL_STATE.get(key) : null;

    // Only alert on a new direction. If the same direction persists, stay silent.
    if (previous !== s.signal) {
      await sendTelegram(env, formatAlert(s));
      if (env.SIGNAL_STATE) await env.SIGNAL_STATE.put(key, s.signal);
      sent.push(s.symbol);
    }
  }

  return {
    scanned: wanted.length,
    missing,
    signals: alerts,
    sent,
    timestamp: new Date().toISOString()
  };
}

async function scanSymbol(item, env, minScore) {
  try {
    const [m15, h1] = await Promise.all([
      getKlines(item.symbol, "Min15", 120),
      getKlines(item.symbol, "Min60", 120)
    ]);

    if (m15.length < 80 || h1.length < 80) {
      return { ...item, signal: "NO SIGNAL", reason: "Not enough candles" };
    }

    const a15 = analyze(m15);
    const a1h = analyze(h1);

    const longScore =
      (a15.close > a15.ema200 ? 1 : 0) +
      (a15.ema20 > a15.ema50 ? 1 : 0) +
      (a1h.close > a1h.ema200 ? 1 : 0) +
      (a1h.ema20 > a1h.ema50 ? 1 : 0) +
      (a15.rsi >= 52 && a15.rsi <= 72 ? 1 : 0) +
      (a15.macd > a15.macdSignal && a15.hist > 0 ? 1 : 0) +
      (a1h.hist > 0 ? 1 : 0) +
      (a15.volume > a15.volumeSma * 1.05 ? 1 : 0) +
      ((a15.breakUp || a15.reclaimUp) ? 1 : 0) +
      (a15.close > a15.open ? 1 : 0);

    const shortScore =
      (a15.close < a15.ema200 ? 1 : 0) +
      (a15.ema20 < a15.ema50 ? 1 : 0) +
      (a1h.close < a1h.ema200 ? 1 : 0) +
      (a1h.ema20 < a1h.ema50 ? 1 : 0) +
      (a15.rsi >= 28 && a15.rsi <= 48 ? 1 : 0) +
      (a15.macd < a15.macdSignal && a15.hist < 0 ? 1 : 0) +
      (a1h.hist < 0 ? 1 : 0) +
      (a15.volume > a15.volumeSma * 1.05 ? 1 : 0) +
      ((a15.breakDown || a15.reclaimDown) ? 1 : 0) +
      (a15.close < a15.open ? 1 : 0);

    const notChasing =
      Math.abs(a15.close - a15.ema20) / Math.max(a15.atr, 1e-12) <= 2.0 &&
      a15.bodyPct <= 0.85;

    let signal = "NO SIGNAL";
    if (notChasing && longScore >= minScore && longScore > shortScore) signal = "BUY";
    if (notChasing && shortScore >= minScore && shortScore > longScore) signal = "SELL";

    const entry = a15.close;
    const risk = a15.atr * 1.5;
    const sl = signal === "BUY" ? entry - risk : signal === "SELL" ? entry + risk : null;
    const tp1 = signal === "BUY" ? entry + risk : signal === "SELL" ? entry - risk : null;
    const tp2 = signal === "BUY" ? entry + risk * 2 : signal === "SELL" ? entry - risk * 2 : null;
    const tp3 = signal === "BUY" ? entry + risk * 3 : signal === "SELL" ? entry - risk * 3 : null;

    return {
      ...item, signal,
      score: signal === "BUY" ? longScore : signal === "SELL" ? shortScore : Math.max(longScore, shortScore),
      longScore, shortScore,
      entry, sl, tp1, tp2, tp3,
      rsi: a15.rsi,
      htfRsi: a1h.rsi,
      candleTime: a15.time
    };
  } catch (e) {
    return { ...item, signal: "NO SIGNAL", reason: String(e?.message || e) };
  }
}

async function getKlines(symbol, interval, limit) {
  const url = `${MEXC}/api/v1/contract/kline/${encodeURIComponent(symbol)}?interval=${interval}&limit=${limit}`;
  const r = await fetch(url);
  if (!r.ok) throw new Error(`Kline ${symbol} ${interval} HTTP ${r.status}`);
  const j = await r.json();
  const d = j?.data;
  if (!d?.time?.length) throw new Error(`No kline data ${symbol} ${interval}`);

  const out = [];
  for (let i = 0; i < d.time.length; i++) {
    out.push({
      time: Number(d.time[i]),
      open: Number(d.open[i]),
      close: Number(d.close[i]),
      high: Number(d.high[i]),
      low: Number(d.low[i]),
      volume: Number(d.vol[i])
    });
  }
  // The final candle may still be forming. Exclude it to prevent intrabar/repainting signals.
  return out.slice(0, -1);
}

function analyze(c) {
  const closes = c.map(x => x.close);
  const highs = c.map(x => x.high);
  const lows = c.map(x => x.low);
  const volumes = c.map(x => x.volume);

  const ema20 = ema(closes, 20).at(-1);
  const ema50 = ema(closes, 50).at(-1);
  const ema200 = ema(closes, 200).at(-1);
  const rsiV = rsi(closes, 14).at(-1);
  const mac = macd(closes, 12, 26, 9);
  const atrV = atr(c, 14);
  const volSma = sma(volumes, 20);

  const last = c.at(-1);
  const previous10High = Math.max(...highs.slice(-11, -1));
  const previous10Low = Math.min(...lows.slice(-11, -1));
  const breakUp = last.close > previous10High;
  const breakDown = last.close < previous10Low;
  const reclaimUp = last.low <= ema20 && last.close > ema20;
  const reclaimDown = last.high >= ema20 && last.close < ema20;

  const range = Math.max(last.high - last.low, Number.EPSILON);
  const bodyPct = Math.abs(last.close - last.open) / range;

  return {
    ...last,
    ema20, ema50, ema200,
    rsi: rsiV,
    macd: mac.line,
    macdSignal: mac.signal,
    hist: mac.hist,
    atr: atrV,
    volumeSma: volSma,
    breakUp, breakDown, reclaimUp, reclaimDown, bodyPct
  };
}

function sma(a, n) {
  if (a.length < n) return a.at(-1);
  let s = 0;
  for (let i = a.length - n; i < a.length; i++) s += a[i];
  return s / n;
}

function ema(a, n) {
  const out = [];
  const k = 2 / (n + 1);
  let e = a.slice(0, n).reduce((x, y) => x + y, 0) / n;
  for (let i = 0; i < n - 1; i++) out.push(NaN);
  out.push(e);
  for (let i = n; i < a.length; i++) {
    e = a[i] * k + e * (1 - k);
    out.push(e);
  }
  return out;
}

function rsi(a, n) {
  const out = Array(a.length).fill(NaN);
  if (a.length <= n) return out;
  let gain = 0, loss = 0;
  for (let i = 1; i <= n; i++) {
    const d = a[i] - a[i - 1];
    if (d >= 0) gain += d; else loss -= d;
  }
  gain /= n; loss /= n;
  out[n] = loss === 0 ? 100 : 100 - (100 / (1 + gain / loss));
  for (let i = n + 1; i < a.length; i++) {
    const d = a[i] - a[i - 1];
    const g = Math.max(d, 0);
    const l = Math.max(-d, 0);
    gain = (gain * (n - 1) + g) / n;
    loss = (loss * (n - 1) + l) / n;
    out[i] = loss === 0 ? 100 : 100 - (100 / (1 + gain / loss));
  }
  return out;
}

function macd(a, fast, slow, sig) {
  const ef = ema(a, fast);
  const es = ema(a, slow);
  const line = a.map((_, i) => ef[i] - es[i]);
  const clean = line.map(x => Number.isFinite(x) ? x : 0);
  const signalArr = ema(clean.slice(Math.max(0, slow - 1)), sig);
  const signal = signalArr.at(-1);
  const m = line.at(-1);
  return { line: m, signal, hist: m - signal };
}

function atr(c, n) {
  if (c.length < n + 1) return 0;
  const tr = [];
  for (let i = 1; i < c.length; i++) {
    tr.push(Math.max(
      c[i].high - c[i].low,
      Math.abs(c[i].high - c[i - 1].close),
      Math.abs(c[i].low - c[i - 1].close)
    ));
  }
  return sma(tr, n);
}

async function sendTelegram(env, text) {
  const token = env.TELEGRAM_BOT_TOKEN;
  const chatId = env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) throw new Error("Telegram secrets are not configured");
  const r = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      chat_id: chatId,
      text,
      disable_web_page_preview: true
    })
  });
  if (!r.ok) throw new Error(`Telegram HTTP ${r.status}`);
}

function formatAlert(s) {
  const f = n => n == null ? "—" : Number(n).toPrecision(8);
  return `${s.signal === "BUY" ? "🟢" : "🔴"} ${s.signal} — ${s.label}
15m + 1H confirmed
Score: ${s.score}/10

Entry: ${f(s.entry)}
SL: ${f(s.sl)}
TP1: ${f(s.tp1)}
TP2: ${f(s.tp2)}
TP3: ${f(s.tp3)}

15m RSI: ${s.rsi?.toFixed(1)}
1H RSI: ${s.htfRsi?.toFixed(1)}

Strict bot: NO AUTO-TRADE`;
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}
