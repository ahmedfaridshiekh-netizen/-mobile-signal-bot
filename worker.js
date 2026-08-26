// Mobile Signal Bot V2 — Cloudflare Worker
// Subrequest-safe rotating scanner
// Notifications: ntfy
//
// Required secret:
// NTFY_TOPIC
//
// Optional:
// MIN_SCORE (default 7)
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

// 12 symbols × 2 timeframe requests = 24 MEXC requests.
// This keeps the invocation safely below Cloudflare's subrequest limit.
const SCAN_BATCH_SIZE = 12;

const STATIC_SYMBOLS = Object.fromEntries(
  CRYPTO_BASES.map(base => [base, `${base}_USDT`])
);

// Commodities are optional because their MEXC contract names can change.
const COMMODITY_CANDIDATES = {
  GOLD: ["GOLD_USDT", "XAU_USDT"],
  SILVER: ["SILVER_USDT", "XAG_USDT"],
  BRENT: ["BRENT_USDT", "OIL_USDT"]
};

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return new Response(JSON.stringify({
        name: "Mobile Signal Bot V2",
        status: "online",
        message: "Rotating scanner runs on Cloudflare Cron. Signals are sent to ntfy.",
        batchSize: SCAN_BATCH_SIZE,
        watchlist: {
          crypto: CRYPTO_BASES,
          commodities: COMMODITIES.map(x => x.label)
        }
      }, null, 2), {
        headers: { "content-type": "application/json" }
      });
    }

    if (url.pathname === "/test") {
      const ok = await sendNtfy(
        env,
        "🧪 Mobile Signal Bot V2 test\nntfy alerts are connected."
      );

      return new Response(
        ok ? "ntfy test notification sent." : "ntfy test failed.",
        { status: ok ? 200 : 502 }
      );
    }

    if (url.pathname === "/scan") {
      try {
        const result = await runScan(env);

        return new Response(JSON.stringify(result, null, 2), {
          headers: { "content-type": "application/json" }
        });
      } catch (e) {
        return new Response(JSON.stringify({
          ok: false,
          error: String(e?.message || e)
        }, null, 2), {
          status: 500,
          headers: { "content-type": "application/json" }
        });
      }
    }

    return new Response("Not found", { status: 404 });
  },

  async scheduled(controller, env, ctx) {
    ctx.waitUntil(
      runScan(env).catch(e =>
        console.error("CRON scan failed:", e)
      )
    );
  }
};

function buildWatchlist() {
  const wanted = [];

  for (const base of CRYPTO_BASES) {
    wanted.push({
      symbol: STATIC_SYMBOLS[base],
      label: base,
      category: "CRYPTO"
    });
  }

  for (const item of COMMODITIES) {
    const candidates = COMMODITY_CANDIDATES[item.label] || [];

    if (candidates.length) {
      wanted.push({
        symbol: candidates[0],
        label: item.label,
        category: "COMMODITY",
        candidates
      });
    }
  }

  return wanted;
}

async function getRotationIndex(env, total) {
  const slot = Math.floor(Date.now() / 300000);

  if (!env.SIGNAL_STATE) {
    return slot % total;
  }

  try {
    const raw = await env.SIGNAL_STATE.get("scan:cursor");
    const previous = Number(raw);

    const index = Number.isFinite(previous)
      ? previous
      : slot % total;

    await env.SIGNAL_STATE.put(
      "scan:cursor",
      String((index + SCAN_BATCH_SIZE) % total)
    );

    return index % total;
  } catch (e) {
    console.error(
      "KV cursor warning:",
      e?.message || e
    );

    return slot % total;
  }
}

function selectBatch(wanted, start) {
  const out = [];

  for (
    let i = 0;
    i < Math.min(SCAN_BATCH_SIZE, wanted.length);
    i++
  ) {
    out.push(
      wanted[(start + i) % wanted.length]
    );
  }

  return out;
}
async function runScan(env) {
  const minScore = Number(
    env.MIN_SCORE || DEFAULT_MIN_SCORE
  );

  const wanted = buildWatchlist();

  const start = await getRotationIndex(
    env,
    wanted.length
  );

  const batch = selectBatch(
    wanted,
    start
  );

  const results = [];

  // Process in groups of 6.
  // Each symbol makes exactly 2 MEXC requests:
  // 15m + 1H.
  for (let i = 0; i < batch.length; i += 6) {
    const chunk = batch.slice(i, i + 6);

    const chunkResults = await Promise.all(
      chunk.map(x =>
        scanSymbol(x, env, minScore)
      )
    );

    results.push(...chunkResults);

    if (i + 6 < batch.length) {
      await sleep(150);
    }
  }

  const alerts = results.filter(
    x =>
      x.signal === "BUY" ||
      x.signal === "SELL"
  );

  const sent = [];

  for (const s of alerts) {
    const key = `state:${s.symbol}`;

    let previous = null;

    if (env.SIGNAL_STATE) {
      try {
        previous =
          await env.SIGNAL_STATE.get(key);
      } catch (e) {
        console.error(
          `KV read warning ${s.symbol}:`,
          e?.message || e
        );
      }
    }

    // Send only when direction changes.
    if (previous !== s.signal) {
      const ok = await sendNtfy(
        env,
        formatAlert(s)
      );

      if (ok && env.SIGNAL_STATE) {
        try {
          await env.SIGNAL_STATE.put(
            key,
            s.signal
          );
        } catch (e) {
          console.error(
            `KV write warning ${s.symbol}:`,
            e?.message || e
          );
        }
      }

      if (ok) {
        sent.push(s.symbol);
      }
    }
  }

  return {
    ok: true,
    scanned: batch.length,
    totalWatchlist: wanted.length,
    rotationStart: start,
    nextRotationStart:
      (start + SCAN_BATCH_SIZE) %
      wanted.length,
    signals: alerts,
    sent,
    timestamp:
      new Date().toISOString()
  };
}


async function scanSymbol(
  item,
  env,
  minScore
) {
  try {
    const [m15, h1] =
      await Promise.all([
        getKlines(
          item.symbol,
          "Min15",
          120
        ),
        getKlines(
          item.symbol,
          "Min60",
          120
        )
      ]);

    if (
      m15.length < 80 ||
      h1.length < 80
    ) {
      return {
        ...item,
        signal: "NO SIGNAL",
        reason:
          "Not enough candles"
      };
    }

    const a15 = analyze(m15);
    const a1h = analyze(h1);

    const longScore =
      (a15.close > a15.ema200 ? 1 : 0) +
      (a15.ema20 > a15.ema50 ? 1 : 0) +
      (a1h.close > a1h.ema200 ? 1 : 0) +
      (a1h.ema20 > a1h.ema50 ? 1 : 0) +
      (
        a15.rsi >= 52 &&
        a15.rsi <= 72
          ? 1
          : 0
      ) +
      (
        a15.macd >
        a15.macdSignal &&
        a15.hist > 0
          ? 1
          : 0
      ) +
      (a1h.hist > 0 ? 1 : 0) +
      (
        a15.volume >
        a15.volumeSma * 1.05
          ? 1
          : 0
      ) +
      (
        a15.breakUp ||
        a15.reclaimUp
          ? 1
          : 0
      ) +
      (
        a15.close > a15.open
          ? 1
          : 0
      );

    const shortScore =
      (a15.close < a15.ema200 ? 1 : 0) +
      (a15.ema20 < a15.ema50 ? 1 : 0) +
      (a1h.close < a1h.ema200 ? 1 : 0) +
      (a1h.ema20 < a1h.ema50 ? 1 : 0) +
      (
        a15.rsi >= 28 &&
        a15.rsi <= 48
          ? 1
          : 0
      ) +
      (
        a15.macd <
        a15.macdSignal &&
        a15.hist < 0
          ? 1
          : 0
      ) +
      (a1h.hist < 0 ? 1 : 0) +
      (
        a15.volume >
        a15.volumeSma * 1.05
          ? 1
          : 0
      ) +
      (
        a15.breakDown ||
        a15.reclaimDown
          ? 1
          : 0
      ) +
      (
        a15.close < a15.open
          ? 1
          : 0
      );

    const notChasing =
      Math.abs(
        a15.close - a15.ema20
      ) /
      Math.max(
        a15.atr,
        1e-12
      ) <= 2.0 &&
      a15.bodyPct <= 0.85;

    let signal = "NO SIGNAL";

    if (
      notChasing &&
      longScore >= minScore &&
      longScore > shortScore
    ) {
      signal = "BUY";
    }

    if (
      notChasing &&
      shortScore >= minScore &&
      shortScore > longScore
    ) {
      signal = "SELL";
    }

    const entry = a15.close;

    const risk =
      a15.atr * 1.5;

    const sl =
      signal === "BUY"
        ? entry - risk
        : signal === "SELL"
          ? entry + risk
          : null;

    const tp1 =
      signal === "BUY"
        ? entry + risk
        : signal === "SELL"
          ? entry - risk
          : null;

    const tp2 =
      signal === "BUY"
        ? entry + risk * 2
        : signal === "SELL"
          ? entry - risk * 2
          : null;

    const tp3 =
      signal === "BUY"
        ? entry + risk * 3
        : signal === "SELL"
          ? entry - risk * 3
          : null;

    return {
      ...item,
      signal,

      score:
        signal === "BUY"
          ? longScore
          : signal === "SELL"
            ? shortScore
            : Math.max(
                longScore,
                shortScore
              ),

      longScore,
      shortScore,

      entry,
      sl,
      tp1,
      tp2,
      tp3,

      rsi: a15.rsi,
      htfRsi: a1h.rsi,
      candleTime: a15.time
    };

  } catch (e) {
    return {
      ...item,
      signal: "NO SIGNAL",
      reason:
        String(
          e?.message || e
        )
    };
  }
}
async function getKlines(symbol, interval, limit) {
  const url =
    `${MEXC}/api/v1/contract/kline/` +
    `${encodeURIComponent(symbol)}` +
    `?interval=${interval}&limit=${limit}`;

  const r = await fetch(url);

  if (!r.ok) {
    throw new Error(
      `Kline ${symbol} ${interval} HTTP ${r.status}`
    );
  }

  const j = await r.json();
  const d = j?.data;

  if (!d?.time?.length) {
    throw new Error(
      `No kline data ${symbol} ${interval}`
    );
  }

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

  // Ignore the currently forming candle.
  return out.slice(0, -1);
}

function analyze(c) {
  const closes = c.map(x => x.close);
  const highs = c.map(x => x.high);
  const lows = c.map(x => x.low);
  const volumes = c.map(x => x.volume);

  function ema(values, period) {
    const k = 2 / (period + 1);
    let e = values[0];

    for (let i = 1; i < values.length; i++) {
      e = values[i] * k + e * (1 - k);
    }

    return e;
  }

  function sma(values, period) {
    const a = values.slice(-period);
    return a.reduce((x, y) => x + y, 0) /
      Math.max(a.length, 1);
  }

  function atr() {
    const trs = [];

    for (let i = 1; i < c.length; i++) {
      trs.push(
        Math.max(
          highs[i] - lows[i],
          Math.abs(
            highs[i] - closes[i - 1]
          ),
          Math.abs(
            lows[i] - closes[i - 1]
          )
        )
      );
    }

    return sma(trs, 14);
  }

  function rsi(values, period = 14) {
    let gain = 0;
    let loss = 0;

    const start =
      Math.max(1, values.length - period);

    for (let i = start; i < values.length; i++) {
      const diff =
        values[i] - values[i - 1];

      if (diff > 0) {
        gain += diff;
      } else {
        loss -= diff;
      }
    }

    const count =
      Math.max(
        values.length - start,
        1
      );

    gain /= count;
    loss /= count;

    if (loss === 0) return 100;

    const rs = gain / loss;

    return 100 - (100 / (1 + rs));
  }

  const ema20 = ema(closes, 20);
  const ema50 = ema(closes, 50);
  const ema200 = ema(closes, 200);

  const fast = ema(closes, 12);
  const slow = ema(closes, 26);

  const macd = fast - slow;

  const macdValues = [];

  for (
    let i = 0;
    i < closes.length;
    i++
  ) {
    const f =
      ema(closes.slice(0, i + 1), 12);

    const s =
      ema(closes.slice(0, i + 1), 26);

    macdValues.push(f - s);
  }

  const macdSignal =
    ema(macdValues, 9);

  const hist =
    macd - macdSignal;

  const last =
    c[c.length - 1];

  const previous =
    c[c.length - 2];

  const recentHigh =
    Math.max(
      ...highs.slice(-21, -1)
    );

  const recentLow =
    Math.min(
      ...lows.slice(-21, -1)
    );

  const breakUp =
    last.close > recentHigh;

  const breakDown =
    last.close < recentLow;

  const reclaimUp =
    previous.close < ema20 &&
    last.close > ema20;

  const reclaimDown =
    previous.close > ema20 &&
    last.close < ema20;

  const body =
    Math.abs(
      last.close - last.open
    );

  const range =
    Math.max(
      last.high - last.low,
      1e-12
    );

  const bodyPct =
    body / range;

  return {
    time: last.time,
    open: last.open,
    close: last.close,
    high: last.high,
    low: last.low,

    ema20,
    ema50,
    ema200,

    rsi: rsi(closes, 14),

    macd,
    macdSignal,
    hist,

    atr: atr(),

    volume: last.volume,

    volumeSma:
      sma(volumes, 20),

    breakUp,
    breakDown,

    reclaimUp,
    reclaimDown,

    bodyPct
  };
}

async function sendNtfy(env, text) {
  const topic = env.NTFY_TOPIC;

  if (!topic) {
    console.error(
      "NTFY_TOPIC is not configured"
    );

    return false;
  }

  try {
    const r = await fetch(
      `https://ntfy.sh/${encodeURIComponent(topic)}`,
      {
        method: "POST",

        headers: {
          "content-type":
            "text/plain; charset=utf-8",

          "Title":
            "Mobile Signal Bot V2",

          "Priority":
            "high",

          "Tags":
            "chart_with_upwards_trend"
        },

        body: text
      }
    );

    if (!r.ok) {
      console.error(
        `ntfy HTTP ${r.status}`
      );

      return false;
    }

    return true;

  } catch (e) {
    console.error(
      "ntfy request failed:",
      e?.message || e
    );

    return false;
  }
}

function formatAlert(s) {
  const direction =
    s.signal === "BUY"
      ? "🟢 BUY"
      : "🔴 SELL";

  const lines = [
    `${direction} — ${s.label}`,
    "",
    `Score: ${s.score}/10`,
    `Entry: ${fmt(s.entry)}`,
    `SL: ${fmt(s.sl)}`,
    `TP1: ${fmt(s.tp1)}`,
    `TP2: ${fmt(s.tp2)}`,
    `TP3: ${fmt(s.tp3)}`,
    "",
    `15m RSI: ${fmt(s.rsi)}`,
    `1H RSI: ${fmt(s.htfRsi)}`,
    "",
    "Mobile Signal Bot V2"
  ];

  return lines.join("\n");
}

function fmt(value) {
  if (
    value === null ||
    value === undefined ||
    !Number.isFinite(Number(value))
  ) {
    return "N/A";
  }

  const n = Number(value);

  if (Math.abs(n) >= 1000) {
    return n.toFixed(2);
  }

  if (Math.abs(n) >= 1) {
    return n.toFixed(4);
  }

  return n.toFixed(8);
}

function sleep(ms) {
  return new Promise(
    resolve => setTimeout(resolve, ms)
  );
}
