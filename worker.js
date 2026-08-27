// Mobile Signal Bot V2 — VANTIQ Quality + 30m Heartbeat
// Cloudflare Worker / NTFY
// Quality-first scanner: fewer, stronger signals.

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

const DEFAULT_MIN_SCORE = 8;
const SCAN_BATCH_SIZE = 12;

const HEARTBEAT_MS = 30 * 60 * 1000;
const MAX_EMA20_DISTANCE_ATR = 1.25;
const MAX_BODY_PCT = 0.70;

const STATIC_SYMBOLS = Object.fromEntries(
  CRYPTO_BASES.map(base => [base, `${base}_USDT`])
);

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
        message: "VANTIQ-quality rotating scanner with ntfy heartbeat.",
        batchSize: SCAN_BATCH_SIZE,
        minScore: Number(env.MIN_SCORE || DEFAULT_MIN_SCORE)
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

        return new Response(
          JSON.stringify(result, null, 2),
          { headers: { "content-type": "application/json" } }
        );
      } catch (e) {
        return new Response(
          JSON.stringify({
            ok: false,
            error: String(e?.message || e)
          }),
          {
            status: 500,
            headers: { "content-type": "application/json" }
          }
        );
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
  const wanted = CRYPTO_BASES.map(base => ({
    symbol: STATIC_SYMBOLS[base],
    label: base,
    category: "CRYPTO"
  }));

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
    const prev = Number(raw);
    const idx = Number.isFinite(prev)
      ? prev
      : slot % total;

    await env.SIGNAL_STATE.put(
      "scan:cursor",
      String((idx + SCAN_BATCH_SIZE) % total)
    );

    return idx % total;
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

    // Same-direction signals are suppressed.
    // Only a genuine direction change creates a notification.
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

  // 30-minute alive heartbeat.
  let heartbeatSent = false;
  const now = Date.now();
  let lastHeartbeat = 0;

  if (env.SIGNAL_STATE) {
    try {
      lastHeartbeat =
        Number(
          await env.SIGNAL_STATE.get(
            "heartbeat:last"
          )
        ) || 0;
    } catch (e) {
      console.error(
        "KV heartbeat read warning:",
        e?.message || e
      );
    }
  }

  if (
    !sent.length &&
    now - lastHeartbeat >= HEARTBEAT_MS
  ) {
    heartbeatSent = await sendNtfy(
      env,
      "🔎 Bot is searching for the best setup for you... ❤️\n" +
      "No valid setup yet. Market is being monitored continuously."
    );

    if (
      heartbeatSent &&
      env.SIGNAL_STATE
    ) {
      try {
        await env.SIGNAL_STATE.put(
          "heartbeat:last",
          String(now)
        );
      } catch (e) {
        console.error(
          "KV heartbeat write warning:",
          e?.message || e
        );
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
    heartbeatSent,
    timestamp:
      new Date().toISOString()
  };
  async function scanSymbol(item, env, minScore) {
  const symbol = item.symbol;

  try {
    const candles15 = await fetchKlines(symbol, "Min15", 220);
    const candles1h = await fetchKlines(symbol, "Min60", 220);

    if (
      !candles15 ||
      candles15.length < 210 ||
      !candles1h ||
      candles1h.length < 210
    ) {
      return {
        symbol,
        signal: "NONE",
        score: 0,
        reason: "Insufficient market data"
      };
    }

    const c15 = normalizeCandles(candles15);
    const c1h = normalizeCandles(candles1h);

    // Work only with CLOSED candles.
    const x15 = c15.slice(0, -1);
    const x1h = c1h.slice(0, -1);

    const close15 = x15.map(x => x.close);
    const close1h = x1h.map(x => x.close);

    const ema20_15 = ema(close15, 20);
    const ema50_15 = ema(close15, 50);
    const ema200_15 = ema(close15, 200);

    const ema20_1h = ema(close1h, 20);
    const ema50_1h = ema(close1h, 50);
    const ema200_1h = ema(close1h, 200);

    const rsi15 = RSI(close15, 14);
    const rsi1h = RSI(close1h, 14);

    const macd15 = MACD(close15);
    const macd1h = MACD(close1h);

    const atr15 = ATR(x15, 14);

    const last = x15[x15.length - 1];
    const prev = x15[x15.length - 2];

    const price = last.close;
    const e20 = ema20_15[ema20_15.length - 1];
    const e50 = ema50_15[ema50_15.length - 1];
    const e200 = ema200_15[ema200_15.length - 1];

    const h20 = ema20_1h[ema20_1h.length - 1];
    const h50 = ema50_1h[ema50_1h.length - 1];
    const h200 = ema200_1h[ema200_1h.length - 1];

    const r15 = rsi15[rsi15.length - 1];
    const r1 = rsi1h[rsi1h.length - 1];

    const m15 = macd15[macd15.length - 1];
    const pm15 = macd15[macd15.length - 2];

    const m1 = macd1h[macd1h.length - 1];
    const pm1 = macd1h[macd1h.length - 2];

    const atr = atr15[atr15.length - 1];

    if (
      !Number.isFinite(price) ||
      !Number.isFinite(e20) ||
      !Number.isFinite(e50) ||
      !Number.isFinite(e200) ||
      !Number.isFinite(h20) ||
      !Number.isFinite(h50) ||
      !Number.isFinite(h200) ||
      !Number.isFinite(r15) ||
      !Number.isFinite(r1) ||
      !Number.isFinite(atr) ||
      atr <= 0
    ) {
      return {
        symbol,
        signal: "NONE",
        score: 0,
        reason: "Indicator data unavailable"
      };
    }

    const body = Math.abs(last.close - last.open);
    const range = Math.max(
      last.high - last.low,
      Number.EPSILON
    );

    const bodyPct = body / range;

    const distAtr =
      Math.abs(price - e20) / atr;

    const recent = x15.slice(-20);

    const resistance = Math.max(
      ...recent
        .slice(0, -1)
        .map(x => x.high)
    );

    const support = Math.min(
      ...recent
        .slice(0, -1)
        .map(x => x.low)
    );

    const volumeAvg =
      SMA(
        x15.slice(-21, -1).map(x => x.volume),
        20
      );

    const volumeOk =
      Number.isFinite(volumeAvg) &&
      last.volume > volumeAvg * 1.05;

    const bullish15 =
      e20 > e50 &&
      e50 > e200;

    const bearish15 =
      e20 < e50 &&
      e50 < e200;

    const bullish1h =
      h20 > h50 &&
      h50 > h200;

    const bearish1h =
      h20 < h50 &&
      h50 < h200;

    const macdBull =
      m15.hist > 0 &&
      m15.hist >= pm15.hist &&
      m1.hist > 0 &&
      m1.hist >= pm1.hist;

    const macdBear =
      m15.hist < 0 &&
      m15.hist <= pm15.hist &&
      m1.hist < 0 &&
      m1.hist <= pm1.hist;

    const pullbackLong =
      prev.low <= e20 * 1.003 &&
      last.close > e20 &&
      last.close > prev.high * 0.997;

    const pullbackShort =
      prev.high >= e20 * 0.997 &&
      last.close < e20 &&
      last.close < prev.low * 1.003;

    const reclaimLong =
      prev.close < e20 &&
      last.close > e20;

    const reclaimShort =
      prev.close > e20 &&
      last.close < e20;

    const breakoutLong =
      last.close > resistance;

    const breakoutShort =
      last.close < support;

    const nearResistance =
      (resistance - price) / atr;

    const nearSupport =
      (price - support) / atr;

    // Exhaustion protection:
    // Do NOT buy the top of an already stretched bullish move.
    const longExhausted =
      distAtr > MAX_EMA20_DISTANCE_ATR ||
      bodyPct > MAX_BODY_PCT ||
      r15 > 74 ||
      r1 > 76 ||
      nearResistance < 0.45;

    // Do NOT short the bottom of an already stretched bearish move.
    const shortExhausted =
      distAtr > MAX_EMA20_DISTANCE_ATR ||
      bodyPct > MAX_BODY_PCT ||
      r15 < 26 ||
      r1 < 24 ||
      nearSupport < 0.45;

    let longScore = 0;
    let shortScore = 0;

    if (bullish1h) longScore++;
    if (bullish15) longScore++;
    if (price > h200) longScore++;
    if (price > e200) longScore++;
    if (r15 >= 50 && r15 <= 68) longScore++;
    if (r1 >= 48 && r1 <= 70) longScore++;
    if (macdBull) longScore++;
    if (volumeOk) longScore++;
    if (pullbackLong || reclaimLong) longScore++;
    if (breakoutLong) longScore++;

    if (bearish1h) shortScore++;
    if (bearish15) shortScore++;
    if (price < h200) shortScore++;
    if (price < e200) shortScore++;
    if (r15 >= 32 && r15 <= 50) shortScore++;
    if (r1 >= 30 && r1 <= 52) shortScore++;
    if (macdBear) shortScore++;
    if (volumeOk) shortScore++;
    if (pullbackShort || reclaimShort) shortScore++;
    if (breakoutShort) shortScore++;

    // A valid signal needs trend + structure + momentum.
    // This prevents a raw indicator-count signal.
    const longStructure =
      bullish1h &&
      bullish15 &&
      (pullbackLong || reclaimLong || breakoutLong);

    const shortStructure =
      bearish1h &&
      bearish15 &&
      (pullbackShort || reclaimShort || breakoutShort);

    const longMomentum =
      macdBull &&
      r15 >= 50 &&
      r15 <= 70;

    const shortMomentum =
      macdBear &&
      r15 >= 30 &&
      r15 <= 50;

    let signal = "NONE";
    let score = 0;
    let reason = "No high-quality setup";
    let strategy = "WAIT";

    if (
      longScore >= minScore &&
      longStructure &&
      longMomentum &&
      !longExhausted
    ) {
      signal = "BUY";
      score = longScore;

      if (pullbackLong || reclaimLong) {
        strategy = "TREND_PULLBACK";
        reason =
          "Bullish HTF trend + 15m pullback/reclaim + momentum";
      } else {
        strategy = "BREAKOUT_RETEST";
        reason =
          "Bullish trend + confirmed breakout structure";
      }
    } else if (
      shortScore >= minScore &&
      shortStructure &&
      shortMomentum &&
      !shortExhausted
    ) {
      signal = "SELL";
      score = shortScore;

      if (pullbackShort || reclaimShort) {
        strategy = "TREND_PULLBACK";
        reason =
          "Bearish HTF trend + 15m pullback/reclaim + momentum";
      } else {
        strategy = "BREAKOUT_RETEST";
        reason =
          "Bearish trend + confirmed breakdown structure";
      }
    } else {
      // Explain why a seemingly bullish/bearish market
      // was rejected instead of producing a bad signal.
      if (longScore >= minScore && longExhausted) {
        reason =
          "Bullish but late/extended — waiting for pullback";
      } else if (shortScore >= minScore && shortExhausted) {
        reason =
          "Bearish but late/extended — waiting for pullback";
      } else if (
        longScore >= minScore ||
        shortScore >= minScore
      ) {
        reason =
          "Indicators align but structure/entry timing is incomplete";
      }
    }

    return {
      symbol,
      label: item.label,
      category: item.category,
      price,
      signal,
      score,
      strategy,
      reason,
      rsi15: round(r15, 2),
      rsi1h: round(r1, 2),
      atr: round(atr, 8),
      ema20: round(e20, 8),
      ema50: round(e50, 8),
      ema200: round(e200, 8),
      distanceAtr: round(distAtr, 2),
      bodyPct: round(bodyPct, 2),
      volumeOk,
      pullbackLong,
      pullbackShort,
      reclaimLong,
      reclaimShort,
      breakoutLong,
      breakoutShort,
      timestamp:
        new Date(last.time || Date.now()).toISOString()
    };

  } catch (e) {
    console.error(
      `SCAN ERROR ${symbol}:`,
      e?.message || e
    );

    return {
      symbol,
      signal: "NONE",
      score: 0,
      reason:
        `Scanner error: ${String(e?.message || e)}`
    };
  }
}

async function fetchKlines(symbol, interval, limit) {
  const url =
    `${MEXC}/api/v1/contract/kline/${encodeURIComponent(symbol)}` +
    `?interval=${encodeURIComponent(interval)}` +
    `&limit=${limit}`;

  let lastError = null;

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const r = await fetch(url, {
        method: "GET",
        headers: {
          "accept": "application/json"
        }
      });

      if (!r.ok) {
        throw new Error(
          `MEXC HTTP ${r.status}`
        );
      }

      const j = await r.json();

      if (!j || !Array.isArray(j.data)) {
        throw new Error(
          "Invalid MEXC kline response"
        );
      }

      return j.data;
    } catch (e) {
      lastError = e;

      if (attempt < 3) {
        await sleep(250 * attempt);
      }
    }
  }

  throw lastError || new Error(
    "MEXC kline request failed"
  );
}

function normalizeCandles(raw) {
  return raw
    .map(x => {
      if (Array.isArray(x)) {
        return {
          time: Number(x[0]),
          open: Number(x[1]),
          high: Number(x[2]),
          low: Number(x[3]),
          close: Number(x[4]),
          volume: Number(x[5])
        };
      }

      return {
        time: Number(x.time ?? x.t ?? x.timestamp),
        open: Number(x.open ?? x.o),
        high: Number(x.high ?? x.h),
        low: Number(x.low ?? x.l),
        close: Number(x.close ?? x.c),
        volume: Number(x.volume ?? x.v)
      };
    })
    .filter(
      x =>
        Number.isFinite(x.open) &&
        Number.isFinite(x.high) &&
        Number.isFinite(x.low) &&
        Number.isFinite(x.close)
    )
    .sort((a, b) => a.time - b.time);
}

function SMA(values, period) {
  if (values.length < period) return NaN;

  let sum = 0;

  for (
    let i = values.length - period;
    i < values.length;
    i++
  ) {
    sum += Number(values[i]) || 0;
  }

  return sum / period;
}

function ema(values, period) {
  if (values.length < period) return [];

  const out = [];
  const k = 2 / (period + 1);

  let prev = SMA(
    values.slice(0, period),
    period
  );

  out.push(prev);

  for (let i = period; i < values.length; i++) {
    prev =
      values[i] * k +
      prev * (1 - k);

    out.push(prev);
  }

  return out;
}

function RSI(values, period = 14) {
  if (values.length <= period) return [];

  let gains = 0;
  let losses = 0;

  for (let i = 1; i <= period; i++) {
    const d = values[i] - values[i - 1];

    if (d >= 0) gains += d;
    else losses -= d;
  }

  let avgGain = gains / period;
  let avgLoss = losses / period;

  const out = [];

  function value() {
    if (avgLoss === 0) return 100;

    const rs = avgGain / avgLoss;
    return 100 - 100 / (1 + rs);
  }

  out.push(value());

  for (
    let i = period + 1;
    i < values.length;
    i++
  ) {
    const d = values[i] - values[i - 1];
    const gain = Math.max(d, 0);
    const loss = Math.max(-d, 0);

    avgGain =
      (avgGain * (period - 1) + gain) /
      period;

    avgLoss =
      (avgLoss * (period - 1) + loss) /
      period;

    out.push(value());
  }

  return out;
}

function MACD(
  values,
  fastPeriod = 12,
  slowPeriod = 26,
  signalPeriod = 9
) {
  const fast = ema(values, fastPeriod);
  const slow = ema(values, slowPeriod);

  const macdLine = [];

  const offset =
    fastPeriod - slowPeriod;

  for (let i = 0; i < slow.length; i++) {
    const fi = i + offset;

    if (fi >= 0 && fi < fast.length) {
      macdLine.push(
        fast[fi] - slow[i]
      );
    }
  }

  const signal = ema(
    macdLine,
    signalPeriod
  );

  const out = [];

  for (
    let i = signalPeriod - 1;
    i < macdLine.length;
    i++
  ) {
    const s =
      signal[i - (signalPeriod - 1)];

    const m = macdLine[i];

    out.push({
      macd: m,
      signal: s,
      hist: m - s
    });
  }

  return out;
}

function ATR(candles, period = 14) {
  if (candles.length <= period) return [];

  const tr = [];

  for (let i = 1; i < candles.length; i++) {
    const c = candles[i];
    const p = candles[i - 1];

    tr.push(
      Math.max(
        c.high - c.low,
        Math.abs(c.high - p.close),
        Math.abs(c.low - p.close)
      )
    );
  }

  let avg =
    tr
      .slice(0, period)
      .reduce((a, b) => a + b, 0) /
    period;

  const out = [avg];

  for (let i = period; i < tr.length; i++) {
    avg =
      (avg * (period - 1) + tr[i]) /
      period;

    out.push(avg);
  }

  return out;
}

function round(value, digits = 4) {
  if (!Number.isFinite(value)) return 0;

  const p = 10 ** digits;

  return Math.round(value * p) / p;
}

function sleep(ms) {
  return new Promise(
    resolve => setTimeout(resolve, ms)
  );
}

function formatAlert(s) {
  const emoji =
    s.signal === "BUY"
      ? "🟢"
      : "🔴";

  const side =
    s.signal === "BUY"
      ? "CONFIRMED LONG"
      : "CONFIRMED SHORT";

  return (
    `${emoji} ${s.label || s.symbol} — ${side}\n` +
    `Quality: ${s.score}/10\n` +
    `Strategy: ${s.strategy}\n` +
    `Price: ${s.price}\n` +
    `RSI 15m: ${s.rsi15}\n` +
    `RSI 1H: ${s.rsi1h}\n` +
    `Reason: ${s.reason}\n` +
    `⚠️ Signal only — confirm risk before trading.`
  );
}

async function sendNtfy(env, message) {
  const topic =
    env.NTFY_TOPIC ||
    env.NTFY_SUBSCRIPTION_TOPIC;

  if (!topic) {
    console.error(
      "NTFY topic is not configured."
    );

    return false;
  }

  try {
    const response = await fetch(
      `https://ntfy.sh/${encodeURIComponent(topic)}`,
      {
        method: "POST",
        headers: {
          "Content-Type": "text/plain; charset=utf-8",
          "Title": "Mobile Signal Bot V2",
          "Priority": "default",
          "Tags": "chart_with_upwards_trend"
        },
        body: message
      }
    );

    if (!response.ok) {
      const body =
        await response.text().catch(
          () => ""
        );

      console.error(
        `NTFY HTTP ${response.status}: ${body}`
      );

      return false;
    }

    return true;
  } catch (e) {
    console.error(
      "NTFY request failed:",
      e?.message || e
    );

    return false;
  }
}
  // Part 3 — keep this section at the very end of worker.js

function safeNumber(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(v, min, max) {
  return Math.min(max, Math.max(min, v));
}

function qualityLabel(score) {
  if (score >= 9) return "A+";
  if (score >= 8) return "A";
  return "WAIT";
}

function makeDashboardSummary(results) {
  const buy = results.filter(
    x => x.signal === "BUY"
  );

  const sell = results.filter(
    x => x.signal === "SELL"
  );

  const waiting = results.filter(
    x => x.signal === "NONE"
  );

  return {
    total: results.length,
    buy: buy.length,
    sell: sell.length,
    waiting: waiting.length,
    bestBuy:
      buy.sort(
        (a, b) => b.score - a.score
      )[0] || null,
    bestSell:
      sell.sort(
        (a, b) => b.score - a.score
      )[0] || null
  };
}

function validateSignal(s) {
  if (!s) return false;

  if (
    s.signal !== "BUY" &&
    s.signal !== "SELL"
  ) {
    return false;
  }

  if (
    !Number.isFinite(Number(s.score)) ||
    Number(s.score) < 8
  ) {
    return false;
  }

  if (
    !Number.isFinite(Number(s.price)) ||
    Number(s.price) <= 0
  ) {
    return false;
  }

  return true;
}

// Keep the public status endpoint useful without exposing secrets.
async function getStatus(env) {
  let heartbeat = null;

  if (env.SIGNAL_STATE) {
    try {
      heartbeat =
        await env.SIGNAL_STATE.get(
          "heartbeat:last"
        );
    } catch (e) {
      console.error(
        "Status KV warning:",
        e?.message || e
      );
    }
  }

  return {
    ok: true,
    bot: "Mobile Signal Bot V2",
    mode: "SIGNAL_ONLY",
    quality: "VANTIQ_STYLE",
    minimumScore: Number(
      env.MIN_SCORE || DEFAULT_MIN_SCORE
    ),
    heartbeatEveryMinutes: 30,
    lastHeartbeat: heartbeat
      ? new Date(
          Number(heartbeat)
        ).toISOString()
      : null,
    ntfyConfigured: Boolean(
      env.NTFY_TOPIC ||
      env.NTFY_SUBSCRIPTION_TOPIC
    )
  };
}
}
