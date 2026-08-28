// Mobile Signal Bot V2 — Cloudflare Worker
// Clean final version
// Rotating scanner + NTFY alerts + no-setup reasons + heartbeat

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
const SCAN_BATCH_SIZE = 12;

const HEARTBEAT_MS = 30 * 60 * 1000;
const HEARTBEAT_KEY = "heartbeat:last";

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
      return new Response(
        JSON.stringify({
          name: "Mobile Signal Bot V2",
          status: "online",
          message:
            "Rotating scanner runs on Cloudflare Cron. Signals are sent to ntfy.",
          batchSize: SCAN_BATCH_SIZE,
          heartbeatMinutes: 30
        }, null, 2),
        {
          headers: {
            "content-type": "application/json"
          }
        }
      );
    }

    if (url.pathname === "/test") {
      const ok = await sendNtfy(
        env,
        "🧪 Mobile Signal Bot V2 test\nntfy alerts are connected."
      );

      return new Response(
        ok
          ? "ntfy test notification sent."
          : "ntfy test failed.",
        {
          status: ok ? 200 : 502
        }
      );
    }

    if (url.pathname === "/scan") {
      try {
        const result = await runScan(env);

        return new Response(
          JSON.stringify(result, null, 2),
          {
            headers: {
              "content-type": "application/json"
            }
          }
        );
      } catch (e) {
        return new Response(
          JSON.stringify({
            ok: false,
            error: String(e?.message || e)
          }),
          {
            status: 500,
            headers: {
              "content-type": "application/json"
            }
          }
        );
      }
    }

    return new Response("Not found", {
      status: 404
    });
  },

  async scheduled(controller, env, ctx) {
    ctx.waitUntil(
      runScan(env).catch(e =>
        console.error(
          "CRON scan failed:",
          e
        )
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
    const candidates =
      COMMODITY_CANDIDATES[item.label] || [];

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
  const slot =
    Math.floor(Date.now() / 300000);

  if (!env.SIGNAL_STATE) {
    return slot % total;
  }

  try {
    const raw =
      await env.SIGNAL_STATE.get(
        "scan:cursor"
      );

    const prev =
      Number(raw);

    const idx =
      Number.isFinite(prev)
        ? prev
        : slot % total;

    await env.SIGNAL_STATE.put(
      "scan:cursor",
      String(
        (idx + SCAN_BATCH_SIZE) %
          total
      )
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

function selectBatch(
  wanted,
  start
) {
  const out = [];

  for (
    let i = 0;
    i < Math.min(
      SCAN_BATCH_SIZE,
      wanted.length
    );
    i++
  ) {
    out.push(
      wanted[
        (start + i) %
          wanted.length
      ]
    );
  }

  return out;
}

async function runScan(env) {
  const minScore =
    Number(
      env.MIN_SCORE ||
      DEFAULT_MIN_SCORE
    );

  const wanted =
    buildWatchlist();

  const start =
    await getRotationIndex(
      env,
      wanted.length
    );

  const batch =
    selectBatch(
      wanted,
      start
    );

  const results = [];

  for (
    let i = 0;
    i < batch.length;
    i += 6
  ) {
    const chunk =
      batch.slice(
        i,
        i + 6
      );

    const chunkResults =
      await Promise.all(
        chunk.map(x =>
          scanSymbol(
            x,
            env,
            minScore
          )
        )
      );

    results.push(
      ...chunkResults
    );

    if (
      i + 6 <
      batch.length
    ) {
      await sleep(150);
    }
  }

  const alerts =
    results.filter(
      x =>
        x.signal === "BUY" ||
        x.signal === "SELL"
    );

  const sent = [];

  const noSetupReasons =
    results
      .filter(
        x =>
          x.signal !== "BUY" &&
          x.signal !== "SELL"
      )
      .map(
        x =>
          `${x.label || x.symbol}: ${
            x.reason ||
            "No valid setup"
          }`
      );

  for (const s of alerts) {
    const key =
      `state:${s.symbol}`;

    let previous = null;

    if (env.SIGNAL_STATE) {
      try {
        previous =
          await env.SIGNAL_STATE.get(
            key
          );
      } catch (e) {
        console.error(
          `KV read warning ${s.symbol}:`,
          e?.message || e
        );
      }
    }

    if (
      previous !== s.signal
    ) {
      const ok =
        await sendNtfy(
          env,
          formatAlert(s)
        );

      if (
        ok &&
        env.SIGNAL_STATE
      ) {
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
        sent.push(
          s.symbol
        );
      }
    }
  }

  // Reliable 30-minute heartbeat.
  const now =
    Date.now();

  let lastHeartbeat = 0;

  if (env.SIGNAL_STATE) {
    try {
      lastHeartbeat =
        Number(
          await env.SIGNAL_STATE.get(
            HEARTBEAT_KEY
          )
        ) || 0;
    } catch (e) {
      console.error(
        "KV heartbeat read warning:",
        e?.message || e
      );
    }
  }

  let heartbeatSent = false;

  if (
    alerts.length === 0 &&
    now - lastHeartbeat >=
      HEARTBEAT_MS
  ) {
    const uniqueReasons =
      [
        ...new Set(
          noSetupReasons
        )
      ];

    const sample =
      uniqueReasons
        .slice(0, 3)
        .join("\n");

    const message =
      "🔎 Bot is searching for the best setup for you... ❤️\n" +
      "No valid setup detected yet.\n\n" +
      (
        sample ||
        "Market conditions are being monitored."
      );

    heartbeatSent =
      await sendNtfy(
        env,
        message
      );

    if (
      heartbeatSent &&
      env.SIGNAL_STATE
    ) {
      try {
        await env.SIGNAL_STATE.put(
          HEARTBEAT_KEY,
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
    totalWatchlist:
      wanted.length,
    rotationStart:
      start,
    nextRotationStart:
      (
        start +
        SCAN_BATCH_SIZE
      ) % wanted.length,
    signals:
      alerts,
    noSetupReasons:
      [
        ...new Set(
          noSetupReasons
        )
      ].slice(0, 10),
    sent,
    heartbeatSent,
    timestamp:
      new Date().toISOString()
  };
}async function scanSymbol(
  item,
  env,
  minScore
) {
  const symbol =
    item.symbol;

  try {
    const candles15 =
      await getKlines(
        symbol,
        "Min15",
        220
      );

    const candles1h =
      await getKlines(
        symbol,
        "Min60",
        220
      );

    if (
      !candles15 ||
      candles15.length < 210 ||
      !candles1h ||
      candles1h.length < 210
    ) {
      return {
        ...item,
        signal: "NONE",
        score: 0,
        reason:
          "Insufficient market data"
      };
    }

    const a15 =
      analyze(candles15);

    const a1h =
      analyze(candles1h);

    const longScore =
      (a15.close >
        a15.ema200 ? 1 : 0) +

      (a15.ema20 >
        a15.ema50 ? 1 : 0) +

      (a1h.close >
        a1h.ema200 ? 1 : 0) +

      (a1h.ema20 >
        a1h.ema50 ? 1 : 0) +

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

      (
        a1h.hist > 0
          ? 1
          : 0
      ) +

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
        a15.close >
          a15.open
          ? 1
          : 0
      );

    const shortScore =
      (a15.close <
        a15.ema200 ? 1 : 0) +

      (a15.ema20 <
        a15.ema50 ? 1 : 0) +

      (a1h.close <
        a1h.ema200 ? 1 : 0) +

      (a1h.ema20 <
        a1h.ema50 ? 1 : 0) +

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

      (
        a1h.hist < 0
          ? 1
          : 0
      ) +

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
        a15.close <
          a15.open
          ? 1
          : 0
      );

    const longStructure =
      a15.close >
        a15.ema200 &&
      a15.ema20 >
        a15.ema50 &&
      (
        a15.breakUp ||
        a15.reclaimUp
      );

    const shortStructure =
      a15.close <
        a15.ema200 &&
      a15.ema20 <
        a15.ema50 &&
      (
        a15.breakDown ||
        a15.reclaimDown
      );

    const longMomentum =
      a15.rsi >= 52 &&
      a15.rsi <= 72 &&
      a15.macd >
        a15.macdSignal &&
      a15.hist > 0 &&
      a1h.hist > 0;

    const shortMomentum =
      a15.rsi >= 28 &&
      a15.rsi <= 48 &&
      a15.macd <
        a15.macdSignal &&
      a15.hist < 0 &&
      a1h.hist < 0;

    const pullbackLong =
      a15.low <=
        a15.ema20 &&
      a15.close >
        a15.ema20;

    const reclaimLong =
      a15.reclaimUp;

    const pullbackShort =
      a15.high >=
        a15.ema20 &&
      a15.close <
        a15.ema20;

    const reclaimShort =
      a15.reclaimDown;

    const notChasing =
      Math.abs(
        a15.close -
        a15.ema20
      ) /
      Math.max(
        a15.atr,
        1e-12
      ) <= 2.0;

    const longExhausted =
      a15.rsi > 72 ||
      (
        a15.close >
          a15.ema20 +
          a15.atr * 2.0
      );

    const shortExhausted =
      a15.rsi < 28 ||
      (
        a15.close <
          a15.ema20 -
          a15.atr * 2.0
      );

    let signal = "NONE";
    let score = 0;
    let reason =
      "No high-quality setup";
    let strategy = "WAIT";

    if (
      longScore >= minScore &&
      longStructure &&
      longMomentum &&
      !longExhausted &&
      notChasing
    ) {
      signal = "BUY";
      score = longScore;

      if (
        pullbackLong ||
        reclaimLong
      ) {
        strategy =
          "TREND_PULLBACK";

        reason =
          "Bullish HTF trend + 15m pullback/reclaim + momentum";
      } else {
        strategy =
          "BREAKOUT_RETEST";

        reason =
          "Bullish trend + confirmed breakout structure";
      }
    } else if (
      shortScore >= minScore &&
      shortStructure &&
      shortMomentum &&
      !shortExhausted &&
      notChasing
    ) {
      signal = "SELL";
      score = shortScore;

      if (
        pullbackShort ||
        reclaimShort
      ) {
        strategy =
          "TREND_PULLBACK";

        reason =
          "Bearish HTF trend + 15m pullback/reclaim + momentum";
      } else {
        strategy =
          "BREAKOUT_RETEST";

        reason =
          "Bearish trend + confirmed breakdown structure";
      }
    } else {
      if (
        longScore >= minScore &&
        longExhausted
      ) {
        reason =
          "Bullish but late/extended — waiting for pullback";
      } else if (
        shortScore >= minScore &&
        shortExhausted
      ) {
        reason =
          "Bearish but late/extended — waiting for pullback";
      } else if (
        longScore >= minScore ||
        shortScore >= minScore
      ) {
        reason =
          "Indicators align but structure/entry timing is incomplete";
      } else if (
        !notChasing
      ) {
        reason =
          "Entry rejected because price is extended";
      }
    }    const entry = a15.close;

    const risk =
      Math.max(
        a15.atr * 1.5,
        a15.close * 0.002
      );

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

      strategy,
      reason,

      entry,
      sl,
      tp1,
      tp2,
      tp3,

      rsi: a15.rsi,
      htfRsi: a1h.rsi,

      candleTime:
        a15.time
    };

  } catch (e) {
    return {
      ...item,
      signal: "NONE",
      score: 0,
      reason:
        `Scan error: ${
          e?.message || e
        }`
    };
  }
}

async function getKlines(
  symbol,
  interval,
  limit
) {
  const url =
    `${MEXC}/api/v1/contract/kline/` +
    `${encodeURIComponent(symbol)}` +
    `?interval=${interval}&limit=${limit}`;

  const response =
    await fetch(url);

  if (!response.ok) {
    throw new Error(
      `Kline ${symbol} ${interval} HTTP ${response.status}`
    );
  }

  const json =
    await response.json();

  const data =
    json?.data;

  if (
    !data ||
    !data.time ||
    !data.time.length
  ) {
    throw new Error(
      `No kline data for ${symbol} ${interval}`
    );
  }

  const candles = [];

  for (
    let i = 0;
    i < data.time.length;
    i++
  ) {
    candles.push({
      time:
        Number(data.time[i]),

      open:
        Number(data.open[i]),

      close:
        Number(data.close[i]),

      high:
        Number(data.high[i]),

      low:
        Number(data.low[i]),

      volume:
        Number(data.vol[i])
    });
  }

  // Remove the currently forming candle.
  return candles.slice(0, -1);
}

function analyze(candles) {
  const closes =
    candles.map(
      x => x.close
    );

  const highs =
    candles.map(
      x => x.high
    );

  const lows =
    candles.map(
      x => x.low
    );

  const volumes =
    candles.map(
      x => x.volume
    );

  const ema20 =
    ema(
      closes,
      20
    ).at(-1);

  const ema50 =
    ema(
      closes,
      50
    ).at(-1);

  const ema200 =
    ema(
      closes,
      200
    ).at(-1);

  const rsiValue =
    rsi(
      closes,
      14
    ).at(-1);

  const mac =
    macd(
      closes,
      12,
      26,
      9
    );

  const atrValue =
    atr(
      candles,
      14
    );

  const volumeSma =
    sma(
      volumes,
      20
    );

  const last =
    candles.at(-1);

  const previousHigh =
    Math.max(
      ...highs.slice(
        -11,
        -1
      )
    );

  const previousLow =
    Math.min(
      ...lows.slice(
        -11,
        -1
      )
    );

  const breakUp =
    last.close >
    previousHigh;

  const breakDown =
    last.close <
    previousLow;

  const reclaimUp =
    last.low <= ema20 &&
    last.close > ema20;

  const reclaimDown =
    last.high >= ema20 &&
    last.close < ema20;

  return {
    ...last,

    ema20,
    ema50,
    ema200,

    rsi:
      rsiValue,

    macd:
      mac.line,

    macdSignal:
      mac.signal,

    hist:
      mac.hist,

    atr:
      atrValue,

    volumeSma,

    breakUp,
    breakDown,

    reclaimUp,
    reclaimDown
  };
}

function sma(values, period) {
  if (
    !values.length
  ) {
    return 0;
  }

  if (
    values.length < period
  ) {
    return values.at(-1) || 0;
  }

  let sum = 0;

  for (
    let i =
      values.length - period;
    i < values.length;
    i++
  ) {
    sum +=
      Number(values[i]) || 0;
  }

  return sum / period;
}

function ema(values, period) {
  if (
    values.length < period
  ) {
    return values.map(
      () => NaN
    );
  }

  const result =
    Array(
      values.length
    ).fill(NaN);

  let value =
    values
      .slice(0, period)
      .reduce(
        (sum, x) =>
          sum + Number(x),
        0
      ) / period;

  result[period - 1] =
    value;

  const multiplier =
    2 /
    (period + 1);

  for (
    let i = period;
    i < values.length;
    i++
  ) {
    value =
      (
        Number(values[i]) -
        value
      ) *
        multiplier +
      value;

    result[i] =
      value;
  }

  return result;
}function rsi(values, period) {
  const result =
    Array(values.length).fill(NaN);

  if (
    values.length <= period
  ) {
    return result;
  }

  let gain = 0;
  let loss = 0;

  for (
    let i = 1;
    i <= period;
    i++
  ) {
    const change =
      values[i] -
      values[i - 1];

    if (change >= 0) {
      gain += change;
    } else {
      loss -= change;
    }
  }

  gain /= period;
  loss /= period;

  result[period] =
    loss === 0
      ? 100
      : 100 -
        (
          100 /
          (
            1 +
            gain / loss
          )
        );

  for (
    let i = period + 1;
    i < values.length;
    i++
  ) {
    const change =
      values[i] -
      values[i - 1];

    const currentGain =
      Math.max(
        change,
        0
      );

    const currentLoss =
      Math.max(
        -change,
        0
      );

    gain =
      (
        gain *
          (period - 1) +
        currentGain
      ) / period;

    loss =
      (
        loss *
          (period - 1) +
        currentLoss
      ) / period;

    result[i] =
      loss === 0
        ? 100
        : 100 -
          (
            100 /
            (
              1 +
              gain / loss
            )
          );
  }

  return result;
}

function macd(
  values,
  fastPeriod,
  slowPeriod,
  signalPeriod
) {
  const fast =
    ema(
      values,
      fastPeriod
    );

  const slow =
    ema(
      values,
      slowPeriod
    );

  const line =
    values.map(
      (_, i) => {
        if (
          !Number.isFinite(
            fast[i]
          ) ||
          !Number.isFinite(
            slow[i]
          )
        ) {
          return NaN;
        }

        return (
          fast[i] -
          slow[i]
        );
      }
    );

  const cleanLine =
    line.map(
      value =>
        Number.isFinite(
          value
        )
          ? value
          : 0
    );

  const signalValues =
    ema(
      cleanLine,
      signalPeriod
    );

  const currentLine =
    line.at(-1);

  const currentSignal =
    signalValues.at(-1);

  return {
    line:
      currentLine,

    signal:
      currentSignal,

    hist:
      currentLine -
      currentSignal
  };
}

function atr(
  candles,
  period
) {
  if (
    candles.length <
    period + 1
  ) {
    return 0;
  }

  const trueRanges = [];

  for (
    let i = 1;
    i < candles.length;
    i++
  ) {
    const current =
      candles[i];

    const previous =
      candles[i - 1];

    const tr =
      Math.max(
        current.high -
          current.low,

        Math.abs(
          current.high -
            previous.close
        ),

        Math.abs(
          current.low -
            previous.close
        )
      );

    trueRanges.push(
      tr
    );
  }

  return sma(
    trueRanges,
    period
  );
}

async function sendNtfy(
  env,
  text
) {
  const topic =
    env.NTFY_TOPIC;

  if (!topic) {
    console.error(
      "NTFY_TOPIC is not configured"
    );

    return false;
  }

  try {
    const response =
      await fetch(
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

    if (!response.ok) {
      console.error(
        `ntfy HTTP ${response.status}`
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
  const formatNumber =
    value =>
      value == null
        ? "—"
        : Number(value)
            .toPrecision(8);

  return (
    `${
      s.signal === "BUY"
        ? "🟢"
        : "🔴"
    } ` +
    `${s.signal} — ${s.label}\n` +

    `15m + 1H confirmed\n` +

    `Score: ${
      s.score
    }/10\n` +

    `Strategy: ${
      s.strategy || "WAIT"
    }\n\n` +

    `Entry: ${
      formatNumber(
        s.entry
      )
    }\n` +

    `SL: ${
      formatNumber(
        s.sl
      )
    }\n` +

    `TP1: ${
      formatNumber(
        s.tp1
      )
    }\n` +

    `TP2: ${
      formatNumber(
        s.tp2
      )
    }\n` +

    `TP3: ${
      formatNumber(
        s.tp3
      )
    }\n\n` +

    `15m RSI: ${
      s.rsi == null
        ? "—"
        : s.rsi.toFixed(1)
    }\n` +

    `1H RSI: ${
      s.htfRsi == null
        ? "—"
        : s.htfRsi.toFixed(1)
    }\n\n` +

    `Reason: ${
      s.reason ||
      "No additional reason"
    }`
  );
}

function sleep(ms) {
  return new Promise(
    resolve =>
      setTimeout(
        resolve,
        ms
      )
  );
}
