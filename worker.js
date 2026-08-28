// Mobile Signal Bot V2 — Cloudflare Worker
// Subrequest-safe rotating scanner.
// NTFY setup is unchanged.
//
// Required secret: NTFY_TOPIC
// Optional: MIN_SCORE
// KV binding: SIGNAL_STATE

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
      return new Response(JSON.stringify({
        name: "Mobile Signal Bot V2",
        status: "online",
        message:
          "Rotating scanner runs on Cloudflare Cron. Signals are sent to ntfy.",
        batchSize: SCAN_BATCH_SIZE,
        heartbeatMinutes: 30
      }, null, 2), {
        headers: {
          "content-type": "application/json"
        }
      });
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

    const prev = Number(raw);

    const idx =
      Number.isFinite(prev)
        ? prev
        : slot % total;

    await env.SIGNAL_STATE.put(
      "scan:cursor",
      String(
        (idx + SCAN_BATCH_SIZE) % total
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

function selectBatch(wanted, start) {
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
      batch.slice(i, i + 6);

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
  const now = Date.now();

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
    !sent.length &&
    now - lastHeartbeat >=
      HEARTBEAT_MS
  ) {
    const uniqueReasons =
      [...new Set(
        noSetupReasons
      )];

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
}

async function scanSymbol(
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
    }
      // Reliable 30-minute heartbeat.
  const now = Date.now();

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
    !sent.length &&
    now - lastHeartbeat >=
      HEARTBEAT_MS
  ) {
    const uniqueReasons =
      [...new Set(
        noSetupReasons
      )];

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
}

async function scanSymbol(
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
    const r =
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
  const f =
    n =>
      n == null
        ? "—"
        : Number(n)
            .toPrecision(8);

  return (
    `${s.signal === "BUY" ? "🟢" : "🔴"} ` +
    `${s.signal} — ${s.label}\n` +

    `15m + 1H confirmed\n` +

    `Score: ${s.score}/10\n` +

    `Strategy: ${
      s.strategy || "WAIT"
    }\n\n` +

    `Entry: ${f(s.entry)}\n` +
    `SL: ${f(s.sl)}\n` +
    `TP1: ${f(s.tp1)}\n` +
    `TP2: ${f(s.tp2)}\n` +
    `TP3: ${f(s.tp3)}\n\n` +

    `15m RSI: ${
      s.rsi?.toFixed(1)
    }\n` +

    `1H RSI: ${
      s.htfRsi?.toFixed(1)
    }\n\n` +

    `Reason: ${
      s.reason ||
      "No additional reason"
    }\n\n` +

    `Strict bot: NO AUTO-TRADE`
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
    
