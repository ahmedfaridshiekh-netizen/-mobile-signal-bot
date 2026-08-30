// Mobile Signal Bot V2 — Cloudflare Worker
// Subrequest-safe rotating scanner.
// NTFY diagnostic build.
//
// Required secret: NTFY_TOPIC
//
// Optional:
// MIN_SCORE (default 8)
//
// KV binding:
// SIGNAL_STATE

const CRYPTO_BASES = [
  "BTC",
  "ETH",
  "BNB",
  "SOL",
  "XRP",
  "DOGE",
  "ADA",
  "AVAX",
  "LINK",
  "DOT",
  "SUI",
  "LTC",
  "BCH",
  "ETC",
  "FIL",
  "ATOM",
  "NEAR",
  "APT",
  "ARB",
  "OP",
  "INJ",
  "SEI",
  "TIA",
  "TRX",
  "HBAR",
  "PEPE",
  "WIF",
  "BONK",
  "FLOKI",
  "SHIB",
  "MATIC",
  "UNI",
  "AAVE"
];

const COMMODITIES = [];

const MEXC =
  "https://contract.mexc.com";

const DEFAULT_MIN_SCORE = 8;

const SCAN_BATCH_SIZE = 12;

const HEARTBEAT_MS =
  30 * 60 * 1000;

const MAX_EMA20_DISTANCE_ATR =
  1.25;

const MAX_BODY_PCT =
  0.70;

const STATIC_SYMBOLS =
  Object.fromEntries(
    CRYPTO_BASES.map(
      base => [
        base,
        `${base}_USDT`
      ]
    )
  );

// Commodities are optional because
// their MEXC contract names can change.
const COMMODITY_CANDIDATES = {
  GOLD: [
    "GOLD_USDT",
    "XAU_USDT"
  ],
  SILVER: [
    "SILVER_USDT",
    "XAG_USDT"
  ]
};

const HEARTBEAT_KEY =
  "heartbeat:last";

export default {
  async fetch(
    request,
    env,
    ctx
  ) {
    const url =
      new URL(request.url);

    const path =
      url.pathname;

    if (path === "/") {
      return new Response(
        JSON.stringify(
          {
            name:
              "Mobile Signal Bot V2",
            status:
              "online",
            message:
              "VANTIQ-quality rotating scanner with ntfy heartbeat.",
            batchSize:
              SCAN_BATCH_SIZE,
            watchlist: {
              crypto:
                CRYPTO_BASES,
              commodities:
                COMMODITIES.map(
                  x => x.label
                )
            },
            minScore:
              Number(
                env.MIN_SCORE ||
                DEFAULT_MIN_SCORE
              )
          },
          null,
          2
        ),
        {
          headers: {
            "content-type":
              "application/json"
          }
        }
      );
    }

    if (path === "/scan") {
      try {
        const result =
          await runScan(env);

        return new Response(
          JSON.stringify(
            result,
            null,
            2
          ),
          {
            headers: {
              "content-type":
                "application/json"
            }
          }
        );
      } catch (e) {
        return new Response(
          JSON.stringify(
            {
              ok: false,
              error:
                String(
                  e?.message || e
                )
            }
          ),
          {
            status: 500,
            headers: {
              "content-type":
                "application/json"
            }
          }
        );
      }
    }

    if (path === "/test") {
      const ntfyResult =
        await sendNtfy(
          env,
          "🟢 Mobile Signal Bot V2 test notification",
          true
        );

      return new Response(
        JSON.stringify(
          {
            ok:
              ntfyResult.ok,
            ntfy:
              ntfyResult.ok
                ? "sent"
                : "failed",
            ntfyStatus:
              ntfyResult.status,
            ntfyResponse:
              ntfyResult.body
          },
          null,
          2
        ),
        {
          headers: {
            "content-type":
              "application/json"
          }
        }
      );
    }

    if (path === "/status") {
      return new Response(
        JSON.stringify(
          await getStatus(env),
          null,
          2
        ),
        {
          headers: {
            "content-type":
              "application/json"
          }
        }
      );
    }

    return new Response(
      "Not found",
      {
        status: 404
      }
    );
  },

  async scheduled(
    event,
    env,
    ctx
  ) {
    ctx.waitUntil(
      runScan(env)
    );
  }
};

function buildWatchlist() {
  const wanted =
    CRYPTO_BASES.map(
      base => ({
        symbol:
          STATIC_SYMBOLS[base],
        label:
          base,
        category:
          "CRYPTO"
      })
    );

  for (
    const item of COMMODITIES
  ) {
    const candidates =
      COMMODITY_CANDIDATES[
        item.label
      ] || [];

    wanted.push({
      symbol:
        candidates[0] ||
        `${item.label}_USDT`,
      label:
        item.label,
      category:
        "COMMODITY"
    });
  }

  return wanted;
}

async function getRotationIndex(
  env,
  total
) {
  const slot =
    Math.floor(
      Date.now() / 60000
    );

  if (
    !env.SIGNAL_STATE ||
    !total
  ) {
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
        (idx +
          SCAN_BATCH_SIZE) %
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
    i < SCAN_BATCH_SIZE;
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

  // Process in groups of 6.
  // Each symbol makes exactly 2 MEXC requests:
  // 15m + 1H.
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

    const part =
      await Promise.all(
        chunk.map(
          item =>
            scanSymbol(
              item,
              env,
              minScore
            )
        )
      );

    results.push(
      ...part
    );
  }

  const alerts =
    results.filter(
      x =>
        x.signal === "BUY" ||
        x.signal === "SELL"
    );

  const noSetupReasons =
    results
      .filter(
        x =>
          x.signal === "NONE"
      )
      .map(
        x =>
          `${x.label}: ${x.reason}`
      );

  const sent = [];

  for (
    const s of alerts
  ) {
    const key =
      `state:${s.symbol}`;

    let previous =
      null;

    if (
      env.SIGNAL_STATE
    ) {
      try {
        previous =
          await env.SIGNAL_STATE.get(
            key
          );
      } catch (e) {
        console.error(
          "KV signal read warning:",
          e?.message || e
        );
      }
    }

    // Only a genuine direction change
    // creates a notification.
    if (
      previous !==
      s.signal
    ) {
      const ok =
        await sendNtfy(
          env,
          formatAlert(s)
        );

      if (ok) {
        sent.push(
          s.symbol
        );

        if (
          env.SIGNAL_STATE
        ) {
          try {
            await env.SIGNAL_STATE.put(
              key,
              s.signal
            );
          } catch (e) {
            console.error(
              "KV signal write warning:",
              e?.message || e
            );
          }
        }
      }
    }
  }

  let heartbeatSent =
    false;

  const now =
    Date.now();

  let lastHeartbeat =
    0;

  if (
    env.SIGNAL_STATE
  ) {
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

  // ALWAYS send an ALIVE message
  // every 30 minutes, regardless of
  // whether a signal was found.
  if (
    now -
      lastHeartbeat >=
    HEARTBEAT_MS
  ) {
    const message =
      "🟢 Mobile Signal Bot V2 is ALIVE\n" +
      "🔎 Scanner is running normally.\n" +
      "No action is required.";

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
    scanned:
      batch.length,
    totalWatchlist:
      wanted.length,
    rotationStart:
      start,
    nextRotationStart:
      (
        start +
        SCAN_BATCH_SIZE
      ) %
      wanted.length,
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
        signal:
          "NONE",
        score: 0,
        reason:
          "Insufficient market data"
      };
    }

    const c15 =
      normalizeCandles(
        candles15
      );

    const c1h =
      normalizeCandles(
        candles1h
      );

    // Exclude currently forming candles.
    const x15 =
      c15.slice(0, -1);

    const x1h =
      c1h.slice(0, -1);

    if (
      x15.length < 205 ||
      x1h.length < 205
    ) {
      return {
        ...item,
        signal:
          "NONE",
        score: 0,
        reason:
          "Not enough closed candles"
      };
    }

    const close15 =
      x15.map(
        x => x.close
      );

    const close1h =
      x1h.map(
        x => x.close
      );

    const ema20_15 =
      ema(
        close15,
        20
      );

    const ema50_15 =
      ema(
        close15,
        50
      );

    const ema200_15 =
      ema(
        close15,
        200
      );

    const ema20_1h =
      ema(
        close1h,
        20
      );

    const ema50_1h =
      ema(
        close1h,
        50
      );

    const ema200_1h =
      ema(
        close1h,
        200
      );

    const rsi15 =
      RSI(
        close15,
        14
      );

    const rsi1h =
      RSI(
        close1h,
        14
      );

    const macd15 =
      MACD(
        close15
      );

    const macd1h =
      MACD(
        close1h
      );

    const atr15 =
      ATR(
        x15,
        14
      );

    const last =
      x15[
        x15.length - 1
      ];

    const prev =
      x15[
        x15.length - 2
      ];

    const price =
      last.close;

    const e20 =
      ema20_15[
        ema20_15.length - 1
      ];

    const e50 =
      ema50_15[
        ema50_15.length - 1
      ];

    const e200 =
      ema200_15[
        ema200_15.length - 1
      ];

    const h20 =
      ema20_1h[
        ema20_1h.length - 1
      ];

    const h50 =
      ema50_1h[
        ema50_1h.length - 1
      ];

    const h200 =
      ema200_1h[
        ema200_1h.length - 1
      ];

    const r15 =
      rsi15[
        rsi15.length - 1
      ];

    const r1 =
      rsi1h[
        rsi1h.length - 1
      ];

    const m15 =
      macd15[
        macd15.length - 1
      ];

    const pm15 =
      macd15[
        macd15.length - 2
      ];

    const m1 =
      macd1h[
        macd1h.length - 1
      ];

    const pm1 =
      macd1h[
        macd1h.length - 2
      ];

    const atr =
      atr15[
        atr15.length - 1
      ];

    if (
      !Number.isFinite(
        price
      ) ||
      !Number.isFinite(
        e20
      ) ||
      !Number.isFinite(
        e50
      ) ||
      !Number.isFinite(
        e200
      ) ||
      !Number.isFinite(
        h20
      ) ||
      !Number.isFinite(
        h50
      ) ||
      !Number.isFinite(
        h200
      ) ||
      !Number.isFinite(
        r15
      ) ||
      !Number.isFinite(
        r1
      ) ||
      !Number.isFinite(
        atr
      ) ||
      atr <= 0
    ) {
      return {
        ...item,
        signal:
          "NONE",
        score: 0,
        reason:
          "Indicator data unavailable"
      };
    }

    const body =
      Math.abs(
        last.close -
        last.open
      );

    const range =
      Math.max(
        last.high -
          last.low,
        Number.EPSILON
      );

    const bodyPct =
      body /
      range;

    const distAtr =
      Math.abs(
        price -
        e20
      ) /
      atr;

    const recent =
      x15.slice(
        -20
      );

    const resistance =
      Math.max(
        ...recent
          .slice(
            0,
            -1
          )
          .map(
            x =>
              x.high
          )
      );

    const support =
      Math.min(
        ...recent
          .slice(
            0,
            -1
          )
          .map(
            x =>
              x.low
          )
      );

    const volumeAvg =
      SMA(
        x15
          .slice(
            -21,
            -1
          )
          .map(
            x =>
              x.volume
          ),
        20
      );

    const volumeOk =
      Number.isFinite(
        volumeAvg
      ) &&
      last.volume >
        volumeAvg *
          1.05;

    const bullish15 =
      e20 >
        e50 &&
      e50 >
        e200;

    const bearish15 =
      e20 <
        e50 &&
      e50 <
        e200;

    const bullish1h =
      h20 >
        h50 &&
      h50 >
        h200;

    const bearish1h =
      h20 <
        h50 &&
      h50 <
        h200;

    const macdBull =
      m15.hist > 0 &&
      m15.hist >=
        pm15.hist &&
      m1.hist > 0 &&
      m1.hist >=
        pm1.hist;

    const macdBear =
      m15.hist < 0 &&
      m15.hist <=
        pm15.hist &&
      m1.hist < 0 &&
      m1.hist <=
        pm1.hist;

    const pullbackLong =
      prev.low <=
        e20 * 1.003 &&
      last.close >
        e20 &&
      last.close >
        prev.high *
          0.997;

    const pullbackShort =
      prev.high >=
        e20 * 0.997 &&
      last.close <
        e20 &&
      last.close <
        prev.low *
          1.003;

    const reclaimLong =
      prev.close <
        e20 &&
      last.close >
        e20;

    const reclaimShort =
      prev.close >
        e20 &&
      last.close <
        e20;

    const breakoutLong =
      last.close >
      resistance;

    const breakoutShort =
      last.close <
      support;

    const nearResistance =
      (
        resistance -
        price
      ) /
      atr;

    const nearSupport =
      (
        price -
        support
      ) /
      atr;

    const longExhausted =
      distAtr >
        MAX_EMA20_DISTANCE_ATR ||
      bodyPct >
        MAX_BODY_PCT ||
      r15 >
        74 ||
      r1 >
        76 ||
      nearResistance <
        0.45;

    const shortExhausted =
      distAtr >
        MAX_EMA20_DISTANCE_ATR ||
      bodyPct >
        MAX_BODY_PCT ||
      r15 <
        26 ||
      r1 <
        24 ||
      nearSupport <
        0.45;

    const notChasing =
      distAtr <=
        2.0 &&
      bodyPct <=
        0.85;

    let longScore =
      0;

    let shortScore =
      0;

    if (
      bullish1h
    )
      longScore++;

    if (
      bullish15
    )
      longScore++;

    if (
      price >
      h200
    )
      longScore++;

    if (
      price >
      e200
    )
      longScore++;

    if (
      r15 >= 50 &&
      r15 <= 68
    )
      longScore++;

    if (
      r1 >= 48 &&
      r1 <= 70
    )
      longScore++;

    if (
      macdBull
    )
      longScore++;

    if (
      volumeOk
    )
      longScore++;

    if (
      pullbackLong ||
      reclaimLong
    )
      longScore++;

    if (
      breakoutLong
    )
      longScore++;

    if (
      bearish1h
    )
      shortScore++;

    if (
      bearish15
    )
      shortScore++;

    if (
      price <
      h200
    )
      shortScore++;

    if (
      price <
      e200
    )
      shortScore++;

    if (
      r15 >= 32 &&
      r15 <= 50
    )
      shortScore++;

    if (
      r1 >= 30 &&
      r1 <= 52
    )
      shortScore++;

    if (
      macdBear
    )
      shortScore++;

    if (
      volumeOk
    )
      shortScore++;

    if (
      pullbackShort ||
      reclaimShort
    )
      shortScore++;

    if (
      breakoutShort
    )
      shortScore++;

    const longStructure =
      bullish1h &&
      bullish15 &&
      (
        pullbackLong ||
        reclaimLong ||
        breakoutLong
      );

    const shortStructure =
      bearish1h &&
      bearish15 &&
      (
        pullbackShort ||
        reclaimShort ||
        breakoutShort
      );

    const longMomentum =
      macdBull &&
      r15 >= 50 &&
      r15 <= 70;

    const shortMomentum =
      macdBear &&
      r15 >= 30 &&
      r15 <= 50;

    let signal =
      "NONE";

    let score =
      0;

    let reason =
      "No high-quality setup";

    let strategy =
      "WAIT";

    if (
      notChasing &&
      longScore >=
        minScore &&
      longStructure &&
      longMomentum &&
      !longExhausted
    ) {
      signal =
        "BUY";

      score =
        longScore;

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
      notChasing &&
      shortScore >=
        minScore &&
      shortStructure &&
      shortMomentum &&
      !shortExhausted
    ) {
      signal =
        "SELL";

      score =
        shortScore;

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
        longScore >=
          minScore &&
        longExhausted
      ) {
        reason =
          "Bullish but late/extended — waiting for pullback";
      } else if (
        shortScore >=
          minScore &&
        shortExhausted
      ) {
        reason =
          "Bearish but late/extended — waiting for pullback";
      } else if (
        longScore >=
          minScore ||
        shortScore >=
          minScore
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

    const entry =
      price;

    const risk =
      atr *
      1.5;

    const sl =
      signal === "BUY"
        ? entry -
          risk
        : signal === "SELL"
          ? entry +
            risk
          : null;

    const tp1 =
      signal === "BUY"
        ? entry +
          risk
        : signal === "SELL"
          ? entry -
            risk
          : null;

    const tp2 =
      signal === "BUY"
        ? entry +
          risk *
            2
        : signal === "SELL"
          ? entry -
            risk *
              2
          : null;

    const tp3 =
      signal === "BUY"
        ? entry +
          risk *
            3
        : signal === "SELL"
          ? entry -
            risk *
              3
          : null;

    return {
      ...item,

      symbol,

      price,

      signal,

      score:
        signal ===
        "BUY"
          ? longScore
          : signal ===
            "SELL"
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

      rsi15:
        round(
          r15,
          2
        ),

      rsi1h:
        round(
          r1,
          2
        ),

      atr:
        round(
          atr,
          8
        ),

      ema20:
        round(
          e20,
          8
        ),

      ema50:
        round(
          e50,
          8
        ),

      ema200:
        round(
          e200,
          8
        ),

      distanceAtr:
        round(
          distAtr,
          2
        ),

      bodyPct:
        round(
          bodyPct,
          2
        ),

      volumeOk,

      pullbackLong,

      pullbackShort,

      reclaimLong,

      reclaimShort,

      breakoutLong,

      breakoutShort,

      strategy,

      reason,

      timestamp:
        new Date(
          last.time ||
          Date.now()
        ).toISOString()
    };

  } catch (e) {

    console.error(
      `SCAN ERROR ${symbol}:`,
      e?.message || e
    );

    return {
      ...item,
      signal:
        "NONE",
      score: 0,
      reason:
        `Scanner error: ${
          String(
            e?.message || e
          )
        }`
    };
  }
}async function getKlines(
  symbol,
  interval,
  limit = 220
) {
  const url =
    `${MEXC}/api/v1/contract/kline/${encodeURIComponent(symbol)}` +
    `?interval=${interval}` +
    `&limit=${limit}`;

  const res =
    await fetch(
      url,
      {
        method: "GET",
        headers: {
          "accept":
            "application/json"
        }
      }
    );

  if (!res.ok) {
    throw new Error(
      `MEXC ${symbol} ${interval}: HTTP ${res.status}`
    );
  }

  const data =
    await res.json();

  if (
    !data ||
    data.success === false
  ) {
    throw new Error(
      `MEXC ${symbol} ${interval}: invalid response`
    );
  }

  const d =
    data.data;

  if (!d) {
    throw new Error(
      `MEXC ${symbol} ${interval}: no data`
    );
  }

  if (
    Array.isArray(d)
  ) {
    return d.map(
      row => ({
        time:
          Number(
            row[0]
          ) * 1000,

        open:
          Number(
            row[1]
          ),

        high:
          Number(
            row[2]
          ),

        low:
          Number(
            row[3]
          ),

        close:
          Number(
            row[4]
          ),

        volume:
          Number(
            row[5]
          )
      })
    );
  }

  if (
    d.time &&
    d.open &&
    d.high &&
    d.low &&
    d.close &&
    d.vol
  ) {
    const n =
      Math.min(
        d.time.length,
        d.open.length,
        d.high.length,
        d.low.length,
        d.close.length,
        d.vol.length
      );

    const rows =
      [];

    for (
      let i = 0;
      i < n;
      i++
    ) {
      rows.push({
        time:
          Number(
            d.time[i]
          ) * 1000,

        open:
          Number(
            d.open[i]
          ),

        high:
          Number(
            d.high[i]
          ),

        low:
          Number(
            d.low[i]
          ),

        close:
          Number(
            d.close[i]
          ),

        volume:
          Number(
            d.vol[i]
          )
      });
    }

    return rows;
  }

  throw new Error(
    `MEXC ${symbol} ${interval}: unsupported kline format`
  );
}

function normalizeCandles(
  candles
) {
  return candles
    .map(
      c => ({
        time:
          Number(
            c.time ??
            c.timestamp ??
            c.ts ??
            0
          ),

        open:
          Number(
            c.open
          ),

        high:
          Number(
            c.high
          ),

        low:
          Number(
            c.low
          ),

        close:
          Number(
            c.close
          ),

        volume:
          Number(
            c.volume ??
            c.vol ??
            0
          )
      })
    )
    .filter(
      c =>
        Number.isFinite(
          c.open
        ) &&
        Number.isFinite(
          c.high
        ) &&
        Number.isFinite(
          c.low
        ) &&
        Number.isFinite(
          c.close
        )
    )
    .sort(
      (
        a,
        b
      ) =>
        a.time -
        b.time
    );
}

function ema(
  values,
  period
) {
  if (
    !values ||
    values.length <
      period
  ) {
    return [];
  }

  const out =
    new Array(
      values.length
    ).fill(
      NaN
    );

  let sum = 0;

  for (
    let i = 0;
    i < period;
    i++
  ) {
    sum +=
      Number(
        values[i]
      );
  }

  let prev =
    sum / period;

  out[
    period - 1
  ] =
    prev;

  const k =
    2 /
    (period + 1);

  for (
    let i = period;
    i < values.length;
    i++
  ) {
    prev =
      Number(
        values[i]
      ) * k +
      prev *
        (1 - k);

    out[i] =
      prev;
  }

  return out;
}

function SMA(
  values,
  period
) {
  if (
    !values ||
    values.length <
      period
  ) {
    return NaN;
  }

  let sum = 0;

  for (
    let i =
      values.length -
      period;
    i <
      values.length;
    i++
  ) {
    sum +=
      Number(
        values[i]
      );
  }

  return (
    sum / period
  );
}

function RSI(
  values,
  period = 14
) {
  if (
    !values ||
    values.length <=
      period
  ) {
    return [];
  }

  const out =
    new Array(
      values.length
    ).fill(
      NaN
    );

  let gains = 0;
  let losses = 0;

  for (
    let i = 1;
    i <= period;
    i++
  ) {
    const diff =
      Number(
        values[i]
      ) -
      Number(
        values[i - 1]
      );

    if (
      diff >= 0
    ) {
      gains +=
        diff;
    } else {
      losses +=
        Math.abs(
          diff
        );
    }
  }

  let avgGain =
    gains / period;

  let avgLoss =
    losses / period;

  out[
    period
  ] =
    avgLoss === 0
      ? 100
      : 100 -
        100 /
          (1 +
            avgGain /
              avgLoss);

  for (
    let i =
      period + 1;
    i <
      values.length;
    i++
  ) {
    const diff =
      Number(
        values[i]
      ) -
      Number(
        values[i - 1]
      );

    const gain =
      diff > 0
        ? diff
        : 0;

    const loss =
      diff < 0
        ? Math.abs(
            diff
          )
        : 0;

    avgGain =
      (
        avgGain *
          (period - 1) +
        gain
      ) /
      period;

    avgLoss =
      (
        avgLoss *
          (period - 1) +
        loss
      ) /
      period;

    out[i] =
      avgLoss === 0
        ? 100
        : 100 -
          100 /
            (1 +
              avgGain /
                avgLoss);
  }

  return out;
}

function ATR(
  candles,
  period = 14
) {
  if (
    !candles ||
    candles.length <=
      period
  ) {
    return [];
  }

  const tr =
    new Array(
      candles.length
    ).fill(
      NaN
    );

  for (
    let i = 1;
    i <
      candles.length;
    i++
  ) {
    const h =
      candles[i].high;

    const l =
      candles[i].low;

    const pc =
      candles[
        i - 1
      ].close;

    tr[i] =
      Math.max(
        h - l,
        Math.abs(
          h - pc
        ),
        Math.abs(
          l - pc
        )
      );
  }

  const out =
    new Array(
      candles.length
    ).fill(
      NaN
    );

  let sum = 0;

  for (
    let i = 1;
    i <= period;
    i++
  ) {
    sum +=
      tr[i];
  }

  let prev =
    sum / period;

  out[
    period
  ] =
    prev;

  for (
    let i =
      period + 1;
    i <
      candles.length;
    i++
  ) {
    prev =
      (
        prev *
          (period - 1) +
        tr[i]
      ) /
      period;

    out[i] =
      prev;
  }

  return out;
}

function MACD(
  values,
  fastPeriod = 12,
  slowPeriod = 26,
  signalPeriod = 9
) {
  if (
    !values ||
    values.length <
      slowPeriod +
        signalPeriod
  ) {
    return [];
  }

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
    new Array(
      values.length
    ).fill(
      NaN
    );

  for (
    let i =
      slowPeriod - 1;
    i <
      values.length;
    i++
  ) {
    if (
      Number.isFinite(
        fast[i]
      ) &&
      Number.isFinite(
        slow[i]
      )
    ) {
      line[i] =
        fast[i] -
        slow[i];
    }
  }

  const clean =
    line.map(
      x =>
        Number.isFinite(
          x
        )
          ? x
          : 0
    );

  const sig =
    ema(
      clean,
      signalPeriod
    );

  return values.map(
    (
      _,
      i
    ) => ({
      macd:
        line[i],

      signal:
        sig[i],

      hist:
        Number.isFinite(
          line[i]
        ) &&
        Number.isFinite(
          sig[i]
        )
          ? line[i] -
            sig[i]
          : NaN
    })
  );
}

function round(
  value,
  decimals = 4
) {
  if (
    !Number.isFinite(
      Number(value)
    )
  ) {
    return null;
  }

  const p =
    10 ** decimals;

  return (
    Math.round(
      Number(value) *
        p
    ) / p
  );
}

function formatPrice(
  value
) {
  if (
    value ===
      null ||
    value ===
      undefined ||
    !Number.isFinite(
      Number(value)
    )
  ) {
    return "N/A";
  }

  const n =
    Number(value);

  if (
    n >= 1000
  ) {
    return n.toFixed(
      2
    );
  }

  if (
    n >= 1
  ) {
    return n.toFixed(
      4
    );
  }

  return n.toFixed(
    6
  );
}async function sendNtfy(
  env,
  message,
  diagnosticMode = false
) {
  if (
    !env.NTFY_TOPIC
  ) {
    console.error(
      "NTFY_TOPIC secret missing"
    );

    if (
      diagnosticMode
    ) {
      return {
        ok: false,
        status: null,
        body:
          "NTFY_TOPIC secret missing"
      };
    }

    return false;
  }

  try {
    const response =
      await fetch(
        `https://ntfy.sh/${encodeURIComponent(
          env.NTFY_TOPIC
        )}`,
        {
          method:
            "POST",

          headers: {
            "Content-Type":
              "text/plain; charset=utf-8",

            "Title":
              "Mobile Signal Bot V2",

            "Priority":
              "default",

            "Tags":
              "chart_with_upwards_trend"
          },

          body:
            String(
              message
            )
        }
      );

    const body =
      await response.text();

    if (
      !response.ok
    ) {
      console.error(
        "NTFY failed:",
        response.status,
        body
      );

      if (
        diagnosticMode
      ) {
        return {
          ok: false,
          status:
            response.status,
          body
        };
      }

      return false;
    }

    if (
      diagnosticMode
    ) {
      return {
        ok: true,
        status:
          response.status,
        body
      };
    }

    return true;

  } catch (e) {

    console.error(
      "NTFY exception:",
      e?.message || e
    );

    if (
      diagnosticMode
    ) {
      return {
        ok: false,
        status: null,
        body:
          String(
            e?.message || e
          )
      };
    }

    return false;
  }
}

function formatAlert(
  s
) {
  const side =
    s.signal ===
    "BUY"
      ? "🟢 LONG"
      : "🔴 SHORT";

  const strategy =
    s.strategy ||
    "SETUP";

  return (
    `${side} — ${s.label}\n\n` +

    `⭐ Score: ${s.score}/10\n` +

    `📊 Strategy: ${strategy}\n` +

    `💰 Entry: ${formatPrice(
      s.entry
    )}\n` +

    `🛑 SL: ${formatPrice(
      s.sl
    )}\n` +

    `🎯 TP1: ${formatPrice(
      s.tp1
    )}\n` +

    `🎯 TP2: ${formatPrice(
      s.tp2
    )}\n` +

    `🎯 TP3: ${formatPrice(
      s.tp3
    )}\n\n` +

    `RSI 15m: ${s.rsi15}\n` +

    `RSI 1H: ${s.rsi1h}\n` +

    `ATR: ${s.atr}\n` +

    `📌 ${s.reason}`
  );
}

async function getStatus(
  env
) {
  let lastHeartbeat =
    null;

  let cursor =
    null;

  if (
    env.SIGNAL_STATE
  ) {
    try {
      lastHeartbeat =
        await env.SIGNAL_STATE.get(
          HEARTBEAT_KEY
        );

      cursor =
        await env.SIGNAL_STATE.get(
          "scan:cursor"
        );

    } catch (e) {
      console.error(
        "KV status warning:",
        e?.message || e
      );
    }
  }

  return {
    ok: true,

    worker:
      "Mobile Signal Bot V2",

    heartbeat:
      lastHeartbeat,

    rotationCursor:
      cursor,

    heartbeatInterval:
      "30 minutes",

    timestamp:
      new Date().toISOString()
  };
}
