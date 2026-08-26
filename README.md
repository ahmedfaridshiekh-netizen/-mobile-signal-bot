# Mobile Signal Bot V1

This is the mobile-only/cloud version of the strict signal system.

## What it scans

30 crypto contracts:
BTC, ETH, BNB, SOL, XRP, DOGE, ADA, AVAX, LINK, DOT,
SUI, LTC, BCH, UNI, NEAR, APT, FIL, ATOM, ARB, OP,
INJ, SEI, TIA, PEPE, WIF, ZEC, JELLYJELLY, TAO, RENDER, ICP

Plus:
- GOLD(XAU)USDT
- SILVER(XAG)USDT
- OIL(BRENT)USDT

The MEXC public futures API provides contract details and K-line data; no exchange API key is needed for these public market-data endpoints.

## Signal logic

- 15m execution timeframe
- 1H trend confirmation
- EMA 20/50/200
- RSI 14
- MACD 12/26/9
- volume > 20-bar SMA × 1.05
- 10-bar breakout or EMA20 reclaim
- anti-chase filter
- minimum strict score 7/10
- only closed candles
- duplicate same-direction alerts suppressed
- SL = 1.5 ATR
- TP1 = 1R, TP2 = 2R, TP3 = 3R

This is a signal/alert system, not an auto-trading system.

## Push notification

Telegram is used for mobile push notifications. Telegram's Bot API supports HTTPS `sendMessage`.

You need:
1. Create a Telegram bot with @BotFather.
2. Start the bot from your Telegram account.
3. Obtain the bot token.
4. Obtain your chat ID.
5. Put both values into Cloudflare Worker Secrets:
   - TELEGRAM_BOT_TOKEN
   - TELEGRAM_CHAT_ID

Never put the bot token into public code or a screenshot.

## Cloud deployment

Cloudflare Workers Cron Triggers run the scheduled worker without a PC/VPS.

Dashboard path:
Workers & Pages → your Worker → Settings → Triggers → Cron Triggers.

Set:
*/5 * * * *

Create a KV namespace and bind it as:
SIGNAL_STATE

Then set the KV namespace ID in wrangler.jsonc if deploying with Wrangler.

After deployment, open:
https://YOUR-WORKER.workers.dev/test

You should receive:
🧪 Mobile Signal Bot V1 test

Then:
https://YOUR-WORKER.workers.dev/scan

This manually runs a scan and returns JSON.

## Important

Cron execution is scheduled in UTC. Cloudflare notes that Cron Trigger changes can take several minutes (up to about 15 minutes) to propagate.

Market data and signal calculations are not a guarantee of profit. Keep risk small and do not auto-trade from this V1.
