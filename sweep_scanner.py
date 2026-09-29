import asyncio
import json
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import requests
import websockets
from aiohttp import web
from telegram import Bot
from telegram.constants import ParseMode

# ── CONFIG (read from Render Environment Variables) ─────
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
OKX_TICKERS_URL = "https://www.okx.com/api/v5/market/tickers"

TIMEFRAME = "1H"             # OKX interval code for 1-hour
LOOKBACK = 20                # bars to find swing high/low
ATR_PERIOD = 14
ATR_MULTIPLIER = 0.3         # wick must exceed this × ATR
COOLDOWN_BARS = 3            # bars before re-alerting same symbol
TOP_N = 120
# ────────────────────────────────────────────────────────

# Browser-like User-Agent to reduce chance of IP-based blocking
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("TELEGRAM_TOKEN and TELEGRAM_CHAT_ID must be set.")

bot = Bot(token=TELEGRAM_TOKEN)

# In-memory candle store
candle_store: dict[str, list[dict]] = {}
last_alert_bar: dict[str, int] = {}


def fetch_top_symbols(n: int = TOP_N) -> list[str]:
    """Return top N perpetual symbols by 24h volume from OKX."""
    params = {"instType": "SWAP"}
    resp = requests.get(
        OKX_TICKERS_URL, params=params, headers=HTTP_HEADERS, timeout=10
    )
    resp.raise_for_status()
    data = resp.json()

    tickers = data.get("data", [])
    # Filter for USDT-margined perpetuals and sort by 24h volume in USDT
    usdt_perps = [t for t in tickers if t.get("instId", "").endswith("USDT-SWAP")]
    usdt_perps.sort(key=lambda x: float(x.get("volCcy24h", 0)), reverse=True)
    symbols = [t["instId"] for t in usdt_perps[:n]]
    print(f"[+] Top {n} symbols: {symbols}")
    return symbols


def compute_atr(candles: list[dict], period: int = ATR_PERIOD) -> float:
    """Compute ATR from a list of candles."""
    if len(candles) < period + 1:
        return 0.0
    df = pd.DataFrame(candles[-period - 1:])
    df["tr"] = np.maximum(
        df["high"] - df["low"],
        np.maximum(
            abs(df["high"] - df["close"].shift(1)),
            abs(df["low"] - df["close"].shift(1)),
        ),
    )
    return float(df["tr"].iloc[1:].mean())


def detect_liquidity_sweep(symbol: str, candles: list[dict]) -> dict | None:
    """Check the most recently closed candle for a liquidity sweep."""
    if len(candles) < LOOKBACK + ATR_PERIOD + 2:
        return None
    current = candles[-1]
    history = candles[-(LOOKBACK + 2):-1]
    swing_high = max(c["high"] for c in history)
    swing_low = min(c["low"] for c in history)
    atr = compute_atr(candles)
    if atr == 0:
        return None

    # Bullish sweep (swept a low, closed back above)
    if current["low"] < swing_low - (atr * ATR_MULTIPLIER):
        if current["close"] > swing_low:
            return {
                "symbol": symbol,
                "direction": "BULLISH SWEEP (swept low)",
                "swept_level": swing_low,
                "candle_low": current["low"],
                "close": current["close"],
                "atr": round(atr, 6),
                "time": current["time"],
            }

    # Bearish sweep (swept a high, closed back below)
    if current["high"] > swing_high + (atr * ATR_MULTIPLIER):
        if current["close"] < swing_high:
            return {
                "symbol": symbol,
                "direction": "BEARISH SWEEP (swept high)",
                "swept_level": swing_high,
                "candle_high": current["high"],
                "close": current["close"],
                "atr": round(atr, 6),
                "time": current["time"],
            }
    return None


async def send_alert(details: dict):
    """Send a formatted Telegram alert."""
    ts = datetime.fromtimestamp(details["time"] / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M UTC"
    )
    direction = details["direction"]
    emoji = "🟢" if "BULLISH" in direction else "🔴"
    msg = (
        f"{emoji} *LIQUIDITY SWEEP DETECTED*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"*Symbol:* `{details['symbol']}`\n"
        f"*Direction:* {direction}\n"
        f"*Swept Level:* `{details['swept_level']:.6f}`\n"
        f"*Close:* `{details['close']:.6f}`\n"
        f"*ATR(14):* `{details['atr']:.6f}`\n"
        f"*Candle Time:* {ts}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"⏱ 1H timeframe"
    )
    try:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=msg,
            parse_mode=ParseMode.MARKDOWN,
        )
        print(f"[ALERT SENT] {details['symbol']} — {direction}")
    except Exception as e:
        print(f"[!] Telegram error: {e}")


async def handle_kline_message(raw: str):
    """Process incoming kline WebSocket messages from OKX."""
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return

    # OKX sends a "event" field for subscription confirmations, ignore those
    if "event" in msg:
        return

    arg = msg.get("arg", {})
    if not arg.get("channel", "").startswith("candle"):
        return

    data = msg.get("data", [])
    if not data:
        return

    # OKX candle format: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
    kline = data[0]
    symbol = arg.get("instId")
    if not symbol:
        return

    # Only process confirmed (closed) candles
    # confirm: "1" = closed, "0" = still forming
    if len(kline) < 9 or kline[8] != "1":
        return

    candle = {
        "time": int(kline[0]),
        "open": float(kline[1]),
        "high": float(kline[2]),
        "low": float(kline[3]),
        "close": float(kline[4]),
    }

    store = candle_store.setdefault(symbol, [])

    # Avoid duplicate entries for the same closed candle
    if store and store[-1]["time"] == candle["time"]:
        return

    store.append(candle)

    # Keep only last ~100 candles to bound memory
    if len(store) > 100:
        candle_store[symbol] = store[-100:]

    # Evaluate the newest closed candle
    if len(store) >= LOOKBACK + ATR_PERIOD + 2:
        result = detect_liquidity_sweep(symbol, store)
        if result:
            bar_index = len(store)
            last = last_alert_bar.get(symbol, -999)
            if bar_index - last >= COOLDOWN_BARS:
                last_alert_bar[symbol] = bar_index
                asyncio.create_task(send_alert(result))


async def subscribe_symbols(symbols: list[str]):
    """Connect to OKX WebSocket and subscribe to 1H klines."""
    async with websockets.connect(OKX_WS_URL, ping_interval=20) as ws:
        # OKX allows batching multiple channels in one subscribe request.
        # Build subscription args for all symbols.
        args = []
        for sym in symbols:
            args.append({"channel": f"candle{TIMEFRAME}", "instId": sym})

        # OKX limits total subscribe/unsubscribe/login to 480 per connection.
        # Sending one large batch is most efficient.
        sub_msg = {"op": "subscribe", "args": args}
        await ws.send(json.dumps(sub_msg))
        print(f"[+] Subscribed to {len(symbols)} symbols on {TIMEFRAME}")

        # Listen for messages
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=60)
                await handle_kline_message(raw)
            except asyncio.TimeoutError:
                # Send ping to keep alive
                await ws.send(json.dumps({"op": "ping"}))
            except websockets.ConnectionClosed:
                print("[!] WebSocket closed. Reconnecting...")
                break


async def main_scanner():
    """Main scanner loop."""
    while True:
        try:
            symbols = fetch_top_symbols(TOP_N)
            await subscribe_symbols(symbols)
        except Exception as e:
            print(f"[!] Error: {e}")
        print("[*] Reconnecting in 10 seconds...")
        await asyncio.sleep(10)


async def health_check(request):
    """Simple health check endpoint for UptimeRobot."""
    return web.Response(text="OK")


async def start_web_server():
    """Start a lightweight web server for health checks."""
    app = web.Application()
    app.router.add_get("/", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"[+] Health check server running on port {port}")


async def main():
    """Run scanner and web server concurrently."""
    await asyncio.gather(
        main_scanner(),
        start_web_server(),
    )


if __name__ == "__main__":
    asyncio.run(main())
