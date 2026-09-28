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

BINANCE_FUTURES_WS = "wss://fstream.binance.com/ws"
BINANCE_TICKERS_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"

TIMEFRAME = "1h"             # Binance interval code for 1-hour
LOOKBACK = 20                # bars to find swing high/low
ATR_PERIOD = 14
ATR_MULTIPLIER = 0.3         # wick must exceed this × ATR
COOLDOWN_BARS = 3            # bars before re-alerting same symbol
TOP_N = 120
# ────────────────────────────────────────────────────────

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("TELEGRAM_TOKEN and TELEGRAM_CHAT_ID must be set.")

bot = Bot(token=TELEGRAM_TOKEN)

# In-memory candle store
candle_store: dict[str, list[dict]] = {}
last_alert_bar: dict[str, int] = {}


def fetch_top_symbols(n: int = TOP_N) -> list[str]:
    """Return top N perpetual symbols by 24h volume."""
    resp = requests.get(BINANCE_TICKERS_URL, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    
    # Filter for USDT pairs and sort by quoteVolume (USDT volume)
    tickers = [t for t in data if t.get("symbol", "").endswith("USDT")]
    tickers.sort(key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)
    symbols = [t["symbol"] for t in tickers[:n]]
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
    """Process incoming kline WebSocket messages from Binance."""
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return

    # Binance kline event
    if msg.get("e") != "kline":
        return

    kline = msg.get("k")
    if not kline or not kline.get("x"):  # 'x' is true only when the kline is closed
        return

    symbol = msg.get("s")
    candle = {
        "time": kline["t"],
        "open": float(kline["o"]),
        "high": float(kline["h"]),
        "low": float(kline["l"]),
        "close": float(kline["c"]),
    }

    store = candle_store.setdefault(symbol, [])
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
    """Connect to Binance WebSocket and subscribe to 1H klines."""
    async with websockets.connect(BINANCE_FUTURES_WS, ping_interval=20) as ws:
        # Subscribe to kline streams
        for sym in symbols:
            sub_msg = {
                "method": "SUBSCRIBE",
                "params": [f"{sym.lower()}@kline_{TIMEFRAME}"],
                "id": 1,
            }
            await ws.send(json.dumps(sub_msg))
            await asyncio.sleep(0.05)  # avoid rate-limit bursts

        print(f"[+] Subscribed to {len(symbols)} symbols on {TIMEFRAME}")

        # Listen for messages
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=60)
                await handle_kline_message(raw)
            except asyncio.TimeoutError:
                # Send ping to keep alive
                await ws.send(json.dumps({"method": "ping"}))
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
