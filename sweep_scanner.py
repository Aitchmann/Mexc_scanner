import asyncio
import json
import os
import urllib.parse
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
PROXY_URL = os.environ.get("PROXY_URL")

MEXC_FUTURES_WS = "wss://contract.mexc.com/ws"
MEXC_TICKERS_URL = "https://contract.mexc.com/api/v1/contract/ticker"

TIMEFRAME = "Min60"          # 1-hour candles (MEXC uses Min60)
LOOKBACK = 20                # bars to find swing high/low
ATR_PERIOD = 14
ATR_MULTIPLIER = 0.2         # You changed this to 0.2
COOLDOWN_BARS = 3            # bars before re-alerting same symbol
TOP_N = 80                   # You changed this to 80
# ────────────────────────────────────────────────────────

# Full browser headers to bypass MEXC's Web Application Firewall
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.mexc.com/",
    "Connection": "keep-alive"
}

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("TELEGRAM_TOKEN and TELEGRAM_CHAT_ID must be set.")
if not PROXY_URL:
    raise ValueError("PROXY_URL environment variable must be set. Check Render settings.")

bot = Bot(token=TELEGRAM_TOKEN)

# In-memory candle store
candle_store: dict[str, list[dict]] = {}
last_alert_bar: dict[str, int] = {}


def fetch_top_symbols(n: int = TOP_N) -> list[str]:
    """Return top N perpetual symbols by 24h volume from MEXC via Cloudflare Proxy."""
    proxied_url = f"{PROXY_URL}/?target={MEXC_TICKERS_URL}"
    
    resp = requests.get(proxied_url, headers=HTTP_HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    tickers = data.get("data", [])
    tickers.sort(key=lambda x: float(x.get("volume24", 0)), reverse=True)
    symbols = [t["symbol"] for t in tickers[:n]]
    print(f"[+] Top {n} symbols: {symbols}")
    return symbols


async def bootstrap_history(symbols: list[str]):
    """Fetch historical 1H candles for all symbols to prime the memory instantly."""
    print(f"[*] Bootstrapping historical candles for {len(symbols)} symbols...")
    
    for sym in symbols:
        try:
            # MEXC historical kline endpoint (limit 50 to cover the 36 needed)
            kline_url = f"https://contract.mexc.com/api/v1/contract/kline/{sym}?interval={TIMEFRAME}&limit=50"
            # URL-encode the target so the nested '?' doesn't break the proxy query string
            encoded_target = urllib.parse.quote(kline_url, safe='')
            proxied_url = f"{PROXY_URL}/?target={encoded_target}"
            
            resp = requests.get(proxied_url, headers=HTTP_HEADERS, timeout=15)
            resp.raise_for_status()
            data = resp.json().get("data", {})
            
            times = data.get("time", [])
            opens = data.get("open", [])
            highs = data.get("high", [])
            lows = data.get("low", [])
            closes = data.get("close", [])
            
            if not times:
                continue
                
            candles = []
            for i in range(len(times)):
                candles.append({
                    "time": int(times[i]),
                    "open": float(opens[i]),
                    "high": float(highs[i]),
                    "low": float(lows[i]),
                    "close": float(closes[i])
                })
            
            candle_store[sym] = candles
            print(f"[+] Bootstrapped {len(candles)} candles for {sym}")
            
            # Small delay to avoid hitting rate limits during the bulk fetch
            await asyncio.sleep(0.6)
            
        except Exception as e:
            print(f"[!] Failed to bootstrap {sym}: {e}")
            
    print("[*] Bootstrap complete. Starting WebSocket subscription...")


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
    ts = datetime.fromtimestamp(details["time"], tz=timezone.utc).strftime(
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
    """Process incoming kline WebSocket messages from MEXC."""
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return

    channel = msg.get("channel", "")
    if channel != "push.kline":
        return

    data = msg.get("data")
    if not data:
        return

    symbol = data.get("symbol")
    if not symbol:
        return

    candle = {
        "time": data.get("t"),
        "open": float(data.get("o", 0)),
        "high": float(data.get("h", 0)),
        "low": float(data.get("l", 0)),
        "close": float(data.get("c", 0)),
    }

    store = candle_store.setdefault(symbol, [])

    if store and store[-1]["time"] == candle["time"]:
        store[-1] = candle
        return
    else:
        if store:
            if len(store) >= LOOKBACK + ATR_PERIOD + 2:
                result = detect_liquidity_sweep(symbol, store)
                if result:
                    bar_index = len(store)
                    last = last_alert_bar.get(symbol, -999)
                    if bar_index - last >= COOLDOWN_BARS:
                        last_alert_bar[symbol] = bar_index
                        asyncio.create_task(send_alert(result))

    store.append(candle)

    if len(store) > 100:
        candle_store[symbol] = store[-100:]


async def subscribe_symbols(symbols: list[str]):
    """Connect to MEXC WebSocket via Cloudflare Worker proxy."""
    ws_base = PROXY_URL.replace("https://", "wss://")
    proxied_ws_url = f"{ws_base}/?target={MEXC_FUTURES_WS}"

    async with websockets.connect(proxied_ws_url, ping_interval=20) as ws:
        for sym in symbols:
            sub_msg = {
                "method": "sub.kline",
                "param": {"symbol": sym, "interval": TIMEFRAME},
            }
            await ws.send(json.dumps(sub_msg))
            await asyncio.sleep(0.05)

        print(f"[+] Subscribed to {len(symbols)} symbols on {TIMEFRAME}")

        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=60)
                await handle_kline_message(raw)
            except asyncio.TimeoutError:
                await ws.send(json.dumps({"method": "ping"}))
            except websockets.ConnectionClosed:
                print("[!] WebSocket closed. Reconnecting...")
                break


async def main_scanner():
    """Main scanner loop."""
    while True:
        try:
            symbols = fetch_top_symbols(TOP_N)
            await bootstrap_history(symbols)  # <-- NEW: Prime the memory instantly
            await subscribe_symbols(symbols)
        except Exception as e:
            print(f"[!] Error: {e}")
            print("[*] Waiting 60 seconds before retrying to avoid rate limits...")
            await asyncio.sleep(60)
            continue
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
