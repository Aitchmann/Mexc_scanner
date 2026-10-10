import asyncio
import gc
import json
import os
from datetime import datetime, timezone

import requests
import websockets
from aiohttp import web
from telegram import Bot

# ── CONFIG (read from Fly.io Environment Variables) ─────
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

HYPERLIQUID_API_URL = "https://api.hyperliquid.xyz/info"
HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"

TIMEFRAME = "1h"             # PRODUCTION: 1-hour candles
LOOKBACK = 20
ATR_PERIOD = 14
ATR_MULTIPLIER = 0.3         # Stricter filter (30% of ATR)
COOLDOWN_BARS = 3
TOP_N = 80
MAX_CANDLES = 50
# ────────────────────────────────────────────────────────

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("TELEGRAM_TOKEN and TELEGRAM_CHAT_ID must be set.")

bot = Bot(token=TELEGRAM_TOKEN)
candle_store: dict[str, list[dict]] = {}
last_alert_time: dict[str, int] = {}


def fetch_top_symbols(n: int = TOP_N) -> list[str]:
    payload = {"type": "metaAndAssetCtxs"}
    resp = requests.post(HYPERLIQUID_API_URL, json=payload, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    meta = data[0]
    asset_ctxs = data[1]

    universe = meta.get("universe", [])
    symbols = []
    for i, asset in enumerate(universe):
        if i >= len(asset_ctxs):
            break
        ctx = asset_ctxs[i]
        if asset.get("isDelisted", False):
            continue
        symbols.append({
            "name": asset["name"],
            "volume": float(ctx.get("dayNtlVlm", 0))
        })

    symbols.sort(key=lambda x: x["volume"], reverse=True)
    top_symbols = [s["name"] for s in symbols[:n]]
    print(f"[+] Top {n} symbols: {top_symbols}")
    return top_symbols


async def bootstrap_history(symbols: list[str]):
    print(f"[*] Bootstrapping historical candles for {len(symbols)} symbols...")
    
    if TIMEFRAME.endswith("m"):
        minutes = int(TIMEFRAME[:-1])
    elif TIMEFRAME.endswith("h"):
        minutes = int(TIMEFRAME[:-1]) * 60
    else:
        minutes = 60
    
    time_window_ms = 50 * minutes * 60 * 1000
    
    for sym in symbols:
        try:
            payload = {
                "type": "candleSnapshot",
                "req": {
                    "coin": sym,
                    "interval": TIMEFRAME,
                    "startTime": int(datetime.now(timezone.utc).timestamp() * 1000) - time_window_ms
                }
            }
            resp = requests.post(HYPERLIQUID_API_URL, json=payload, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            if not data:
                continue

            candles = []
            for k in data:
                candles.append({
                    "time": int(k["t"]) // 1000,
                    "open": float(k["o"]),
                    "high": float(k["h"]),
                    "low": float(k["l"]),
                    "close": float(k["c"])
                })

            candle_store[sym] = candles
            print(f"[+] Bootstrapped {len(candles)} candles for {sym}")
            await asyncio.sleep(0.2)

        except Exception as e:
            print(f"[!] Failed to bootstrap {sym}: {e}")

    print("[*] Bootstrap complete. Starting WebSocket subscription...")
    gc.collect()


def compute_atr(candles: list[dict], period: int = ATR_PERIOD) -> float:
    if len(candles) < period + 1:
        return 0.0

    trs = []
    for i in range(len(candles) - period, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i-1]["close"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)

    return sum(trs) / period


def detect_liquidity_sweep(symbol: str, candles: list[dict]) -> dict | None:
    if len(candles) < LOOKBACK + ATR_PERIOD + 2:
        return None
    current = candles[-1]
    history = candles[-(LOOKBACK + 2):-1]
    swing_high = max(c["high"] for c in history)
    swing_low = min(c["low"] for c in history)
    atr = compute_atr(candles)
    if atr == 0:
        return None

    # Bullish sweep (swept low, closed green)
    if current["low"] < swing_low - (atr * ATR_MULTIPLIER):
        if current["close"] > swing_low:
            # Reversal Confirmation: Requires a green candle
            # This allows both Pin Bars AND Bullish Engulfing Bars
            if current["close"] > current["open"]:
                return {
                    "symbol": symbol,
                    "direction": "BULLISH SWEEP (swept low)",
                    "swept_level": swing_low,
                    "candle_low": current["low"],
                    "close": current["close"],
                    "atr": round(atr, 6),
                    "time": current["time"],
                }

    # Bearish sweep (swept high, closed red)
    if current["high"] > swing_high + (atr * ATR_MULTIPLIER):
        if current["close"] < swing_high:
            # Reversal Confirmation: Requires a red candle
            # This allows both Pin Bars AND Bearish Engulfing Bars
            if current["close"] < current["open"]:
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
    ts = datetime.fromtimestamp(details["time"], tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M UTC"
    )
    direction = details["direction"]
    emoji = "🟢" if "BULLISH" in direction else "🔴"

    msg = (
        f"{emoji} LIQUIDITY SWEEP DETECTED\n"
        f"--------------------------\n"
        f"Symbol: {details['symbol']}\n"
        f"Direction: {direction}\n"
        f"Swept Level: {details['swept_level']:.6f}\n"
        f"Close: {details['close']:.6f}\n"
        f"ATR(14): {details['atr']:.6f}\n"
        f"Candle Time: {ts}\n"
        f"--------------------------\n"
        f"Timeframe: {TIMEFRAME}"
    )
    try:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=msg,
        )
        print(f"\n[ALERT SENT] {details['symbol']} — {direction}")
    except Exception as e:
        print(f"\n[!] Telegram error: {e}")


async def handle_kline_message(raw: str):
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return

    channel = msg.get("channel", "")
    if channel != "candle":
        return

    data = msg.get("data")
    if not data:
        return

    symbol = data.get("s")
    if not symbol:
        return

    candle = {
        "time": int(data.get("t", 0)) // 1000,
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
                    current_time = candle["time"]
                    last_time = last_alert_time.get(symbol, 0)
                    cooldown_seconds = COOLDOWN_BARS * 60 * 60

                    if current_time - last_time >= cooldown_seconds:
                        last_alert_time[symbol] = current_time
                        asyncio.create_task(send_alert(result))

    store.append(candle)
    if len(store) > MAX_CANDLES:
        candle_store[symbol] = store[-MAX_CANDLES:]


async def subscribe_symbols(symbols: list[str]):
    print(f"\n[*] Connecting directly to Hyperliquid WebSocket...")

    async with websockets.connect(HYPERLIQUID_WS_URL, ping_interval=20) as ws:
        for sym in symbols:
            sub_msg = {
                "method": "subscribe",
                "subscription": {
                    "type": "candle",
                    "coin": sym,
                    "interval": TIMEFRAME
                }
            }
            await ws.send(json.dumps(sub_msg))
            await asyncio.sleep(0.05)

        print(f"\n[+] Subscribed to {len(symbols)} symbols on {TIMEFRAME}")

        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=60)
                await handle_kline_message(raw)
            except asyncio.TimeoutError:
                await ws.send(json.dumps({"method": "ping"}))
            except websockets.ConnectionClosed:
                print("\n[!] WebSocket closed. Reconnecting...")
                break


async def main_scanner():
    symbols = fetch_top_symbols(TOP_N)
    await bootstrap_history(symbols)

    try:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text="✅ Bot successfully started. ATR 0.3 with Reversal Body Filter (Pin Bar / Engulfing)."
        )
        print("[+] Startup Telegram message sent.")
    except Exception as e:
        print(f"[!] Startup Telegram error: {e}")

    while True:
        try:
            await subscribe_symbols(symbols)
        except Exception as e:
            print(f"\n[!] WebSocket error: {e}")
            print("[*] Waiting 10 seconds before reconnecting...")
            await asyncio.sleep(10)


async def health_check(request):
    gc.collect()
    return web.Response(text="OK")


async def start_web_server():
    app = web.Application()
    app.router.add_get("/", health_check)
    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"[+] Health check server running on port {port}")


async def main():
    await asyncio.gather(
        main_scanner(),
        start_web_server(),
    )


if __name__ == "__main__":
    asyncio.run(main())
