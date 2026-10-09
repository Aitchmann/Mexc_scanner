import asyncio
import gc
import json
import os
from datetime import datetime, timezone

import requests
import websockets
from aiohttp import web
from telegram import Bot
from telegram.constants import ParseMode

# ── CONFIG (read from Fly.io Environment Variables) ─────
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
# Note: PROXY_URL is NO LONGER NEEDED. You can delete it from Fly.io secrets.

OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
OKX_TICKERS_URL = "https://www.okx.com/api/v5/market/tickers"

TIMEFRAME = "15"             # TESTING: 15-minute candles (OKX interval code)
LOOKBACK = 20
ATR_PERIOD = 14
ATR_MULTIPLIER = 0.01        # TESTING: Extremely sensitive
COOLDOWN_BARS = 3            # 3 bars = 45 minutes on 15m timeframe
TOP_N = 80
MAX_CANDLES = 50             # Optimized memory footprint
# ────────────────────────────────────────────────────────

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("TELEGRAM_TOKEN and TELEGRAM_CHAT_ID must be set.")

bot = Bot(token=TELEGRAM_TOKEN)
candle_store: dict[str, list[dict]] = {}
last_alert_time: dict[str, int] = {}


def fetch_top_symbols(n: int = TOP_N) -> list[str]:
    """Fetch top n perpetual symbols by 24h volume from OKX."""
    params = {"instType": "SWAP"}
    resp = requests.get(OKX_TICKERS_URL, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    tickers = data.get("data", [])
    # Filter for USDT-margined perpetuals and sort by 24h volume in USDT
    usdt_perps = [t for t in tickers if t.get("instId", "").endswith("USDT-SWAP")]
    usdt_perps.sort(key=lambda x: float(x.get("volCcy24h", 0)), reverse=True)
    symbols = [t["instId"] for t in usdt_perps[:n]]
    print(f"[+] Top {n} symbols: {symbols}")
    return symbols


async def bootstrap_history(symbols: list[str]):
    print(f"[*] Bootstrapping historical candles for {len(symbols)} symbols...")
    for sym in symbols:
        try:
            # OKX kline endpoint
            kline_url = f"https://www.okx.com/api/v5/market/candles?instId={sym}&bar={TIMEFRAME}m&limit=50"
            resp = requests.get(kline_url, timeout=15)
            resp.raise_for_status()
            
            data = resp.json().get("data", [])
            if not data:
                continue
            
            # OKX returns candles newest first, so reverse to oldest first
            data.reverse()
                
            candles = []
            for k in data:
                candles.append({
                    "time": int(k[0]) // 1000,  # Convert ms to seconds
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4])
                })
            
            candle_store[sym] = candles
            print(f"[+] Bootstrapped {len(candles)} candles for {sym}")
            await asyncio.sleep(0.4) # Respect OKX rate limits
            
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
        f"Timeframe: {TIMEFRAME}m"
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
    # DIAGNOSTIC: Print a dot for every message received
    print(".", end="", flush=True)
    
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return

    # OKX push format: {"arg": {"channel": "candle15m", "instId": "BTC-USDT-SWAP"}, "data": [[ts, o, h, l, c, ...]]}
    arg = msg.get("arg", {})
    if not arg.get("channel", "").startswith(f"candle{TIMEFRAME}"):
        return

    data = msg.get("data", [])
    if not data:
        return

    kline = data[0]
    symbol = arg.get("instId")
    if not symbol:
        return

    # OKX candle format: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
    # Only process confirmed (closed) candles (confirm == "1")
    if len(kline) < 9 or kline[8] != "1":
        return

    candle = {
        "time": int(kline[0]) // 1000,  # Convert ms to seconds
        "open": float(kline[1]),
        "high": float(kline[2]),
        "low": float(kline[3]),
        "close": float(kline[4]),
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
                    cooldown_seconds = COOLDOWN_BARS * 15 * 60
                    
                    if current_time - last_time >= cooldown_seconds:
                        last_alert_time[symbol] = current_time
                        asyncio.create_task(send_alert(result))

    store.append(candle)
    if len(store) > MAX_CANDLES:
        candle_store[symbol] = store[-MAX_CANDLES:]


async def subscribe_symbols(symbols: list[str]):
    print(f"\n[*] Connecting directly to OKX WebSocket...")
    
    async with websockets.connect(OKX_WS_URL, ping_interval=20) as ws:
        # OKX supports batch subscription
        args = [{"channel": f"candle{TIMEFRAME}m", "instId": sym} for sym in symbols]
        sub_msg = {"op": "subscribe", "args": args}
        
        await ws.send(json.dumps(sub_msg))
        print(f"\n[+] Subscribed to {len(symbols)} symbols on {TIMEFRAME}m")

        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=60)
                await handle_kline_message(raw)
            except asyncio.TimeoutError:
                await ws.send(json.dumps({"op": "ping"}))
            except websockets.ConnectionClosed:
                print("\n[!] WebSocket closed. Reconnecting...")
                break


async def main_scanner():
    symbols = fetch_top_symbols(TOP_N)
    await bootstrap_history(symbols)
    
    try:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text="✅ Bot successfully started. Switched to OKX. Entering testing mode."
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
