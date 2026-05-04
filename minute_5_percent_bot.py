import os
import time
import random
import subprocess
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus


# ===== API KEYS =====
API_KEY = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")

if not API_KEY or not SECRET_KEY:
    print("Missing ALPACA API keys")
    raise SystemExit


# ===== CLIENTS =====
client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
trading_client = TradingClient(API_KEY, SECRET_KEY)


# ===== SETTINGS =====
DATA_FEED = DataFeed.SIP

MIN_PRICE = 0.20
MAX_PRICE = 20.0

MAX_SYMBOLS = 10000
CHUNK_SIZE = 500
SCAN_EVERY_SECONDS = 5

MIN_VOLUME = 50000
MIN_VOLUME_RATIO = 2.0

COOLDOWN_SECONDS = 90

LOOKBACK_MINUTES = 12

last_alert = {}


def israel_time():
    return datetime.now(
        ZoneInfo("Asia/Jerusalem")
    ).strftime("%Y-%m-%d %H:%M:%S")


def send_telegram(message):
    subprocess.run(
        ["python3", "scripts/send_telegram.py", message],
        check=False
    )


def change_percent(start_price, current_price):
    if start_price <= 0:
        return 0

    return ((current_price - start_price) / start_price) * 100


def can_send(symbol):
    now = time.time()
    last_time = last_alert.get(symbol, 0)

    if now - last_time >= COOLDOWN_SECONDS:
        last_alert[symbol] = now
        return True

    return False


def split_chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def get_symbols():
    request = GetAssetsRequest(
        asset_class=AssetClass.US_EQUITY,
        status=AssetStatus.ACTIVE
    )

    assets = trading_client.get_all_assets(request)
    symbols = []

    for asset in assets:
        symbol = asset.symbol

        if not asset.tradable:
            continue

        if not symbol:
            continue

        if "." in symbol or "/" in symbol:
            continue

        if asset.exchange not in ["NYSE", "NASDAQ", "AMEX"]:
            continue

        symbols.append(symbol)

    random.shuffle(symbols)
    symbols = symbols[:MAX_SYMBOLS]

    print(f"Loaded symbols: {len(symbols)}")
    return symbols


def get_bars(symbols):
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=LOOKBACK_MINUTES)

    request = StockBarsRequest(
        symbol_or_symbols=symbols,
        timeframe=TimeFrame.Minute,
        start=start_time,
        end=end_time,
        feed=DATA_FEED
    )

    return client.get_stock_bars(request)


def get_window_change(bars, minutes):
    if len(bars) < minutes:
        return 0

    start_price = float(bars[-minutes].open)
    current_price = float(bars[-1].close)

    return change_percent(start_price, current_price)


def is_near_recent_high(bars):
    if len(bars) < 3:
        return False

    current_price = float(bars[-1].close)
    previous_bars = bars[:-1]

    recent_high = max(float(bar.high) for bar in previous_bars)

    return current_price >= recent_high * 0.997


def get_volume_ratio(bars):
    if len(bars) < 3:
        return 0

    last_volume = int(bars[-1].volume)
    previous_bars = bars[:-1]

    avg_volume = sum(int(bar.volume) for bar in previous_bars) / len(previous_bars)

    if avg_volume <= 0:
        return 0

    return last_volume / avg_volume


def get_alert_level(change_1m, change_3m, change_5m, change_10m):
    strong_alert = (
        change_1m >= 5
        or change_5m >= 8
        or change_10m >= 15
    )

    medium_alert = (
        change_1m >= 3
        or change_3m >= 5
        or change_5m >= 5
        or change_10m >= 10
    )

    early_alert = (
        change_1m >= 1.5
        or change_3m >= 3
        or change_5m >= 4
    )

    if strong_alert:
        return "strong"

    if medium_alert:
        return "medium"

    if early_alert:
        return "early"

    return None


def check_momentum(symbols):
    for chunk in split_chunks(symbols, CHUNK_SIZE):
        try:
            barset = get_bars(chunk)
        except Exception as e:
            print(f"Bars error for chunk: {e}")
            continue

        for symbol in chunk:
            bars = barset.data.get(symbol, [])

            if len(bars) < 3:
                continue

            bars = sorted(bars, key=lambda b: b.timestamp)

            last_bar = bars[-1]

            last_open = float(last_bar.open)
            current_price = float(last_bar.close)
            last_volume = int(last_bar.volume)

            if last_open <= 0 or current_price <= 0:
                continue

            if current_price < MIN_PRICE or current_price > MAX_PRICE:
                continue

            change_1m = change_percent(last_open, current_price)
            change_3m = get_window_change(bars, 3)
            change_5m = get_window_change(bars, 5)
            change_10m = get_window_change(bars, 10)

            volume_ratio = get_volume_ratio(bars)

            is_green_bar = current_price > last_open
            near_high = is_near_recent_high(bars)

            alert_level = get_alert_level(
                change_1m,
                change_3m,
                change_5m,
                change_10m
            )

            print(
                f"{symbol} | "
                f"1m: {change_1m:.2f}% | "
                f"3m: {change_3m:.2f}% | "
                f"5m: {change_5m:.2f}% | "
                f"10m: {change_10m:.2f}% | "
                f"price: {current_price} | "
                f"volume: {last_volume:,} | "
                f"ratio: {volume_ratio:.2f}x | "
                f"green: {is_green_bar} | "
                f"near high: {near_high}"
            )

            is_candidate = (
                alert_level is not None
                and last_volume >= MIN_VOLUME
                and volume_ratio >= MIN_VOLUME_RATIO
                and is_green_bar
                and near_high
            )

            if not is_candidate:
                continue

            if not can_send(symbol):
                continue

            message = (
    f"מניה: {symbol}\n"
    f"מחיר נוכחי: {current_price}\n"
    f"שינוי בדקה האחרונה: {change_1m:.2f}%\n"
    f"ווליום בדקה האחרונה: {last_volume:,}\n"
    f"מומנטום: 3דק: {change_3m:.2f}% | 5דק: {change_5m:.2f}% | 10דק: {change_10m:.2f}%"
            )

            send_telegram(message)


SYMBOLS = get_symbols()

print("MOMENTUM BOT STARTED")
print(f"TOTAL SYMBOLS: {len(SYMBOLS)}")
print(f"PRICE RANGE: {MIN_PRICE}$ - {MAX_PRICE}$")
print(f"SCAN EVERY: {SCAN_EVERY_SECONDS} seconds")
print(f"MIN VOLUME: {MIN_VOLUME:,}")
print(f"MIN VOLUME RATIO: {MIN_VOLUME_RATIO}x")
print(f"COOLDOWN: {COOLDOWN_SECONDS} seconds per symbol")
print("---------------------------------------------")

while True:
    try:
        check_momentum(SYMBOLS)
        time.sleep(SCAN_EVERY_SECONDS)

    except KeyboardInterrupt:
        print("MOMENTUM BOT STOPPED BY USER")
        break

    except Exception as e:
        print("ERROR:", e)
        time.sleep(10)
