import os
import time
import random
import subprocess
import json
from urllib.request import urlopen
from urllib.parse import urlencode, quote_plus
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus


API_KEY = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

if not API_KEY or not SECRET_KEY:
    print("Missing ALPACA API keys")
    raise SystemExit

client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
trading_client = TradingClient(API_KEY, SECRET_KEY)

DATA_FEED = DataFeed.SIP

MIN_PRICE = 0.20
MAX_PRICE = 20.0
MAX_SYMBOLS = 10000

SCAN_EVERY_SECONDS = 5
CHUNK_SIZE = 500

MIN_ALERT_PERCENT =5
MIN_VOLUME = 200000
VOLUME_RATIO = 2.5
COOLDOWN_SECONDS = 60

NEWS_LOOKBACK_SECONDS = 3600
NEWS_MIN_SCORE = 2

POSITIVE_NEWS_KEYWORDS = [
    "fda", "approval", "approved", "clearance",
    "phase", "trial", "clinical", "positive results",
    "contract", "agreement", "partnership",
    "collaboration", "purchase order",
    "earnings beat", "record revenue", "raises guidance",
    "merger", "acquisition", "buyout",
    "investment", "patent", "launch", "license",
    "ai", "artificial intelligence", "crypto",
    "bitcoin", "defense", "military", "ev"
]

NEGATIVE_NEWS_KEYWORDS = [
    "offering", "public offering", "dilution",
    "reverse split", "delisting", "bankruptcy",
    "lawsuit", "investigation", "resigns"
]

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


def investing_search_link(symbol):
    query = quote_plus(symbol)
    return f"https://www.investing.com/search/?q={query}"


def get_alert_title(percent, has_strong_news):
    if percent >= 12:
        base_title = "💣 12%+ בדקה"
    elif percent >= 8:
        base_title = "🚀 8%+ בדקה"
    else:
        base_title = "🔥 5%+ בדקה"

    if has_strong_news:
        return f"{base_title} + חדשות 📰"

    return base_title


def score_news_text(text):
    text = text.lower()
    score = 0

    for word in POSITIVE_NEWS_KEYWORDS:
        if word in text:
            score += 1

    for word in NEGATIVE_NEWS_KEYWORDS:
        if word in text:
            score -= 3

    return score


def get_recent_news(symbol):
    try:
        if not FINNHUB_API_KEY:
            return None, 0

        today = datetime.now(timezone.utc).date()
        yesterday = today - timedelta(days=1)

        params = urlencode({
            "symbol": symbol,
            "from": yesterday.isoformat(),
            "to": today.isoformat(),
            "token": FINNHUB_API_KEY
        })

        url = f"https://finnhub.io/api/v1/company-news?{params}"
        response = urlopen(url, timeout=5)
        data = json.loads(response.read().decode("utf-8"))

        cutoff_time = int(time.time()) - NEWS_LOOKBACK_SECONDS

        best_news = None
        best_score = 0

        for news in data:
            news_time = news.get("datetime", 0)

            if news_time < cutoff_time:
                continue

            headline = news.get("headline", "")
            summary = news.get("summary", "")
            score = score_news_text(f"{headline} {summary}")

            if score > best_score:
                best_score = score
                best_news = news

        return best_news, best_score

    except Exception as e:
        print(f"News error for {symbol}: {e}")
        return None, 0


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
    start_time = end_time - timedelta(minutes=4)

    request = StockBarsRequest(
        symbol_or_symbols=symbols,
        timeframe=TimeFrame.Minute,
        start=start_time,
        end=end_time,
        feed=DATA_FEED
    )

    return client.get_stock_bars(request)


def get_buy_sell_pressure(last_bar):
    open_price = float(last_bar.open)
    close_price = float(last_bar.close)
    volume = int(last_bar.volume)

    if close_price > open_price:
        return volume, 0, "קונים שולטים"

    if close_price < open_price:
        return 0, volume, "מוכרים שולטים"

    return 0, 0, "מאוזן"


def check_momentum(symbols):
    for chunk in split_chunks(symbols, CHUNK_SIZE):
        barset = get_bars(chunk)

        for symbol in chunk:
            bars = barset.data.get(symbol, [])

            if len(bars) < 2:
                continue

            bars = sorted(bars, key=lambda b: b.timestamp)

            last_bar = bars[-1]
            previous_bars = bars[:-1]

            start_price = float(last_bar.open)
            current_price = float(last_bar.close)
            volume = int(last_bar.volume)

            if start_price <= 0 or current_price <= 0:
                continue

            if current_price < MIN_PRICE or current_price > MAX_PRICE:
                continue

            avg_volume = (
                sum(int(bar.volume) for bar in previous_bars)
                / len(previous_bars)
            )

            if avg_volume <= 0:
                continue

            percent = change_percent(start_price, current_price)
            volume_ratio = volume / avg_volume

            buy_pressure, sell_pressure, pressure_text = get_buy_sell_pressure(last_bar)

            print(
                f"[1 MIN] {symbol} | "
                f"change: {percent:.2f}% | "
                f"price: {current_price} | "
                f"volume: {volume:,} | "
                f"ratio: {volume_ratio:.2f}x | "
                f"{pressure_text}"
            )

            is_candidate = (
                percent >= MIN_ALERT_PERCENT
                and volume >= MIN_VOLUME
                and volume_ratio >= VOLUME_RATIO
                and buy_pressure > sell_pressure
            )

            if not is_candidate:
                continue

            if not can_send(symbol):
                continue

            news, news_score = get_recent_news(symbol)

            has_strong_news = (
                news is not None
                and news_score >= NEWS_MIN_SCORE
            )

            title = get_alert_title(percent, has_strong_news)

            if has_strong_news:
                headline = news.get("headline", "")
                source = news.get("source", "")
                news_url = news.get("url", "")

                news_part = (
                    f"📰 כותרת: {headline}\n"
                    f"ציון חדשות: {news_score}\n"
                    f"מקור: {source}\n"
                    f"קישור חדשות: {news_url}\n"
                )
            else:
                news_part = "חדשות חזקות בשעה האחרונה: לא נמצאו\n"

            investing_link = investing_search_link(symbol)

            message = (
                f"{title}\n"
                f"מניה: {symbol}\n"
                f"מחיר נוכחי: {current_price}\n"
                f"שינוי בדקה האחרונה: {percent:.2f}%\n"
                f"ווליום דקה אחרונה: {volume:,}\n"
                f"יחס ווליום: {volume_ratio:.2f}x\n"
                f"לחץ קונים משוער: {buy_pressure:,}\n"
                f"לחץ מוכרים משוער: {sell_pressure:,}\n"
                f"כיוון נר: {pressure_text}\n"
                f"{news_part}"
                f"🔎 Investing חיפוש: {investing_link}\n"
                f"שעה בישראל: {israel_time()}"
            )

            send_telegram(message)


SYMBOLS = get_symbols()

print("5% PER MINUTE BOT STARTED")
print(f"TOTAL SYMBOLS: {len(SYMBOLS)}")
print(f"PRICE RANGE: {MIN_PRICE}$ - {MAX_PRICE}$")
print(f"SCAN EVERY: {SCAN_EVERY_SECONDS} seconds")
print("ALERT LEVELS: 5%, 8%, 12% in 1 minute")
print(f"MIN VOLUME: {MIN_VOLUME:,}")
print(f"MIN VOLUME RATIO: {VOLUME_RATIO}x")
print(f"COOLDOWN: {COOLDOWN_SECONDS} seconds per symbol")
print("---------------------------------------------")

while True:
    try:
        check_momentum(SYMBOLS)
        time.sleep(SCAN_EVERY_SECONDS)

    except KeyboardInterrupt:
        print("5% PER MINUTE BOT STOPPED BY USER")
        break

    except Exception as e:
        print("ERROR:", e)
        time.sleep(10)
