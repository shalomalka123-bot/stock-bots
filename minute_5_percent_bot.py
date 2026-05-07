import os
import time
import json
import random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus

from urllib.request import urlopen, Request
from urllib.parse import urlencode


# =========================================================
# API KEYS
# =========================================================

ALPACA_API_KEY = (
    os.getenv("ALPACA_API_KEY")
    or os.getenv("APCA_API_KEY_ID")
)

ALPACA_SECRET_KEY = (
    os.getenv("ALPACA_SECRET_KEY")
    or os.getenv("APCA_API_SECRET_KEY")
)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    print("Missing Alpaca API keys")
    raise SystemExit


# =========================================================
# CLIENTS
# =========================================================

stock_client = StockHistoricalDataClient(
    ALPACA_API_KEY,
    ALPACA_SECRET_KEY
)

trading_client = TradingClient(
    ALPACA_API_KEY,
    ALPACA_SECRET_KEY
)


# =========================================================
# GENERAL SETTINGS
# =========================================================

DATA_FEED = DataFeed.SIP

MIN_PRICE = 0.50
MAX_PRICE = 20.00

MAX_SYMBOLS = 6000
CHUNK_SIZE = 200

SCAN_EVERY_SECONDS = 8
SLEEP_BETWEEN_CHUNKS = 0.15

LOOKBACK_MINUTES = 35

CANDIDATE_MAX_AGE_SECONDS = 6 * 60
CANDIDATE_MIN_CONFIRMATION_SECONDS = 45

ALERT_COOLDOWN_SECONDS = 4 * 60

STATS_FILE = "session_stats.json"

ISRAEL_TZ = ZoneInfo("Asia/Jerusalem")


# =========================================================
# SESSION-SPECIFIC RULES
# =========================================================
# הרעיון:
# פרה־מרקט ואפטר־מרקט מסוכנים יותר בגלל נזילות נמוכה,
# לכן שם הסינון קשוח יותר.

SESSION_RULES = {
    "premarket": {
        "min_last_volume": 40000,
        "min_candidate_volume_ratio": 3.0,
        "min_alert_volume_ratio": 4.0,
        "min_candidate_dollar_volume": 50000,
        "min_alert_dollar_volume": 80000,
        "quiet_range_max_percent": 6.0,
        "max_10m_change_before_alert": 22.0,
        "min_3m_candidate_change": 2.0,
        "min_5m_candidate_change": 3.5,
        "min_10m_candidate_change": 5.0,
        "min_3m_alert_change": 3.0,
        "min_5m_alert_change": 5.0,
        "min_10m_alert_change": 7.0,
    },

    "regular": {
        "min_last_volume": 30000,
        "min_candidate_volume_ratio": 2.2,
        "min_alert_volume_ratio": 3.0,
        "min_candidate_dollar_volume": 40000,
        "min_alert_dollar_volume": 70000,
        "quiet_range_max_percent": 7.0,
        "max_10m_change_before_alert": 25.0,
        "min_3m_candidate_change": 1.8,
        "min_5m_candidate_change": 3.0,
        "min_10m_candidate_change": 4.5,
        "min_3m_alert_change": 2.8,
        "min_5m_alert_change": 4.5,
        "min_10m_alert_change": 6.5,
    },

    "afterhours": {
        "min_last_volume": 50000,
        "min_candidate_volume_ratio": 3.5,
        "min_alert_volume_ratio": 5.0,
        "min_candidate_dollar_volume": 60000,
        "min_alert_dollar_volume": 100000,
        "quiet_range_max_percent": 5.5,
        "max_10m_change_before_alert": 20.0,
        "min_3m_candidate_change": 2.5,
        "min_5m_candidate_change": 4.0,
        "min_10m_candidate_change": 6.0,
        "min_3m_alert_change": 3.5,
        "min_5m_alert_change": 5.5,
        "min_10m_alert_change": 8.0,
    },
}


# =========================================================
# MEMORY
# =========================================================

candidates = {}

last_alert_time = {}
last_alert_price = {}
last_seen_alert_bar_time = {}

last_session = None


# =========================================================
# TELEGRAM
# =========================================================

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        return False

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

        data = urlencode({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message
        }).encode("utf-8")

        request = Request(
            url,
            data=data,
            method="POST"
        )

        with urlopen(request, timeout=25) as response:
            response.read()

        print("Telegram message sent")
        return True

    except Exception as e:
        print("Telegram send error:", e)
        return False


# =========================================================
# TIME / MARKET SESSION
# =========================================================

def get_israel_time():
    return datetime.now(ISRAEL_TZ)


def get_market_session():
    now = get_israel_time()

    current_minutes = now.hour * 60 + now.minute

    premarket_start = 11 * 60
    regular_start = 16 * 60 + 30
    regular_end = 23 * 60
    afterhours_end = 3 * 60

    if premarket_start <= current_minutes < regular_start:
        return "premarket"

    if regular_start <= current_minutes < regular_end:
        return "regular"

    if current_minutes >= regular_end or current_minutes < afterhours_end:
        return "afterhours"

    return None


def is_market_day_for_us_session():
    now = get_israel_time()
    weekday = now.weekday()
    current_minutes = now.hour * 60 + now.minute

    # Monday-Friday normally
    if weekday < 5:
        return True

    # Saturday before 03:00 Israel can still be Friday after-hours in the US
    if weekday == 5 and current_minutes < 3 * 60:
        return True

    return False


# =========================================================
# SESSION STATS
# =========================================================

def create_empty_session_stats():
    return {
        "premarket": {
            "total_alerts": 0,
            "symbols": {},
            "prices": {}
        },
        "regular": {
            "total_alerts": 0,
            "symbols": {},
            "prices": {}
        },
        "afterhours": {
            "total_alerts": 0,
            "symbols": {},
            "prices": {}
        }
    }


def load_session_stats():
    if not os.path.exists(STATS_FILE):
        return create_empty_session_stats()

    try:
        with open(STATS_FILE, "r") as file:
            return json.load(file)

    except Exception as e:
        print("Stats load error:", e)
        return create_empty_session_stats()


session_stats = load_session_stats()


def save_session_stats():
    try:
        with open(STATS_FILE, "w") as file:
            json.dump(session_stats, file)

    except Exception as e:
        print("Stats save error:", e)


def update_session_stats(session_name, symbol, price):
    stats = session_stats[session_name]

    stats["total_alerts"] += 1

    stats["symbols"][symbol] = stats["symbols"].get(symbol, 0) + 1

    if symbol not in stats["prices"]:
        stats["prices"][symbol] = {
            "first": price,
            "last": price
        }
    else:
        stats["prices"][symbol]["last"] = price

    save_session_stats()


def reset_session_stats(session_name):
    session_stats[session_name] = {
        "total_alerts": 0,
        "symbols": {},
        "prices": {}
    }

    save_session_stats()


def change_percent(start_price, current_price):
    if start_price <= 0:
        return 0.0

    return ((current_price - start_price) / start_price) * 100


def send_session_summary(session_name):
    stats = session_stats[session_name]
    total_alerts = stats["total_alerts"]

    names = {
        "premarket": "פרה־מרקט",
        "regular": "מסחר רגיל",
        "afterhours": "אפטר־מרקט"
    }

    display_name = names.get(session_name, session_name)

    if total_alerts == 0:
        send_telegram(
            f"📊 סיכום {display_name}\n\n"
            f"לא נשלחו התראות בסשן הזה."
        )
        return

    top_symbols = sorted(
        stats["symbols"].items(),
        key=lambda x: x[1],
        reverse=True
    )[:5]

    best_symbol = None
    best_change = -999

    for symbol, data in stats["prices"].items():
        first_price = data["first"]
        last_price = data["last"]

        move = change_percent(first_price, last_price)

        if move > best_change:
            best_change = move
            best_symbol = (
                symbol,
                first_price,
                last_price,
                move
            )

    message = (
        f"📊 סיכום {display_name}\n\n"
        f"סה״כ התראות: {total_alerts}\n\n"
        f"🔥 הכי הרבה הופיעו:\n"
    )

    for i, (symbol, count) in enumerate(top_symbols, start=1):
        message += f"{i}. {symbol} — {count} פעמים\n"

    if best_symbol:
        symbol, first_price, last_price, move = best_symbol

        message += (
            f"\n🚀 המעקב הכי חזק מתוך ההתראות:\n"
            f"{symbol}\n"
            f"מחיר ראשון: {first_price:.4f}$\n"
            f"מחיר אחרון: {last_price:.4f}$\n"
            f"שינוי אחרי ההתראה הראשונה: {move:.2f}%"
        )

    send_telegram(message)


def handle_session_change():
    global last_session

    current_session = get_market_session()

    if current_session != last_session:
        if last_session is not None:
            print(f"Session ended: {last_session}")
            send_session_summary(last_session)
            reset_session_stats(last_session)

        if current_session is not None:
            print(f"Session started: {current_session}")

        last_session = current_session


# =========================================================
# BASIC HELPERS
# =========================================================

def split_chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def safe_float(value):
    try:
        return float(value)
    except Exception:
        return 0.0


def safe_int(value):
    try:
        return int(value)
    except Exception:
        return 0


def get_bar_time_string(bar):
    try:
        return bar.timestamp.isoformat()
    except Exception:
        return str(bar.timestamp)


# =========================================================
# SYMBOLS
# =========================================================

def get_symbols():
    request = GetAssetsRequest(
        asset_class=AssetClass.US_EQUITY,
        status=AssetStatus.ACTIVE
    )

    assets = trading_client.get_all_assets(request)

    symbols = []

    for asset in assets:
        if not asset.tradable:
            continue

        symbol = asset.symbol

        if not symbol:
            continue

        if "." in symbol:
            continue

        if "/" in symbol:
            continue

        if asset.exchange not in ["NYSE", "NASDAQ", "AMEX"]:
            continue

        symbols.append(symbol)

    random.shuffle(symbols)

    symbols = symbols[:MAX_SYMBOLS]

    print(f"Loaded symbols: {len(symbols)}")

    return symbols


# =========================================================
# MARKET DATA
# =========================================================

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

    return stock_client.get_stock_bars(request)


# =========================================================
# BAR ANALYSIS
# =========================================================

def get_window_change(bars, minutes):
    if len(bars) < minutes + 1:
        return 0.0

    start_price = safe_float(bars[-minutes].open)
    current_price = safe_float(bars[-1].close)

    return change_percent(start_price, current_price)


def get_range_percent(bars):
    if not bars:
        return 0.0

    highs = [safe_float(bar.high) for bar in bars]
    lows = [safe_float(bar.low) for bar in bars]

    highest = max(highs)
    lowest = min(lows)

    if lowest <= 0:
        return 0.0

    return change_percent(lowest, highest)


def get_volume_ratio(bars, lookback=15):
    if len(bars) < lookback + 1:
        return 0.0

    last_volume = safe_int(bars[-1].volume)

    previous_bars = bars[-lookback - 1:-1]

    volumes = [
        safe_int(bar.volume)
        for bar in previous_bars
        if safe_int(bar.volume) > 0
    ]

    if not volumes:
        return 0.0

    average_volume = sum(volumes) / len(volumes)

    if average_volume <= 0:
        return 0.0

    return last_volume / average_volume


def get_recent_high(bars, lookback=10):
    if len(bars) < lookback + 1:
        return None

    previous_bars = bars[-lookback - 1:-1]

    return max(
        safe_float(bar.high)
        for bar in previous_bars
    )


def is_new_high(bars, lookback=10):
    recent_high = get_recent_high(bars, lookback)

    if recent_high is None:
        return False

    current_price = safe_float(bars[-1].close)

    return current_price > recent_high


def is_near_high(bars, lookback=10, factor=0.995):
    recent_high = get_recent_high(bars, lookback)

    if recent_high is None:
        return False

    current_price = safe_float(bars[-1].close)

    return current_price >= recent_high * factor


def count_green_bars(bars, amount=5):
    recent_bars = bars[-amount:]

    green = 0

    for bar in recent_bars:
        if safe_float(bar.close) > safe_float(bar.open):
            green += 1

    return green


def candle_close_strength(bar):
    high = safe_float(bar.high)
    low = safe_float(bar.low)
    close = safe_float(bar.close)

    if high <= low:
        return 0.0

    return (close - low) / (high - low)


def is_good_close(bar):
    # סגירה בחלק העליון של הנר
    return candle_close_strength(bar) >= 0.68


def is_current_bar_green(bar):
    return safe_float(bar.close) > safe_float(bar.open)


def is_accelerating(change_1m, change_3m, change_5m):
    if change_1m <= 0:
        return False

    avg_3m = change_3m / 3
    avg_5m = change_5m / 5

    return (
        change_1m >= avg_3m
        and change_1m >= avg_5m
    )


def detect_consolidation_breakout(bars, lookback=6, max_range_percent=6.0):
    if len(bars) < lookback + 1:
        return False, False

    last_bar = bars[-1]

    consolidation_bars = bars[-lookback - 1:-1]

    consolidation_high = max(
        safe_float(bar.high)
        for bar in consolidation_bars
    )

    consolidation_low = min(
        safe_float(bar.low)
        for bar in consolidation_bars
    )

    if consolidation_low <= 0:
        return False, False

    consolidation_range = change_percent(
        consolidation_low,
        consolidation_high
    )

    has_consolidation = consolidation_range <= max_range_percent

    last_close = safe_float(last_bar.close)
    last_open = safe_float(last_bar.open)
    last_volume = safe_int(last_bar.volume)

    previous_volumes = [
        safe_int(bar.volume)
        for bar in consolidation_bars
        if safe_int(bar.volume) > 0
    ]

    if not previous_volumes:
        return has_consolidation, False

    average_volume = sum(previous_volumes) / len(previous_volumes)

    breakout = (
        has_consolidation
        and last_close > last_open
        and last_close > consolidation_high
        and last_volume >= average_volume * 2.0
        and is_good_close(last_bar)
    )

    return has_consolidation, breakout


def was_quiet_before_move(bars, rules):
    if len(bars) < 25:
        return False

    # בודקים את האזור שלפני 5 הדקות האחרונות.
    # אם שם המניה כבר הייתה פראית, זו לא התחלה נקייה.
    quiet_bars = bars[-25:-6]

    quiet_range = get_range_percent(quiet_bars)

    return quiet_range <= rules["quiet_range_max_percent"]


def is_too_extended(change_10m, rules, consolidation_breakout):
    if consolidation_breakout:
        return False

    return change_10m > rules["max_10m_change_before_alert"]


def get_day_change_from_available_bars(bars):
    if not bars:
        return 0.0

    first_open = safe_float(bars[0].open)
    current_price = safe_float(bars[-1].close)

    return change_percent(first_open, current_price)


def dollar_volume(price, volume):
    return price * volume


# =========================================================
# CANDIDATE LOGIC
# =========================================================

def is_candidate_setup(
    symbol,
    bars,
    rules,
    current_session
):
    if len(bars) < 25:
        return False, {}

    last_bar = bars[-1]

    current_price = safe_float(last_bar.close)
    last_open = safe_float(last_bar.open)
    last_volume = safe_int(last_bar.volume)

    if current_price <= 0:
        return False, {}

    if current_price < MIN_PRICE or current_price > MAX_PRICE:
        return False, {}

    if not is_current_bar_green(last_bar):
        return False, {}

    change_1m = change_percent(last_open, current_price)
    change_3m = get_window_change(bars, 3)
    change_5m = get_window_change(bars, 5)
    change_10m = get_window_change(bars, 10)

    volume_ratio = get_volume_ratio(bars, 15)
    dollar_vol = dollar_volume(current_price, last_volume)

    green_last_3 = count_green_bars(bars, 3)
    green_last_5 = count_green_bars(bars, 5)

    near_high_10 = is_near_high(bars, 10)
    new_high_10 = is_new_high(bars, 10)

    accelerating = is_accelerating(
        change_1m,
        change_3m,
        change_5m
    )

    has_consolidation, consolidation_breakout = detect_consolidation_breakout(
        bars,
        lookback=6,
        max_range_percent=rules["quiet_range_max_percent"]
    )

    quiet_before = was_quiet_before_move(bars, rules)

    current_good_close = is_good_close(last_bar)

    enough_price_move = (
        change_3m >= rules["min_3m_candidate_change"]
        or change_5m >= rules["min_5m_candidate_change"]
        or change_10m >= rules["min_10m_candidate_change"]
    )

    enough_volume = (
        last_volume >= rules["min_last_volume"]
        and volume_ratio >= rules["min_candidate_volume_ratio"]
        and dollar_vol >= rules["min_candidate_dollar_volume"]
    )

    has_structure = (
        near_high_10
        or new_high_10
        or consolidation_breakout
    )

    not_too_late = not is_too_extended(
        change_10m,
        rules,
        consolidation_breakout
    )

    # תנאי מועמדת:
    # לא שולחים עדיין, רק מכניסים למעקב.
    is_candidate = (
        enough_price_move
        and enough_volume
        and has_structure
        and quiet_before
        and current_good_close
        and green_last_3 >= 2
        and not_too_late
    )

    data = {
        "symbol": symbol,
        "session": current_session,
        "price": current_price,
        "last_volume": last_volume,
        "dollar_volume": dollar_vol,
        "volume_ratio": volume_ratio,
        "change_1m": change_1m,
        "change_3m": change_3m,
        "change_5m": change_5m,
        "change_10m": change_10m,
        "green_last_3": green_last_3,
        "green_last_5": green_last_5,
        "near_high_10": near_high_10,
        "new_high_10": new_high_10,
        "accelerating": accelerating,
        "has_consolidation": has_consolidation,
        "consolidation_breakout": consolidation_breakout,
        "quiet_before": quiet_before,
        "good_close": current_good_close,
        "bar_time": get_bar_time_string(last_bar),
        "day_change_from_bars": get_day_change_from_available_bars(bars)
    }

    return is_candidate, data


def add_or_update_candidate(symbol, data):
    now = time.time()

    if symbol not in candidates:
        candidates[symbol] = {
            "created_at": now,
            "first_price": data["price"],
            "best_price": data["price"],
            "first_bar_time": data["bar_time"],
            "last_seen": now,
            "last_data": data
        }

        print(
            f"CANDIDATE START | {symbol} | "
            f"price={data['price']:.4f} | "
            f"3m={data['change_3m']:.2f}% | "
            f"5m={data['change_5m']:.2f}% | "
            f"vol_ratio={data['volume_ratio']:.2f}x | "
            f"$vol={data['dollar_volume']:.0f}"
        )

    else:
        candidates[symbol]["last_seen"] = now
        candidates[symbol]["last_data"] = data

        if data["price"] > candidates[symbol]["best_price"]:
            candidates[symbol]["best_price"] = data["price"]


def remove_old_candidates():
    now = time.time()

    to_delete = []

    for symbol, info in candidates.items():
        age = now - info["created_at"]

        if age > CANDIDATE_MAX_AGE_SECONDS:
            to_delete.append(symbol)
            continue

        last_seen_gap = now - info["last_seen"]

        if last_seen_gap > 120:
            to_delete.append(symbol)
            continue

    for symbol in to_delete:
        print(f"CANDIDATE REMOVED | {symbol} | expired or weak")
        del candidates[symbol]


def candidate_is_confirmed(symbol, bars, rules):
    if symbol not in candidates:
        return False, {}

    info = candidates[symbol]
    now = time.time()

    candidate_age = now - info["created_at"]

    if candidate_age < CANDIDATE_MIN_CONFIRMATION_SECONDS:
        return False, {}

    last_bar = bars[-1]

    current_price = safe_float(last_bar.close)
    last_open = safe_float(last_bar.open)
    last_volume = safe_int(last_bar.volume)

    first_price = info["first_price"]

    if current_price <= first_price:
        return False, {}

    change_from_candidate = change_percent(first_price, current_price)

    change_1m = change_percent(last_open, current_price)
    change_3m = get_window_change(bars, 3)
    change_5m = get_window_change(bars, 5)
    change_10m = get_window_change(bars, 10)

    volume_ratio = get_volume_ratio(bars, 15)
    dollar_vol = dollar_volume(current_price, last_volume)

    green_last_3 = count_green_bars(bars, 3)
    green_last_5 = count_green_bars(bars, 5)

    new_high_10 = is_new_high(bars, 10)
    near_high_10 = is_near_high(bars, 10)

    has_consolidation, consolidation_breakout = detect_consolidation_breakout(
        bars,
        lookback=6,
        max_range_percent=rules["quiet_range_max_percent"]
    )

    current_good_close = is_good_close(last_bar)

    accelerating = is_accelerating(
        change_1m,
        change_3m,
        change_5m
    )

    enough_alert_move = (
        change_3m >= rules["min_3m_alert_change"]
        or change_5m >= rules["min_5m_alert_change"]
        or change_10m >= rules["min_10m_alert_change"]
    )

    enough_alert_volume = (
        last_volume >= rules["min_last_volume"]
        and volume_ratio >= rules["min_alert_volume_ratio"]
        and dollar_vol >= rules["min_alert_dollar_volume"]
    )

    confirmed_breakout = (
        new_high_10
        or consolidation_breakout
    )

    holds_above_candidate = (
        change_from_candidate >= 0.7
    )

    not_too_late = not is_too_extended(
        change_10m,
        rules,
        consolidation_breakout
    )

    confirmed = (
        enough_alert_move
        and enough_alert_volume
        and confirmed_breakout
        and holds_above_candidate
        and current_good_close
        and green_last_3 >= 2
        and not_too_late
    )

    data = {
        "symbol": symbol,
        "price": current_price,
        "first_candidate_price": first_price,
        "change_from_candidate": change_from_candidate,
        "last_volume": last_volume,
        "dollar_volume": dollar_vol,
        "volume_ratio": volume_ratio,
        "change_1m": change_1m,
        "change_3m": change_3m,
        "change_5m": change_5m,
        "change_10m": change_10m,
        "green_last_3": green_last_3,
        "green_last_5": green_last_5,
        "near_high_10": near_high_10,
        "new_high_10": new_high_10,
        "accelerating": accelerating,
        "has_consolidation": has_consolidation,
        "consolidation_breakout": consolidation_breakout,
        "good_close": current_good_close,
        "candidate_age_seconds": candidate_age,
        "bar_time": get_bar_time_string(last_bar),
        "day_change_from_bars": get_day_change_from_available_bars(bars)
    }

    return confirmed, data


# =========================================================
# ALERT CONTROL
# =========================================================

def can_send_alert(symbol, price, bar_time):
    now = time.time()

    previous_time = last_alert_time.get(symbol, 0)
    previous_price = last_alert_price.get(symbol)
    previous_bar_time = last_seen_alert_bar_time.get(symbol)

    if previous_bar_time == bar_time:
        return False

    if now - previous_time < ALERT_COOLDOWN_SECONDS:
        return False

    if previous_price is not None and price <= previous_price:
        return False

    last_alert_time[symbol] = now
    last_alert_price[symbol] = price
    last_seen_alert_bar_time[symbol] = bar_time

    return True


def get_alert_strength(data):
    strong_conditions = 0

    if data["change_5m"] >= 8:
        strong_conditions += 1

    if data["change_10m"] >= 10:
        strong_conditions += 1

    if data["volume_ratio"] >= 6:
        strong_conditions += 1

    if data["dollar_volume"] >= 150000:
        strong_conditions += 1

    if data["consolidation_breakout"]:
        strong_conditions += 1

    if data["new_high_10"]:
        strong_conditions += 1

    if strong_conditions >= 4:
        return "STRONG"

    if strong_conditions >= 2:
        return "GOOD"

    return "EARLY"


def build_alert_message(symbol, data, current_session):
    session_names = {
        "premarket": "פרה־מרקט",
        "regular": "מסחר רגיל",
        "afterhours": "אפטר־מרקט"
    }

    session_display = session_names.get(current_session, current_session)

    alert_strength = get_alert_strength(data)

    message = (
        f"🚨 מניה: {symbol}\n"
        f"סוג התראה: {alert_strength}\n"
        f"סשן: {session_display}\n\n"

        f"💰 מחיר נוכחי: {data['price']:.4f}$\n"
        f"📍 מחיר בתחילת המעקב: {data['first_candidate_price']:.4f}$\n"
        f"📈 שינוי מאז כניסה למעקב: {data['change_from_candidate']:.2f}%\n\n"

        f"📈 שינוי 1 דק׳: {data['change_1m']:.2f}%\n"
        f"📈 שינוי 3 דק׳: {data['change_3m']:.2f}%\n"
        f"📈 שינוי 5 דק׳: {data['change_5m']:.2f}%\n"
        f"📈 שינוי 10 דק׳: {data['change_10m']:.2f}%\n\n"

        f"📊 ווליום דקה אחרונה: {data['last_volume']:,}\n"
        f"💵 דולר ווליום דקה אחרונה: ${data['dollar_volume']:,.0f}\n"
        f"📊 יחס ווליום: {data['volume_ratio']:.2f}x\n\n"

        f"🟢 נרות ירוקים אחרונים: {data['green_last_3']}/3\n"
        f"🏆 שיא 10 דק׳ חדש: {'כן' if data['new_high_10'] else 'לא'}\n"
        f"📦 פריצה מהתכנסות: {'כן' if data['consolidation_breakout'] else 'לא'}\n"
        f"🧱 סגירה חזקה בנר: {'כן' if data['good_close'] else 'לא'}\n"
        f"⚡ האצה בתנועה: {'כן' if data['accelerating'] else 'לא'}\n\n"

        f"🧠 פירוש:\n"
        f"המניה התחילה לזוז, נכנסה למעקב, ואז אישרה המשכיות עם ווליום + פריצה + החזקת מחיר."
    )

    return message


# =========================================================
# MAIN SCANNER
# =========================================================

def check_momentum(symbols):
    current_session = get_market_session()

    if current_session is None:
        return

    rules = SESSION_RULES[current_session]

    remove_old_candidates()

    for chunk in split_chunks(symbols, CHUNK_SIZE):
        try:
            barset = get_bars(chunk)

        except Exception as e:
            print(f"Bars error: {e}")
            time.sleep(2)
            continue

        for symbol in chunk:
            try:
                bars = barset.data.get(symbol, [])

                if len(bars) < 25:
                    continue

                bars = sorted(
                    bars,
                    key=lambda b: b.timestamp
                )

                # קודם בודקים האם מועמדת קיימת קיבלה אישור.
                if symbol in candidates:
                    confirmed, confirmed_data = candidate_is_confirmed(
                        symbol,
                        bars,
                        rules
                    )

                    if confirmed:
                        price = confirmed_data["price"]
                        bar_time = confirmed_data["bar_time"]

                        if can_send_alert(symbol, price, bar_time):
                            update_session_stats(
                                current_session,
                                symbol,
                                price
                            )

                            print(
                                f"ALERT SENT | {symbol} | "
                                f"price={price:.4f} | "
                                f"from_candidate={confirmed_data['change_from_candidate']:.2f}% | "
                                f"3m={confirmed_data['change_3m']:.2f}% | "
                                f"5m={confirmed_data['change_5m']:.2f}% | "
                                f"10m={confirmed_data['change_10m']:.2f}% | "
                                f"vol_ratio={confirmed_data['volume_ratio']:.2f}x | "
                                f"$vol={confirmed_data['dollar_volume']:.0f}"
                            )

                            message = build_alert_message(
                                symbol,
                                confirmed_data,
                                current_session
                            )

                            send_telegram(message)

                            # אחרי התראה מוחקים מהמועמדות,
                            # כדי שלא יישלח שוב מיד.
                            if symbol in candidates:
                                del candidates[symbol]

                        continue

                # אם אין מועמדת קיימת, בודקים האם להתחיל מעקב.
                is_candidate, candidate_data = is_candidate_setup(
                    symbol,
                    bars,
                    rules,
                    current_session
                )

                if is_candidate:
                    add_or_update_candidate(
                        symbol,
                        candidate_data
                    )

            except Exception as e:
                print(f"Symbol error {symbol}: {e}")
                continue

        time.sleep(SLEEP_BETWEEN_CHUNKS)


# =========================================================
# START
# =========================================================

SYMBOLS = get_symbols()

print("EARLY MOMENTUM CONFIRMATION BOT STARTED")
print(f"TOTAL SYMBOLS: {len(SYMBOLS)}")
print("----------------------------------")

send_telegram(
    "✅ בוט מומנטום מוקדם הופעל\n\n"
    "הלוגיקה החדשה פעילה:\n"
    "1. קודם מזהה מניה כמועמדת\n"
    "2. מחכה לאישור המשכיות\n"
    "3. שולח רק אם יש פריצה + ווליום + החזקת מחיר\n\n"
    "המטרה: פחות רעש, יותר מניות בתחילת מהלך אמיתי."
)

while True:
    try:
        if is_market_day_for_us_session():
            handle_session_change()
            check_momentum(SYMBOLS)

        else:
            print("Market closed / weekend - sleeping")

        time.sleep(SCAN_EVERY_SECONDS)

    except KeyboardInterrupt:
        print("BOT STOPPED")
        break

    except Exception as e:
        print("MAIN LOOP ERROR:", e)
        time.sleep(15)
