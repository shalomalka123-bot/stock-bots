import os
import time
import json
import random
import subprocess

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus


# =========================================================
# API KEYS
# =========================================================
API_KEY = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")

SECRET_KEY = (
    os.getenv("ALPACA_SECRET_KEY")
    or os.getenv("APCA_API_SECRET_KEY")
)

if not API_KEY or not SECRET_KEY:
    print("Missing Alpaca API keys")
    raise SystemExit


# =========================================================
# CLIENTS
# =========================================================
client = StockHistoricalDataClient(
    API_KEY,
    SECRET_KEY
)

trading_client = TradingClient(
    API_KEY,
    SECRET_KEY
)


# =========================================================
# SETTINGS
# =========================================================
DATA_FEED = DataFeed.SIP

MIN_PRICE = 0.20
MAX_PRICE = 20.0

MAX_SYMBOLS = 6000

CHUNK_SIZE = 200

SCAN_EVERY_SECONDS = 8

LOOKBACK_MINUTES = 15

MIN_VOLUME = 25000

MIN_VOLUME_RATIO = 1.4

MIN_GREEN_BARS_LAST_5 = 3

MIN_STRUCTURE_SCORE = 3

MIN_QUALITY_SCORE = 60

COOLDOWN_SECONDS = 120

NEAR_HIGH_FACTOR = 0.990

CONSOLIDATION_LOOKBACK = 5

MAX_CONSOLIDATION_RANGE_PERCENT = 6.0

LATE_CHASE_DAY_CHANGE = 25

LATE_CHASE_10M_CHANGE = 12

LATE_CHASE_MIN_VOLUME_RATIO = 3

LATE_CHASE_MAX_STRUCTURE = 2

STATS_FILE = "session_stats.json"


# =========================================================
# MEMORY
# =========================================================
last_alert_time = {}

last_alert_price = {}

summary_sent_today = {
    "premarket": None,
    "regular": None,
    "afterhours": None
}


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


def save_session_stats():

    try:

        with open(STATS_FILE, "w") as file:

            json.dump(
                session_stats,
                file
            )

    except Exception as e:

        print("Stats save error:", e)


session_stats = load_session_stats()


# =========================================================
# TELEGRAM
# =========================================================
def send_telegram(message):

    try:

        result = subprocess.run(
            [
                "python3",
                "scripts/send_telegram.py",
                message
            ],
            capture_output=True,
            text=True,
            timeout=25
        )

        if result.returncode != 0:

            print(
                "Telegram send failed:",
                result.stderr
            )

            return False

        return True

    except Exception as e:

        print("Telegram exception:", e)

        return False


# =========================================================
# TIME
# =========================================================
def get_israel_time():

    return datetime.now(
        ZoneInfo("Asia/Jerusalem")
    )


def get_market_session():

    now = get_israel_time()

    current_minutes = (
        now.hour * 60
        + now.minute
    )

    premarket_start = 11 * 60

    regular_start = 16 * 60 + 30

    regular_end = 23 * 60

    afterhours_end = 3 * 60

    if (
        premarket_start
        <= current_minutes
        < regular_start
    ):
        return "premarket"

    if (
        regular_start
        <= current_minutes
        < regular_end
    ):
        return "regular"

    if (
        current_minutes >= regular_end
        or current_minutes < afterhours_end
    ):
        return "afterhours"

    return None


def is_market_day():

    now = get_israel_time()

    weekday = now.weekday()

    return weekday < 5


# =========================================================
# HELPERS
# =========================================================
def change_percent(
    start_price,
    current_price
):

    if start_price <= 0:
        return 0

    return (
        (
            current_price - start_price
        ) / start_price
    ) * 100


def split_chunks(items, size):

    for i in range(
        0,
        len(items),
        size
    ):

        yield items[i:i + size]


# =========================================================
# SYMBOLS
# =========================================================
def get_symbols():

    request = GetAssetsRequest(
        asset_class=AssetClass.US_EQUITY,
        status=AssetStatus.ACTIVE
    )

    assets = trading_client.get_all_assets(
        request
    )

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

        if asset.exchange not in [
            "NYSE",
            "NASDAQ",
            "AMEX"
        ]:
            continue

        symbols.append(symbol)

    random.shuffle(symbols)

    symbols = symbols[:MAX_SYMBOLS]

    print(
        f"Loaded symbols: "
        f"{len(symbols)}"
    )

    return symbols


# =========================================================
# MARKET DATA
# =========================================================
def get_bars(symbols):

    end_time = datetime.now(
        timezone.utc
    )

    start_time = (
        end_time
        - timedelta(
            minutes=LOOKBACK_MINUTES
        )
    )

    request = StockBarsRequest(
        symbol_or_symbols=symbols,
        timeframe=TimeFrame.Minute,
        start=start_time,
        end=end_time,
        feed=DATA_FEED
    )

    return client.get_stock_bars(
        request
    )


def get_day_bars(symbol):

    try:

        now = datetime.now(
            timezone.utc
        )

        start = now.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0
        )

        request = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame.Minute,
            start=start,
            end=now,
            feed=DATA_FEED
        )

        barset = client.get_stock_bars(
            request
        )

        bars = barset.data.get(
            symbol,
            []
        )

        return sorted(
            bars,
            key=lambda b: b.timestamp
        )

    except Exception as e:

        print(
            f"Day bars error "
            f"for {symbol}: {e}"
        )

        return []


def get_day_stats(
    symbol,
    current_price
):

    bars = get_day_bars(symbol)

    if not bars:
        return 0, None

    day_open = float(
        bars[0].open
    )

    day_high = max(
        float(bar.high)
        for bar in bars
    )

    day_change = change_percent(
        day_open,
        current_price
    )

    return (
        day_change,
        day_high
    )


def get_window_change(
    bars,
    minutes
):

    if len(bars) < minutes:
        return 0

    start_price = float(
        bars[-minutes].open
    )

    current_price = float(
        bars[-1].close
    )

    return change_percent(
        start_price,
        current_price
    )


# =========================================================
# ANALYSIS
# =========================================================
def get_recent_high_data(bars):

    if len(bars) < 3:
        return False, False

    current_price = float(
        bars[-1].close
    )

    previous_bars = bars[:-1]

    recent_high = max(
        float(bar.high)
        for bar in previous_bars
    )

    near_high = (
        current_price
        >= recent_high
        * NEAR_HIGH_FACTOR
    )

    new_high = (
        current_price
        >= recent_high
    )

    return (
        near_high,
        new_high
    )


def get_volume_ratio(bars):

    if len(bars) < 3:
        return 0

    last_volume = int(
        bars[-1].volume
    )

    previous_bars = bars[:-1]

    average_volume = (
        sum(
            int(bar.volume)
            for bar in previous_bars
        ) / len(previous_bars)
    )

    if average_volume <= 0:
        return 0

    return (
        last_volume
        / average_volume
    )


def is_smart_volume(bars):

    if len(bars) < 6:
        return False

    last_volume = int(
        bars[-1].volume
    )

    previous_volumes = [
        int(bar.volume)
        for bar in bars[-6:-1]
    ]

    average_volume = (
        sum(previous_volumes)
        / len(previous_volumes)
    )

    highest_recent = max(
        previous_volumes
    )

    return (
        last_volume > average_volume
        and last_volume >= highest_recent
    )


def is_accelerating(
    change_1m,
    change_3m,
    change_5m
):

    if change_1m <= 0:
        return False

    avg_3m = change_3m / 3

    avg_5m = change_5m / 5

    return (
        change_1m >= avg_3m
        and change_1m >= avg_5m
    )


def count_green_bars(
    bars,
    amount=5
):

    recent_bars = bars[-amount:]

    green = 0

    for bar in recent_bars:

        if (
            float(bar.close)
            > float(bar.open)
        ):

            green += 1

    return green


def detect_consolidation_breakout(
    bars
):

    if (
        len(bars)
        < CONSOLIDATION_LOOKBACK + 1
    ):
        return False, False

    last_bar = bars[-1]

    consolidation_bars = bars[
        -(CONSOLIDATION_LOOKBACK + 1):-1
    ]

    consolidation_high = max(
        float(bar.high)
        for bar in consolidation_bars
    )

    consolidation_low = min(
        float(bar.low)
        for bar in consolidation_bars
    )

    if consolidation_low <= 0:
        return False, False

    consolidation_range = (
        change_percent(
            consolidation_low,
            consolidation_high
        )
    )

    has_consolidation = (
        consolidation_range
        <= MAX_CONSOLIDATION_RANGE_PERCENT
    )

    last_close = float(
        last_bar.close
    )

    last_open = float(
        last_bar.open
    )

    last_volume = int(
        last_bar.volume
    )

    average_volume = (
        sum(
            int(bar.volume)
            for bar in consolidation_bars
        ) / len(consolidation_bars)
    )

    breakout = (
        has_consolidation
        and last_close > last_open
        and last_close > consolidation_high
        and last_volume
        > average_volume * 1.4
    )

    return (
        has_consolidation,
        breakout
    )


# =========================================================
# SCORING
# =========================================================
def get_alert_level(
    change_1m,
    change_3m,
    change_5m,
    change_10m
):

    if (
        change_1m >= 5
        or change_5m >= 8
        or change_10m >= 15
    ):
        return "strong"

    if (
        change_1m >= 3
        or change_3m >= 5
        or change_5m >= 5
        or change_10m >= 10
    ):
        return "medium"

    if (
        change_1m >= 0.8
        or change_3m >= 1.8
        or change_5m >= 2.8
        or change_10m >= 4.5
    ):
        return "early"

    return None


def get_structure_score(
    near_high,
    new_high,
    smart_volume,
    accelerating
):

    score = 0

    if near_high:
        score += 1

    if smart_volume:
        score += 1

    if accelerating:
        score += 1

    if new_high:
        score += 2

    return score


def get_overextension_penalty(
    change_5m,
    change_10m
):

    penalty = 0

    if change_10m > 25:
        penalty -= 5

    if change_10m > 40:
        penalty -= 10

    if change_5m > 30:
        penalty -= 7

    return penalty


def quality_score(
    change_1m,
    change_3m,
    change_5m,
    volume_ratio,
    smart_volume,
    near_high,
    new_high,
    accelerating,
    has_consolidation,
    consolidation_breakout
):

    score = 0

    if change_1m >= 5:
        score += 20

    elif change_1m >= 3:
        score += 14

    elif change_1m >= 0.8:
        score += 8

    if change_3m >= 8:
        score += 15

    elif change_3m >= 5:
        score += 11

    elif change_3m >= 1.8:
        score += 7

    if change_5m >= 10:
        score += 15

    elif change_5m >= 6:
        score += 11

    elif change_5m >= 2.8:
        score += 7

    if accelerating:
        score += 15

    if volume_ratio >= 5:
        score += 15

    elif volume_ratio >= 3:
        score += 10

    elif volume_ratio >= 1.4:
        score += 6

    if smart_volume:
        score += 10

    if new_high:
        score += 10

    elif near_high:
        score += 5

    if consolidation_breakout:
        score += 15

    elif has_consolidation:
        score += 5

    return score


# =========================================================
# LATE CHASE FILTER
# =========================================================
def is_late_chase(
    day_change,
    change_10m,
    volume_ratio,
    structure_score,
    consolidation_breakout
):

    if (
        day_change
        <= LATE_CHASE_DAY_CHANGE
    ):
        return False

    if consolidation_breakout:
        return False

    weak_structure = (
        structure_score
        <= LATE_CHASE_MAX_STRUCTURE
    )

    weak_volume = (
        volume_ratio
        < LATE_CHASE_MIN_VOLUME_RATIO
    )

    too_vertical = (
        change_10m
        > LATE_CHASE_10M_CHANGE
    )

    return (
        weak_structure
        and weak_volume
        and too_vertical
    )


# =========================================================
# ALERT CONTROL
# =========================================================
def can_send(
    symbol,
    current_price
):

    now = time.time()

    last_time = (
        last_alert_time.get(
            symbol,
            0
        )
    )

    previous_price = (
        last_alert_price.get(
            symbol
        )
    )

    if (
        now - last_time
        < COOLDOWN_SECONDS
    ):
        return False

    if previous_price is not None:

        if (
            current_price
            <= previous_price
        ):
            return False

    last_alert_time[symbol] = now

    last_alert_price[symbol] = current_price

    return True


# =========================================================
# SESSION STATS
# =========================================================
def update_session_stats(
    session_name,
    symbol,
    price
):

    stats = session_stats[
        session_name
    ]

    stats["total_alerts"] += 1

    stats["symbols"][symbol] = (
        stats["symbols"].get(
            symbol,
            0
        ) + 1
    )

    if symbol not in stats["prices"]:

        stats["prices"][symbol] = {
            "first": price,
            "last": price
        }

    else:

        stats["prices"][symbol][
            "last"
        ] = price

    save_session_stats()


def reset_session_stats(
    session_name
):

    session_stats[session_name] = {
        "total_alerts": 0,
        "symbols": {},
        "prices": {}
    }

    save_session_stats()


def send_session_summary(
    session_name
):

    stats = session_stats[
        session_name
    ]

    total_alerts = stats[
        "total_alerts"
    ]

    names = {
        "premarket": "פרה־מרקט",
        "regular": "מסחר רגיל",
        "afterhours": "אפטר־מרקט"
    }

    display_name = names[
        session_name
    ]

    if total_alerts == 0:

        send_telegram(
            f"📊 סיכום "
            f"{display_name}\n\n"
            f"לא נשלחו התראות."
        )

        return

    top_symbols = sorted(
        stats["symbols"].items(),
        key=lambda x: x[1],
        reverse=True
    )[:3]

    best_symbol = None

    best_change = -999

    for symbol, data in stats[
        "prices"
    ].items():

        first_price = data[
            "first"
        ]

        last_price = data[
            "last"
        ]

        move = change_percent(
            first_price,
            last_price
        )

        if move > best_change:

            best_change = move

            best_symbol = (
                symbol,
                first_price,
                last_price,
                move
            )

    message = (
        f"📊 סיכום "
        f"{display_name}\n\n"

        f"סה״כ התראות: "
        f"{total_alerts}\n\n"

        f"🔥 הכי הרבה הופיעו:\n"
    )

    for i, (
        symbol,
        count
    ) in enumerate(
        top_symbols,
        start=1
    ):

        message += (
            f"{i}. "
            f"{symbol} — "
            f"{count} פעמים\n"
        )

    if best_symbol:

        (
            symbol,
            first_price,
            last_price,
            move
        ) = best_symbol

        message += (
            f"\n🚀 העלייה הכי חזקה:\n"
            f"{symbol}\n"

            f"מחיר ראשון: "
            f"{first_price:.4f}$\n"

            f"מחיר אחרון: "
            f"{last_price:.4f}$\n"

            f"שינוי: "
            f"{move:.2f}%"
        )

    send_telegram(message)


# =========================================================
# SUMMARY CHECK
# =========================================================
def check_session_summaries():

    now = get_israel_time()

    current_date = now.date()

    current_minutes = (
        now.hour * 60
        + now.minute
    )

    targets = {
        "premarket": 16 * 60 + 29,
        "regular": 22 * 60 + 59,
        "afterhours": 2 * 60 + 59
    }

    for session_name, target in (
        targets.items()
    ):

        already_sent = (
            summary_sent_today[
                session_name
            ] == current_date
        )

        if already_sent:
            continue

        if current_minutes >= target:

            send_session_summary(
                session_name
            )

            summary_sent_today[
                session_name
            ] = current_date

            reset_session_stats(
                session_name
            )


# =========================================================
# MAIN SCAN
# =========================================================
def check_momentum(symbols):

    current_session = (
        get_market_session()
    )

    if current_session is None:
        return

    for chunk in split_chunks(
        symbols,
        CHUNK_SIZE
    ):

        try:

            barset = get_bars(
                chunk
            )

        except Exception as e:

            print(
                f"Bars error: {e}"
            )

            continue

        for symbol in chunk:

            try:

                bars = (
                    barset.data.get(
                        symbol,
                        []
                    )
                )

                if len(bars) < 10:
                    continue

                bars = sorted(
                    bars,
                    key=lambda b: b.timestamp
                )

                last_bar = bars[-1]

                current_price = float(
                    last_bar.close
                )

                last_open = float(
                    last_bar.open
                )

                last_volume = int(
                    last_bar.volume
                )

                if current_price <= 0:
                    continue

                if current_price < MIN_PRICE:
                    continue

                if current_price > MAX_PRICE:
                    continue

                current_green = (
                    current_price
                    > last_open
                )

                if not current_green:
                    continue

                change_1m = (
                    change_percent(
                        last_open,
                        current_price
                    )
                )

                change_3m = (
                    get_window_change(
                        bars,
                        3
                    )
                )

                change_5m = (
                    get_window_change(
                        bars,
                        5
                    )
                )

                change_10m = (
                    get_window_change(
                        bars,
                        10
                    )
                )

                day_change, day_high = (
                    get_day_stats(
                        symbol,
                        current_price
                    )
                )

                volume_ratio = (
                    get_volume_ratio(
                        bars
                    )
                )

                near_high, new_high = (
                    get_recent_high_data(
                        bars
                    )
                )

                smart_volume = (
                    is_smart_volume(
                        bars
                    )
                )

                accelerating = (
                    is_accelerating(
                        change_1m,
                        change_3m,
                        change_5m
                    )
                )

                green_bars_last_5 = (
                    count_green_bars(
                        bars,
                        5
                    )
                )

                (
                    has_consolidation,
                    consolidation_breakout
                ) = (
                    detect_consolidation_breakout(
                        bars
                    )
                )

                structure_score = (
                    get_structure_score(
                        near_high,
                        new_high,
                        smart_volume,
                        accelerating
                    )
                )

                alert_level = (
                    get_alert_level(
                        change_1m,
                        change_3m,
                        change_5m,
                        change_10m
                    )
                )

                base_score = (
                    quality_score(
                        change_1m,
                        change_3m,
                        change_5m,
                        volume_ratio,
                        smart_volume,
                        near_high,
                        new_high,
                        accelerating,
                        has_consolidation,
                        consolidation_breakout
                    )
                )

                penalty = (
                    get_overextension_penalty(
                        change_5m,
                        change_10m
                    )
                )

                final_score = (
                    base_score
                    + penalty
                )

                is_candidate = (
                    alert_level is not None

                    and last_volume
                    >= MIN_VOLUME

                    and volume_ratio
                    >= MIN_VOLUME_RATIO

                    and green_bars_last_5
                    >= MIN_GREEN_BARS_LAST_5

                    and structure_score
                    >= MIN_STRUCTURE_SCORE

                    and final_score
                    >= MIN_QUALITY_SCORE
                )

                if not is_candidate:
                    continue

                late_chase = (
                    is_late_chase(
                        day_change,
                        change_10m,
                        volume_ratio,
                        structure_score,
                        consolidation_breakout
                    )
                )

                if late_chase:

                    print(
                        f"{symbol} "
                        f"skipped "
                        f"- late chase"
                    )

                    continue

                if not can_send(
                    symbol,
                    current_price
                ):
                    continue

                update_session_stats(
                    current_session,
                    symbol,
                    current_price
                )

                print(
                    f"ALERT | "
                    f"{symbol} | "
                    f"score: "
                    f"{final_score}"
                )

                message = (
                    f"🚨 מניה: "
                    f"{symbol}\n"

                    f"רמת התראה: "
                    f"{alert_level}\n"

                    f"ציון איכות: "
                    f"{final_score}/100\n\n"

                    f"💰 מחיר: "
                    f"{current_price:.4f}$\n"

                    f"📊 שינוי יומי: "
                    f"{day_change:.2f}%\n"

                    f"📈 שינוי 1דק: "
                    f"{change_1m:.2f}%\n"

                    f"📈 שינוי 3דק: "
                    f"{change_3m:.2f}%\n"

                    f"📈 שינוי 5דק: "
                    f"{change_5m:.2f}%\n"

                    f"📈 שינוי 10דק: "
                    f"{change_10m:.2f}%\n\n"

                    f"📊 ווליום: "
                    f"{last_volume:,}\n"

                    f"📊 יחס ווליום: "
                    f"{volume_ratio:.2f}x\n\n"

                    f"🟢 נרות ירוקים: "
                    f"{green_bars_last_5}/5\n"

                    f"🏆 מבנה פריצה: "
                    f"{structure_score}/5\n"

                    f"🔥 שיא חדש: "
                    f"{'כן' if new_high else 'לא'}\n"

                    f"📦 התכנסות: "
                    f"{'כן' if has_consolidation else 'לא'}\n"

                    f"🚀 פריצה "
                    f"מהתכנסות: "
                    f"{'כן' if consolidation_breakout else 'לא'}"
                )

                send_telegram(
                    message
                )

            except Exception as e:

                print(
                    f"Symbol error "
                    f"{symbol}: {e}"
                )

                continue


# =========================================================
# START
# =========================================================
SYMBOLS = get_symbols()

print("MOMENTUM BOT STARTED")

print(
    f"TOTAL SYMBOLS: "
    f"{len(SYMBOLS)}"
)

print("----------------------------------")


while True:

    try:

        if is_market_day():

            check_session_summaries()

            check_momentum(
                SYMBOLS
            )

        else:

            print(
                "Weekend "
                "- sleeping"
            )

        time.sleep(
            SCAN_EVERY_SECONDS
        )

    except KeyboardInterrupt:

        print("BOT STOPPED")

        break

    except Exception as e:

        print("MAIN LOOP ERROR:", e)

        time.sleep(15)
