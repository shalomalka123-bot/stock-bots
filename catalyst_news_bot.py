import os
import time
import json
import re
import html
from urllib.request import urlopen, Request
from urllib.parse import quote
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import (
    StockLatestQuoteRequest,
    StockLatestTradeRequest,
    StockBarsRequest,
    StockSnapshotRequest,
)
from alpaca.data.enums import DataFeed
from alpaca.data.timeframe import TimeFrame


# =========================================================
# API KEYS
# =========================================================
CATALYST_BOT_TOKEN = os.getenv("CATALYST_BOT_TOKEN")
CATALYST_CHAT_ID = os.getenv("CATALYST_CHAT_ID")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")

# Optional only. Bot works without it.
OPENFDA_API_KEY = os.getenv("OPENFDA_API_KEY")


if not CATALYST_BOT_TOKEN or not CATALYST_CHAT_ID:
    print("Missing CATALYST_BOT_TOKEN or CATALYST_CHAT_ID")
    raise SystemExit

if not FINNHUB_API_KEY:
    print("Missing FINNHUB_API_KEY")
    raise SystemExit

if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    print("Missing ALPACA API keys")
    raise SystemExit


# =========================================================
# CLIENTS
# =========================================================
trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

DATA_FEED = DataFeed.SIP


# =========================================================
# SETTINGS
# =========================================================
MARKET_NEWS_INTERVAL = 60
PRESS_RELEASE_INTERVAL = 60
FDA_INTERVAL = 300
USASPENDING_INTERVAL = 1800
MOVERS_SCAN_INTERVAL = 900

MAX_PRICE = 70.0

# Instead of only 1 hour, we scan 6 hours back.
# This catches delayed RSS/Finnhub updates and late reactions after a catalyst.
NEWS_MAX_AGE_SECONDS = 6 * 3600

USASPENDING_MIN_AMOUNT = 10_000_000

DUPLICATE_COOLDOWN_SECONDS = 1800

# New two-level alert system
WATCHLIST_SCORE = 60
STRONG_SCORE = 75

# Reverse scanner:
# Send only if stock is up 10%-20% today and has real news.
MOVER_MIN_DAY_GAIN = 10.0
MOVER_MAX_DAY_GAIN = 20.0

# Avoid scanning too many expensive calls at once
SNAPSHOT_CHUNK_SIZE = 200
MAX_MOVER_NEWS_CHECKS_PER_SCAN = 10


# =========================================================
# STATE
# =========================================================
sent_alerts = set()
last_symbol_category_alert = {}
last_market_news_scan = 0
last_press_release_scan = 0
last_fda_scan = 0
last_usaspending_scan = 0
last_movers_scan = 0

all_us_tickers = set()
ticker_to_company = {}
ticker_metadata = {}
sector_translation_cache = {}

mover_checked_recently = {}


# =========================================================
# HEBREW TRANSLATION
# =========================================================
SECTOR_HEBREW = {
    "Technology": "טכנולוגיה",
    "Health Care": "בריאות וביוטכנולוגיה",
    "Healthcare": "בריאות וביוטכנולוגיה",
    "Biotechnology": "ביוטכנולוגיה",
    "Pharmaceuticals": "פרמצבטיקה",
    "Financial Services": "שירותים פיננסיים",
    "Financials": "פיננסים",
    "Finance": "פיננסים",
    "Consumer Cyclical": "צריכה לא חיונית",
    "Consumer Defensive": "צריכה חיונית",
    "Communication Services": "תקשורת ומדיה",
    "Industrials": "תעשייה",
    "Energy": "אנרגיה",
    "Basic Materials": "חומרי גלם",
    "Materials": "חומרי גלם",
    "Real Estate": 'נדל"ן',
    "Utilities": "שירותים ציבוריים",
    "Software": "תוכנה",
    "Semiconductors": "מוליכים למחצה",
    "Banks": "בנקאות",
    "Insurance": "ביטוח",
    "Oil & Gas": "נפט וגז",
    "Aerospace & Defense": "תעופה וביטחון",
    "Medical Devices": "מכשור רפואי",
    "Diagnostics & Research": "אבחון ומחקר",
    "Drug Manufacturers—Specialty & Generic": "יצרני תרופות",
    "Capital Markets": "שוק ההון",
}


def clean_text(text):
    if not text:
        return ""
    text = html.unescape(str(text))
    text = re.sub(r"<script.*?</script>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def translate_to_hebrew(text):
    """
    Translation happens only after filtering.
    This keeps the bot fast but still sends Hebrew alerts.
    """
    if not text:
        return ""

    try:
        text = clean_text(text)[:900]
        encoded = quote(text)
        url = (
            "https://translate.googleapis.com/translate_a/single"
            f"?client=gtx&sl=en&tl=iw&dt=t&q={encoded}"
        )

        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())

        translated = ""
        if data and isinstance(data, list) and data[0]:
            for chunk in data[0]:
                if chunk and chunk[0]:
                    translated += chunk[0]

        return translated.strip() if translated else text

    except Exception as e:
        print(f"Translate error: {e}")
        return text


def translate_sector(sector_en):
    if not sector_en:
        return "לא ידוע"

    if sector_en in SECTOR_HEBREW:
        return SECTOR_HEBREW[sector_en]

    if sector_en in sector_translation_cache:
        return sector_translation_cache[sector_en]

    translated = translate_to_hebrew(sector_en)
    sector_translation_cache[sector_en] = translated
    return translated


# =========================================================
# CATALYST DEFINITIONS
# =========================================================
POSITIVE_CATALYSTS = {
    "🟢 אישור FDA סופי": {
        "strength": 5,
        "category": "FDA_APPROVAL_FINAL",
        "keywords": [
            "fda approves",
            "fda approved",
            "receives fda approval",
            "received fda approval",
            "granted fda approval",
            "fda grants approval",
            "approved by fda",
            "approval from the fda",
            "u.s. food and drug administration approved",
            "marketing authorization",
            "regulatory approval",
            "snda approval",
            "bla approval",
            "nda approval",
        ],
    },
    "🟢 FDA 510(k) / Clearance": {
        "strength": 4,
        "category": "FDA_CLEARANCE_510K",
        "keywords": [
            "fda clearance",
            "fda cleared",
            "510(k) clearance",
            "510k clearance",
            "receives 510(k)",
            "received 510(k)",
            "fda grants 510(k)",
            "cleared by the fda",
        ],
    },
    "🟡 FDA קיבל בקשה לבדיקה": {
        "strength": 3,
        "category": "FDA_ACCEPTANCE",
        "keywords": [
            "fda accepts nda",
            "fda accepted nda",
            "nda accepted",
            "fda accepts bla",
            "fda accepted bla",
            "bla accepted",
            "fda accepts application",
            "application accepted by fda",
            "pdufa date",
            "priority review",
        ],
    },
    "🟡 מעמד FDA מיוחד": {
        "strength": 3,
        "category": "FDA_DESIGNATION",
        "keywords": [
            "fast track designation",
            "orphan drug designation",
            "breakthrough therapy designation",
            "rare pediatric disease designation",
            "regenerative medicine advanced therapy",
            "rmat designation",
            "qualified infectious disease product",
            "qidp designation",
        ],
    },
    "🧪 ניסוי קליני חיובי": {
        "strength": 5,
        "category": "PHASE_SUCCESS",
        "keywords": [
            "met primary endpoint",
            "achieved primary endpoint",
            "positive topline results",
            "positive phase 3",
            "phase 3 positive",
            "phase iii positive",
            "successful phase 3",
            "statistically significant",
            "met primary and secondary endpoints",
            "clinically meaningful",
            "significant improvement",
        ],
    },
    "🤝 רכישה / מיזוג": {
        "strength": 5,
        "category": "ACQUISITION",
        "keywords": [
            "agreement to acquire",
            "agreement to be acquired",
            "to be acquired by",
            "definitive merger agreement",
            "definitive agreement to acquire",
            "all-cash transaction",
            "tender offer",
            "acquisition agreement",
            "deal valued at",
            "transaction valued at",
            "per share in cash",
            "premium of approximately",
            "take-private transaction",
        ],
    },
    "💰 השקעה אסטרטגית": {
        "strength": 4,
        "category": "STRATEGIC_INVESTMENT",
        "keywords": [
            "strategic investment",
            "minority stake",
            "equity stake",
            "nvidia invests",
            "microsoft invests",
            "google invests",
            "amazon invests",
            "openai invests",
            "takes stake",
            "preferred stock investment",
        ],
    },
    "🤝 שותפות חזקה": {
        "strength": 4,
        "category": "PARTNERSHIP",
        "keywords": [
            "partnership with nvidia",
            "partnership with microsoft",
            "partnership with google",
            "partnership with amazon",
            "collaboration with openai",
            "selected by microsoft",
            "selected by amazon",
            "exclusive partner",
            "strategic partnership",
            "commercial partnership",
        ],
    },
    "🏛️ חוזה ממשלתי / חוזה גדול": {
        "strength": 4,
        "category": "CONTRACT",
        "keywords": [
            "government contract awarded",
            "federal contract",
            "dod contract",
            "department of defense contract",
            "u.s. army contract",
            "u.s. navy contract",
            "nasa contract",
            "awarded contract",
            "wins contract",
            "won contract",
            "multi-year agreement",
            "billion-dollar deal",
            "million contract awarded",
            "purchase order",
        ],
    },
    "₿ Crypto Treasury": {
        "strength": 5,
        "category": "CRYPTO_TREASURY",
        "keywords": [
            "bitcoin treasury",
            "ethereum treasury",
            "crypto treasury",
            "digital asset treasury",
            "adds bitcoin to treasury",
            "purchases bitcoin",
            "purchases ethereum",
            "crypto reserves",
            "solana treasury",
            "xrp treasury",
        ],
    },
    "🤖 Pivot ל-AI": {
        "strength": 4,
        "category": "AI_PIVOT",
        "keywords": [
            "pivots to ai",
            "pivot to artificial intelligence",
            "rebrands as ai",
            "ai-focused",
            "new ai division",
            "ai strategy launch",
            "artificial intelligence strategy",
            "launches ai platform",
            "generative ai platform",
        ],
    },
    "📈 דוחות חזקים": {
        "strength": 3,
        "category": "EARNINGS_BEAT",
        "keywords": [
            "earnings beat",
            "beats earnings estimates",
            "record revenue",
            "record quarterly revenue",
            "raises guidance",
            "raises full-year guidance",
            "beats revenue expectations",
            "above consensus",
            "raises outlook",
        ],
    },
    "🔬 פטנט / פריצת דרך": {
        "strength": 3,
        "category": "BREAKTHROUGH",
        "keywords": [
            "patent granted",
            "patent issued",
            "breakthrough technology",
            "scientific breakthrough",
            "first-in-class",
            "first-of-its-kind",
            "groundbreaking",
        ],
    },
    "💳 החזרי ביטוח / CMS / Medicare": {
        "strength": 4,
        "category": "REIMBURSEMENT",
        "keywords": [
            "cms reimbursement",
            "medicare reimbursement",
            "reimbursement approval",
            "coverage determination",
            "medicare coverage",
            "new reimbursement code",
            "cpt code",
        ],
    },
    "✅ חזרה לעמידה בדרישות נאסדק": {
        "strength": 3,
        "category": "NASDAQ_COMPLIANCE",
        "keywords": [
            "regained compliance",
            "nasdaq compliance",
            "compliance with nasdaq",
            "minimum bid price requirement",
        ],
    },
}


NEGATIVE_CATALYSTS = {
    "⚖️ תביעה / חקירה": {
        "strength": 4,
        "category": "LEGAL",
        "keywords": [
            "class action lawsuit",
            "securities fraud lawsuit",
            "shareholder lawsuit",
            "sec investigation",
            "doj investigation",
            "fbi investigation",
            "wells notice",
        ],
    },
    "💸 דילול": {
        "strength": 3,
        "category": "DILUTION",
        "keywords": [
            "registered direct offering",
            "public offering",
            "at-the-market offering",
            "atm offering",
            "shelf offering",
            "warrants",
            "convertible notes",
            "priced offering",
            "private placement",
        ],
    },
    "💀 סיכון קיצוני": {
        "strength": 5,
        "category": "EXTREME_RISK",
        "keywords": [
            "chapter 11",
            "bankruptcy",
            "going concern",
            "nasdaq delisting",
            "delisting determination",
            "reverse stock split",
            "complete response letter",
            "fda rejects",
            "fda rejection",
            "clinical hold",
        ],
    },
}


# =========================================================
# FILTER PATTERNS
# =========================================================
NOISE_PATTERNS = [
    "top stocks",
    "best stocks",
    "stocks to watch",
    "stocks to buy",
    "stock picks",
    "weekly summary",
    "weekly recap",
    "market commentary",
    "market outlook",
    "market recap",
    "morning brief",
    "daily recap",
    "portfolio update",
    "fund holdings",
    "investor letter",
    "should you buy",
    "should you sell",
    "why i'm bullish",
    "why i'm bearish",
    "dividend stocks",
    "closed-end fund",
]

LAW_FIRM_SPAM_PATTERNS = [
    "lead plaintiff",
    "lead plaintiff deadline",
    "class action deadline",
    "securities fraud lawsuit",
    "class action lawsuit",
    "shareholder alert",
    "investor alert",
    "law offices of",
    "rosen law firm",
    "pomerantz law firm",
    "levi & korsinsky",
    "bronstein, gewirtz",
    "glancy prongay",
    "faruqi & faruqi",
    "kessler topaz",
    "the schall law firm",
    "berger montague",
    "block & leviton",
    "bragar eagel",
    "gross law firm",
    "kirby mcinerney",
    "labaton",
    "class period",
    "reminds investors",
    "encourages investors",
    "recover losses",
    "contact the firm",
    "no cost to you",
]

BLOCK_PATTERNS = [
    "announces participation",
    "to present at",
    "fireside chat",
    "webcast",
    "investor conference",
    "annual meeting",
    "shareholder meeting",
    "conference call details",
    "presentation at",
    "earnings call",
]

WEAK_PR_PATTERNS = [
    "launches new website",
    "launches initiative",
    "expands platform",
    "announces new brand",
    "corporate update",
    "business update",
    "letter to shareholders",
    "appoints",
    "appointment of",
    "joins board",
    "advisory board",
    "marketing campaign",
]

DILUTION_RISK_PATTERNS = [
    "registered direct offering",
    "public offering",
    "at-the-market offering",
    "atm offering",
    "shelf offering",
    "warrants",
    "convertible note",
    "convertible notes",
    "priced offering",
    "private placement",
]

EXTREME_NEGATIVE_PATTERNS = [
    "chapter 11",
    "bankruptcy",
    "going concern",
    "nasdaq delisting",
    "delisting determination",
    "minimum bid notification",
    "reverse stock split",
    "complete response letter",
    "clinical hold",
    "fda rejects",
    "fda rejection",
]


# =========================================================
# BASIC HELPERS
# =========================================================
def has_any(text, patterns):
    t = (text or "").lower()
    return any(p in t for p in patterns)


def is_noise(headline):
    h = (headline or "").lower()

    for pat in NOISE_PATTERNS:
        if pat in h:
            return True

    tickers_in_title = re.findall(r"\([A-Z]{1,5}\)", headline or "")
    if len(tickers_in_title) >= 3:
        return True

    if re.search(r"\d+\s+(stocks|picks|companies|ideas)", h):
        return True

    return False


def is_law_firm_spam(headline, summary, source):
    text = f"{headline} {summary} {source}".lower()

    if has_any(text, LAW_FIRM_SPAM_PATTERNS):
        return True

    legal_count = sum(
        1
        for w in [
            "law firm",
            "lead plaintiff",
            "class action",
            "securities fraud",
            "shareholder lawsuit",
        ]
        if w in text
    )

    return legal_count >= 2


def get_source_score(source):
    s = (source or "").lower()

    if "sec" in s or "edgar" in s:
        return 12, "אמינות גבוהה מאוד - SEC"
    if "fda" in s:
        return 14, "אמינות גבוהה מאוד - FDA רשמי"
    if "usaspending" in s:
        return 12, "אמינות גבוהה מאוד - מקור ממשלתי"
    if "businesswire" in s:
        return 8, "אמינות גבוהה - BusinessWire"
    if "globenewswire" in s:
        return 7, "אמינות טובה - GlobeNewswire"
    if "prnewswire" in s:
        return 0, "מקור PR - נבדק לפי איכות התוכן"
    if "finnhub company" in s:
        return 4, "חדשות חברה דרך Finnhub"
    if "finnhub" in s:
        return 2, "אמינות בינונית - Finnhub"

    return 0, "מקור רגיל"


def get_quality_label(score):
    if score >= 93:
        return "💎 נדיר מאוד"
    if score >= 85:
        return "🚀 חזק מאוד"
    if score >= 75:
        return "🔥 חזק"
    if score >= 60:
        return "🟡 לבדיקה"
    return "⚪ חלש"


def extract_money_amount(text):
    t = (text or "").lower().replace(",", "")

    patterns = [
        (r"\$([\d\.]+)\s*billion", 1_000_000_000),
        (r"\$([\d\.]+)\s*bn", 1_000_000_000),
        (r"\$([\d\.]+)b\b", 1_000_000_000),
        (r"\$([\d\.]+)\s*million", 1_000_000),
        (r"\$([\d\.]+)\s*mln", 1_000_000),
        (r"\$([\d\.]+)m\b", 1_000_000),
    ]

    for pattern, multiplier in patterns:
        match = re.search(pattern, t)
        if match:
            return float(match.group(1)) * multiplier

    return 0


def detect_catalysts(text, catalyst_dict):
    found = []
    text_lower = (text or "").lower()
    seen_names = set()

    for cat_name, cat_data in catalyst_dict.items():
        if cat_name in seen_names:
            continue

        for kw in cat_data["keywords"]:
            if kw in text_lower:
                found.append(
                    {
                        "catalyst": cat_name,
                        "keyword": kw,
                        "strength": cat_data["strength"],
                        "category": cat_data["category"],
                    }
                )
                seen_names.add(cat_name)
                break

    return found


def get_market_cap_label(market_cap):
    if not market_cap:
        return "לא זמין"
    if market_cap < 50_000_000:
        return f"⚡ Nano Cap (${market_cap / 1e6:.1f}M)"
    if market_cap < 300_000_000:
        return f"⚡ Micro Cap (${market_cap / 1e6:.0f}M)"
    if market_cap < 2_000_000_000:
        return f"Small Cap (${market_cap / 1e6:.0f}M)"
    if market_cap < 10_000_000_000:
        return f"Mid Cap (${market_cap / 1e9:.1f}B)"
    if market_cap < 200_000_000_000:
        return f"Large Cap (${market_cap / 1e9:.0f}B)"
    return f"Mega Cap (${market_cap / 1e9:.0f}B)"


def format_us_eastern_time(timestamp):
    try:
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        est = dt.astimezone(ZoneInfo("America/New_York"))
        return est.strftime("%d.%m.%Y %H:%M ET")
    except Exception:
        return "לא ידוע"


def format_israel_time(timestamp):
    try:
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        il = dt.astimezone(ZoneInfo("Asia/Jerusalem"))
        return il.strftime("%d.%m.%Y %H:%M שעון ישראל")
    except Exception:
        return "לא ידוע"


def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{CATALYST_BOT_TOKEN}/sendMessage"

        data = json.dumps(
            {
                "chat_id": CATALYST_CHAT_ID,
                "text": message,
                "disable_web_page_preview": False,
            }
        ).encode()

        req = Request(url, data=data, headers={"Content-Type": "application/json"})
        urlopen(req, timeout=10).read()
        return True

    except Exception as e:
        print(f"Telegram error: {e}")
        return False


def make_smart_alert_hash(symbol, headline):
    h = clean_text(headline).lower()
    h = re.sub(r"[^a-z0-9\s]", " ", h)
    h = re.sub(r"\s+", " ", h)

    remove_words = {
        "announces",
        "reports",
        "today",
        "inc",
        "corp",
        "corporation",
        "company",
        "shares",
        "stock",
        "new",
        "the",
        "and",
        "with",
        "ltd",
        "plc",
        "group",
        "holdings",
    }

    words = [w for w in h.split() if w not in remove_words and len(w) > 2]
    core = " ".join(words[:12])

    return f"{symbol}|{core}"


# =========================================================
# COMPANY / TICKER DATA
# =========================================================
def normalize_company_name(name):
    if not name:
        return ""

    n = name.upper()
    n = re.sub(r"[^A-Z0-9\s]", " ", n)

    suffixes = [
        " INC",
        " CORP",
        " CORPORATION",
        " LLC",
        " LTD",
        " LIMITED",
        " PLC",
        " HOLDINGS",
        " HOLDING",
        " GROUP",
        " CO",
        " COMPANY",
        " SA",
        " AG",
        " NV",
        " LP",
        " THE",
    ]

    for suffix in suffixes:
        n = n.replace(suffix, " ")

    n = re.sub(r"\s+", " ", n).strip()
    return n


def get_important_company_words(name):
    normalized = normalize_company_name(name)
    if not normalized:
        return []

    weak_words = {
        "THE",
        "INC",
        "CORP",
        "CORPORATION",
        "COMPANY",
        "GROUP",
        "HOLDINGS",
        "HOLDING",
        "THERAPEUTICS",
        "PHARMA",
        "PHARMACEUTICALS",
        "BIOTECH",
        "TECHNOLOGIES",
        "TECHNOLOGY",
        "SYSTEMS",
        "SOLUTIONS",
        "INTERNATIONAL",
        "GLOBAL",
        "LIMITED",
        "LTD",
        "PLC",
        "LLC",
    }

    words = [w for w in normalized.split() if w not in weak_words and len(w) >= 3]
    return words[:4]


def get_finnhub_company_profile(symbol):
    if symbol in ticker_metadata:
        return ticker_metadata[symbol]

    try:
        url = f"https://finnhub.io/api/v1/stock/profile2?symbol={symbol}&token={FINNHUB_API_KEY}"
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})

        with urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())

        if isinstance(data, dict):
            sector_en = data.get("finnhubIndustry", "")
            market_cap = data.get("marketCapitalization", 0)

            if market_cap:
                market_cap *= 1_000_000

            metadata = {
                "sector_en": sector_en,
                "sector_he": translate_sector(sector_en),
                "market_cap": market_cap,
                "name": data.get("name", ""),
            }

            ticker_metadata[symbol] = metadata
            return metadata

    except Exception as e:
        print(f"Finnhub profile error for {symbol}: {e}")

    return {
        "sector_en": "",
        "sector_he": "לא ידוע",
        "market_cap": 0,
        "name": "",
    }


def load_us_tickers():
    global all_us_tickers, ticker_to_company

    try:
        request = GetAssetsRequest(
            asset_class=AssetClass.US_EQUITY,
            status=AssetStatus.ACTIVE,
        )

        assets = trading_client.get_all_assets(request)

        for asset in assets:
            if not asset.tradable or not asset.symbol:
                continue
            if "." in asset.symbol or "/" in asset.symbol:
                continue
            if asset.exchange not in ["NYSE", "NASDAQ", "AMEX"]:
                continue

            all_us_tickers.add(asset.symbol)

            if asset.name:
                ticker_to_company[asset.symbol] = asset.name

        print(f"Loaded {len(all_us_tickers)} US tickers")

    except Exception as e:
        print(f"Error loading tickers: {e}")


def find_ticker_for_company(company_name):
    if not company_name:
        return None

    clean_name = normalize_company_name(company_name)

    if not clean_name:
        return None

    manual_map = {
        "LOCKHEED MARTIN": "LMT",
        "RAYTHEON": "RTX",
        "RTX": "RTX",
        "NORTHROP GRUMMAN": "NOC",
        "BOEING": "BA",
        "GENERAL DYNAMICS": "GD",
        "PALANTIR": "PLTR",
        "LEIDOS": "LDOS",
        "KRATOS": "KTOS",
        "AEROJET": "AJRD",
    }

    for key, ticker in manual_map.items():
        if key in clean_name and ticker in all_us_tickers:
            return ticker

    for ticker, name in ticker_to_company.items():
        if not name:
            continue

        name_clean = normalize_company_name(name)

        if clean_name == name_clean:
            return ticker

        if len(clean_name) >= 6 and clean_name in name_clean:
            return ticker

        if len(name_clean) >= 6 and name_clean in clean_name:
            return ticker

    return None


def find_tickers_by_company_name_in_text(text, max_results=5):
    text_upper = f" {clean_text(text).upper()} "
    found = []

    for ticker, company_name in ticker_to_company.items():
        if not company_name:
            continue

        words = get_important_company_words(company_name)
        if not words:
            continue

        hits = 0
        for word in words:
            if f" {word} " in text_upper:
                hits += 1

        if hits >= 1 and len(words[0]) >= 4:
            found.append((ticker, hits, company_name))

    found.sort(key=lambda x: x[1], reverse=True)
    return [x[0] for x in found[:max_results]]


def extract_ticker_candidates_from_news(related, headline, summary):
    candidates = []
    seen = set()

    def add_candidate(ticker):
        ticker = ticker.strip().upper()
        if ticker in all_us_tickers and ticker not in seen:
            candidates.append(ticker)
            seen.add(ticker)

    if related:
        for candidate in related.split(","):
            add_candidate(candidate)

    text = f"{headline} {summary}"

    patterns = [
        r"\(([A-Z]{1,5})\)",
        r"NASDAQ:\s*([A-Z]{1,5})",
        r"NYSE:\s*([A-Z]{1,5})",
        r"AMEX:\s*([A-Z]{1,5})",
        r"\$([A-Z]{1,5})\b",
    ]

    for pattern in patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            add_candidate(match)

    for ticker in find_tickers_by_company_name_in_text(text, max_results=5):
        add_candidate(ticker)

    return candidates[:8]


def score_ticker_match(ticker, headline, summary):
    score = 0

    headline_upper = (headline or "").upper()
    summary_upper = (summary or "").upper()
    full_upper = f"{headline_upper} {summary_upper}"

    if re.search(rf"\b{ticker}\b", headline_upper):
        score += 40

    if f"({ticker})" in headline_upper or f"${ticker}" in headline_upper:
        score += 35

    if re.search(rf"\b{ticker}\b", summary_upper):
        score += 20

    company_name = ticker_to_company.get(ticker, "")
    important_words = get_important_company_words(company_name)

    for word in important_words:
        if f" {word} " in f" {headline_upper} ":
            score += 25
        elif f" {word} " in f" {full_upper} ":
            score += 10

    return score


def choose_best_ticker(candidates, headline, summary):
    if not candidates:
        return None

    scored = []
    for ticker in candidates:
        score = score_ticker_match(ticker, headline, summary)
        scored.append((ticker, score))

    scored.sort(key=lambda x: x[1], reverse=True)

    best_ticker, best_score = scored[0]

    if best_score < 10 and len(candidates) > 1:
        return None

    return best_ticker


def is_company_focused(ticker, headline, summary):
    if not ticker:
        return False

    score = score_ticker_match(ticker, headline, summary)

    if score >= 20:
        return True

    full_text = f"{headline} {summary}".upper()
    ticker_count = len(re.findall(rf"\b{ticker}\b", full_text))

    return ticker_count >= 2


# =========================================================
# PRICE FUNCTIONS
# =========================================================
def get_current_price(symbol):
    """
    More reliable price function:
    1. Latest trade
    2. Latest quote
    3. Last minute bar
    """
    try:
        req = StockLatestTradeRequest(symbol_or_symbols=symbol, feed=DATA_FEED)
        trades = data_client.get_stock_latest_trade(req)

        if symbol in trades:
            trade = trades[symbol]
            price = float(trade.price) if trade.price else 0
            if price > 0:
                return price

    except Exception as e:
        print(f"Latest trade error for {symbol}: {e}")

    try:
        request = StockLatestQuoteRequest(symbol_or_symbols=symbol, feed=DATA_FEED)
        quotes = data_client.get_stock_latest_quote(request)

        if symbol in quotes:
            quote_obj = quotes[symbol]

            ask = float(quote_obj.ask_price) if quote_obj.ask_price else 0
            bid = float(quote_obj.bid_price) if quote_obj.bid_price else 0

            if ask > 0 and bid > 0:
                return (ask + bid) / 2
            if ask > 0:
                return ask
            if bid > 0:
                return bid

    except Exception as e:
        print(f"Latest quote error for {symbol}: {e}")

    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=20)

        req = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame.Minute,
            start=start,
            end=end,
            feed=DATA_FEED,
        )

        bars_response = data_client.get_stock_bars(req)
        bars = list(bars_response.data.get(symbol, []))

        if bars:
            last_bar = bars[-1]
            price = float(last_bar.close)
            if price > 0:
                return price

    except Exception as e:
        print(f"Last bar price error for {symbol}: {e}")

    return None


def get_minute_bars(symbol, start, end):
    try:
        req = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame.Minute,
            start=start,
            end=end,
            feed=DATA_FEED,
        )

        bars_response = data_client.get_stock_bars(req)
        return list(bars_response.data.get(symbol, []))

    except Exception as e:
        print(f"Minute bars error for {symbol}: {e}")
        return []


def find_bar_at_or_before(bars, target_time):
    if not bars:
        return None

    selected = None
    for bar in bars:
        try:
            ts = bar.timestamp
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)

            if ts <= target_time:
                selected = bar
            else:
                break
        except Exception:
            continue

    return selected


def calculate_price_reaction(symbol, published_timestamp):
    """
    Checks if the stock starts moving after the news.
    This is what turns the bot from a news bot into a catalyst + price reaction bot.
    """
    result = {
        "available": False,
        "score": 0,
        "current_price": None,
        "change_since_news_pct": None,
        "change_5m_pct": None,
        "change_15m_pct": None,
        "change_60m_pct": None,
        "volume_15m": 0,
        "breakout": False,
        "warning": "",
        "summary_he": "אין מספיק נתוני מחיר.",
    }

    try:
        now_dt = datetime.now(timezone.utc)
        published_dt = datetime.fromtimestamp(published_timestamp, tz=timezone.utc)

        start = published_dt - timedelta(minutes=20)
        if start < now_dt - timedelta(hours=8):
            start = now_dt - timedelta(hours=8)

        bars = get_minute_bars(symbol, start, now_dt)

        if len(bars) < 3:
            return result

        result["available"] = True

        current_bar = bars[-1]
        current_price = float(current_bar.close)
        result["current_price"] = current_price

        news_bar = None
        for bar in bars:
            ts = bar.timestamp
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts >= published_dt:
                news_bar = bar
                break

        if not news_bar:
            news_bar = bars[0]

        news_price = float(news_bar.close)

        if news_price > 0:
            change_since_news = ((current_price - news_price) / news_price) * 100
            result["change_since_news_pct"] = change_since_news
        else:
            change_since_news = 0

        for minutes, key in [
            (5, "change_5m_pct"),
            (15, "change_15m_pct"),
            (60, "change_60m_pct"),
        ]:
            target = now_dt - timedelta(minutes=minutes)
            old_bar = find_bar_at_or_before(bars, target)
            if old_bar and float(old_bar.close) > 0:
                old_price = float(old_bar.close)
                result[key] = ((current_price - old_price) / old_price) * 100

        last_15_bars = bars[-15:] if len(bars) >= 15 else bars
        volume_15m = sum(float(b.volume or 0) for b in last_15_bars)
        result["volume_15m"] = volume_15m

        if len(bars) >= 20:
            previous_high = max(float(b.high) for b in bars[:-1])
            if current_price >= previous_high:
                result["breakout"] = True

        score = 0

        if result["change_since_news_pct"] is not None:
            c = result["change_since_news_pct"]

            if 1 <= c <= 8:
                score += 18
            elif 8 < c <= 20:
                score += 12
            elif 20 < c <= 50:
                score -= 5
                result["warning"] = "המניה כבר עלתה חזק אחרי הידיעה."
            elif c > 50:
                score -= 20
                result["warning"] = "המניה כבר עלתה מעל 50% אחרי הידיעה - זה עלול להיות מאוחר."

        if result["change_5m_pct"] is not None:
            c5 = result["change_5m_pct"]
            if 0.8 <= c5 <= 6:
                score += 8
            elif c5 > 12:
                score -= 4

        if result["change_15m_pct"] is not None:
            c15 = result["change_15m_pct"]
            if 1.5 <= c15 <= 12:
                score += 10
            elif c15 > 20:
                score -= 6

        if volume_15m >= 100_000:
            score += 8
        elif volume_15m >= 50_000:
            score += 5
        elif volume_15m >= 20_000:
            score += 3

        if result["breakout"]:
            score += 8

        result["score"] = max(-25, min(30, int(score)))

        parts = []

        if result["change_since_news_pct"] is not None:
            parts.append(f"שינוי מאז הידיעה: {result['change_since_news_pct']:.2f}%")

        if result["change_5m_pct"] is not None:
            parts.append(f"שינוי 5 דקות: {result['change_5m_pct']:.2f}%")

        if result["change_15m_pct"] is not None:
            parts.append(f"שינוי 15 דקות: {result['change_15m_pct']:.2f}%")

        parts.append(f"ווליום 15 דקות: {volume_15m:,.0f}")

        if result["breakout"]:
            parts.append("יש סימן לפריצה / גבוה חדש בטווח הבדיקה")

        if result["warning"]:
            parts.append(result["warning"])

        result["summary_he"] = "\n".join([f"• {p}" for p in parts])

        return result

    except Exception as e:
        print(f"Price reaction error for {symbol}: {e}")
        return result


# =========================================================
# QUALITY SCORING
# =========================================================
def calculate_news_quality(
    ticker,
    headline,
    summary,
    source,
    positive_found,
    negative_found,
    meta,
    price_reaction=None,
    reverse_mode=False,
):
    headline = clean_text(headline)
    summary = clean_text(summary)
    text = f"{headline} {summary}"
    text_lower = text.lower()

    reasons = []

    if is_law_firm_spam(headline, summary, source):
        return {
            "send": False,
            "score": 0,
            "label": "❌ חסום",
            "reason": "פרסום עורכי דין / תביעה ייצוגית",
            "reasons": ["פרסום עורכי דין / תביעה ייצוגית"],
            "news_type": "רעש משפטי",
            "primary_category": "LAW_FIRM_SPAM",
        }

    if not positive_found:
        return {
            "send": False,
            "score": 0,
            "label": "❌ לא חיובי",
            "reason": "אין קטליזטור חיובי",
            "reasons": ["אין קטליזטור חיובי"],
            "news_type": "לא רלוונטי",
            "primary_category": "NO_POSITIVE",
        }

    if has_any(text_lower, BLOCK_PATTERNS):
        return {
            "send": False,
            "score": 0,
            "label": "❌ חסום",
            "reason": "כנס / מצגת / שיחת רווחים",
            "reasons": ["כנס / מצגת / שיחת רווחים"],
            "news_type": "יח״צ חלש",
            "primary_category": "BLOCKED_PR",
        }

    if has_any(text_lower, EXTREME_NEGATIVE_PATTERNS):
        return {
            "send": False,
            "score": 0,
            "label": "❌ חסום",
            "reason": "סיכון שלילי קיצוני",
            "reasons": ["סיכון שלילי קיצוני"],
            "news_type": "סיכון גבוה",
            "primary_category": "EXTREME_RISK",
        }

    best = max(positive_found, key=lambda x: x["strength"])
    primary_category = best["category"]

    category_scores = {
        "FDA_APPROVAL_FINAL": 90,
        "FDA_CLEARANCE_510K": 78,
        "FDA_ACCEPTANCE": 68,
        "FDA_DESIGNATION": 62,
        "PHASE_SUCCESS": 90,
        "ACQUISITION": 92,
        "STRATEGIC_INVESTMENT": 76,
        "PARTNERSHIP": 74,
        "CONTRACT": 68,
        "CRYPTO_TREASURY": 82,
        "AI_PIVOT": 62,
        "EARNINGS_BEAT": 62,
        "BREAKTHROUGH": 65,
        "REIMBURSEMENT": 76,
        "NASDAQ_COMPLIANCE": 62,
    }

    score = category_scores.get(primary_category, 55)
    reasons.append(f"קטגוריה ראשית: {primary_category}")

    source_bonus, source_reason = get_source_score(source)
    score += source_bonus
    reasons.append(source_reason)

    strong_phrases = [
        "fda approves",
        "receives fda approval",
        "fda cleared",
        "510(k) clearance",
        "met primary endpoint",
        "achieved primary endpoint",
        "positive topline results",
        "definitive agreement to acquire",
        "to be acquired by",
        "all-cash transaction",
        "strategic investment",
        "awarded contract",
        "wins contract",
        "government contract awarded",
        "cms reimbursement",
        "medicare coverage",
    ]

    if has_any(text_lower, strong_phrases):
        score += 8
        reasons.append("נמצאה תבנית משפט חזקה")

    if has_any(text_lower, WEAK_PR_PATTERNS):
        score -= 18
        reasons.append("יח״צ חלש / הודעה שיווקית")

    if has_any(text_lower, DILUTION_RISK_PATTERNS):
        score -= 25
        reasons.append("סיכון דילול / גיוס הון")

    amount = extract_money_amount(text)
    market_cap = meta.get("market_cap", 0) if meta else 0

    if amount and market_cap:
        ratio = amount / market_cap

        if ratio >= 0.10:
            score += 18
            reasons.append(f"סכום מעל 10% משווי החברה ({ratio * 100:.1f}%)")
        elif ratio >= 0.03:
            score += 10
            reasons.append(f"סכום 3%-10% משווי החברה ({ratio * 100:.1f}%)")
        elif ratio >= 0.01:
            score += 4
            reasons.append(f"סכום 1%-3% משווי החברה ({ratio * 100:.1f}%)")
        else:
            score -= 8
            reasons.append(f"סכום קטן ביחס לשווי החברה ({ratio * 100:.2f}%)")

    hot_words = [
        "artificial intelligence",
        " ai ",
        "nvidia",
        "openai",
        "quantum",
        "bitcoin",
        "ethereum",
        "crypto",
        "solana",
        "xrp",
    ]

    if has_any(f" {text_lower} ", hot_words):
        score += 5
        reasons.append("תחום חם: AI / Crypto / Quantum")

    if price_reaction and price_reaction.get("available"):
        reaction_score = price_reaction.get("score", 0)
        score += reaction_score

        if reaction_score > 0:
            reasons.append(f"תגובת מחיר חיובית: +{reaction_score}")
        elif reaction_score < 0:
            reasons.append(f"תגובת מחיר בעייתית: {reaction_score}")

    if reverse_mode:
        reasons.append("זוהה במסלול הפוך: המניה עלתה ואז נמצאה ידיעה")
        score += 4

    score = max(0, min(100, int(score)))
    label = get_quality_label(score)

    if score >= 85:
        news_type = "ידיעה חיובית חזקה מאוד"
    elif score >= STRONG_SCORE:
        news_type = "ידיעה חיובית חזקה"
    elif score >= WATCHLIST_SCORE:
        news_type = "ידיעה לבדיקה - לא כניסה אוטומטית"
    else:
        news_type = "ידיעה חלשה"

    return {
        "send": score >= WATCHLIST_SCORE,
        "score": score,
        "label": label,
        "reason": " + ".join(reasons[:4]),
        "reasons": reasons,
        "news_type": news_type,
        "primary_category": primary_category,
    }


# =========================================================
# RSS PARSING
# =========================================================
def parse_rss_items(rss_content):
    items = []
    item_blocks = re.findall(r"<item\b[^>]*>(.*?)</item>", rss_content, re.DOTALL | re.IGNORECASE)

    for block in item_blocks:
        try:
            title_match = re.search(
                r"<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>",
                block,
                re.DOTALL | re.IGNORECASE,
            )
            link_match = re.search(r"<link>(.*?)</link>", block, re.DOTALL | re.IGNORECASE)
            desc_match = re.search(
                r"<description>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</description>",
                block,
                re.DOTALL | re.IGNORECASE,
            )
            date_match = re.search(r"<pubDate>(.*?)</pubDate>", block, re.DOTALL | re.IGNORECASE)

            title = clean_text(title_match.group(1)) if title_match else ""
            link = clean_text(link_match.group(1)) if link_match else ""
            desc = clean_text(desc_match.group(1)) if desc_match else ""

            timestamp = time.time()

            if date_match:
                date_str = clean_text(date_match.group(1))

                for fmt in [
                    "%a, %d %b %Y %H:%M:%S %z",
                    "%a, %d %b %Y %H:%M:%S GMT",
                    "%a, %d %b %Y %H:%M:%S %Z",
                ]:
                    try:
                        dt = datetime.strptime(date_str, fmt)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        timestamp = dt.timestamp()
                        break
                    except Exception:
                        pass

            items.append(
                {
                    "title": title,
                    "link": link,
                    "description": desc,
                    "timestamp": timestamp,
                }
            )

        except Exception:
            continue

    return items


def parse_fda_page_links(html_content):
    """
    Fallback if RSS is not available.
    Extracts FDA press-announcement links from the FDA page.
    """
    items = []

    matches = re.findall(
        r'<a[^>]+href="([^"]*press-announcements[^"]*)"[^>]*>(.*?)</a>',
        html_content,
        flags=re.DOTALL | re.IGNORECASE,
    )

    for href, title_html in matches:
        title = clean_text(title_html)

        if not title or len(title) < 10:
            continue

        if href.startswith("/"):
            link = f"https://www.fda.gov{href}"
        elif href.startswith("http"):
            link = href
        else:
            continue

        items.append(
            {
                "title": title,
                "link": link,
                "description": title,
                "timestamp": time.time(),
            }
        )

    return items[:30]


# =========================================================
# SCANNERS
# =========================================================
def scan_market_news():
    print("[Market News] scanning...")

    try:
        url = f"https://finnhub.io/api/v1/news?category=general&token={FINNHUB_API_KEY}"
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})

        with urlopen(req, timeout=15) as response:
            news_items = json.loads(response.read().decode())

        if not isinstance(news_items, list):
            return

        now = time.time()
        recent_news = [
            item
            for item in news_items
            if now - item.get("datetime", 0) <= NEWS_MAX_AGE_SECONDS
        ]

        print(f"[Market News] {len(recent_news)} articles in last 6h (out of {len(news_items)})")

        for news in recent_news:
            process_news_item(news, source_override="Finnhub")

    except Exception as e:
        print(f"[Market News] error: {e}")


def scan_globenewswire():
    print("[GlobeNewswire] scanning...")

    try:
        url = "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire+-+News+about+Public+Companies"
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})

        with urlopen(req, timeout=15) as response:
            content = response.read().decode("utf-8", errors="ignore")

        items = parse_rss_items(content)
        now = time.time()
        recent = [item for item in items if now - item["timestamp"] <= NEWS_MAX_AGE_SECONDS]

        print(f"[GlobeNewswire] {len(recent)} releases in last 6h (out of {len(items)})")

        for item in recent:
            news = {
                "headline": item["title"],
                "summary": item["description"],
                "url": item["link"],
                "datetime": item["timestamp"],
                "related": "",
            }
            process_news_item(news, source_override="GlobeNewswire")

    except Exception as e:
        print(f"[GlobeNewswire] error: {e}")


def scan_prnewswire():
    print("[PRNewswire] scanning...")

    try:
        url = "https://www.prnewswire.com/rss/news-releases-list.rss"
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})

        with urlopen(req, timeout=15) as response:
            content = response.read().decode("utf-8", errors="ignore")

        items = parse_rss_items(content)
        now = time.time()
        recent = [item for item in items if now - item["timestamp"] <= NEWS_MAX_AGE_SECONDS]

        print(f"[PRNewswire] {len(recent)} releases in last 6h (out of {len(items)})")

        for item in recent:
            news = {
                "headline": item["title"],
                "summary": item["description"],
                "url": item["link"],
                "datetime": item["timestamp"],
                "related": "",
            }
            process_news_item(news, source_override="PRNewswire")

    except Exception as e:
        print(f"[PRNewswire] error: {e}")


def scan_fda_press_announcements():
    print("[FDA] scanning official FDA announcements...")

    fda_urls = [
        "https://www.fda.gov/news-events/fda-newsroom/press-announcements/rss.xml",
        "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/press-announcements/rss.xml",
    ]

    items = []

    for url in fda_urls:
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=15) as response:
                content = response.read().decode("utf-8", errors="ignore")

            parsed = parse_rss_items(content)
            if parsed:
                items.extend(parsed)
                break

        except Exception as e:
            print(f"[FDA RSS] failed {url}: {e}")

    if not items:
        try:
            page_url = "https://www.fda.gov/news-events/fda-newsroom/press-announcements"
            req = Request(page_url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=15) as response:
                content = response.read().decode("utf-8", errors="ignore")

            items = parse_fda_page_links(content)

        except Exception as e:
            print(f"[FDA page fallback] error: {e}")
            return

    now = time.time()

    fda_relevant_words = [
        "approves",
        "approved",
        "approval",
        "clearance",
        "cleared",
        "510(k)",
        "510k",
        "grants",
        "accepts",
        "priority review",
        "fast track",
        "orphan drug",
        "breakthrough therapy",
        "pdufa",
        "drug",
        "therapy",
        "device",
        "treatment",
    ]

    recent = [
        item
        for item in items
        if now - item["timestamp"] <= NEWS_MAX_AGE_SECONDS or item["timestamp"] == now
    ]

    print(f"[FDA] {len(recent)} official items to check")

    for item in recent:
        title = item["title"]
        desc = item["description"]

        if not has_any(f"{title} {desc}", fda_relevant_words):
            continue

        news = {
            "headline": title,
            "summary": desc,
            "url": item["link"],
            "datetime": item["timestamp"],
            "related": "",
        }

        process_news_item(news, source_override="FDA Official")


def scan_usaspending():
    print("[USAspending] scanning...")

    try:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
        payload = {
            "filters": {
                "award_type_codes": ["A", "B", "C", "D"],
                "time_period": [{"start_date": start_date, "end_date": end_date}],
                "award_amounts": [{"lower_bound": USASPENDING_MIN_AMOUNT}],
            },
            "fields": [
                "Award ID",
                "Recipient Name",
                "Award Amount",
                "Awarding Agency",
                "Description",
                "Start Date",
            ],
            "page": 1,
            "limit": 100,
            "sort": "Award Amount",
            "order": "desc",
        }

        req = Request(
            url,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )

        with urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode())

        results = data.get("results", [])
        print(f"[USAspending] {len(results)} contracts")

        for contract in results:
            recipient = contract.get("Recipient Name", "")
            amount = contract.get("Award Amount", 0)
            agency = contract.get("Awarding Agency", "")
            description = contract.get("Description", "")
            award_id = contract.get("Award ID", "")

            ticker = find_ticker_for_company(recipient)
            if not ticker:
                continue

            price = get_current_price(ticker)
            if price and price > MAX_PRICE:
                continue

            category = "CONTRACT"
            duplicate_key = f"{ticker}|{category}"
            now = time.time()

            if duplicate_key in last_symbol_category_alert:
                if now - last_symbol_category_alert[duplicate_key] < DUPLICATE_COOLDOWN_SECONDS:
                    print(f"[DUPLICATE SKIP] {ticker} | USAspending")
                    continue

            alert_hash = f"{ticker}|USASPENDING|{award_id}"
            if alert_hash in sent_alerts:
                continue

            send_usaspending_alert(ticker, recipient, amount, agency, description, award_id, price)

            sent_alerts.add(alert_hash)
            last_symbol_category_alert[duplicate_key] = now

    except Exception as e:
        print(f"[USAspending] error: {e}")


# =========================================================
# REVERSE MOVER SCANNER
# =========================================================
def chunks(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def get_intraday_movers_from_alpaca():
    """
    Reverse path:
    Find stocks up 10%-20% today.
    If above 20%, skip to avoid chasing.
    """
    movers = []

    symbols = list(all_us_tickers)

    for chunk_symbols in chunks(symbols, SNAPSHOT_CHUNK_SIZE):
        try:
            req = StockSnapshotRequest(
                symbol_or_symbols=chunk_symbols,
                feed=DATA_FEED,
            )

            snapshots = data_client.get_stock_snapshot(req)

            for symbol, snap in snapshots.items():
                try:
                    latest_trade = getattr(snap, "latest_trade", None)
                    daily_bar = getattr(snap, "daily_bar", None)
                    prev_daily_bar = getattr(snap, "previous_daily_bar", None)

                    current_price = None

                    if latest_trade and getattr(latest_trade, "price", None):
                        current_price = float(latest_trade.price)
                    elif daily_bar and getattr(daily_bar, "close", None):
                        current_price = float(daily_bar.close)

                    if not current_price or current_price <= 0:
                        continue

                    if current_price > MAX_PRICE:
                        continue

                    prev_close = None

                    if prev_daily_bar and getattr(prev_daily_bar, "close", None):
                        prev_close = float(prev_daily_bar.close)

                    if not prev_close or prev_close <= 0:
                        continue

                    day_gain = ((current_price - prev_close) / prev_close) * 100

                    if day_gain >= MOVER_MAX_DAY_GAIN:
                        continue

                    if day_gain >= MOVER_MIN_DAY_GAIN:
                        movers.append(
                            {
                                "symbol": symbol,
                                "current_price": current_price,
                                "prev_close": prev_close,
                                "day_gain": day_gain,
                            }
                        )

                except Exception:
                    continue

        except Exception as e:
            print(f"[Movers Snapshot] chunk error: {e}")
            time.sleep(1)

    movers.sort(key=lambda x: x["day_gain"], reverse=True)
    return movers


def get_finnhub_company_news(symbol):
    try:
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=1)

        url = (
            "https://finnhub.io/api/v1/company-news"
            f"?symbol={symbol}"
            f"&from={start_date}"
            f"&to={end_date}"
            f"&token={FINNHUB_API_KEY}"
        )

        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})

        with urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode())

        if isinstance(data, list):
            return data

    except Exception as e:
        print(f"Finnhub company news error for {symbol}: {e}")

    return []


def scan_intraday_movers_then_news():
    print("[Reverse Movers] scanning stocks up 10%-20% today...")

    try:
        movers = get_intraday_movers_from_alpaca()
        print(f"[Reverse Movers] found {len(movers)} movers between 10%-20%")

        checked = 0
        now = time.time()

        for mover in movers:
            symbol = mover["symbol"]

            if checked >= MAX_MOVER_NEWS_CHECKS_PER_SCAN:
                break

            last_checked = mover_checked_recently.get(symbol, 0)
            if now - last_checked < 900:
                continue

            mover_checked_recently[symbol] = now
            checked += 1

            news_items = get_finnhub_company_news(symbol)

            if not news_items:
                continue

            recent_news = [
                n
                for n in news_items
                if now - n.get("datetime", 0) <= NEWS_MAX_AGE_SECONDS
            ]

            if not recent_news:
                continue

            for news in recent_news[:5]:
                headline = clean_text(news.get("headline", ""))
                summary = clean_text(news.get("summary", ""))

                full_text = f"{headline} {summary}"
                positive_found = detect_catalysts(full_text, POSITIVE_CATALYSTS)

                if not positive_found:
                    continue

                news["related"] = symbol

                process_news_item(
                    news,
                    source_override="Finnhub Company News - Reverse Mover",
                    force_ticker=symbol,
                    reverse_mode=True,
                    mover_data=mover,
                )

                break

        if len(mover_checked_recently) > 5000:
            mover_checked_recently.clear()

    except Exception as e:
        print(f"[Reverse Movers] error: {e}")


# =========================================================
# PROCESS NEWS ITEM
# =========================================================
def process_news_item(
    news,
    source_override=None,
    force_ticker=None,
    reverse_mode=False,
    mover_data=None,
):
    headline = clean_text(news.get("headline", ""))
    summary = clean_text(news.get("summary", ""))
    url_link = news.get("url", "")
    related = news.get("related", "")
    published = news.get("datetime", time.time())
    source = source_override or news.get("source", "Unknown")

    if not headline:
        return

    if time.time() - published > NEWS_MAX_AGE_SECONDS:
        return

    if is_noise(headline):
        return

    full_text = f"{headline} {summary}"

    positive_found = detect_catalysts(full_text, POSITIVE_CATALYSTS)
    negative_found = detect_catalysts(full_text, NEGATIVE_CATALYSTS)

    if force_ticker:
        ticker = force_ticker
    else:
        candidates = extract_ticker_candidates_from_news(related, headline, summary)
        ticker = choose_best_ticker(candidates, headline, summary)

    if not ticker:
        return

    if not force_ticker and not is_company_focused(ticker, headline, summary):
        return

    price = get_current_price(ticker)

    if price and price > MAX_PRICE:
        return

    meta = get_finnhub_company_profile(ticker)

    price_reaction = calculate_price_reaction(ticker, published)

    quality = calculate_news_quality(
        ticker=ticker,
        headline=headline,
        summary=summary,
        source=source,
        positive_found=positive_found,
        negative_found=negative_found,
        meta=meta,
        price_reaction=price_reaction,
        reverse_mode=reverse_mode,
    )

    if not quality["send"]:
        print(f"[SKIP] {ticker} | {quality['reason']} | {headline[:90]}")
        return

    primary_category = quality.get("primary_category", "UNKNOWN")
    duplicate_key = f"{ticker}|{primary_category}"
    now = time.time()

    if duplicate_key in last_symbol_category_alert:
        last_time = last_symbol_category_alert[duplicate_key]
        if now - last_time < DUPLICATE_COOLDOWN_SECONDS:
            print(f"[DUPLICATE SKIP] {ticker} | {primary_category}")
            return

    alert_hash = make_smart_alert_hash(ticker, headline)

    if alert_hash in sent_alerts:
        return

    send_news_alert(
        ticker=ticker,
        headline=headline,
        summary=summary,
        source=source,
        url_link=url_link,
        published=published,
        price=price,
        meta=meta,
        quality=quality,
        price_reaction=price_reaction,
        reverse_mode=reverse_mode,
        mover_data=mover_data,
    )

    sent_alerts.add(alert_hash)
    last_symbol_category_alert[duplicate_key] = now


# =========================================================
# ALERTS
# =========================================================
def send_news_alert(
    ticker,
    headline,
    summary,
    source,
    url_link,
    published,
    price,
    meta,
    quality,
    price_reaction=None,
    reverse_mode=False,
    mover_data=None,
):
    company_name = meta.get("name") or ticker_to_company.get(ticker, "לא ידוע")
    sector_he = meta.get("sector_he", "לא ידוע")
    market_cap_label = get_market_cap_label(meta.get("market_cap", 0))

    # Translate only after filtering.
    headline_he = translate_to_hebrew(headline)

    summary_clean = clean_text(summary)
    if not summary_clean:
        summary_clean = headline

    summary_he = translate_to_hebrew(summary_clean[:900])

    publish_time_et = format_us_eastern_time(published)
    publish_time_il = format_israel_time(published)

    reasons_text = "\n".join([f"• {reason}" for reason in quality.get("reasons", [])[:6]])

    price_text = f"${price:.2f}" if price else "לא זמין"

    reaction_text = "אין מספיק נתוני מחיר."
    if price_reaction:
        reaction_text = price_reaction.get("summary_he", "אין מספיק נתוני מחיר.")

    reverse_text = ""
    if reverse_mode and mover_data:
        reverse_text = (
            f"\n🔁 זוהה במסלול הפוך:\n"
            f"• המניה כבר עולה היום: {mover_data['day_gain']:.2f}%\n"
            f"• מחיר קודם: ${mover_data['prev_close']:.2f}\n"
            f"• מחיר נוכחי: ${mover_data['current_price']:.2f}\n"
            f"• נשלח רק כי נמצאה ידיעה רלוונטית והמניה עדיין מתחת ל־20% יומי\n"
        )

    warning_text = ""
    if price_reaction and price_reaction.get("warning"):
        warning_text = f"\n⚠️ אזהרה:\n{price_reaction['warning']}\n"

    message = (
        f"{quality['label']} | ציון איכות: {quality['score']}/100\n"
        f"{quality['news_type']}\n"
        f"\n"
        f"📊 סימבול: {ticker}\n"
        f"🏢 שם החברה: {company_name}\n"
        f"💼 שווי שוק: {market_cap_label}\n"
        f"💵 מחיר מניה: {price_text}\n"
        f"🏭 מגזר: {sector_he}\n"
        f"{reverse_text}"
        f"\n"
        f"📰 כותרת בעברית:\n"
        f"{headline_he}\n"
        f"\n"
        f"🧾 כותרת מקור באנגלית:\n"
        f"{headline}\n"
        f"\n"
        f"📋 תוכן הידיעה בעברית:\n"
        f"{summary_he[:1000]}\n"
        f"\n"
        f"📈 תגובת מחיר / פריצה:\n"
        f"{reaction_text}\n"
        f"{warning_text}"
        f"\n"
        f"🧠 סיבת הדירוג:\n"
        f"{reasons_text}\n"
        f"\n"
        f"📅 פורסם:\n"
        f"{publish_time_il}\n"
        f"{publish_time_et}\n"
        f"\n"
        f"📡 מקור: {source}\n"
        f"\n"
        f"🔗 קישור לכתבה:\n"
        f"{url_link}"
    )

    if send_telegram(message):
        print(f"[ALERT] {ticker} | score {quality['score']}/100 | {price_text}")


def send_usaspending_alert(ticker, recipient, amount, agency, description, award_id, price):
    meta = get_finnhub_company_profile(ticker)
    company_name = meta.get("name") or ticker_to_company.get(ticker, recipient)
    sector_he = meta.get("sector_he", "לא ידוע")
    market_cap_label = get_market_cap_label(meta.get("market_cap", 0))
    link = f"https://www.usaspending.gov/award/{award_id}/"

    description_he = translate_to_hebrew(description[:700]) if description else "אין תיאור"
    price_text = f"${price:.2f}" if price else "לא זמין"

    message = (
        f"🔥 חוזה ממשלתי | ציון איכות: 80/100\n"
        f"ידיעה חיובית חזקה\n"
        f"\n"
        f"📊 סימבול: {ticker}\n"
        f"🏢 שם החברה: {company_name}\n"
        f"💼 שווי שוק: {market_cap_label}\n"
        f"💵 מחיר מניה: {price_text}\n"
        f"🏭 מגזר: {sector_he}\n"
        f"\n"
        f"📰 כותרת:\n"
        f"חוזה ממשלתי בסך ${amount:,.0f} מ-{agency}\n"
        f"\n"
        f"📋 תוכן הידיעה:\n"
        f"{description_he[:900]}\n"
        f"\n"
        f"🧠 סיבת הדירוג:\n"
        f"• מקור ממשלתי רשמי\n"
        f"• חוזה מעל ${USASPENDING_MIN_AMOUNT:,.0f}\n"
        f"• חברה ציבורית מזוהה\n"
        f"\n"
        f"📅 פורסם בתאריך: היום\n"
        f"📡 מקור: USAspending.gov\n"
        f"\n"
        f"🔗 קישור לכתבה:\n"
        f"{link}"
    )

    if send_telegram(message):
        print(f"[USAspending ALERT] {ticker} | ${amount:,.0f}")


# =========================================================
# MAIN
# =========================================================
print("=" * 70)
print("CATALYST NEWS BOT V5 STARTED")
print("=" * 70)

load_us_tickers()

print(f"Market News: every {MARKET_NEWS_INTERVAL}s")
print(f"GlobeNewswire / PRNewswire: every {PRESS_RELEASE_INTERVAL}s")
print(f"FDA Official: every {FDA_INTERVAL}s")
print(f"USAspending: every {USASPENDING_INTERVAL}s")
print(f"Reverse Movers: every {MOVERS_SCAN_INTERVAL}s")
print(f"News max age: {NEWS_MAX_AGE_SECONDS / 3600:.0f} hours")
print(f"Max stock price: ${MAX_PRICE}")
print(f"Reverse mover range: {MOVER_MIN_DAY_GAIN}%-{MOVER_MAX_DAY_GAIN}%")
print("=" * 70)

send_telegram(
    f"🚀 בוט קטליזטורים V5 הופעל\n"
    f"\n"
    f"📡 מקורות:\n"
    f"• Finnhub Market News\n"
    f"• Finnhub Company News למניות שעולות\n"
    f"• GlobeNewswire RSS\n"
    f"• PRNewswire RSS\n"
    f"• FDA Official\n"
    f"• USAspending.gov\n"
    f"\n"
    f"🎯 מנטר {len(all_us_tickers)} מניות אמריקאיות\n"
    f"🕒 סורק חדשות עד 6 שעות אחורה\n"
    f"🔁 מסלול הפוך פעיל: מניות שעלו 10%-20% ואז חיפוש חדשות\n"
    f"⛔ מעל 20% יומי: לא שולח במסלול ההפוך\n"
    f"✅ תרגום לעברית אחרי סינון\n"
    f"✅ בדיקת תגובת מחיר / פריצה\n"
    f"✅ סינון עורכי דין וכפילויות\n"
)

while True:
    try:
        now = time.time()

        if now - last_market_news_scan >= MARKET_NEWS_INTERVAL:
            scan_market_news()
            last_market_news_scan = now

        if now - last_press_release_scan >= PRESS_RELEASE_INTERVAL:
            scan_globenewswire()
            scan_prnewswire()
            last_press_release_scan = now

        if now - last_fda_scan >= FDA_INTERVAL:
            scan_fda_press_announcements()
            last_fda_scan = now

        if now - last_usaspending_scan >= USASPENDING_INTERVAL:
            scan_usaspending()
            last_usaspending_scan = now

        if now - last_movers_scan >= MOVERS_SCAN_INTERVAL:
            scan_intraday_movers_then_news()
            last_movers_scan = now

        if len(sent_alerts) > 20000:
            sent_alerts.clear()
            last_symbol_category_alert.clear()
            print("[Cleanup] sent_alerts and cooldowns cleared")

        time.sleep(10)

    except KeyboardInterrupt:
        print("CATALYST NEWS BOT STOPPED BY USER")
        break

    except Exception as e:
        print(f"MAIN LOOP ERROR: {e}")
        time.sleep(30)
