import os
import time
import json
import re
import html
from urllib.request import urlopen, Request
from urllib.parse import quote
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import (
    StockLatestQuoteRequest,
    StockLatestTradeRequest,
)
from alpaca.data.enums import DataFeed


# =========================================================
# CATALYST NEWS BOT V7
# =========================================================
# מטרה:
# בוט חדשות בלבד.
#
# הבוט לא מחפש מניות שעלו ואז חדשות.
# הבוט מחפש חדשות, מאמת שהידיעה באמת שייכת לטיקר,
# מסנן PR חלש / שיוכים שגויים / קרנות / Warrants,
# ושולח הודעת טלגרם קצרה, מדויקת וברורה.
# =========================================================


# =========================================================
# API KEYS
# =========================================================
CATALYST_BOT_TOKEN = os.getenv("CATALYST_BOT_TOKEN")
CATALYST_CHAT_ID = os.getenv("CATALYST_CHAT_ID")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY") or os.getenv("APCA_API_KEY_ID")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")

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

DATA_FEED_NAME = os.getenv("ALPACA_DATA_FEED", "SIP").upper().strip()
DATA_FEED = DataFeed.IEX if DATA_FEED_NAME == "IEX" else DataFeed.SIP


# =========================================================
# SETTINGS
# =========================================================
FINNHUB_MARKET_NEWS_INTERVAL = 90
GLOBENEWSWIRE_INTERVAL = 90
PRNEWSWIRE_INTERVAL = 90
FDA_INTERVAL = 300
USASPENDING_INTERVAL = 1800

NEWS_MAX_AGE_SECONDS = 6 * 3600

MAX_PRICE = 70.0

WATCHLIST_SCORE = 65
STRONG_SCORE = 78

DUPLICATE_COOLDOWN_SECONDS = 45 * 60

USASPENDING_MIN_AMOUNT = 10_000_000

MIN_TICKER_VERIFICATION_SCORE = 75

MAX_SUMMARY_CHARS_TELEGRAM = 450


# =========================================================
# STATE
# =========================================================
sent_alerts = set()
last_symbol_category_alert = {}

all_us_tickers = set()
ticker_to_company = {}
ticker_to_exchange = {}
ticker_metadata = {}
sector_translation_cache = {}


# =========================================================
# HEBREW SECTORS
# =========================================================
SECTOR_HEBREW = {
    "Technology": "טכנולוגיה",
    "Health Care": "בריאות",
    "Healthcare": "בריאות",
    "Biotechnology": "ביוטכנולוגיה",
    "Pharmaceuticals": "פארמה",
    "Financial Services": "פיננסים",
    "Financials": "פיננסים",
    "Finance": "פיננסים",
    "Consumer Cyclical": "צריכה מחזורית",
    "Consumer Defensive": "צריכה בסיסית",
    "Communication Services": "תקשורת ומדיה",
    "Industrials": "תעשייה",
    "Energy": "אנרגיה",
    "Basic Materials": "חומרי גלם",
    "Materials": "חומרי גלם",
    "Real Estate": 'נדל"ן',
    "Utilities": "תשתיות",
    "Software": "תוכנה",
    "Semiconductors": "שבבים",
    "Banks": "בנקאות",
    "Insurance": "ביטוח",
    "Oil & Gas": "נפט וגז",
    "Aerospace & Defense": "תעופה וביטחון",
    "Medical Devices": "מכשור רפואי",
    "Diagnostics & Research": "אבחון ומחקר",
    "Capital Markets": "שוק ההון",
}


# =========================================================
# BASIC HELPERS
# =========================================================
def clean_text(text):
    if not text:
        return ""

    text = html.unescape(str(text))
    text = re.sub(r"<script.*?</script>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def has_any(text, patterns):
    t = (text or "").lower()
    return any(p.lower() in t for p in patterns)


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def translate_to_hebrew(text):
    """
    תרגום רק אחרי שהידיעה עברה סינון.
    חשוב:
    התרגום הוא כלי עזר בלבד.
    הכותרת המקורית באנגלית תמיד תישלח כדי למנוע עיוות שמות חברות.
    """
    if not text:
        return ""

    try:
        text = clean_text(text)[:700]
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


def format_us_eastern_time(timestamp):
    try:
        dt = datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
        et = dt.astimezone(ZoneInfo("America/New_York"))
        return et.strftime("%d.%m.%Y %H:%M ET")
    except Exception:
        return "לא ידוע"


def format_israel_time(timestamp):
    try:
        dt = datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
        il = dt.astimezone(ZoneInfo("Asia/Jerusalem"))
        return il.strftime("%d.%m.%Y %H:%M ישראל")
    except Exception:
        return "לא ידוע"


def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{CATALYST_BOT_TOKEN}/sendMessage"

        payload = {
            "chat_id": CATALYST_CHAT_ID,
            "text": message,
            "disable_web_page_preview": False,
        }

        req = Request(
            url,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )

        urlopen(req, timeout=10).read()
        return True

    except Exception as e:
        print(f"Telegram error: {e}")
        return False


# =========================================================
# ASSET FILTERING
# =========================================================
BAD_ASSET_NAME_PATTERNS = [
    " warrant",
    " warrants",
    " wt ",
    " unit",
    " units",
    " right",
    " rights",
    " preferred",
    " preference",
    " depositary",
    " notes",
    " note due",
    " senior note",
    " bond",
    " etf",
    " fund",
    " closed-end",
    " closed end",
    " income fund",
    " trust",
    " index",
    " spdr",
    " ishares",
    " invesco",
    " proshares",
    " direxion",
    " leveraged",
    " 2x ",
    " 3x ",
]


def is_allowed_common_stock(symbol, name):
    symbol = (symbol or "").upper().strip()
    name_l = f" {clean_text(name).lower()} "

    if not symbol:
        return False

    if "." in symbol or "/" in symbol or "-" in symbol:
        return False

    if len(symbol) > 5:
        return False

    for pat in BAD_ASSET_NAME_PATTERNS:
        if pat in name_l:
            return False

    return True


# =========================================================
# COMPANY NORMALIZATION
# =========================================================
LEGAL_SUFFIXES = [
    " INCORPORATED",
    " CORPORATION",
    " CORP",
    " COMPANY",
    " INC",
    " LIMITED",
    " LTD",
    " PLC",
    " LLC",
    " L L C",
    " N V",
    " NV",
    " S A",
    " SA",
    " AG",
    " SE",
    " LP",
    " L P",
    " THE",
    " CO",
    " GROUP",
    " HOLDINGS",
    " HOLDING",
    " INTERNATIONAL",
]

GENERIC_COMPANY_WORDS = {
    "THE",
    "INC",
    "CORP",
    "CORPORATION",
    "COMPANY",
    "GROUP",
    "HOLDINGS",
    "HOLDING",
    "LIMITED",
    "LTD",
    "PLC",
    "LLC",
    "NV",
    "SA",
    "AG",
    "SE",
    "LP",
    "CO",
    "TECHNOLOGY",
    "TECHNOLOGIES",
    "THERAPEUTICS",
    "PHARMA",
    "PHARMACEUTICALS",
    "BIOTECH",
    "HEALTH",
    "HEALTHCARE",
    "MEDICAL",
    "SYSTEMS",
    "SOLUTIONS",
    "GLOBAL",
    "INTERNATIONAL",
    "STRATEGIC",
    "PARTNERS",
    "PARTNER",
    "CAPITAL",
    "INVESTMENT",
    "INVESTMENTS",
    "POWER",
    "ENERGY",
    "DIGITAL",
    "ASSET",
    "ACQUISITION",
    "FUND",
    "TRUST",
    "INCOME",
    "TOTAL",
    "RETURN",
    "AMERICAN",
    "AMERICA",
}


def normalize_company_name(name):
    if not name:
        return ""

    n = clean_text(name).upper()
    n = n.replace("&", " AND ")
    n = re.sub(r"[^A-Z0-9\s]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()

    changed = True

    while changed:
        changed = False

        for suffix in LEGAL_SUFFIXES:
            if n.endswith(suffix):
                n = n[: -len(suffix)].strip()
                changed = True

    n = re.sub(r"\s+", " ", n).strip()
    return n


def important_company_words(name):
    n = normalize_company_name(name)

    if not n:
        return []

    words = []

    for w in n.split():
        if len(w) < 3:
            continue

        if w in GENERIC_COMPANY_WORDS:
            continue

        words.append(w)

    return words[:5]


def company_name_match_score(company_name, headline, summary):
    official = normalize_company_name(company_name)

    text = f" {clean_text(headline).upper()} {clean_text(summary).upper()} "
    text = text.replace("&", " AND ")
    text = re.sub(r"[^A-Z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)

    if not official:
        return 0, "אין שם חברה רשמי"

    if f" {official} " in text:
        return 100, "שם החברה הרשמי מופיע בידיעה"

    words = important_company_words(company_name)

    if not words:
        return 0, "אין מילים ייחודיות בשם החברה"

    hits = []
    for w in words:
        if f" {w} " in text:
            hits.append(w)

    if len(words) == 1:
        only = words[0]

        if len(only) >= 6 and only not in GENERIC_COMPANY_WORDS and len(hits) == 1:
            return 80, f"התאמה לפי שם ייחודי: {only}"

        return 0, "התאמה חלשה מדי לפי מילה אחת"

    if len(hits) >= 3:
        return 95, f"התאמת שם חזקה: {', '.join(hits)}"

    if len(hits) >= 2:
        return 82, f"התאמת שם טובה: {', '.join(hits)}"

    return 0, "רק מילה אחת נמצאה בשם החברה"


# =========================================================
# CATALYSTS
# =========================================================
POSITIVE_CATALYSTS = {
    "FDA_APPROVAL_FINAL": {
        "label": "אישור FDA סופי",
        "base": 88,
        "keywords": [
            "fda approves",
            "fda approved",
            "receives fda approval",
            "received fda approval",
            "fda grants approval",
            "approved by fda",
            "approval from the fda",
            "nda approval",
            "bla approval",
            "snda approval",
            "marketing authorization",
        ],
    },
    "FDA_CLEARANCE_510K": {
        "label": "FDA Clearance / 510(k)",
        "base": 76,
        "keywords": [
            "fda clearance",
            "fda cleared",
            "510(k) clearance",
            "510k clearance",
            "receives 510(k)",
            "received 510(k)",
            "cleared by the fda",
        ],
    },
    "FDA_ACCEPTANCE": {
        "label": "FDA קיבל בקשה לבדיקה",
        "base": 66,
        "keywords": [
            "fda accepts nda",
            "fda accepted nda",
            "nda accepted",
            "fda accepts bla",
            "fda accepted bla",
            "bla accepted",
            "application accepted by fda",
            "pdufa date",
            "priority review",
        ],
    },
    "FDA_DESIGNATION": {
        "label": "מעמד FDA מיוחד",
        "base": 60,
        "keywords": [
            "fast track designation",
            "orphan drug designation",
            "breakthrough therapy designation",
            "rare pediatric disease designation",
            "rmat designation",
            "qidp designation",
        ],
    },
    "PHASE_SUCCESS": {
        "label": "ניסוי קליני חיובי",
        "base": 88,
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
    "ACQUISITION": {
        "label": "רכישה / מיזוג",
        "base": 82,
        "keywords": [
            "definitive agreement to acquire",
            "definitive merger agreement",
            "agreement to acquire",
            "agreement to be acquired",
            "to be acquired by",
            "all-cash transaction",
            "tender offer",
            "transaction valued at",
            "deal valued at",
            "per share in cash",
            "take-private",
            "take private",
        ],
    },
    "STRATEGIC_INVESTMENT": {
        "label": "השקעה אסטרטגית",
        "base": 74,
        "keywords": [
            "strategic investment",
            "minority stake",
            "equity stake",
            "takes stake",
            "preferred stock investment",
            "nvidia invests",
            "microsoft invests",
            "google invests",
            "amazon invests",
            "openai invests",
        ],
    },
    "PARTNERSHIP": {
        "label": "שותפות משמעותית",
        "base": 68,
        "keywords": [
            "strategic partnership",
            "commercial partnership",
            "exclusive partnership",
            "partnership with nvidia",
            "partnership with microsoft",
            "partnership with google",
            "partnership with amazon",
            "collaboration with openai",
            "selected by microsoft",
            "selected by amazon",
        ],
    },
    "CONTRACT": {
        "label": "חוזה / הזמנה גדולה",
        "base": 70,
        "keywords": [
            "awarded contract",
            "wins contract",
            "won contract",
            "government contract awarded",
            "federal contract",
            "dod contract",
            "department of defense contract",
            "u.s. army contract",
            "u.s. navy contract",
            "nasa contract",
            "multi-year agreement",
            "purchase order",
        ],
    },
    "CRYPTO_TREASURY": {
        "label": "Crypto Treasury",
        "base": 78,
        "keywords": [
            "bitcoin treasury",
            "ethereum treasury",
            "crypto treasury",
            "digital asset treasury",
            "adds bitcoin to treasury",
            "purchases bitcoin",
            "purchases ethereum",
            "solana treasury",
            "xrp treasury",
        ],
    },
    "AI_PIVOT": {
        "label": "מהלך AI",
        "base": 60,
        "keywords": [
            "pivots to ai",
            "pivot to artificial intelligence",
            "ai-focused",
            "new ai division",
            "ai strategy",
            "launches ai platform",
            "generative ai platform",
            "artificial intelligence platform",
        ],
    },
    "EARNINGS_BEAT": {
        "label": "דוחות חזקים",
        "base": 62,
        "keywords": [
            "earnings beat",
            "beats earnings estimates",
            "record revenue",
            "record quarterly revenue",
            "raises guidance",
            "raises full-year guidance",
            "beats revenue expectations",
            "above consensus",
        ],
    },
    "BREAKTHROUGH": {
        "label": "פטנט / פריצת דרך",
        "base": 58,
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
    "REIMBURSEMENT": {
        "label": "CMS / Medicare / שיפוי",
        "base": 74,
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
    "NASDAQ_COMPLIANCE": {
        "label": "חזרה לעמידה בנאסדק",
        "base": 50,
        "keywords": [
            "regained compliance",
            "nasdaq compliance",
            "compliance with nasdaq",
            "minimum bid price requirement",
        ],
    },
}


NEGATIVE_RISK_PATTERNS = [
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
    "registered direct offering",
    "public offering",
    "at-the-market offering",
    "atm offering",
    "shelf offering",
    "priced offering",
    "private placement",
    "convertible notes",
    "warrants",
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
    "forum held",
    "forum to mark",
    "anniversary",
    "intercultural dialogue",
    "cultural dialogue",
    "embassy",
    "xinhua",
    "hotel stay",
    "limited-time hotel",
    "tour dates",
    "grammy-nominated",
    "inducement grants",
    "nasdaq listing rule 5635",
    "5635(c)(4)",
    "monthly distributions",
    "continuing monthly distributions",
    "distribution plan",
    "declares monthly distribution",
    "dividend declaration",
    "closed-end fund",
    "fund announces",
    "funds announce",
    "net asset value",
    "nav per share",
    "etf",
    "webinar",
    "podcast",
    "newsletter",
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
    "recover losses",
    "contact the firm",
    "no cost to you",
]


WEAK_PR_PATTERNS = [
    "launches new website",
    "launches initiative",
    "announces new brand",
    "corporate update",
    "business update",
    "letter to shareholders",
    "appoints",
    "appointment of",
    "joins board",
    "advisory board",
    "marketing campaign",
    "sponsorship",
    "brand ambassador",
]


FINAL_EVENT_PATTERNS = [
    "completion of take-private",
    "completed take-private",
    "completion of merger",
    "completed merger",
    "completed acquisition",
    "ceased trading",
    "will no longer be listed",
    "no longer listed",
    "delisted from",
]


def detect_positive_catalysts(text):
    found = []
    t = f" {clean_text(text).lower()} "

    for category, data in POSITIVE_CATALYSTS.items():
        for kw in data["keywords"]:
            if kw in t:
                found.append(
                    {
                        "category": category,
                        "label": data["label"],
                        "base": data["base"],
                        "keyword": kw,
                    }
                )
                break

    found.sort(key=lambda x: x["base"], reverse=True)
    return found


# =========================================================
# SOURCE SCORING
# =========================================================
def get_source_score(source):
    s = (source or "").lower()

    if "fda official" in s:
        return 15, "FDA רשמי"

    if "usaspending" in s:
        return 14, "מקור ממשלתי"

    if "sec" in s or "edgar" in s:
        return 14, "SEC"

    if "businesswire" in s:
        return 8, "BusinessWire"

    if "globenewswire" in s:
        return 6, "GlobeNewswire"

    if "prnewswire" in s:
        return 3, "PRNewswire"

    if "finnhub company news" in s:
        return 7, "Finnhub Company News"

    if "finnhub" in s:
        return 3, "Finnhub"

    return 0, "מקור רגיל"


def quality_label(score):
    if score >= 90:
        return "💎 נדיר"
    if score >= 82:
        return "🚀 חזק מאוד"
    if score >= 72:
        return "🔥 חזק"
    if score >= 65:
        return "🟡 חשוב לבדיקה"
    return "⚪ חלש"


# =========================================================
# MONEY EXTRACTION
# =========================================================
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
            try:
                return float(match.group(1)) * multiplier
            except Exception:
                return 0

    return 0


# =========================================================
# TICKER DATA
# =========================================================
def load_us_tickers():
    global all_us_tickers
    global ticker_to_company
    global ticker_to_exchange

    try:
        request = GetAssetsRequest(
            asset_class=AssetClass.US_EQUITY,
            status=AssetStatus.ACTIVE,
        )

        assets = trading_client.get_all_assets(request)

        loaded = 0
        skipped = 0

        for asset in assets:
            try:
                symbol = asset.symbol.upper().strip()
                name = asset.name or ""

                if not asset.tradable:
                    skipped += 1
                    continue

                if asset.exchange not in ["NYSE", "NASDAQ", "AMEX"]:
                    skipped += 1
                    continue

                if not is_allowed_common_stock(symbol, name):
                    skipped += 1
                    continue

                all_us_tickers.add(symbol)
                ticker_to_company[symbol] = name
                ticker_to_exchange[symbol] = asset.exchange

                loaded += 1

            except Exception:
                skipped += 1

        print(f"Loaded {loaded} common-stock-like US tickers. Skipped {skipped} weak assets.")

    except Exception as e:
        print(f"Error loading tickers: {e}")


def get_finnhub_company_profile(symbol):
    if symbol in ticker_metadata:
        return ticker_metadata[symbol]

    metadata = {
        "sector_en": "",
        "sector_he": "לא ידוע",
        "market_cap": 0,
        "name": ticker_to_company.get(symbol, ""),
    }

    try:
        url = f"https://finnhub.io/api/v1/stock/profile2?symbol={symbol}&token={FINNHUB_API_KEY}"
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})

        with urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())

        if isinstance(data, dict):
            sector_en = data.get("finnhubIndustry", "") or ""
            market_cap = data.get("marketCapitalization", 0) or 0
            name = data.get("name", "") or ticker_to_company.get(symbol, "")

            if market_cap:
                market_cap *= 1_000_000

            metadata = {
                "sector_en": sector_en,
                "sector_he": translate_sector(sector_en),
                "market_cap": market_cap,
                "name": name,
            }

    except Exception as e:
        print(f"Finnhub profile error for {symbol}: {e}")

    ticker_metadata[symbol] = metadata
    return metadata


# =========================================================
# TICKER EXTRACTION + VERIFICATION
# =========================================================
def extract_explicit_tickers(related, headline, summary):
    found = []
    seen = set()

    def add(symbol):
        s = (symbol or "").upper().strip()

        if s in all_us_tickers and s not in seen:
            found.append(s)
            seen.add(s)

    if related:
        for part in str(related).replace(";", ",").split(","):
            add(part)

    text = f"{headline} {summary}"

    patterns = [
        r"\bNASDAQ\s*:\s*([A-Z]{1,5})\b",
        r"\bNYSE\s*:\s*([A-Z]{1,5})\b",
        r"\bNYSE\s+AMERICAN\s*:\s*([A-Z]{1,5})\b",
        r"\bAMEX\s*:\s*([A-Z]{1,5})\b",
        r"\$([A-Z]{1,5})\b",
        r"\(([A-Z]{1,5})\)",
    ]

    for pattern in patterns:
        matches = re.findall(pattern, text, flags=re.IGNORECASE)

        for match in matches:
            add(match)

    return found[:10]


def verify_news_belongs_to_ticker(ticker, headline, summary, related):
    meta = get_finnhub_company_profile(ticker)
    official_name = meta.get("name") or ticker_to_company.get(ticker, "")

    explicit = extract_explicit_tickers(related, headline, summary)

    if ticker in explicit:
        return {
            "ok": True,
            "score": 100,
            "reason": "הטיקר מופיע במפורש בידיעה",
        }

    score, reason = company_name_match_score(official_name, headline, summary)

    if score >= MIN_TICKER_VERIFICATION_SCORE:
        return {
            "ok": True,
            "score": score,
            "reason": reason,
        }

    return {
        "ok": False,
        "score": score,
        "reason": f"לא אומת קשר מספיק חזק בין הידיעה לטיקר. {reason}",
    }


def find_company_name_candidates(headline, summary, max_results=8):
    candidates = []

    for ticker, company_name in ticker_to_company.items():
        score, reason = company_name_match_score(company_name, headline, summary)

        if score >= MIN_TICKER_VERIFICATION_SCORE:
            candidates.append((ticker, score, reason))

    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[:max_results]


def choose_verified_ticker(news):
    headline = clean_text(news.get("headline", ""))
    summary = clean_text(news.get("summary", ""))
    related = news.get("related", "")

    explicit = extract_explicit_tickers(related, headline, summary)

    if explicit:
        verified = []

        for ticker in explicit:
            verification = verify_news_belongs_to_ticker(ticker, headline, summary, related)

            if verification["ok"]:
                verified.append((ticker, verification["score"], verification))

        if verified:
            verified.sort(key=lambda x: x[1], reverse=True)
            return verified[0][0], verified[0][2]

    name_candidates = find_company_name_candidates(headline, summary)

    if name_candidates:
        ticker, score, reason = name_candidates[0]

        return ticker, {
            "ok": True,
            "score": score,
            "reason": reason,
        }

    return None, {
        "ok": False,
        "score": 0,
        "reason": "לא נמצא טיקר מפורש או שם חברה מדויק",
    }


# =========================================================
# PRICE
# =========================================================
def get_current_price(symbol):
    try:
        req = StockLatestTradeRequest(symbol_or_symbols=symbol, feed=DATA_FEED)
        trades = data_client.get_stock_latest_trade(req)

        if symbol in trades:
            trade = trades[symbol]
            price = safe_float(trade.price)

            if price > 0:
                return price

    except Exception as e:
        print(f"Latest trade error for {symbol}: {e}")

    try:
        req = StockLatestQuoteRequest(symbol_or_symbols=symbol, feed=DATA_FEED)
        quotes = data_client.get_stock_latest_quote(req)

        if symbol in quotes:
            quote_obj = quotes[symbol]

            ask = safe_float(quote_obj.ask_price)
            bid = safe_float(quote_obj.bid_price)

            if ask > 0 and bid > 0:
                return (ask + bid) / 2

            if ask > 0:
                return ask

            if bid > 0:
                return bid

    except Exception as e:
        print(f"Latest quote error for {symbol}: {e}")

    return None


def get_market_cap_label(market_cap):
    if not market_cap:
        return "לא זמין"

    if market_cap < 50_000_000:
        return f"Nano ${market_cap / 1e6:.1f}M"

    if market_cap < 300_000_000:
        return f"Micro ${market_cap / 1e6:.0f}M"

    if market_cap < 2_000_000_000:
        return f"Small ${market_cap / 1e6:.0f}M"

    if market_cap < 10_000_000_000:
        return f"Mid ${market_cap / 1e9:.1f}B"

    if market_cap < 200_000_000_000:
        return f"Large ${market_cap / 1e9:.0f}B"

    return f"Mega ${market_cap / 1e9:.0f}B"


# =========================================================
# QUALITY
# =========================================================
def reject_quality(reason, category):
    return {
        "send": False,
        "score": 0,
        "label": "❌ חסום",
        "news_type": "לא נשלח",
        "primary_category": category,
        "reasons": [reason],
        "reason": reason,
    }


def calculate_news_quality(
    ticker,
    headline,
    summary,
    source,
    catalysts,
    meta,
    verification,
):
    text = f"{headline} {summary}"
    text_lower = text.lower()
    reasons = []

    if has_any(text_lower, LAW_FIRM_SPAM_PATTERNS):
        return reject_quality("פרסום עורכי דין / תביעה ייצוגית", "LAW_FIRM_SPAM")

    if has_any(text_lower, BLOCK_PATTERNS):
        return reject_quality("PR חלש / לא חדשות מסחריות", "BLOCKED_PR")

    if has_any(text_lower, FINAL_EVENT_PATTERNS):
        return reject_quality("אירוע סופי / פחות רלוונטי לכניסה חדשה", "FINAL_EVENT")

    if has_any(text_lower, NEGATIVE_RISK_PATTERNS):
        return reject_quality("סיכון שלילי / דילול / אירוע בעייתי", "NEGATIVE_RISK")

    if not verification or not verification.get("ok"):
        return reject_quality("הטיקר לא אומת מול הידיעה", "BAD_TICKER_MATCH")

    if not catalysts:
        return reject_quality("אין קטליזטור חיובי ברור", "NO_CATALYST")

    best = catalysts[0]
    primary_category = best["category"]

    score = best["base"]
    reasons.append(best["label"])
    reasons.append(f"אימות: {verification.get('reason', '')}")

    source_bonus, source_reason = get_source_score(source)
    score += source_bonus
    reasons.append(source_reason)

    market_cap = meta.get("market_cap", 0) if meta else 0
    amount = extract_money_amount(text)

    if amount and market_cap:
        ratio = amount / market_cap

        if ratio >= 0.10:
            score += 16
            reasons.append(f"סכום גדול מול שווי החברה ({ratio * 100:.1f}%)")
        elif ratio >= 0.03:
            score += 9
            reasons.append(f"סכום בינוני מול שווי החברה ({ratio * 100:.1f}%)")
        elif ratio >= 0.01:
            score += 4
            reasons.append(f"סכום קטן-בינוני מול שווי החברה ({ratio * 100:.1f}%)")
        else:
            score -= 6
            reasons.append("סכום קטן יחסית לשווי החברה")

    if has_any(text_lower, WEAK_PR_PATTERNS):
        score -= 12
        reasons.append("נמצאה שפת PR חלשה")

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
        score += 4
        reasons.append("תחום חם")

    score = max(0, min(100, int(score)))

    if score >= 85:
        news_type = "חדשה חזקה מאוד"
    elif score >= STRONG_SCORE:
        news_type = "חדשה חזקה"
    elif score >= WATCHLIST_SCORE:
        news_type = "חדשה חשובה לבדיקה"
    else:
        news_type = "חלשה"

    return {
        "send": score >= WATCHLIST_SCORE,
        "score": score,
        "label": quality_label(score),
        "news_type": news_type,
        "primary_category": primary_category,
        "reasons": reasons,
        "reason": " | ".join(reasons[:3]),
    }


# =========================================================
# RSS PARSING
# =========================================================
def parse_rss_items(rss_content):
    items = []

    item_blocks = re.findall(
        r"<item\b[^>]*>(.*?)</item>",
        rss_content,
        re.DOTALL | re.IGNORECASE,
    )

    for block in item_blocks:
        try:
            title = ""
            link = ""
            desc = ""
            timestamp = time.time()

            title_match = re.search(
                r"<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>",
                block,
                re.DOTALL | re.IGNORECASE,
            )

            if title_match:
                title = clean_text(title_match.group(1))

            link_match = re.search(
                r"<link>(.*?)</link>",
                block,
                re.DOTALL | re.IGNORECASE,
            )

            if link_match:
                link = clean_text(link_match.group(1))

            desc_match = re.search(
                r"<description>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</description>",
                block,
                re.DOTALL | re.IGNORECASE,
            )

            if desc_match:
                desc = clean_text(desc_match.group(1))

            date_match = re.search(
                r"<pubDate>(.*?)</pubDate>",
                block,
                re.DOTALL | re.IGNORECASE,
            )

            if date_match:
                date_str = clean_text(date_match.group(1))

                try:
                    dt = parsedate_to_datetime(date_str)

                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)

                    timestamp = dt.timestamp()

                except Exception:
                    pass

            if title and link:
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
# DEDUPLICATION
# =========================================================
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
# ALERT FORMAT
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
    verification,
):
    company_name = meta.get("name") or ticker_to_company.get(ticker, "לא ידוע")
    sector_he = meta.get("sector_he", "לא ידוע")
    market_cap_label = get_market_cap_label(meta.get("market_cap", 0))

    price_text = f"${price:.2f}" if price else "לא זמין"

    summary_clean = clean_text(summary)
    summary_he = translate_to_hebrew(summary_clean[:MAX_SUMMARY_CHARS_TELEGRAM]) if summary_clean else ""

    reasons_text = " | ".join(quality.get("reasons", [])[:3])

    message = (
        f"{quality['label']} {ticker} | {quality['score']}/100\n"
        f"{quality['news_type']} | {price_text} | {market_cap_label}\n"
        f"{company_name}\n"
        f"ענף: {sector_he}\n"
        f"\n"
        f"📰 כותרת מקור:\n"
        f"{headline}\n"
        f"\n"
        f"📌 תקציר:\n"
        f"{summary_he[:MAX_SUMMARY_CHARS_TELEGRAM] if summary_he else 'אין תקציר'}\n"
        f"\n"
        f"✅ אימות טיקר: {verification.get('reason', 'אומת')}\n"
        f"🧠 סיבה: {reasons_text}\n"
        f"\n"
        f"🕒 {format_israel_time(published)} | {format_us_eastern_time(published)}\n"
        f"📡 מקור: {source}\n"
        f"🔗 {url_link}"
    )

    if send_telegram(message):
        print(f"[ALERT] {ticker} | score {quality['score']}/100 | {price_text}")


def send_usaspending_alert(ticker, recipient, amount, agency, description, award_id, price):
    meta = get_finnhub_company_profile(ticker)

    company_name = meta.get("name") or ticker_to_company.get(ticker, recipient)
    market_cap_label = get_market_cap_label(meta.get("market_cap", 0))
    sector_he = meta.get("sector_he", "לא ידוע")
    price_text = f"${price:.2f}" if price else "לא זמין"

    link = f"https://www.usaspending.gov/award/{award_id}/"

    description_he = translate_to_hebrew(description[:400]) if description else "אין תיאור"

    message = (
        f"🔥 {ticker} | חוזה ממשלתי | 80/100\n"
        f"{price_text} | {market_cap_label}\n"
        f"{company_name}\n"
        f"ענף: {sector_he}\n"
        f"\n"
        f"🏛️ חוזה: ${amount:,.0f}\n"
        f"סוכנות: {agency}\n"
        f"פרטים: {description_he}\n"
        f"\n"
        f"✅ אימות: USAspending + חברה ציבורית מזוהה\n"
        f"📡 מקור: USAspending.gov\n"
        f"🔗 {link}"
    )

    if send_telegram(message):
        print(f"[USAspending ALERT] {ticker} | ${amount:,.0f}")


# =========================================================
# PROCESS NEWS ITEM
# =========================================================
def process_news_item(news, source_override=None):
    headline = clean_text(news.get("headline", ""))
    summary = clean_text(news.get("summary", ""))
    url_link = news.get("url", "")
    related = news.get("related", "")
    published = news.get("datetime", time.time())
    source = source_override or news.get("source", "Unknown")

    if not headline:
        return

    if time.time() - safe_float(published, time.time()) > NEWS_MAX_AGE_SECONDS:
        return

    ticker, verification = choose_verified_ticker(news)

    if not ticker:
        print(f"[SKIP NO TICKER] {verification.get('reason')} | {headline[:90]}")
        return

    price = get_current_price(ticker)

    if price and price > MAX_PRICE:
        print(f"[SKIP PRICE] {ticker} price ${price:.2f} > ${MAX_PRICE}")
        return

    meta = get_finnhub_company_profile(ticker)
    catalysts = detect_positive_catalysts(f"{headline} {summary}")

    quality = calculate_news_quality(
        ticker=ticker,
        headline=headline,
        summary=summary,
        source=source,
        catalysts=catalysts,
        meta=meta,
        verification=verification,
    )

    if not quality["send"]:
        print(f"[SKIP] {ticker} | {quality['reason']} | {headline[:90]}")
        return

    primary_category = quality.get("primary_category", "UNKNOWN")
    duplicate_key = f"{ticker}|{primary_category}"
    now = time.time()

    if duplicate_key in last_symbol_category_alert:
        if now - last_symbol_category_alert[duplicate_key] < DUPLICATE_COOLDOWN_SECONDS:
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
        verification=verification,
    )

    sent_alerts.add(alert_hash)
    last_symbol_category_alert[duplicate_key] = now


# =========================================================
# SCANNERS
# =========================================================
def scan_finnhub_market_news():
    print("[Finnhub Market News] scanning...")

    try:
        url = f"https://finnhub.io/api/v1/news?category=general&token={FINNHUB_API_KEY}"
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})

        with urlopen(req, timeout=15) as response:
            news_items = json.loads(response.read().decode())

        if not isinstance(news_items, list):
            return

        now = time.time()

        recent = [
            item for item in news_items
            if now - safe_float(item.get("datetime", 0)) <= NEWS_MAX_AGE_SECONDS
        ]

        print(f"[Finnhub Market News] {len(recent)} recent articles")

        for item in recent:
            process_news_item(item, source_override="Finnhub")

    except Exception as e:
        print(f"[Finnhub Market News] error: {e}")


def scan_globenewswire():
    print("[GlobeNewswire] scanning...")

    try:
        url = "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire+-+News+about+Public+Companies"
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})

        with urlopen(req, timeout=15) as response:
            content = response.read().decode("utf-8", errors="ignore")

        items = parse_rss_items(content)
        now = time.time()

        recent = [
            item for item in items
            if now - item["timestamp"] <= NEWS_MAX_AGE_SECONDS
        ]

        print(f"[GlobeNewswire] {len(recent)} recent releases")

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

        recent = [
            item for item in items
            if now - item["timestamp"] <= NEWS_MAX_AGE_SECONDS
        ]

        print(f"[PRNewswire] {len(recent)} recent releases")

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

    fda_relevant_words = [
        "approves",
        "approved",
        "approval",
        "clearance",
        "cleared",
        "510(k)",
        "510k",
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

    now = time.time()

    recent = [
        item for item in items
        if now - item["timestamp"] <= NEWS_MAX_AGE_SECONDS or abs(item["timestamp"] - now) <= 60
    ]

    print(f"[FDA] {len(recent)} official items to check")

    for item in recent:
        text = f"{item['title']} {item['description']}"

        if not has_any(text, fda_relevant_words):
            continue

        news = {
            "headline": item["title"],
            "summary": item["description"],
            "url": item["link"],
            "datetime": item["timestamp"],
            "related": "",
        }

        process_news_item(news, source_override="FDA Official")


def find_ticker_for_usaspending_recipient(recipient):
    if not recipient:
        return None

    best_ticker = None
    best_score = 0

    for ticker, company_name in ticker_to_company.items():
        score, reason = company_name_match_score(company_name, recipient, recipient)

        if score > best_score:
            best_score = score
            best_ticker = ticker

    if best_ticker and best_score >= 90:
        return best_ticker

    return None


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
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0",
            },
        )

        with urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode())

        results = data.get("results", [])

        print(f"[USAspending] {len(results)} contracts")

        for contract in results:
            recipient = contract.get("Recipient Name", "")
            amount = safe_float(contract.get("Award Amount", 0))
            agency = contract.get("Awarding Agency", "")
            description = contract.get("Description", "")
            award_id = contract.get("Award ID", "")

            ticker = find_ticker_for_usaspending_recipient(recipient)

            if not ticker:
                continue

            price = get_current_price(ticker)

            if price and price > MAX_PRICE:
                continue

            duplicate_key = f"{ticker}|USASPENDING_CONTRACT"
            now = time.time()

            if duplicate_key in last_symbol_category_alert:
                if now - last_symbol_category_alert[duplicate_key] < DUPLICATE_COOLDOWN_SECONDS:
                    continue

            alert_hash = f"{ticker}|USASPENDING|{award_id}"

            if alert_hash in sent_alerts:
                continue

            send_usaspending_alert(
                ticker=ticker,
                recipient=recipient,
                amount=amount,
                agency=agency,
                description=description,
                award_id=award_id,
                price=price,
            )

            sent_alerts.add(alert_hash)
            last_symbol_category_alert[duplicate_key] = now

    except Exception as e:
        print(f"[USAspending] error: {e}")


# =========================================================
# MAIN
# =========================================================
print("=" * 70)
print("CATALYST NEWS BOT V7 STARTED")
print("=" * 70)

load_us_tickers()

print(f"Data feed: {DATA_FEED_NAME}")
print(f"Finnhub Market News: every {FINNHUB_MARKET_NEWS_INTERVAL}s")
print(f"GlobeNewswire: every {GLOBENEWSWIRE_INTERVAL}s")
print(f"PRNewswire: every {PRNEWSWIRE_INTERVAL}s")
print(f"FDA Official: every {FDA_INTERVAL}s")
print(f"USAspending: every {USASPENDING_INTERVAL}s")
print(f"News max age: {NEWS_MAX_AGE_SECONDS / 3600:.0f} hours")
print(f"Max stock price: ${MAX_PRICE}")
print(f"Minimum ticker verification score: {MIN_TICKER_VERIFICATION_SCORE}")
print("Reverse mover scanner: OFF")
print("=" * 70)

send_telegram(
    f"🚀 בוט חדשות מניות V7 הופעל\n"
    f"🎯 מצב: חדשות בלבד\n"
    f"✅ בלי סריקת מניות שעלו\n"
    f"✅ אימות טיקר קשוח לפני שליחה\n"
    f"✅ חסימת PR חלש / קרנות / Warrants / Units\n"
    f"📡 מקורות: Finnhub, GlobeNewswire, PRNewswire, FDA, USAspending\n"
    f"📊 מנטר {len(all_us_tickers)} מניות רגילות"
)

last_finnhub_market_news_scan = 0
last_globenewswire_scan = 0
last_prnewswire_scan = 0
last_fda_scan = 0
last_usaspending_scan = 0

while True:
    try:
        now = time.time()

        if now - last_finnhub_market_news_scan >= FINNHUB_MARKET_NEWS_INTERVAL:
            scan_finnhub_market_news()
            last_finnhub_market_news_scan = now

        if now - last_globenewswire_scan >= GLOBENEWSWIRE_INTERVAL:
            scan_globenewswire()
            last_globenewswire_scan = now

        if now - last_prnewswire_scan >= PRNEWSWIRE_INTERVAL:
            scan_prnewswire()
            last_prnewswire_scan = now

        if now - last_fda_scan >= FDA_INTERVAL:
            scan_fda_press_announcements()
            last_fda_scan = now

        if now - last_usaspending_scan >= USASPENDING_INTERVAL:
            scan_usaspending()
            last_usaspending_scan = now

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
