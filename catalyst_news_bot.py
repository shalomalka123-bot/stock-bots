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
from alpaca.data.requests import StockLatestQuoteRequest
from alpaca.data.enums import DataFeed


# ===== API KEYS =====
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


# ===== CLIENTS =====
trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
DATA_FEED = DataFeed.SIP


# ===== SETTINGS =====
MARKET_NEWS_INTERVAL = 60
PRESS_RELEASE_INTERVAL = 60
USASPENDING_INTERVAL = 1800

MAX_PRICE = 70.0
NEWS_MAX_AGE_SECONDS = 3600
USASPENDING_MIN_AMOUNT = 10_000_000

DUPLICATE_COOLDOWN_SECONDS = 1800
MIN_QUALITY_SCORE = 70


# ===== STATE =====
sent_alerts = set()
last_symbol_category_alert = {}
last_market_news_scan = 0
last_press_release_scan = 0
last_usaspending_scan = 0

all_us_tickers = set()
ticker_to_company = {}
ticker_metadata = {}
sector_translation_cache = {}


# ===== TRANSLATION =====
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
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def translate_to_hebrew(text):
    if not text:
        return ""
    try:
        text = clean_text(text)[:700]
        encoded = quote(text)
        url = f"https://translate.googleapis.com/translate_a/single?client=gtx&sl=en&tl=iw&dt=t&q={encoded}"
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=8) as response:
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


# ===== CATALYSTS =====
POSITIVE_CATALYSTS = {
    "🟢 אישור FDA": {
        "strength": 5,
        "category": "FDA_APPROVAL",
        "keywords": [
            "fda approval", "fda approves", "fda approved",
            "receives fda approval", "received fda approval",
            "granted fda approval", "fda grants approval",
            "approved by fda", "fda clearance", "fda cleared",
            "marketing authorization", "regulatory approval"
        ]
    },
    "🧪 ניסוי קליני חיובי": {
        "strength": 5,
        "category": "PHASE_SUCCESS",
        "keywords": [
            "met primary endpoint", "achieved primary endpoint",
            "positive topline results", "positive phase 3",
            "phase 3 positive", "phase iii positive",
            "successful phase 3", "statistically significant",
            "met primary and secondary endpoints"
        ]
    },
    "🤝 רכישה / מיזוג": {
        "strength": 5,
        "category": "ACQUISITION",
        "keywords": [
            "agreement to acquire", "agreement to be acquired",
            "to be acquired by", "definitive merger agreement",
            "definitive agreement to acquire", "all-cash transaction",
            "tender offer", "acquisition agreement",
            "deal valued at", "transaction valued at",
            "per share in cash", "premium of approximately"
        ]
    },
    "💰 השקעה אסטרטגית": {
        "strength": 4,
        "category": "STRATEGIC_INVESTMENT",
        "keywords": [
            "strategic investment", "minority stake", "equity stake",
            "nvidia invests", "microsoft invests", "google invests",
            "amazon invests", "openai invests", "takes stake",
            "preferred stock investment"
        ]
    },
    "🤝 שותפות חזקה": {
        "strength": 4,
        "category": "PARTNERSHIP",
        "keywords": [
            "partnership with nvidia", "partnership with microsoft",
            "partnership with google", "partnership with amazon",
            "collaboration with openai", "selected by microsoft",
            "selected by amazon", "exclusive partner",
            "strategic partnership"
        ]
    },
    "🏛️ חוזה ממשלתי / חוזה גדול": {
        "strength": 4,
        "category": "CONTRACT",
        "keywords": [
            "government contract awarded", "federal contract",
            "dod contract", "department of defense contract",
            "u.s. army contract", "u.s. navy contract",
            "nasa contract", "awarded contract",
            "wins contract", "won contract",
            "multi-year agreement", "billion-dollar deal",
            "million contract awarded"
        ]
    },
    "₿ Crypto Treasury": {
        "strength": 5,
        "category": "CRYPTO_TREASURY",
        "keywords": [
            "bitcoin treasury", "ethereum treasury",
            "crypto treasury", "digital asset treasury",
            "adds bitcoin to treasury", "purchases bitcoin",
            "purchases ethereum", "crypto reserves"
        ]
    },
    "🤖 Pivot ל-AI": {
        "strength": 4,
        "category": "AI_PIVOT",
        "keywords": [
            "pivots to ai", "pivot to artificial intelligence",
            "rebrands as ai", "ai-focused", "new ai division",
            "ai strategy launch", "artificial intelligence strategy"
        ]
    },
    "📈 דוחות חזקים": {
        "strength": 3,
        "category": "EARNINGS_BEAT",
        "keywords": [
            "earnings beat", "beats earnings estimates",
            "record revenue", "record quarterly revenue",
            "raises guidance", "raises full-year guidance",
            "beats revenue expectations", "above consensus"
        ]
    },
    "🔬 פטנט / פריצת דרך": {
        "strength": 3,
        "category": "BREAKTHROUGH",
        "keywords": [
            "patent granted", "patent issued",
            "breakthrough technology", "scientific breakthrough",
            "first-in-class", "first-of-its-kind", "groundbreaking"
        ]
    },
}


NEGATIVE_CATALYSTS = {
    "⚖️ תביעה / חקירה": {
        "strength": 4,
        "category": "LEGAL",
        "keywords": [
            "class action lawsuit", "securities fraud lawsuit",
            "shareholder lawsuit", "sec investigation",
            "doj investigation", "fbi investigation",
            "wells notice"
        ]
    },
    "💸 דילול": {
        "strength": 3,
        "category": "DILUTION",
        "keywords": [
            "registered direct offering", "public offering",
            "at-the-market offering", "atm offering",
            "shelf offering", "warrants", "convertible notes",
            "priced offering", "private placement"
        ]
    },
    "💀 סיכון קיצוני": {
        "strength": 5,
        "category": "EXTREME_RISK",
        "keywords": [
            "chapter 11", "bankruptcy", "going concern",
            "nasdaq delisting", "delisting determination",
            "reverse stock split"
        ]
    },
}


# ===== FILTERS =====
NOISE_PATTERNS = [
    "top stocks", "best stocks", "stocks to watch", "stocks to buy",
    "stock picks", "weekly summary", "weekly recap", "market commentary",
    "market outlook", "market recap", "morning brief", "daily recap",
    "portfolio update", "fund holdings", "investor letter",
    "should you buy", "should you sell", "why i'm bullish",
    "why i'm bearish", "dividend stocks", "closed-end fund"
]

LAW_FIRM_SPAM_PATTERNS = [
    "lead plaintiff", "lead plaintiff deadline", "class action deadline",
    "securities fraud lawsuit", "class action lawsuit",
    "shareholder alert", "investor alert", "law offices of",
    "rosen law firm", "pomerantz law firm", "levi & korsinsky",
    "bronstein, gewirtz", "glancy prongay", "faruqi & faruqi",
    "kessler topaz", "the schall law firm", "berger montague",
    "block & leviton", "bragar eagel", "gross law firm",
    "kirby mcinerney", "labaton", "class period",
    "reminds investors", "encourages investors", "recover losses",
    "contact the firm", "no cost to you"
]

BLOCK_PATTERNS = [
    "announces participation", "to present at", "fireside chat",
    "webcast", "investor conference", "annual meeting",
    "shareholder meeting", "conference call details",
    "presentation at", "earnings call"
]

WEAK_PR_PATTERNS = [
    "launches new website", "launches initiative",
    "expands platform", "announces new brand",
    "corporate update", "business update",
    "letter to shareholders", "appoints",
    "appointment of", "joins board", "advisory board",
    "marketing campaign"
]

DILUTION_RISK_PATTERNS = [
    "registered direct offering", "public offering",
    "at-the-market offering", "atm offering", "shelf offering",
    "warrants", "convertible note", "convertible notes",
    "priced offering", "private placement"
]

EXTREME_NEGATIVE_PATTERNS = [
    "chapter 11", "bankruptcy", "going concern",
    "nasdaq delisting", "delisting determination",
    "minimum bid notification", "reverse stock split"
]


# ===== HELPERS =====
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
        1 for w in [
            "law firm", "lead plaintiff", "class action",
            "securities fraud", "shareholder lawsuit"
        ]
        if w in text
    )

    return legal_count >= 2


def get_source_score(source):
    s = (source or "").lower()

    if "sec" in s or "edgar" in s:
        return 12, "אמינות גבוהה מאוד - SEC"
    if "fda" in s:
        return 12, "אמינות גבוהה מאוד - FDA"
    if "usaspending" in s:
        return 12, "אמינות גבוהה מאוד - מקור ממשלתי"
    if "businesswire" in s:
        return 8, "אמינות גבוהה - BusinessWire"
    if "globenewswire" in s:
        return 7, "אמינות טובה - GlobeNewswire"
    if "prnewswire" in s:
        return -3, "אמינות זהירה - PRNewswire"
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
    if score >= 70:
        return "🟡 בינוני־חזק"
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
                found.append({
                    "catalyst": cat_name,
                    "keyword": kw,
                    "strength": cat_data["strength"],
                    "category": cat_data["category"]
                })
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
        return est.strftime("%d.%m.%Y %H:%M EST")
    except Exception:
        return "לא ידוע"


def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{CATALYST_BOT_TOKEN}/sendMessage"
        data = json.dumps({
            "chat_id": CATALYST_CHAT_ID,
            "text": message,
            "disable_web_page_preview": False
        }).encode()

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
        "announces", "reports", "today", "inc", "corp",
        "corporation", "company", "shares", "stock",
        "new", "the", "and", "with", "ltd", "plc"
    }

    words = [w for w in h.split() if w not in remove_words and len(w) > 2]
    core = " ".join(words[:10])

    return f"{symbol}|{core}"


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
                "name": data.get("name", "")
            }

            ticker_metadata[symbol] = metadata
            return metadata

    except Exception as e:
        print(f"Finnhub profile error for {symbol}: {e}")

    return {
        "sector_en": "",
        "sector_he": "לא ידוע",
        "market_cap": 0,
        "name": ""
    }


def get_current_price(symbol):
    try:
        request = StockLatestQuoteRequest(symbol_or_symbols=symbol, feed=DATA_FEED)
        quotes = data_client.get_stock_latest_quote(request)

        if symbol in quotes:
            quote = quotes[symbol]
            price = float(quote.ask_price) if quote.ask_price else float(quote.bid_price)
            if price > 0:
                return price

    except Exception as e:
        print(f"Price error for {symbol}: {e}")

    return None


def calculate_news_quality(ticker, headline, summary, source, positive_found, negative_found, meta):
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
            "primary_category": "LAW_FIRM_SPAM"
        }

    if not positive_found:
        return {
            "send": False,
            "score": 0,
            "label": "❌ לא חיובי",
            "reason": "אין קטליזטור חיובי",
            "reasons": ["אין קטליזטור חיובי"],
            "news_type": "לא רלוונטי",
            "primary_category": "NO_POSITIVE"
        }

    if has_any(text_lower, BLOCK_PATTERNS):
        return {
            "send": False,
            "score": 0,
            "label": "❌ חסום",
            "reason": "כנס / מצגת / שיחת רווחים",
            "reasons": ["כנס / מצגת / שיחת רווחים"],
            "news_type": "יח״צ חלש",
            "primary_category": "BLOCKED_PR"
        }

    if has_any(text_lower, EXTREME_NEGATIVE_PATTERNS):
        return {
            "send": False,
            "score": 0,
            "label": "❌ חסום",
            "reason": "סיכון שלילי קיצוני",
            "reasons": ["סיכון שלילי קיצוני"],
            "news_type": "סיכון גבוה",
            "primary_category": "EXTREME_RISK"
        }

    best = max(positive_found, key=lambda x: x["strength"])
    primary_category = best["category"]

    category_scores = {
        "FDA_APPROVAL": 88,
        "PHASE_SUCCESS": 90,
        "ACQUISITION": 92,
        "STRATEGIC_INVESTMENT": 76,
        "PARTNERSHIP": 74,
        "CONTRACT": 68,
        "CRYPTO_TREASURY": 82,
        "AI_PIVOT": 62,
        "EARNINGS_BEAT": 62,
        "BREAKTHROUGH": 65
    }

    score = category_scores.get(primary_category, 55)
    reasons.append(f"קטגוריה ראשית: {primary_category}")

    source_bonus, source_reason = get_source_score(source)
    score += source_bonus
    reasons.append(source_reason)

    strong_phrases = [
        "fda approves", "receives fda approval",
        "met primary endpoint", "achieved primary endpoint",
        "positive topline results", "definitive agreement to acquire",
        "to be acquired by", "all-cash transaction",
        "strategic investment", "awarded contract",
        "wins contract", "government contract awarded"
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
        "artificial intelligence", " ai ", "nvidia",
        "openai", "quantum", "bitcoin", "ethereum", "crypto"
    ]

    if has_any(f" {text_lower} ", hot_words):
        score += 5
        reasons.append("תחום חם: AI / Crypto / Quantum")

    score = max(0, min(100, int(score)))
    label = get_quality_label(score)

    if score >= 75:
        news_type = "ידיעה חיובית חזקה"
    elif score >= 70:
        news_type = "ידיעה בינונית־חזקה לבדיקה"
    else:
        news_type = "ידיעה חלשה"

    return {
        "send": score >= MIN_QUALITY_SCORE,
        "score": score,
        "label": label,
        "reason": " + ".join(reasons[:3]),
        "reasons": reasons,
        "news_type": news_type,
        "primary_category": primary_category
    }


def is_company_focused(ticker, headline, summary):
    if not ticker:
        return False

    headline_upper = (headline or "").upper()
    summary_upper = (summary or "").upper()

    if re.search(rf"\b{ticker}\b", headline_upper):
        return True

    if f"({ticker})" in headline_upper or f"${ticker}" in headline_upper:
        return True

    company_name = ticker_to_company.get(ticker, "")
    if company_name:
        first_word = company_name.split()[0].upper()
        if len(first_word) >= 4 and first_word in headline_upper:
            return True

    full_text = f"{headline_upper} {summary_upper}"
    ticker_count = len(re.findall(rf"\b{ticker}\b", full_text))

    return ticker_count >= 2


def load_us_tickers():
    global all_us_tickers, ticker_to_company

    try:
        request = GetAssetsRequest(
            asset_class=AssetClass.US_EQUITY,
            status=AssetStatus.ACTIVE
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

    clean_name = company_name.upper()

    for suffix in [
        " INC", " CORP", " CORPORATION", " LLC", " LTD",
        " HOLDINGS", " GROUP", " CO", " COMPANY", ", INC",
        ", LLC", ", CORP", ".", ","
    ]:
        clean_name = clean_name.replace(suffix, "").strip()

    for ticker, name in ticker_to_company.items():
        if not name:
            continue

        name_upper = name.upper()

        for suffix in [
            " INC", " CORP", " CORPORATION", " LLC", " LTD",
            " HOLDINGS", " GROUP", " CO", " COMPANY"
        ]:
            name_upper = name_upper.replace(suffix, "").strip()

        if clean_name == name_upper:
            return ticker

        if len(clean_name) >= 6 and clean_name in name_upper:
            return ticker

    return None


def extract_ticker_from_news(related, headline, summary):
    if related:
        candidates = related.split(",")

        for candidate in candidates:
            candidate = candidate.strip().upper()
            if candidate in all_us_tickers:
                return candidate

    text = f"{headline} {summary}"

    patterns = [
        r"\(([A-Z]{1,5})\)",
        r"NASDAQ:\s*([A-Z]{1,5})",
        r"NYSE:\s*([A-Z]{1,5})",
        r"\$([A-Z]{1,5})\b"
    ]

    for pattern in patterns:
        matches = re.findall(pattern, text)

        for match in matches:
            if match in all_us_tickers:
                return match

    return None


# ===== SCANNERS =====
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
            item for item in news_items
            if now - item.get("datetime", 0) <= NEWS_MAX_AGE_SECONDS
        ]

        print(f"[Market News] {len(recent_news)} articles in last hour (out of {len(news_items)})")

        for news in recent_news:
            process_news_item(news, source_override="Finnhub")

    except Exception as e:
        print(f"[Market News] error: {e}")


def parse_rss_items(rss_content):
    items = []
    item_blocks = re.findall(r"<item>(.*?)</item>", rss_content, re.DOTALL)

    for block in item_blocks:
        try:
            title_match = re.search(r"<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", block, re.DOTALL)
            link_match = re.search(r"<link>(.*?)</link>", block, re.DOTALL)
            desc_match = re.search(r"<description>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</description>", block, re.DOTALL)
            date_match = re.search(r"<pubDate>(.*?)</pubDate>", block, re.DOTALL)

            title = clean_text(title_match.group(1)) if title_match else ""
            link = clean_text(link_match.group(1)) if link_match else ""
            desc = clean_text(desc_match.group(1)) if desc_match else ""

            timestamp = time.time()

            if date_match:
                date_str = clean_text(date_match.group(1))

                for fmt in [
                    "%a, %d %b %Y %H:%M:%S %z",
                    "%a, %d %b %Y %H:%M:%S GMT"
                ]:
                    try:
                        dt = datetime.strptime(date_str, fmt)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        timestamp = dt.timestamp()
                        break
                    except Exception:
                        pass

            items.append({
                "title": title,
                "link": link,
                "description": desc,
                "timestamp": timestamp
            })

        except Exception:
            continue

    return items


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

        print(f"[GlobeNewswire] {len(recent)} releases in last hour (out of {len(items)})")

        for item in recent:
            news = {
                "headline": item["title"],
                "summary": item["description"],
                "url": item["link"],
                "datetime": item["timestamp"],
                "related": ""
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

        print(f"[PRNewswire] {len(recent)} releases in last hour (out of {len(items)})")

        for item in recent:
            news = {
                "headline": item["title"],
                "summary": item["description"],
                "url": item["link"],
                "datetime": item["timestamp"],
                "related": ""
            }
            process_news_item(news, source_override="PRNewswire")

    except Exception as e:
        print(f"[PRNewswire] error: {e}")


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
                "award_amounts": [{"lower_bound": USASPENDING_MIN_AMOUNT}]
            },
            "fields": [
                "Award ID", "Recipient Name", "Award Amount",
                "Awarding Agency", "Description", "Start Date"
            ],
            "page": 1,
            "limit": 100,
            "sort": "Award Amount",
            "order": "desc"
        }

        req = Request(
            url,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"}
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
            if not price or price > MAX_PRICE:
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


# ===== ALERTS =====
def process_news_item(news, source_override=None):
    headline = clean_text(news.get("headline", ""))
    summary = clean_text(news.get("summary", ""))
    url_link = news.get("url", "")
    related = news.get("related", "")
    published = news.get("datetime", time.time())
    source = source_override or news.get("source", "Unknown")

    if time.time() - published > NEWS_MAX_AGE_SECONDS:
        return

    if is_noise(headline):
        return

    full_text = f"{headline} {summary}"

    positive_found = detect_catalysts(full_text, POSITIVE_CATALYSTS)
    negative_found = detect_catalysts(full_text, NEGATIVE_CATALYSTS)

    ticker = extract_ticker_from_news(related, headline, summary)
    if not ticker:
        return

    if not is_company_focused(ticker, headline, summary):
        return

    price = get_current_price(ticker)
    if not price or price > MAX_PRICE:
        return

    meta = get_finnhub_company_profile(ticker)

    quality = calculate_news_quality(
        ticker=ticker,
        headline=headline,
        summary=summary,
        source=source,
        positive_found=positive_found,
        negative_found=negative_found,
        meta=meta
    )

    if not quality["send"]:
        print(f"[SKIP] {ticker} | {quality['reason']} | {headline[:80]}")
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
        quality=quality
    )

    sent_alerts.add(alert_hash)
    last_symbol_category_alert[duplicate_key] = now


def send_news_alert(ticker, headline, summary, source, url_link, published, price, meta, quality):
    company_name = meta.get("name") or ticker_to_company.get(ticker, "לא ידוע")
    sector_he = meta.get("sector_he", "לא ידוע")
    market_cap_label = get_market_cap_label(meta.get("market_cap", 0))

    headline_he = translate_to_hebrew(headline)

    summary_clean = clean_text(summary)
    if not summary_clean:
        summary_clean = headline

    summary_he = translate_to_hebrew(summary_clean[:700])
    publish_time = format_us_eastern_time(published)

    reasons_text = "\n".join([f"• {reason}" for reason in quality.get("reasons", [])[:4]])

    message = (
        f"{quality['label']} | ציון איכות: {quality['score']}/100\n"
        f"{quality['news_type']}\n"
        f"\n"
        f"📊 סימבול: {ticker}\n"
        f"🏢 שם החברה: {company_name}\n"
        f"💼 שווי שוק: {market_cap_label}\n"
        f"💵 מחיר מניה: ${price:.2f}\n"
        f"🏭 מגזר: {sector_he}\n"
        f"\n"
        f"📰 כותרת:\n"
        f"{headline_he}\n"
        f"\n"
        f"📋 תוכן הידיעה:\n"
        f"{summary_he[:900]}\n"
        f"\n"
        f"🧠 סיבת הדירוג:\n"
        f"{reasons_text}\n"
        f"\n"
        f"📅 פורסם בתאריך: {publish_time}\n"
        f"📡 מקור: {source}\n"
        f"\n"
        f"🔗 קישור לכתבה:\n"
        f"{url_link}"
    )

    if send_telegram(message):
        print(f"[ALERT] {ticker} | score {quality['score']}/100 | ${price:.2f}")


def send_usaspending_alert(ticker, recipient, amount, agency, description, award_id, price):
    meta = get_finnhub_company_profile(ticker)
    company_name = meta.get("name") or ticker_to_company.get(ticker, recipient)
    sector_he = meta.get("sector_he", "לא ידוע")
    market_cap_label = get_market_cap_label(meta.get("market_cap", 0))
    link = f"https://www.usaspending.gov/award/{award_id}/"

    description_he = translate_to_hebrew(description[:500]) if description else "אין תיאור"

    message = (
        f"🔥 חוזה ממשלתי | ציון איכות: 80/100\n"
        f"ידיעה חיובית חזקה\n"
        f"\n"
        f"📊 סימבול: {ticker}\n"
        f"🏢 שם החברה: {company_name}\n"
        f"💼 שווי שוק: {market_cap_label}\n"
        f"💵 מחיר מניה: ${price:.2f}\n"
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


# ===== MAIN =====
print("=" * 60)
print("CATALYST NEWS BOT V4 STARTED")
print("=" * 60)

load_us_tickers()

print(f"Market News: every {MARKET_NEWS_INTERVAL}s")
print(f"GlobeNewswire: every {PRESS_RELEASE_INTERVAL}s")
print(f"PRNewswire: every {PRESS_RELEASE_INTERVAL}s")
print(f"USAspending: every {USASPENDING_INTERVAL}s")
print(f"Max stock price: ${MAX_PRICE}")
print("=" * 60)

send_telegram(
    f"🚀 בוט קטליזטורים V4 הופעל\n"
    f"\n"
    f"📡 מקורות:\n"
    f"• Finnhub Market News\n"
    f"• GlobeNewswire RSS\n"
    f"• PRNewswire RSS\n"
    f"• USAspending.gov\n"
    f"\n"
    f"🎯 מנטר {len(all_us_tickers)} מניות אמריקאיות\n"
    f"✅ סינון כפילויות פעיל\n"
    f"✅ סינון עורכי דין פעיל\n"
    f"✅ דירוג איכות פעיל\n"
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

        if now - last_usaspending_scan >= USASPENDING_INTERVAL:
            scan_usaspending()
            last_usaspending_scan = now

        if len(sent_alerts) > 10000:
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
