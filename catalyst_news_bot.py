import os
import time
import json
import re
from urllib.request import urlopen, Request
from urllib.parse import urlencode, quote
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


# ===== CLIENTS =====
trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
DATA_FEED = DataFeed.SIP


# ===== SETTINGS =====
MARKET_NEWS_INTERVAL = 60
USASPENDING_INTERVAL = 1800
PRESS_RELEASE_INTERVAL = 60

# מקסימום מחיר מניה - אם יקר מזה, לא שולחים
MAX_PRICE = 70.0

# חדשות מהשעה האחרונה בלבד
NEWS_MAX_AGE_SECONDS = 3600

USASPENDING_MIN_AMOUNT = 10000000
FINNHUB_DELAY = 1.1


# ===== STATE =====
sent_alerts = set()
last_market_news_scan = 0
last_usaspending_scan = 0
last_press_release_scan = 0
all_us_tickers = set()
ticker_to_company = {}
ticker_metadata = {}
sector_translation_cache = {}


# ============================================================
# מילון מגזרים מורחב - עברית מלאה
# ============================================================
SECTOR_HEBREW = {
    # General
    "Technology": "טכנולוגיה",
    "Health Care": "בריאות וביוטכנולוגיה",
    "Healthcare": "בריאות וביוטכנולוגיה",
    "Financial Services": "שירותים פיננסיים",
    "Finance": "פיננסים",
    "Financials": "פיננסים",
    "Consumer Cyclical": "צריכה לא חיונית",
    "Consumer Discretionary": "צריכה לא חיונית",
    "Consumer Defensive": "צריכה חיונית",
    "Consumer Staples": "צריכה חיונית",
    "Communication Services": "תקשורת ומדיה",
    "Communications": "תקשורת",
    "Industrials": "תעשייה",
    "Energy": "אנרגיה",
    "Basic Materials": "חומרי גלם",
    "Materials": "חומרי גלם",
    "Real Estate": 'נדל"ן',
    "Utilities": "שירותים ציבוריים",
    
    # Specific industries (Finnhub uses these)
    "Semiconductors": "מוליכים למחצה (שבבים)",
    "Biotechnology": "ביוטכנולוגיה",
    "Pharmaceuticals": "פרמצבטיקה",
    "Aerospace & Defense": "תעופה וביטחון",
    "Software": "תוכנה",
    "Hardware": "חומרה",
    "Banks": "בנקאות",
    "Banking": "בנקאות",
    "Insurance": "ביטוח",
    "Retail": "קמעונאות",
    "Automotive": "רכב",
    "Auto Manufacturers": "יצרני רכב",
    "Transportation": "תחבורה",
    "Airlines": "תעופה (חברות תעופה)",
    "Mining": "כרייה",
    "Oil & Gas": "נפט וגז",
    "Oil, Gas & Consumable Fuels": "נפט, גז ודלקים",
    
    # Finnhub specific tags
    "Life Sciences Tools & Services": "כלים ושירותים למדעי החיים",
    "Health Care Equipment & Supplies": "ציוד וחומרים רפואיים",
    "Health Care Providers & Services": "שירותי בריאות",
    "Health Care Technology": "טכנולוגיה רפואית",
    "Capital Markets": "שוק ההון",
    "Specialty Chemicals": "כימיקלים מיוחדים",
    "Chemicals": "כימיקלים",
    "Specialty Industrial Machinery": "ציוד תעשייתי מיוחד",
    "Specialty Retail": "קמעונאות מיוחדת",
    "Discount Stores": "חנויות דיסקאונט",
    "Department Stores": "חנויות כל-בו",
    "Internet Retail": "קמעונאות אינטרנט",
    "Internet Content & Information": "אינטרנט - תוכן ומידע",
    "Software—Application": "תוכנה - אפליקציות",
    "Software—Infrastructure": "תוכנה - תשתיות",
    "Information Technology Services": "שירותי טכנולוגיית מידע",
    "Computer Hardware": "חומרת מחשב",
    "Consumer Electronics": "אלקטרוניקה לצרכן",
    "Electronic Gaming & Multimedia": "משחקים אלקטרוניים ומולטימדיה",
    "Communication Equipment": "ציוד תקשורת",
    "Telecom Services": "שירותי תקשורת",
    "Media": "מדיה",
    "Entertainment": "בידור",
    "Restaurants": "מסעדנות",
    "Lodging": "תיירות ולינה",
    "Travel Services": "שירותי תיירות",
    "Apparel Retail": "קמעונאות בגדים",
    "Apparel Manufacturing": "ייצור בגדים",
    "Footwear & Accessories": "הנעלה ואביזרים",
    "Luxury Goods": "מוצרי יוקרה",
    "Beverages—Non-Alcoholic": "משקאות לא אלכוהוליים",
    "Beverages—Wineries & Distilleries": "משקאות אלכוהוליים",
    "Beverages—Brewers": "מבשלות בירה",
    "Tobacco": "טבק",
    "Food Distribution": "הפצת מזון",
    "Packaged Foods": "מזון ארוז",
    "Confectioners": "מתוקים וקונדיטוריה",
    "Farm Products": "תוצרת חקלאית",
    "Agricultural Inputs": "תשומות חקלאיות",
    "Solar": "אנרגיה סולארית",
    "Renewable Utilities": "אנרגיה מתחדשת",
    "Utilities—Renewable": "שירותים ציבוריים - אנרגיה מתחדשת",
    "Utilities—Regulated Electric": "חשמל מוסדר",
    "Utilities—Regulated Gas": "גז מוסדר",
    "Utilities—Regulated Water": "מים מוסדרים",
    "Uranium": "אורניום",
    "Coal": "פחם",
    "Oil & Gas E&P": "נפט וגז - חיפוש והפקה",
    "Oil & Gas Integrated": "נפט וגז משולב",
    "Oil & Gas Refining & Marketing": "זיקוק ושיווק נפט וגז",
    "Oil & Gas Equipment & Services": "ציוד ושירותי נפט וגז",
    "Oil & Gas Midstream": "תשתיות נפט וגז",
    "Oil & Gas Drilling": "קידוחי נפט וגז",
    "Steel": "פלדה",
    "Aluminum": "אלומיניום",
    "Copper": "נחושת",
    "Gold": "זהב",
    "Silver": "כסף",
    "Other Precious Metals & Mining": "מתכות יקרות וכרייה",
    "Other Industrial Metals & Mining": "מתכות תעשייתיות וכרייה",
    "Building Materials": "חומרי בניין",
    "Building Products & Equipment": "מוצרי וציוד בנייה",
    "Lumber & Wood Production": "ייצור עצים",
    "Paper & Paper Products": "נייר ומוצרי נייר",
    "Containers & Packaging": "מכלים ואריזה",
    "Packaging & Containers": "אריזה ומכלים",
    "Chemicals (Diversified)": "כימיקלים מגוונים",
    "REIT—Industrial": 'נדל"ן תעשייתי (REIT)',
    "REIT—Office": 'נדל"ן משרדים (REIT)',
    "REIT—Retail": 'נדל"ן מסחרי (REIT)',
    "REIT—Residential": 'נדל"ן למגורים (REIT)',
    "REIT—Healthcare Facilities": 'נדל"ן בריאות (REIT)',
    "REIT—Hotel & Motel": 'נדל"ן מלונאות (REIT)',
    "REIT—Mortgage": 'משכנתאות (REIT)',
    "REIT—Specialty": 'נדל"ן מיוחד (REIT)',
    "REIT—Diversified": 'נדל"ן מגוון (REIT)',
    "Real Estate Services": 'שירותי נדל"ן',
    "Real Estate—Development": 'פיתוח נדל"ן',
    "Real Estate—Diversified": 'נדל"ן מגוון',
    "Asset Management": "ניהול נכסים",
    "Banks—Diversified": "בנקאות מגוונת",
    "Banks—Regional": "בנקאות אזורית",
    "Mortgage Finance": "מימון משכנתאות",
    "Insurance—Life": "ביטוח חיים",
    "Insurance—Property & Casualty": "ביטוח רכוש ונזיקין",
    "Insurance—Reinsurance": "ביטוח משנה",
    "Insurance—Specialty": "ביטוח מיוחד",
    "Insurance—Diversified": "ביטוח מגוון",
    "Insurance Brokers": "מתווכי ביטוח",
    "Credit Services": "שירותי אשראי",
    "Financial Conglomerates": "קונגלומרטים פיננסיים",
    "Financial Data & Stock Exchanges": "נתונים פיננסיים ובורסות",
    "Shell Companies": "חברות קליפה",
    "Education & Training Services": "חינוך והכשרה",
    "Personal Services": "שירותים אישיים",
    "Leisure": "פנאי",
    "Resorts & Casinos": "אתרי נופש וקזינו",
    "Gambling": "הימורים",
    "Healthcare Plans": "תוכניות בריאות",
    "Medical Care Facilities": "מתקני טיפול רפואי",
    "Medical Devices": "מכשור רפואי",
    "Medical Distribution": "הפצה רפואית",
    "Medical Instruments & Supplies": "כלים וחומרים רפואיים",
    "Diagnostics & Research": "אבחון ומחקר",
    "Drug Manufacturers—General": "יצרני תרופות - כללי",
    "Drug Manufacturers—Specialty & Generic": "יצרני תרופות - מיוחד וגנרי",
    "Engineering & Construction": "הנדסה ובנייה",
    "Infrastructure Operations": "תפעול תשתיות",
    "Construction": "בנייה",
    "Industrial Distribution": "הפצה תעשייתית",
    "Business Equipment & Supplies": "ציוד וחומרים עסקיים",
    "Staffing & Employment Services": "שירותי כוח אדם",
    "Consulting Services": "ייעוץ",
    "Specialty Business Services": "שירותים עסקיים מיוחדים",
    "Security & Protection Services": "אבטחה והגנה",
    "Waste Management": "ניהול פסולת",
    "Pollution & Treatment Controls": "בקרת זיהום וטיפול",
    "Tools & Accessories": "כלים ואביזרים",
    "Conglomerates": "קונגלומרטים",
    "Trucking": "הובלות",
    "Railroads": "רכבות",
    "Marine Shipping": "הובלה ימית",
    "Airports & Air Services": "שדות תעופה ושירותי אוויר",
    "Auto Parts": "חלקי חילוף לרכב",
    "Auto & Truck Dealerships": "סוכנויות רכב ומשאיות",
    "Recreational Vehicles": "כלי רכב נופש",
    "Furnishings, Fixtures & Appliances": "ריהוט וכלי בית",
    "Residential Construction": "בנייה למגורים",
    "Household & Personal Products": "מוצרי בית ואישיים",
    "Grocery Stores": "מרכולים",
    "Drug Retailers": "בתי מרקחת",
    "Specialty Industrial Machinery": "ציוד תעשייתי מיוחד",
    "Farm & Heavy Construction Machinery": "ציוד חקלאי וכבד",
    "Electrical Equipment & Parts": "ציוד וחלקי חשמל",
    "Electronic Components": "רכיבים אלקטרוניים",
    "Electronics & Computer Distribution": "הפצת אלקטרוניקה ומחשבים",
    "Scientific & Technical Instruments": "מכשירים מדעיים וטכניים",
    "Semiconductor Equipment & Materials": "ציוד וחומרים למוליכים למחצה",
    "Computer Systems": "מערכות מחשב",
    "Electronic Equipment, Instruments & Components": "ציוד ורכיבים אלקטרוניים",
}


def translate_to_hebrew(text):
    """תרגום אוטומטי דרך Google Translate חינמי"""
    if not text:
        return ""
    try:
        text = text[:500]
        encoded = quote(text)
        url = f"https://translate.googleapis.com/translate_a/single?client=gtx&sl=en&tl=iw&dt=t&q={encoded}"
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=8) as response:
            data = json.loads(response.read().decode())
        if data and isinstance(data, list) and data[0]:
            translated = ""
            for chunk in data[0]:
                if chunk and chunk[0]:
                    translated += chunk[0]
            return translated.strip()
        return text
    except Exception as e:
        print(f"Translate error: {e}")
        return text


def translate_sector(sector_en):
    """מתרגם מגזר - קודם מהמילון, ואם לא מצא - גוגל טרנסלייט"""
    if not sector_en:
        return "לא ידוע"
    if sector_en in SECTOR_HEBREW:
        return SECTOR_HEBREW[sector_en]
    if sector_en in sector_translation_cache:
        return sector_translation_cache[sector_en]
    # תרגום אוטומטי
    translated = translate_to_hebrew(sector_en)
    sector_translation_cache[sector_en] = translated
    return translated


# ============================================================
# מאגר מקרים אמיתיים - מורחב מאוד עם נקודות אחיזה רבות
# ============================================================
HISTORICAL_CASES = {
    # ===== POSITIVE CATALYSTS =====
    
    # FDA Approval - by sub-category
    "FDA_APPROVAL_BIOTECH_SMALL": [
        {"ticker": "OMER", "company": "Omeros", "change": "+150%", "date": "24.12.2025",
         "reason": "אישור FDA ל-Yartemlea (TA-TMA)",
         "url": "https://www.omeros.com/news"},
        {"ticker": "INSM", "company": "Insmed", "change": "+120%", "date": "08.2025",
         "reason": "אישור FDA ל-Brinsupri",
         "url": "https://www.insmed.com/news"},
        {"ticker": "IONS", "company": "Ionis Pharma", "change": "+100%", "date": "12.2024",
         "reason": "אישור FDA ל-Tryngolza",
         "url": "https://www.ionispharma.com/news"}
    ],
    "FDA_APPROVAL_PHARMA_LARGE": [
        {"ticker": "MRK", "company": "Merck", "change": "+5%", "date": "21.11.2025",
         "reason": "FDA אישר Keytruda+Padcev לסרטן שלפוחית",
         "url": "https://www.merck.com/news"}
    ],
    "FDA_APPROVAL_DEVICES": [
        {"ticker": "SWAV", "company": "Shockwave Medical", "change": "+40%", "date": "2024",
         "reason": "FDA approval IVL device",
         "url": "https://shockwavemedical.com"}
    ],
    
    # Phase 3 Success
    "PHASE_3_SUCCESS_BIOTECH": [
        {"ticker": "ABVX", "company": "Abivax", "change": "+586%", "date": "23.07.2025",
         "reason": "Phase 3 חיובי - obefazimod לקוליטיס",
         "url": "https://abivax.com/news"},
        {"ticker": "SPRO", "company": "Spero", "change": "+245%", "date": "28.05.2025",
         "reason": "Phase 3 הופסק מוקדם - tebipenem",
         "url": "https://sperotherapeutics.com"},
        {"ticker": "CDTX", "company": "Cidara", "change": "+114%", "date": "23.06.2025",
         "reason": "Phase 2b חיובי - CD388 (שפעת)",
         "url": "https://www.cidara.com"},
        {"ticker": "GPCR", "company": "Structure Therapeutics", "change": "+127%", "date": "2025",
         "reason": "ACCESS phase trials - aleniglipron (השמנה)",
         "url": "https://www.structuretx.com"}
    ],
    "PHASE_3_FAILURE_BIOTECH": [
        {"ticker": "RZLT", "company": "Rezolute", "change": "-87%", "date": "12.2025",
         "reason": "Phase 3 נכשל - ersodetug",
         "url": "https://www.fiercebiotech.com"},
        {"ticker": "IMMP", "company": "Immutep", "change": "-88%", "date": "13.03.2026",
         "reason": "TACTI-004 הופסק - efti לסרטן ריאות",
         "url": "https://pharmaphorum.com"},
        {"ticker": "SAGE", "company": "Sage Therapeutics", "change": "-90%", "date": "2024",
         "reason": "3 כשלונות רצופים של dalzanemdor",
         "url": "https://www.biospace.com"},
        {"ticker": "ITOS", "company": "iTeos Therapeutics", "change": "-75%", "date": "05.2025",
         "reason": "כשלון של belrestotug (TIGIT)",
         "url": "https://www.biospace.com"}
    ],
    
    # Acquisitions - by sector
    "ACQUISITION_BIOTECH": [
        {"ticker": "CDTX", "company": "Cidara", "change": "+105%", "date": "14.11.2025",
         "reason": "Merck רוכשת ב-$9.2B",
         "url": "https://www.merck.com/news"},
        {"ticker": "SWAV", "company": "Shockwave Medical", "change": "+90%", "date": "04.2024",
         "reason": "J&J רוכשת ב-$13.1B",
         "url": "https://www.jnj.com/news"},
        {"ticker": "CBAY", "company": "CymaBay", "change": "+120%", "date": "02.2024",
         "reason": "Gilead רוכשת ב-$4.3B",
         "url": "https://www.gilead.com/news"},
        {"ticker": "CTLT", "company": "Catalent", "change": "+85%", "date": "2024",
         "reason": "Novo Holdings רוכשת ב-$16.5B",
         "url": "https://www.catalent.com/news"}
    ],
    "ACQUISITION_TECH": [
        {"ticker": "ANSS", "company": "Ansys", "change": "+30%", "date": "01.2024",
         "reason": "Synopsys רוכש ב-$35B",
         "url": "https://www.synopsys.com/news"},
        {"ticker": "CYBR", "company": "CyberArk", "change": "+25%", "date": "2025",
         "reason": "Palo Alto Networks רוכש ב-$25B",
         "url": "https://www.paloaltonetworks.com/news"},
        {"ticker": "WIZ", "company": "Wiz", "change": "Private deal", "date": "03.2025",
         "reason": "Google רוכש ב-$32B",
         "url": "https://www.google.com/news"}
    ],
    "ACQUISITION_ENERGY": [
        {"ticker": "MRO", "company": "Marathon Oil", "change": "+30%", "date": "2024",
         "reason": "ConocoPhillips רוכשת ב-$22.5B",
         "url": "https://www.conocophillips.com"}
    ],
    "ACQUISITION_FINANCIAL": [
        {"ticker": "DFS", "company": "Discover Financial", "change": "+15%", "date": "02.2024",
         "reason": "Capital One מתמזגת איתה (all-stock)",
         "url": "https://www.capitalone.com"}
    ],
    
    # AI / Tech
    "AI_PARTNERSHIP_TECH": [
        {"ticker": "SOUN", "company": "SoundHound AI", "change": "+836%", "date": "2024",
         "reason": "Nvidia השקיעה - 13F filing",
         "url": "https://www.fool.com"},
        {"ticker": "BBAI", "company": "BigBear.ai", "change": "+200%", "date": "02.2024",
         "reason": "שותפות אסטרטגית עם Palantir",
         "url": "https://bigbear.ai"},
        {"ticker": "WRD", "company": "WeRide", "change": "+81%", "date": "02.2025",
         "reason": "Nvidia הפכה למשקיעה",
         "url": "https://www.weride.ai"},
        {"ticker": "WDC", "company": "Western Digital", "change": "+282%", "date": "2025",
         "reason": "ביקוש AI ל-HDD",
         "url": "https://www.westerndigital.com"},
        {"ticker": "MU", "company": "Micron", "change": "+220%", "date": "2025",
         "reason": "Memory leader for AI - HBM shortage",
         "url": "https://investors.micron.com"}
    ],
    "QUANTUM_HYPE_TECH": [
        {"ticker": "QUBT", "company": "Quantum Computing Inc", "change": "+5400%", "date": "2024-2025",
         "reason": "Google Willow chip + AI hype",
         "url": "https://quantumcomputinginc.com"},
        {"ticker": "RGTI", "company": "Rigetti Computing", "change": "+545%", "date": "2025",
         "reason": "פריצות דרך quantum computing",
         "url": "https://www.rigetti.com"},
        {"ticker": "QBTS", "company": "D-Wave Quantum", "change": "+458%", "date": "2025",
         "reason": "JPMorgan Security Initiative",
         "url": "https://www.dwavequantum.com"},
        {"ticker": "IONQ", "company": "IonQ", "change": "+170%", "date": "2025",
         "reason": "$1B equity raise",
         "url": "https://ionq.com"}
    ],
    
    # Crypto Treasury
    "CRYPTO_TREASURY_PIVOT": [
        {"ticker": "OCTO", "company": "Eightco Holdings", "change": "+3000%", "date": "09.2025",
         "reason": "Worldcoin treasury strategy",
         "url": "https://eightco.com"},
        {"ticker": "MSTR", "company": "Strategy", "change": "+565%", "date": "12 חודשים",
         "reason": "Bitcoin treasury - 471k+ BTC",
         "url": "https://www.strategy.com"},
        {"ticker": "BMNR", "company": "BitMine Immersion", "change": "+400%", "date": "2025",
         "reason": "Ethereum treasury - 2M ETH",
         "url": "https://bitminetech.io"},
        {"ticker": "MEIP", "company": "MEI Pharma", "change": "+100%", "date": "07.2025",
         "reason": "Litecoin treasury - $100M",
         "url": "https://www.meipharma.com"}
    ],
    
    # AI Pivot
    "AI_PIVOT_GENERAL": [
        {"ticker": "SOUN", "company": "SoundHound AI", "change": "+836%", "date": "2024",
         "reason": "AI pivot + Nvidia investment",
         "url": "https://www.soundhound.com"},
        {"ticker": "Multiple", "company": "33 חברות AI 2023-2026", "change": "+100% עד +500%",
         "date": "מצטבר", "reason": "שינוי שם / pivot ל-AI",
         "url": "https://www.acadian-asset.com"}
    ],
    
    # Government Contracts
    "DOD_CONTRACT_DEFENSE": [
        {"ticker": "TNXP", "company": "Tonix Pharmaceuticals", "change": "+570%", "date": "2025",
         "reason": "חוזה $34M משרד ההגנה",
         "url": "https://www.tonixpharma.com"},
        {"ticker": "Multiple", "company": "ספקי DoD שונים", "change": "+50% עד +200%",
         "date": "מצטבר", "reason": "חוזי הגנה גדולים",
         "url": "https://www.usaspending.gov"}
    ],
    "DOD_CONTRACT_TECH": [
        {"ticker": "PLTR", "company": "Palantir", "change": "+50% מתמשך", "date": "2024-2025",
         "reason": "חוזי DoD למערכות AI",
         "url": "https://www.palantir.com/news"}
    ],
    
    # Strategic Investment
    "STRATEGIC_INVESTMENT_TECH": [
        {"ticker": "FUBO", "company": "FuboTV", "change": "+100%", "date": "01.2025",
         "reason": "Disney מקבלת 70% מהמניות",
         "url": "https://corporate.fubo.tv"},
        {"ticker": "WRD", "company": "WeRide", "change": "+81%", "date": "02.2025",
         "reason": "Nvidia השקעה אסטרטגית",
         "url": "https://www.weride.ai"}
    ],
    
    # ===== NEGATIVE CATALYSTS =====
    
    "SEC_INVESTIGATION_TECH": [
        {"ticker": "KD", "company": "Kyndryl Holdings", "change": "-55%", "date": "09.02.2026",
         "reason": "SEC enforcement + material weakness",
         "url": "https://www.investmentnews.com"},
        {"ticker": "DRVN", "company": "Driven Brands", "change": "-50%", "date": "02.2026",
         "reason": "Restatement + delisting notice",
         "url": "https://www.prnewswire.com"}
    ],
    "SEC_INVESTIGATION_GENERAL": [
        {"ticker": "KD", "company": "Kyndryl Holdings", "change": "-55%", "date": "09.02.2026",
         "reason": "SEC enforcement + material weakness",
         "url": "https://www.investmentnews.com"},
        {"ticker": "ADM", "company": "Archer-Daniels-Midland", "change": "-24%", "date": "01.2024",
         "reason": "SEC חקירה - הגרוע מאז 1929",
         "url": "https://www.rgrdlaw.com"}
    ],
    "ACCOUNTING_FRAUD": [
        {"ticker": "ADM", "company": "Archer-Daniels-Midland", "change": "-24%", "date": "01.2024",
         "reason": "Scandal חשבונאי - CFO הושעה",
         "url": "https://www.rgrdlaw.com"},
        {"ticker": "KD", "company": "Kyndryl", "change": "-55%", "date": "02.2026",
         "reason": "Material weakness + tone at the top",
         "url": "https://www.investmentnews.com"}
    ],
    "SHORT_REPORT": [
        {"ticker": "NKLA", "company": "Nikola", "change": "-40%", "date": "09.2020",
         "reason": "Hindenburg report - fraud",
         "url": "https://en.wikipedia.org/wiki/Hindenburg_Research"},
        {"ticker": "SMCI", "company": "Super Micro", "change": "-30%", "date": "2024",
         "reason": "Hindenburg report",
         "url": "https://seekingalpha.com"},
        {"ticker": "ADANIENT", "company": "Adani Group", "change": "-50%", "date": "2023",
         "reason": "Hindenburg - $108B אבדן",
         "url": "https://www.integrity-research.com"},
        {"ticker": "TINGO", "company": "Tingo Group", "change": "-80%", "date": "06.2023",
         "reason": "Hindenburg - fraud",
         "url": "https://en.wikipedia.org/wiki/Hindenburg_Research"}
    ],
    "BANKRUPTCY": [
        {"ticker": "NKLA", "company": "Nikola", "change": "Chapter 11", "date": "19.02.2025",
         "reason": "פשיטת רגל בעקבות Hindenburg",
         "url": "https://en.wikipedia.org/wiki/Hindenburg_Research"},
        {"ticker": "RIDE", "company": "Lordstown Motors", "change": "Bankruptcy", "date": "2023",
         "reason": "פשיטת רגל - חקירת Hindenburg",
         "url": "https://seekingalpha.com"}
    ]
}


CATEGORY_STATS = {
    "FDA_APPROVAL": "ביוטק + אישור FDA: לרוב +30% עד +200% ביום",
    "PHASE_3_SUCCESS": "ניסוי שלב 3 חיובי בביוטק: לרוב +50% עד +500% ביום",
    "PHASE_3_FAILURE": "ניסוי שלב 3 שנכשל: לרוב -50% עד -90% ביום",
    "ACQUISITION": "רכישת חברה: זינוק של 30%-105% ליעד הרכישה (פרמיה)",
    "AI_PARTNERSHIP": "שותפות AI עם Nvidia/MSFT/Google: +50% עד +800%",
    "QUANTUM_HYPE": "Quantum hype: עליות 100%-5400% ב-2024-2025",
    "CRYPTO_TREASURY": "אסטרטגיית crypto treasury: ממוצע +150% ב-24 שעות",
    "AI_PIVOT": "Pivot ל-AI / שינוי שם: 33 מקרים של עליות משמעותיות",
    "DOD_CONTRACT": "חוזה ממשלתי גדול: +50% עד +570% (תלוי בגודל החברה)",
    "STRATEGIC_INVESTMENT": "השקעה אסטרטגית מענקית: +30% עד +100%",
    "SEC_INVESTIGATION": "חקירת SEC: לרוב -24% עד -55% ביום אחד",
    "ACCOUNTING_FRAUD": "Scandal חשבונאות: לרוב -24% עד -90%",
    "SHORT_REPORT": "דוח Hindenburg/Citron: ירידה ממוצעת -30% עד -80%",
    "BANKRUPTCY": "פשיטת רגל: ירידה -90% עד delisting"
}


# ============================================================
# קטליזטורים
# ============================================================
POSITIVE_CATALYSTS = {
    "🟢 אישור FDA": {
        "strength": 5, "category": "FDA_APPROVAL",
        "keywords": [
            "fda approval", "fda approves", "fda approved", "fda has approved",
            "approved by fda", "biologics license application approved",
            "bla approved", "nda approval", "nda approved",
            "receives fda approval", "received fda approval", "granted fda approval",
            "fda grants approval", "fda clearance", "fda cleared",
            "wins fda approval", "secures fda approval", "regulatory approval",
            "marketing authorization", "fda nod", "green light from fda"
        ]
    },
    "🧪 ניסוי שלב 3 הצליח": {
        "strength": 5, "category": "PHASE_3_SUCCESS",
        "keywords": [
            "phase 3 met primary endpoint", "phase iii met primary endpoint",
            "phase 3 positive topline", "phase iii positive topline",
            "phase 3 positive results", "phase iii positive results",
            "met primary and secondary endpoints", "achieved primary endpoint",
            "pivotal trial success", "topline results positive",
            "successful phase 3", "phase 3 success", "phase iii success",
            "statistically significant", "p<0.001", "p<0.0001",
            "trial demonstrated efficacy", "study demonstrated efficacy"
        ]
    },
    "🤝 רכישה / מיזוג": {
        "strength": 5, "category": "ACQUISITION",
        "keywords": [
            "agreement to acquire", "agreement to be acquired",
            "to be acquired by", "merger agreement", "definitive merger agreement",
            "tender offer", "all-cash transaction", "definitive agreement to acquire",
            "buyout offer", "to be taken private", "acquisition agreement",
            "announced acquisition", "deal valued at", "transaction valued at",
            "non-binding proposal", "premium of approximately", "per share in cash"
        ]
    },
    "💰 השקעה אסטרטגית": {
        "strength": 4, "category": "STRATEGIC_INVESTMENT",
        "keywords": [
            "strategic investment in", "minority stake", "equity stake",
            "nvidia invests", "microsoft invests", "google invests",
            "amazon invests", "apple invests", "meta invests",
            "tesla invests", "openai invests", "nvidia takes stake",
            "tech giant invests", "strategic partnership with",
            "led by", "round led by", "preferred stock investment"
        ]
    },
    "⚡ Breakthrough / Fast Track": {
        "strength": 4, "category": "FDA_APPROVAL",
        "keywords": [
            "breakthrough therapy designation", "breakthrough designation",
            "fast track designation", "fast tracked",
            "orphan drug designation", "priority review granted",
            "rmat designation", "accelerated approval",
            "fda granted breakthrough", "fda granted fast track",
            "rare pediatric disease designation"
        ]
    },
    "🏛️ חוזה ממשלתי גדול": {
        "strength": 4, "category": "DOD_CONTRACT",
        "keywords": [
            "department of defense contract", "dod contract", "dod awarded",
            "darpa contract", "barda contract", "barda awarded",
            "government contract awarded", "federal contract",
            "u.s. army contract", "u.s. navy contract", "space force contract",
            "nasa contract", "homeland security contract"
        ]
    },
    "🤖 Pivot ל-AI": {
        "strength": 4, "category": "AI_PIVOT",
        "keywords": [
            "pivots to ai", "pivot to artificial intelligence",
            "rebrands as ai", "transforms into ai", "becomes ai company",
            "ai-focused", "shifts focus to ai", "ai strategy launch",
            "transforming to ai", "transitioning to ai", "new ai division"
        ]
    },
    "₿ Crypto Treasury": {
        "strength": 5, "category": "CRYPTO_TREASURY",
        "keywords": [
            "bitcoin treasury", "ethereum treasury", "crypto treasury",
            "digital asset treasury", "adds bitcoin to treasury",
            "purchases bitcoin", "purchases ethereum", "purchases solana",
            "worldcoin treasury", "xrp treasury", "solana treasury",
            "btc treasury", "eth treasury", "crypto reserves",
            "adopts bitcoin standard"
        ]
    },
    "📛 שינוי שם לקטגוריה חמה": {
        "strength": 3, "category": "AI_PIVOT",
        "keywords": [
            "rebrands as", "rebranded as", "name change to",
            "changes name to", "changing its name to",
            "new corporate name", "rebrand to"
        ]
    },
    "💎 גיוס משמעותי": {
        "strength": 3, "category": "STRATEGIC_INVESTMENT",
        "keywords": [
            "secures financing", "completes financing", "successful capital raise",
            "oversubscribed offering", "non-dilutive financing", "royalty financing",
            "completes private placement", "raises capital", "funding round",
            "credit facility", "convertible notes", "pipe financing"
        ]
    },
    "📈 דוחות חזקים מהצפי": {
        "strength": 3, "category": "FDA_APPROVAL",
        "keywords": [
            "earnings beat", "beats earnings estimates", "exceeds analyst expectations",
            "topped analyst expectations", "tops estimates", "record quarterly revenue",
            "record revenue", "record quarter", "raises full-year guidance",
            "raises guidance", "lifted guidance", "boosts guidance",
            "beats revenue expectations", "blowout quarter",
            "above consensus", "ahead of consensus"
        ]
    },
    "🤝 שותפות עם ענקית": {
        "strength": 4, "category": "AI_PARTNERSHIP",
        "keywords": [
            "partnership with microsoft", "partnership with apple",
            "partnership with amazon", "partnership with google",
            "partnership with nvidia", "partnership with openai",
            "selected by microsoft", "selected by amazon",
            "collaboration with openai", "collaboration with microsoft",
            "joint venture with", "preferred supplier to",
            "exclusive partner", "alliance with microsoft"
        ]
    },
    "💵 חוזה גדול / הזמנה גדולה": {
        "strength": 3, "category": "DOD_CONTRACT",
        "keywords": [
            "multi-year agreement", "billion contract", "billion-dollar deal",
            "million contract awarded", "long-term supply agreement",
            "exclusive distribution agreement", "exclusive license",
            "licensing agreement", "wins contract", "won contract", "awarded contract"
        ]
    },
    "🦌 Short Squeeze פוטנציאלי": {
        "strength": 3, "category": "AI_PIVOT",
        "keywords": [
            "short squeeze", "high short interest", "heavily shorted",
            "most shorted", "wallstreetbets", "wsb favorite",
            "meme stock", "retail favorite", "retail frenzy"
        ]
    },
    "👔 Insider Buying": {
        "strength": 2, "category": "STRATEGIC_INVESTMENT",
        "keywords": [
            "insider buying", "insider purchases", "ceo buys shares",
            "executives buying", "form 4 filing", "10b5-1 plan"
        ]
    },
    "🔬 פטנט / פריצת דרך": {
        "strength": 3, "category": "AI_PARTNERSHIP",
        "keywords": [
            "patent granted", "patent approved", "patent issued",
            "awarded patent", "breakthrough technology", "scientific breakthrough",
            "first-in-class", "first-of-its-kind", "world's first",
            "groundbreaking", "novel technology"
        ]
    },
    "📊 הוספה לאינדקס": {
        "strength": 2, "category": "STRATEGIC_INVESTMENT",
        "keywords": [
            "added to s&p 500", "added to russell 2000", "added to russell 1000",
            "nasdaq 100 inclusion", "index inclusion", "joining s&p"
        ]
    },
    "📰 שדרוג אנליסטים מסיבי": {
        "strength": 2, "category": "STRATEGIC_INVESTMENT",
        "keywords": [
            "double upgrade", "raised price target", "upgraded to strong buy",
            "upgraded to buy", "upgraded to overweight", "upgraded to outperform",
            "initiated with buy"
        ]
    },
    "🚀 Uplisting / IPO": {
        "strength": 3, "category": "STRATEGIC_INVESTMENT",
        "keywords": [
            "uplisting to nasdaq", "uplisting to nyse", "approved for listing",
            "begins trading on nasdaq", "direct listing", "moves to nasdaq"
        ]
    }
}

NEGATIVE_CATALYSTS = {
    "⚖️ חקירת SEC": {
        "strength": 5, "category": "SEC_INVESTIGATION",
        "keywords": [
            "sec investigation", "sec subpoena", "sec inquiry",
            "sec enforcement", "sec division of enforcement",
            "voluntary document requests from the sec",
            "sec probe", "wells notice", "sec charges"
        ]
    },
    "🏛️ חקירת DOJ / FBI": {
        "strength": 5, "category": "SEC_INVESTIGATION",
        "keywords": [
            "doj investigation", "department of justice investigation",
            "fbi investigation", "criminal investigation",
            "criminal charges filed", "indicted on", "indictment unsealed",
            "doj probe", "federal prosecutors"
        ]
    },
    "❌ דחיית FDA": {
        "strength": 5, "category": "PHASE_3_FAILURE",
        "keywords": [
            "fda rejection", "fda rejects", "rejected by fda",
            "complete response letter", "crl from fda", "received crl",
            "fda declined", "fda refuses", "fda denial",
            "fda does not approve", "advisory committee voted against"
        ]
    },
    "🚫 ניסוי קליני נכשל": {
        "strength": 5, "category": "PHASE_3_FAILURE",
        "keywords": [
            "trial failed", "failed primary endpoint", "missed primary endpoint",
            "did not meet primary endpoint", "discontinued trial",
            "halts clinical trial", "halted clinical trial",
            "trial halt", "trial suspension", "discontinues development",
            "terminates trial", "phase 3 failure", "phase 2 failure",
            "study failed", "fails to meet"
        ]
    },
    "💀 פשיטת רגל / קשיים": {
        "strength": 5, "category": "BANKRUPTCY",
        "keywords": [
            "chapter 11", "chapter 7", "bankruptcy filing",
            "files for bankruptcy", "bankruptcy protection",
            "going concern doubt", "going concern",
            "ability to continue operations", "substantial doubt about",
            "insolvent", "ceases operations"
        ]
    },
    "📄 בעיות בדוחות כספיים": {
        "strength": 4, "category": "ACCOUNTING_FRAUD",
        "keywords": [
            "unable to file", "delayed filing", "missed filing deadline",
            "restatement", "restated financials", "material weakness",
            "accounting irregularities", "accounting errors",
            "should no longer be relied upon", "internal control failures",
            "audit committee review", "non-reliance on"
        ]
    },
    "🔥 דוח Short Seller": {
        "strength": 4, "category": "SHORT_REPORT",
        "keywords": [
            "hindenburg research", "muddy waters research",
            "citron research", "kerrisdale capital", "spruce point",
            "short seller report", "short report alleges",
            "short seller alleges", "activist short seller",
            "short report", "viceroy research"
        ]
    },
    "🚪 התפטרות פתאומית": {
        "strength": 3, "category": "ACCOUNTING_FRAUD",
        "keywords": [
            "abrupt resignation", "ceo resigns", "cfo resigns",
            "ceo steps down", "cfo steps down", "sudden departure",
            "abruptly departed", "resigns effective immediately",
            "placed on administrative leave", "removed from position"
        ]
    },
    "🩺 ניסוי קליני - תופעה חמורה": {
        "strength": 4, "category": "PHASE_3_FAILURE",
        "keywords": [
            "patient death", "patient died", "serious adverse event",
            "trial halt due to safety", "safety concerns",
            "fda places clinical hold", "clinical hold", "trial paused safety",
            "study paused", "fatal adverse event"
        ]
    },
    "📦 Recall גדול": {
        "strength": 3, "category": "PHASE_3_FAILURE",
        "keywords": [
            "fda recall", "voluntary recall", "product recall",
            "class i recall", "expanded recall", "nationwide recall",
            "safety recall"
        ]
    },
    "🚨 הסרה מבורסה (Delisting)": {
        "strength": 4, "category": "BANKRUPTCY",
        "keywords": [
            "nasdaq delisting", "nyse delisting", "delisting notice",
            "minimum bid notification", "delisting determination",
            "deficiency notice", "non-compliance notice", "to be delisted"
        ]
    },
    "📉 הורדת תחזית / Guidance": {
        "strength": 3, "category": "ACCOUNTING_FRAUD",
        "keywords": [
            "lowers guidance", "withdraws guidance", "cuts outlook",
            "guidance cut", "warns on outlook", "missed earnings",
            "earnings miss", "below expectations", "missed revenue",
            "weak guidance", "disappointing results", "below consensus"
        ]
    },
    "💸 דילול מסיבי": {
        "strength": 3, "category": "BANKRUPTCY",
        "keywords": [
            "massive dilution", "reverse stock split", "reverse split",
            "registered direct offering", "at-the-market offering",
            "atm offering", "shelf offering", "dilutive offering"
        ]
    }
}


# ============================================================
# סינון רעש מורחב מאוד
# ============================================================
NOISE_PATTERNS = [
    # רשימות וטופ
    "top stocks", "top 10 stocks", "top 5 stocks", "top 3 stocks",
    "best stocks", "stocks to watch", "stocks to buy", "stocks to consider",
    "stocks to avoid", "stocks to sell", "stocks under $", "best performing",
    "top performers", "5 stocks", "3 stocks", "10 stocks", "screaming buys",
    "screaming buy", "stock picks", "best stock to buy",
    
    # סקירות שבועיות / פרשנות שוק
    "weekly summary", "weekly recap", "weekly review", "week ahead",
    "week in review", "weekly market", "weekly stock", "weekly wrap",
    "this week's", "weekly outlook", "weekly preview", "weekly analysis",
    
    # סקירות רבעוניות / שנתיות
    "quarterly review", "q1 portfolio", "q2 portfolio", "q3 portfolio",
    "q4 portfolio", "portfolio review", "portfolio update", "holdings review",
    "year in review", "year-end review", "annual review",
    
    # קרנות / תיקים
    "fund holdings", "fund report", "fund update", "fund commentary",
    "mid cap fund", "large cap fund", "small cap fund", "mutual fund",
    "etf holdings", "etf review", "investor letter", "shareholder letter",
    
    # פרשנות כללית
    "market commentary", "market analysis", "market wrap", "market outlook",
    "market preview", "market update", "market recap", "market summary",
    "morning brief", "morning report", "daily wrap", "daily recap",
    "trading day", "trading update", "trading recap",
    
    # תחזיות וניתוח
    "outlook for 2025", "outlook for 2026", "outlook 2025", "outlook 2026",
    "predictions for", "what to expect", "what's next for",
    "things to watch", "what we're watching", "what's ahead",
    
    # מאמרי דעה
    "why i'm bullish", "why i'm bearish", "should you buy", "should you sell",
    "is it time to buy", "is it time to sell", "bull case", "bear case",
    
    # CEF / סקירות תיקים
    "cef weekly", "cef update", "closed-end fund", "preferred stock list",
    "dividend stocks", "dividend report", "dividend portfolio"
]


def is_noise(headline):
    """בודק אם הכותרת היא רעש"""
    h = headline.lower()
    for pat in NOISE_PATTERNS:
        if pat in h:
            return True
    # אם בכותרת יש 3+ טיקרים בסוגריים, זו רשימה
    tickers_in_title = re.findall(r'\([A-Z]{1,5}\)', headline)
    if len(tickers_in_title) >= 3:
        return True
    # אם הכותרת מכילה מספר + "stocks" או "picks"
    if re.search(r'\d+\s+(stocks|picks|companies|ideas)', h):
        return True
    return False


def is_company_focused(ticker, headline, summary):
    """
    בודק שהכתבה באמת על חברה אחת ספציפית.
    הטיקר/שם חייב להיות בכותרת, או מוזכר 3+ פעמים בטקסט.
    """
    if not ticker:
        return False
    headline_upper = headline.upper()
    summary_upper = (summary or "").upper()
    
    # האם הטיקר בכותרת?
    ticker_in_headline = (
        re.search(rf'\b{ticker}\b', headline_upper) is not None or
        f"({ticker})" in headline_upper or
        f"${ticker}" in headline_upper
    )
    
    # שם החברה בכותרת?
    company_name = ticker_to_company.get(ticker, "")
    company_in_headline = False
    if company_name:
        # מסיר מילים גנריות
        first_word = company_name.split()[0].upper() if company_name else ""
        if first_word and len(first_word) >= 4 and first_word in headline_upper:
            company_in_headline = True
    
    if ticker_in_headline or company_in_headline:
        return True
    
    # אם לא בכותרת - ספור הופעות בכל הטקסט
    full_text = headline_upper + " " + summary_upper
    ticker_count = len(re.findall(rf'\b{ticker}\b', full_text))
    if ticker_count >= 3:
        return True
    
    return False


def israel_time():
    return datetime.now(ZoneInfo("Asia/Jerusalem")).strftime("%Y-%m-%d %H:%M:%S")


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


def make_alert_hash(symbol, headline):
    return f"{symbol}|{headline[:100]}"


def detect_catalysts(text, catalyst_dict):
    found = []
    text_lower = text.lower()
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


def get_strength_emoji(strength):
    return "🔥" * strength


def get_market_cap_label(market_cap):
    if not market_cap:
        return "לא זמין"
    if market_cap < 50_000_000:
        return f"⚡ Nano Cap (${market_cap/1e6:.1f}M)"
    elif market_cap < 300_000_000:
        return f"⚡ Micro Cap (${market_cap/1e6:.0f}M)"
    elif market_cap < 2_000_000_000:
        return f"Small Cap (${market_cap/1e6:.0f}M)"
    elif market_cap < 10_000_000_000:
        return f"Mid Cap (${market_cap/1e9:.1f}B)"
    elif market_cap < 200_000_000_000:
        return f"Large Cap (${market_cap/1e9:.0f}B)"
    else:
        return f"Mega Cap (${market_cap/1e9:.0f}B)"


def time_ago_label(published_timestamp):
    now = time.time()
    diff_seconds = now - published_timestamp
    if diff_seconds < 0:
        return "🟢 כעת"
    minutes = diff_seconds / 60
    if minutes < 60:
        return f"🟢 לפני {int(minutes)} דקות"
    else:
        hours = minutes / 60
        return f"🟡 לפני {hours:.1f} שעות"


def format_us_eastern_time(timestamp):
    try:
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        est = dt.astimezone(ZoneInfo("America/New_York"))
        return est.strftime("%d.%m.%Y %H:%M EST")
    except Exception:
        return "לא ידוע"


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
                market_cap = market_cap * 1_000_000
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
    return {"sector_en": "", "sector_he": "לא ידוע", "market_cap": 0, "name": ""}


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


def find_similar_case(category, sector_en, market_cap):
    """
    מחפש מקרה דומה במאגר עם נקודות אחיזה מרובות:
    1. category + sector + size match
    2. category + sector match
    3. category alone
    """
    sector_lower = (sector_en or "").lower()
    
    # שלב 1: זיהוי תת-מגזר
    sector_keys = []
    if any(k in sector_lower for k in ["biotech", "pharma", "drug manuf", "life sciences"]):
        sector_keys.append("BIOTECH")
    if any(k in sector_lower for k in ["medical device", "health care equip"]):
        sector_keys.append("DEVICES")
    if any(k in sector_lower for k in ["tech", "software", "semicond", "hardware",
                                        "internet", "computer"]):
        sector_keys.append("TECH")
    if any(k in sector_lower for k in ["defense", "aerospace"]):
        sector_keys.append("DEFENSE")
    if any(k in sector_lower for k in ["energy", "oil", "gas", "uranium", "coal"]):
        sector_keys.append("ENERGY")
    if any(k in sector_lower for k in ["bank", "financial", "insurance", "credit"]):
        sector_keys.append("FINANCIAL")
    if any(k in sector_lower for k in ["pharma"]):
        sector_keys.append("PHARMA")
    
    # שלב 2: גודל החברה
    size_key = "SMALL"
    if market_cap and market_cap > 10_000_000_000:
        size_key = "LARGE"
    elif market_cap and market_cap > 2_000_000_000:
        size_key = "MID"
    
    # שלב 3: חיפוש - הכי ספציפי קודם
    # רמה 1: category + sector + size
    for sk in sector_keys:
        for suffix in [f"_{sk}_{size_key}", f"_{sk}"]:
            key = f"{category}{suffix}"
            if key in HISTORICAL_CASES and HISTORICAL_CASES[key]:
                return HISTORICAL_CASES[key][0]
    
    # רמה 2: category לבד - מנסה כמה אופציות
    for k in HISTORICAL_CASES:
        if k.startswith(category):
            cases = HISTORICAL_CASES[k]
            if cases:
                return cases[0]
    
    # רמה 3: שם הקטגוריה במפתח
    for k in HISTORICAL_CASES:
        if category.split("_")[0] in k:
            cases = HISTORICAL_CASES[k]
            if cases:
                return cases[0]
    
    return None


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
    company_upper = company_name.upper()
    for suffix in [" INC", " CORP", " CORPORATION", " LLC", " LTD",
                   " HOLDINGS", " GROUP", " CO", " COMPANY", ", INC",
                   ", LLC", ", CORP", ".", ","]:
        company_upper = company_upper.replace(suffix, "").strip()
    for ticker, name in ticker_to_company.items():
        if not name:
            continue
        name_upper = name.upper()
        for suffix in [" INC", " CORP", " CORPORATION", " LLC", " LTD",
                       " HOLDINGS", " GROUP", " CO", " COMPANY"]:
            name_upper = name_upper.replace(suffix, "").strip()
        if company_upper == name_upper:
            return ticker
        if len(company_upper) >= 6 and company_upper in name_upper:
            return ticker
    return None


def extract_ticker_from_news(related, headline, summary):
    if related:
        candidates = related.split(",")
        for c in candidates:
            c = c.strip().upper()
            if c in all_us_tickers:
                return c
    patterns = [
        r'\(([A-Z]{1,5})\)',
        r'NASDAQ:\s*([A-Z]{1,5})',
        r'NYSE:\s*([A-Z]{1,5})',
        r'\$([A-Z]{1,5})\b'
    ]
    for pat in patterns:
        for match in re.findall(pat, headline + " " + summary):
            if match in all_us_tickers:
                return match
    return None


# ============================================================
# מנוע 1: USAspending.gov
# ============================================================
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
            "fields": ["Award ID", "Recipient Name", "Award Amount",
                       "Awarding Agency", "Description", "Start Date"],
            "page": 1, "limit": 100, "sort": "Award Amount", "order": "desc"
        }
        req = Request(url, data=json.dumps(payload).encode(),
                      headers={"Content-Type": "application/json"})
        with urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode())

        results = data.get("results", [])
        print(f"[USAspending] {len(results)} contracts")

        for contract in results:
            recipient = contract.get("Recipient Name", "").upper()
            amount = contract.get("Award Amount", 0)
            agency = contract.get("Awarding Agency", "")
            description = contract.get("Description", "")
            award_id = contract.get("Award ID", "")

            ticker = find_ticker_for_company(recipient)
            
            # אם אין טיקר - לא שולחים (לא חברה ציבורית)
            if not ticker:
                continue
            
            # סינון מחיר
            price = get_current_price(ticker)
            if not price or price > MAX_PRICE:
                continue
            
            alert_hash = make_alert_hash(ticker, f"USASPENDING_{award_id}")
            if alert_hash in sent_alerts:
                continue

            send_usaspending_alert(ticker, recipient, amount, agency, description, award_id, price)
            sent_alerts.add(alert_hash)

    except Exception as e:
        print(f"[USAspending] error: {e}")


def send_usaspending_alert(ticker, recipient, amount, agency, description, award_id, price):
    link = f"https://www.usaspending.gov/award/{award_id}/"
    
    meta = get_finnhub_company_profile(ticker)
    sector_he = meta["sector_he"]
    market_cap_label = get_market_cap_label(meta["market_cap"])

    similar = find_similar_case("DOD_CONTRACT", meta["sector_en"], meta["market_cap"])
    if similar:
        similar_text = (
            f"📊 מקרה דומה: {similar['ticker']} - {similar['change']} ({similar['date']})\n"
            f"   סיבה: {similar['reason']}\n"
            f"   🔗 {similar['url']}"
        )
    else:
        similar_text = (
            f"📊 מקרה דומה: אין מקרה דומה במאגר\n"
            f"   📈 {CATEGORY_STATS.get('DOD_CONTRACT', '')}"
        )

    description_he = translate_to_hebrew(description[:300]) if description else "אין תיאור"
    headline_he = f"חוזה ממשלתי בסך ${amount:,.0f} מ-{agency}"

    message = (
        f"🏛️💰 חוזה ממשלתי חדש\n"
        f"\n"
        f"📊 סימבול: {ticker}\n"
        f"💼 שווי שוק: {market_cap_label}\n"
        f"🏢 מגזר: {sector_he}\n"
        f"\n"
        f"📰 כותרת בעברית:\n{headline_he}\n"
        f"📋 תיאור: {description_he[:200]}\n"
        f"\n"
        f"📡 מקור: USAspending.gov ({agency})\n"
        f"🔗 קישור לחוזה:\n{link}\n"
        f"\n"
        f"💵 מחיר נוכחי: ${price:.2f}\n"
        f"📅 פורסם: היום (US Federal)\n"
        f"\n"
        f"🔥 עוצמה: 🔥🔥🔥🔥 (4/5)\n"
        f"\n"
        f"{similar_text}"
    )

    if send_telegram(message):
        print(f"[USAspending ALERT] {ticker} | ${amount:,.0f}")


# ============================================================
# מנוע 2: Finnhub Market News
# ============================================================
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
        recent_news = [n for n in news_items if now - n.get("datetime", 0) <= NEWS_MAX_AGE_SECONDS]
        print(f"[Market News] {len(recent_news)} articles in last hour (out of {len(news_items)})")

        for news in recent_news:
            process_news_item(news, source_override="Finnhub")

    except Exception as e:
        print(f"[Market News] error: {e}")


# ============================================================
# מנוע 3: GlobeNewswire RSS - הודעות לעיתונות רשמיות
# ============================================================
def scan_globenewswire():
    print("[GlobeNewswire] scanning...")
    try:
        url = "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire+-+News+about+Public+Companies"
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=15) as response:
            content = response.read().decode("utf-8", errors="ignore")
        
        items = parse_rss_items(content)
        now = time.time()
        recent = [i for i in items if now - i["timestamp"] <= NEWS_MAX_AGE_SECONDS]
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


# ============================================================
# מנוע 4: PRNewswire RSS
# ============================================================
def scan_prnewswire():
    print("[PRNewswire] scanning...")
    try:
        url = "https://www.prnewswire.com/rss/news-releases-list.rss"
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=15) as response:
            content = response.read().decode("utf-8", errors="ignore")
        
        items = parse_rss_items(content)
        now = time.time()
        recent = [i for i in items if now - i["timestamp"] <= NEWS_MAX_AGE_SECONDS]
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


def parse_rss_items(rss_content):
    """מנתח RSS feed פשוט"""
    items = []
    item_blocks = re.findall(r'<item>(.*?)</item>', rss_content, re.DOTALL)
    
    for block in item_blocks:
        try:
            title_match = re.search(r'<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>', block, re.DOTALL)
            link_match = re.search(r'<link>(.*?)</link>', block, re.DOTALL)
            desc_match = re.search(r'<description>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</description>', block, re.DOTALL)
            date_match = re.search(r'<pubDate>(.*?)</pubDate>', block, re.DOTALL)
            
            title = title_match.group(1).strip() if title_match else ""
            link = link_match.group(1).strip() if link_match else ""
            desc = desc_match.group(1).strip() if desc_match else ""
            
            # ניקוי HTML
            desc = re.sub(r'<[^>]+>', '', desc)
            
            # פרסור תאריך
            timestamp = time.time()
            if date_match:
                date_str = date_match.group(1).strip()
                try:
                    dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
                    timestamp = dt.timestamp()
                except Exception:
                    try:
                        dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S GMT")
                        timestamp = dt.replace(tzinfo=timezone.utc).timestamp()
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


# ============================================================
# מעבד כללי לכתבת חדשות
# ============================================================
def process_news_item(news, source_override=None):
    headline = news.get("headline", "")
    summary = news.get("summary", "")
    url_link = news.get("url", "")
    related = news.get("related", "")
    published = news.get("datetime", time.time())

    # סינון: שעה אחרונה בלבד
    if time.time() - published > NEWS_MAX_AGE_SECONDS:
        return
    
    # סינון רעש
    if is_noise(headline):
        return
    
    full_text = f"{headline} {summary}"
    positive_found = detect_catalysts(full_text, POSITIVE_CATALYSTS)
    negative_found = detect_catalysts(full_text, NEGATIVE_CATALYSTS)

    if not positive_found and not negative_found:
        return

    ticker = extract_ticker_from_news(related, headline, summary)
    if not ticker:
        return
    
    # סינון: הכתבה חייבת להיות על חברה אחת מרכזית
    if not is_company_focused(ticker, headline, summary):
        return
    
    # סינון מחיר - עד $70
    price = get_current_price(ticker)
    if not price or price > MAX_PRICE:
        return

    alert_hash = make_alert_hash(ticker, headline)
    if alert_hash in sent_alerts:
        return

    source = source_override or news.get("source", "Unknown")
    send_news_alert(ticker, headline, source, url_link,
                    positive_found, negative_found, published, price)
    sent_alerts.add(alert_hash)


def send_news_alert(ticker, headline, source, url_link, positive, negative, published, price):
    if positive and not negative:
        emoji_header = "🚀🔥💥"
        type_text = "ידיעה חיובית - פוטנציאל זינוק"
        catalysts = positive
    elif negative and not positive:
        emoji_header = "🚨⚠️📉"
        type_text = "ידיעה שלילית - פוטנציאל ירידה"
        catalysts = negative
    else:
        emoji_header = "🚨🔄"
        type_text = "ידיעה מעורבת"
        catalysts = positive + negative

    max_strength = max([c["strength"] for c in catalysts]) if catalysts else 1
    strength_emoji = get_strength_emoji(max_strength)
    primary_category = catalysts[0]["category"] if catalysts else "FDA_APPROVAL"

    meta = get_finnhub_company_profile(ticker)
    sector_he = meta["sector_he"]
    sector_en = meta["sector_en"]
    market_cap_label = get_market_cap_label(meta["market_cap"])

    # תרגום הכותרת המקורית של הכתבה לעברית
    headline_he = translate_to_hebrew(headline)

    time_label = time_ago_label(published)
    publish_time = format_us_eastern_time(published)

    catalyst_lines = []
    seen = set()
    for c in catalysts:
        if c["catalyst"] in seen:
            continue
        seen.add(c["catalyst"])
        catalyst_lines.append(c["catalyst"])

    similar = find_similar_case(primary_category, sector_en, meta["market_cap"])
    if similar:
        similar_text = (
            f"📊 מקרה דומה: {similar['ticker']} - {similar['change']} ({similar['date']})\n"
            f"   סיבה: {similar['reason']}\n"
            f"   🔗 {similar['url']}"
        )
    else:
        stat = CATEGORY_STATS.get(primary_category, "אין סטטיסטיקה זמינה")
        similar_text = (
            f"📊 מקרה דומה: אין מקרה דומה במאגר\n"
            f"   📈 סטטיסטיקה: {stat}"
        )

    message = (
        f"{emoji_header} {type_text}\n"
        f"\n"
        f"📊 סימבול: {ticker}\n"
        f"💼 שווי שוק: {market_cap_label}\n"
        f"🏢 מגזר: {sector_he}\n"
        f"\n"
        f"📰 כותרת בעברית:\n{headline_he}\n"
        f"\n"
        f"🎯 קטליזטורים שזוהו:\n"
        f"{chr(10).join(catalyst_lines)}\n"
        f"\n"
        f"📡 מקור: {source}\n"
        f"🔗 קישור לכתבה:\n{url_link}\n"
        f"\n"
        f"💵 מחיר נוכחי: ${price:.2f}\n"
        f"📅 פורסם: {publish_time}\n"
        f"⏰ {time_label}\n"
        f"\n"
        f"🔥 עוצמה: {strength_emoji} ({max_strength}/5)\n"
        f"\n"
        f"{similar_text}"
    )

    if send_telegram(message):
        print(f"[ALERT] {ticker} | ${price:.2f} | strength {max_strength}/5")


# ============================================================
# INIT + MAIN LOOP
# ============================================================
print("=" * 60)
print("CATALYST NEWS BOT V4 STARTED")
print("=" * 60)

load_us_tickers()

print(f"Market News: every {MARKET_NEWS_INTERVAL}s (last hour only)")
print(f"GlobeNewswire: every {PRESS_RELEASE_INTERVAL}s (last hour only)")
print(f"PRNewswire: every {PRESS_RELEASE_INTERVAL}s (last hour only)")
print(f"USAspending: every {USASPENDING_INTERVAL}s")
print(f"Max stock price: ${MAX_PRICE}")
print(f"Historical cases: {sum(len(v) for v in HISTORICAL_CASES.values())}")
print("=" * 60)

send_telegram(
    f"🚀 בוט קטליזטורים V4 הופעל\n"
    f"\n"
    f"📡 מקורות:\n"
    f"  • Finnhub Market News (חדשות שוק)\n"
    f"  • GlobeNewswire RSS (הודעות רשמיות)\n"
    f"  • PRNewswire RSS (הודעות רשמיות)\n"
    f"  • USAspending.gov (חוזים $10M+)\n"
    f"\n"
    f"🎯 מנטר {len(all_us_tickers)} מניות אמריקאיות\n"
    f"📊 {len(POSITIVE_CATALYSTS)} קטליזטורים חיוביים\n"
    f"⚠️ {len(NEGATIVE_CATALYSTS)} קטליזטורים שליליים\n"
    f"📚 {sum(len(v) for v in HISTORICAL_CASES.values())} מקרים היסטוריים\n"
    f"\n"
    f"✨ סינונים פעילים:\n"
    f"  • רק חדשות מהשעה האחרונה\n"
    f"  • רק מניות עד ${MAX_PRICE}\n"
    f"  • רק כתבות שמתמקדות בחברה אחת\n"
    f"  • סינון אוטומטי של סקירות/רשימות\n"
    f"  • דירוג עוצמה 1-5 🔥\n"
    f"  • תרגום אוטומטי לעברית\n"
    f"  • מקרה דומה לכל התראה"
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
            print("[Cleanup] sent_alerts cleared")

        time.sleep(10)

    except KeyboardInterrupt:
        print("CATALYST NEWS BOT STOPPED BY USER")
        break
    except Exception as e:
        print(f"MAIN LOOP ERROR: {e}")
        time.sleep(30)
