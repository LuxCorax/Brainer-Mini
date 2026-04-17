"""Configuration for Brainer Mini App backend."""
import os

# --- Telegram ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://luxcorax.github.io/Brainer-Mini/")
# Owner admin access — comma-separated list of Telegram user IDs.
# Example: OWNER_CHAT_ID=123456789,987654321
# Admin commands check membership in OWNER_CHAT_IDS; waitlist signups DM each.
_owner_raw = os.getenv("OWNER_CHAT_ID", "")
OWNER_CHAT_IDS: list[int] = []
for _piece in _owner_raw.split(","):
    _piece = _piece.strip()
    if _piece.isdigit() or (_piece.startswith("-") and _piece[1:].isdigit()):
        OWNER_CHAT_IDS.append(int(_piece))
# Backward-compat scalar (first owner; empty string if none configured)
OWNER_CHAT_ID = str(OWNER_CHAT_IDS[0]) if OWNER_CHAT_IDS else ""


def is_owner(tg_id: int) -> bool:
    """True if the given Telegram user ID is configured as an owner/admin."""
    return tg_id in OWNER_CHAT_IDS

# --- Binance ---
BINANCE_BASE = "https://api.binance.com"

# --- Webhook ---
# B3: Refuse to broadcast on the default secret. If the env var isn't set,
# `WEBHOOK_SECRET` equals `WEBHOOK_SECRET_DEFAULT` and the webhook handler
# refuses all requests (logs loudly). Never accept the public default.
WEBHOOK_SECRET_DEFAULT = "brainer-secret-change-me"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", WEBHOOK_SECRET_DEFAULT)


def webhook_secret_is_default() -> bool:
    return WEBHOOK_SECRET == WEBHOOK_SECRET_DEFAULT

# --- Database ---
DB_PATH = os.getenv("DB_PATH", "brainer.db")

# --- Contact ---
WAITLIST_EMAIL = "raven@brainer.pro"
BRAINER_URL = "https://brainer.pro"
COMMUNITY_URL = "https://t.me/BrainerProAnalyses"

# --- Timeframes (FIXED — this app is a teaser, always 5m/1m/15m) ---
CTF = "5m"
LTF = "1m"
HTF = "15m"

# --- Pairs ---
# Fetched dynamically from Binance on startup (all USDT pairs)
# Fallback list if Binance unreachable:
FALLBACK_PAIRS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "MATICUSDT",
    "LINKUSDT", "LTCUSDT", "UNIUSDT", "ATOMUSDT", "NEARUSDT",
    "APTUSDT", "ARBUSDT", "OPUSDT", "FILUSDT", "INJUSDT",
    "SUIUSDT", "SEIUSDT", "TIAUSDT", "JUPUSDT", "WLDUSDT",
    "FETUSDT", "RNDRUSDT", "GRTUSDT", "AAVEUSDT", "MKRUSDT",
    "RUNEUSDT", "PENDLEUSDT", "ENAUSDT", "STXUSDT", "IMXUSDT",
    "PEPEUSDT", "SHIBUSDT", "FLOKIUSDT", "WIFUSDT", "BONKUSDT",
    "ONDOUSDT", "TAOUSDT", "KASUSDT", "RENDERUSDT", "ALGOUSDT",
    "FTMUSDT", "SANDUSDT", "MANAUSDT", "AXSUSDT", "GALAUSDT",
]

# --- Candle limits ---
CANDLE_LIMIT = 300  # Enough for EMA200 + warmup

# --- Refresh ---
CACHE_TTL_SECONDS = 30  # How long analysis cache is valid
