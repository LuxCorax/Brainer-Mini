"""
Brainer Mini App — FastAPI Backend
Routes: /api/analysis, /api/brainwaves, /api/waitlist, /api/events, /api/webhook
"""
import asyncio
import json
import logging
import os
import hmac
import hashlib
import time

from contextlib import asynccontextmanager
from urllib.parse import parse_qsl
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, Field
from typing import Optional

from config import (
    BOT_TOKEN, WEBHOOK_SECRET, WEBAPP_URL,
    WAITLIST_EMAIL, OWNER_CHAT_ID, webhook_secret_is_default,
)
from database import (
    init_db, add_to_waitlist, store_signal, store_event,
    store_session, get_stats, upsert_user,
)
from analysis import get_full_analysis
from binance_client import validate_symbol, get_supported_pairs, load_all_usdt_pairs
from bot import create_bot_app, setup_menu_button, broadcast_alert

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

# ── Global bot app reference ──
bot_app = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown."""
    global bot_app

    # Init DB
    init_db()
    logger.info("Database ready")

    # B3: warn loudly if WEBHOOK_SECRET is the default (env var unset).
    # Webhook requests will be refused until owner sets the env var.
    if webhook_secret_is_default():
        logger.error(
            "SECURITY: WEBHOOK_SECRET is the default value — webhook endpoint "
            "will refuse all requests. Set WEBHOOK_SECRET env var to enable."
        )

    # Load all USDT pairs from Binance
    pair_count = await load_all_usdt_pairs()
    logger.info(f"Loaded {pair_count} trading pairs")

    # Init bot
    if BOT_TOKEN:
        bot_app = create_bot_app()
        if bot_app:
            await bot_app.initialize()
            await bot_app.start()
            await bot_app.updater.start_polling(drop_pending_updates=True)
            await setup_menu_button(bot_app)
            logger.info("Telegram bot started (polling)")
    else:
        logger.warning("No BOT_TOKEN — bot disabled")

    yield

    # Shutdown
    if bot_app:
        await bot_app.updater.stop()
        await bot_app.stop()
        await bot_app.shutdown()
        logger.info("Bot stopped")


app = FastAPI(
    title="Brainer Mini App API",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow GitHub Pages frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://luxcorax.github.io",
        "http://localhost:3000",
        "http://localhost:8000",
        "*",  # Telegram WebView sends various origins
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ═══════════════════════════════════════════════════════════════
#  MODELS
# ═══════════════════════════════════════════════════════════════

class WaitlistRequest(BaseModel):
    email: str
    tg_username: Optional[str] = None
    tg_id: Optional[int] = None


class EventRequest(BaseModel):
    tg_id: Optional[int] = None
    event_type: str
    event_data: Optional[str] = None
    session_duration: Optional[float] = None
    scroll_depth: Optional[float] = None
    timestamp: Optional[float] = None


class UserOpenRequest(BaseModel):
    """Payload from Mini App on page load. `init_data` is the raw query-string
    from `window.Telegram.WebApp.initData`. Accepts camelCase `initData` via alias.
    """
    init_data: str = Field(alias="initData")
    model_config = {"populate_by_name": True}


# ═══════════════════════════════════════════════════════════════
#  API ROUTES
# ═══════════════════════════════════════════════════════════════

@app.get("/")
async def root():
    return {"status": "ok", "service": "Brainer Mini App API", "version": "1.0.0"}


@app.get("/api/health")
async def health():
    stats = get_stats()
    return {"status": "healthy", "stats": stats}


@app.get("/api/pairs")
async def get_pairs():
    """Return list of all supported USDT trading pairs from Binance."""
    pairs = get_supported_pairs()
    return {
        "pairs": [
            {"symbol": p, "base": p.replace("USDT", ""), "quote": "USDT"}
            for p in pairs
        ]
    }


@app.get("/api/analysis/{symbol}")
async def get_analysis(symbol: str):
    """
    Full analysis for a trading pair.
    Returns: price, change, brainwaves, mtf, volume, signals, levels.
    """
    symbol = symbol.upper()
    if not validate_symbol(symbol):
        raise HTTPException(400, f"Unsupported pair: {symbol}")

    try:
        result = await get_full_analysis(symbol)
    except Exception as e:
        logger.error(f"Analysis error for {symbol}: {e}", exc_info=True)
        raise HTTPException(500, f"Analysis failed: {str(e)}")

    if result is None:
        raise HTTPException(502, f"Could not fetch data for {symbol}")

    return result


@app.get("/api/brainwaves/{symbol}")
async def get_brainwaves(symbol: str):
    """BrainWaves chart data only (lighter endpoint)."""
    symbol = symbol.upper()
    if not validate_symbol(symbol):
        raise HTTPException(400, f"Unsupported pair: {symbol}")

    result = await get_full_analysis(symbol)
    if result is None or result.get("brainwaves") is None:
        raise HTTPException(502, f"Could not compute BrainWaves for {symbol}")

    return {
        "symbol": symbol,
        "price": result["price"],
        "brainwaves": result["brainwaves"],
    }


# ── Waitlist ──

@app.post("/api/waitlist")
async def join_waitlist(req: WaitlistRequest):
    """Add user to NoBrainer app waitlist."""
    email = req.email.strip().lower()
    if not email or "@" not in email:
        raise HTTPException(400, "Invalid email")

    is_new = add_to_waitlist(email, req.tg_username, req.tg_id)

    # Track user if we have TG info
    if req.tg_id:
        upsert_user(tg_id=req.tg_id, username=req.tg_username)
        store_event(req.tg_id, "waitlist_signup", json.dumps({"email": email}))

    # Notify owner
    if bot_app and OWNER_CHAT_ID:
        try:
            msg = f"📝 *New Waitlist Signup*\nEmail: {email}"
            if req.tg_username:
                msg += f"\nTG: @{req.tg_username}"
            await bot_app.bot.send_message(
                chat_id=int(OWNER_CHAT_ID),
                text=msg,
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.error(f"Failed to notify owner: {e}")

    return {
        "success": True,
        "new": is_new,
        "message": "Welcome to the waitlist!" if is_new else "You're already on the list!",
    }


# ── Events / Analytics ──

def _validate_init_data(init_data: str, max_age_seconds: int = 86400) -> Optional[dict]:
    """Verify Telegram WebApp initData HMAC signature and return parsed payload.
    Returns dict with user + auth_date on success, None on any failure.
    Spec: https://core.telegram.org/bots/webapps#validating-data-received-via-the-mini-app
    Algorithm:
      1. parse init_data as query string
      2. extract `hash`, build data_check_string = sorted "k=v\\n..." of remaining pairs
      3. secret_key = HMAC_SHA256(key="WebAppData", msg=BOT_TOKEN)
      4. expected   = HMAC_SHA256(key=secret_key, msg=data_check_string).hexdigest()
      5. constant-time compare; reject if auth_date is stale (default 24h).
    """
    if not BOT_TOKEN or not init_data:
        return None
    try:
        parsed = dict(parse_qsl(init_data, keep_blank_values=True))
    except Exception:
        return None
    received_hash = parsed.pop("hash", None)
    if not received_hash:
        return None
    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
    secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    expected = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, received_hash):
        return None
    try:
        auth_date = int(parsed.get("auth_date", "0"))
        if auth_date and (int(time.time()) - auth_date) > max_age_seconds:
            return None
    except ValueError:
        return None
    user = None
    if "user" in parsed:
        try:
            user = json.loads(parsed["user"])
        except Exception:
            user = None
    return {
        "user": user,
        "auth_date": parsed.get("auth_date"),
        "query_id": parsed.get("query_id"),
        "start_param": parsed.get("start_param"),
    }


@app.post("/api/user/open")
async def user_open(req: UserOpenRequest):
    """Called by Mini App on page load. Validates Telegram initData HMAC,
    upserts user (increments open_count), logs app_open event, returns user
    info for frontend personalization.
    """
    parsed = _validate_init_data(req.init_data)
    if not parsed or not parsed.get("user"):
        raise HTTPException(401, "Invalid initData")
    u = parsed["user"]
    tg_id = u.get("id")
    if not tg_id:
        raise HTTPException(400, "Missing user id")
    # Capture Telegram's start_param for attribution.
    # First-touch: `first_start_param` on users is set on INSERT only (COALESCE
    # keeps the original value forever). Every-touch: written to events.event_data
    # as JSON so repeat opens via different tagged links are analyzable later.
    start_param = parsed.get("start_param")
    upsert_user(
        tg_id=tg_id,
        username=u.get("username"),
        first_name=u.get("first_name"),
        last_name=u.get("last_name"),
        language=u.get("language_code") or "en",
        is_premium=bool(u.get("is_premium", False)),
        photo_url=u.get("photo_url"),
        increment_opens=True,
        first_start_param=start_param,
    )
    event_data = json.dumps({"start_param": start_param}) if start_param else None
    store_event(tg_id, "app_open", event_data)
    return {
        "ok": True,
        "user": {
            "tg_id": tg_id,
            "first_name": u.get("first_name"),
            "username": u.get("username"),
            "photo_url": u.get("photo_url"),
            "is_premium": bool(u.get("is_premium", False)),
        },
    }


@app.post("/api/events")
async def track_event(req: EventRequest):
    """Track analytics events from the Mini App."""
    if req.tg_id:
        store_event(req.tg_id, req.event_type, req.event_data)
    if req.event_type == "session_end" and req.tg_id:
        store_session(
            req.tg_id,
            duration_seconds=req.session_duration or 0,
            scroll_depth=req.scroll_depth or 0,
            pairs_viewed=req.event_data or "",
        )
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════
#  TRADINGVIEW WEBHOOK
# ═══════════════════════════════════════════════════════════════

@app.post("/api/webhook/tradingview")
async def tradingview_webhook(request: Request):
    """
    Receive TradingView server-side alerts.
    Expected JSON body:
    {
        "secret": "...",
        "type": "Market Manipulation",
        "pair": "BTCUSDT",
        "timeframe": "5m",
        "direction": "bull",
        "price": 85000.00,
        "message": "..."
    }
    """
    # B1: TradingView's default content-type is text/plain, so request.json()
    # raises. Read raw body and json.loads manually instead.
    try:
        raw = await request.body()
        body = json.loads(raw)
    except Exception:
        raise HTTPException(400, "Invalid JSON")

    # B3: refuse all webhook requests if server secret is still the default.
    if webhook_secret_is_default():
        raise HTTPException(503, "Webhook disabled — server WEBHOOK_SECRET not configured")

    # Verify secret
    if body.get("secret") != WEBHOOK_SECRET:
        raise HTTPException(403, "Invalid secret")

    signal_type = body.get("type", "Unknown")
    pair = body.get("pair", "BTCUSDT")
    timeframe = body.get("timeframe")
    direction = body.get("direction")
    price = body.get("price")
    message = body.get("message", "")

    # Store signal
    store_signal(
        signal_type=signal_type,
        pair=pair,
        timeframe=timeframe,
        direction=direction,
        price=price,
        data=json.dumps(body),
    )

    logger.info(f"Webhook signal: {signal_type} {pair} {direction} @ {price}")

    # B2: fire-and-forget so webhook returns 200 immediately. Broadcasting
    # to all users serially (await) was hanging the response 10+ seconds at
    # scale, causing TradingView to retry and store duplicate signals.
    if bot_app:
        asyncio.create_task(broadcast_alert(bot_app, {
            "type": signal_type,
            "pair": pair,
            "timeframe": timeframe,
            "direction": direction,
            "price": price,
        }))

    return {"status": "received", "type": signal_type, "pair": pair}


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
