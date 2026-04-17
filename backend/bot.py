"""
Telegram bot: /start handshake, owner-only admin commands, alert delivery,
Mini App menu button.
Uses python-telegram-bot v20+ (async).
"""
import csv
import io
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    MenuButtonWebApp, WebAppInfo, InputFile,
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes,
)

from config import (
    BOT_TOKEN, WEBAPP_URL, BRAINER_URL, COMMUNITY_URL,
    OWNER_CHAT_IDS, is_owner,
)
from database import (
    upsert_user, store_event,
    get_recent_users, get_waitlist_rows, get_recent_events, get_stats_extended,
)

logger = logging.getLogger(__name__)


def create_bot_app() -> Optional[Application]:
    """Create and configure the bot application."""
    if not BOT_TOKEN:
        logger.warning("BOT_TOKEN not set — bot disabled")
        return None

    app = Application.builder().token(BOT_TOKEN).build()

    # Public handshake — everyone
    app.add_handler(CommandHandler("start", cmd_start))

    # Owner-only admin commands (silently ignored for non-owners)
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("waitlist", cmd_waitlist))
    app.add_handler(CommandHandler("recent", cmd_recent))
    app.add_handler(CommandHandler("export_users", cmd_export_users))
    app.add_handler(CommandHandler("export_waitlist", cmd_export_waitlist))
    app.add_handler(CommandHandler("stats", cmd_stats))

    # Pagination callback (for /users and /waitlist "Next/Prev" buttons)
    app.add_handler(CallbackQueryHandler(on_admin_callback, pattern=r"^admin:"))

    return app


async def setup_menu_button(app: Application):
    """Set the Mini App menu button on the bot."""
    try:
        await app.bot.set_chat_menu_button(
            menu_button=MenuButtonWebApp(
                text="Open App",
                web_app=WebAppInfo(url=WEBAPP_URL),
            )
        )
        logger.info("Menu button set to Mini App")
    except Exception as e:
        logger.error(f"Failed to set menu button: {e}")


def _open_app_keyboard():
    """Single-button keyboard for alert messages."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🌀 Open Brainer Mini", web_app=WebAppInfo(url=WEBAPP_URL))]
    ])


def _start_keyboard():
    """Single-button keyboard — just Open App."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🌀 Open App", web_app=WebAppInfo(url=WEBAPP_URL))],
    ])


# ═══════════════════════════════════════════════════════════════
#  COMMAND HANDLERS
# ═══════════════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Public handshake. Captures Telegram deep-link start_param for attribution,
    then shows the 4-button launcher. This fires the moment a user taps Start,
    before they've opened the Mini App — so tagged-link attribution is captured
    even for users who never tap Open App.
    """
    user = update.effective_user
    if not user:
        return

    # Extract start_param if user arrived via t.me/TomyLoBot?start=<tag>
    start_param = context.args[0] if context.args else None

    # Upsert user with first-touch attribution (COALESCE preserves prior value)
    upsert_user(
        tg_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
        language=user.language_code or "en",
        is_premium=getattr(user, "is_premium", False) or False,
        first_start_param=start_param,
    )

    # Log the start event (with source tag when present)
    event_data = json.dumps({"start_param": start_param}) if start_param else None
    store_event(user.id, "bot_start", event_data)

    text = (
        "⚡ *Brainer Mini*\n\n"
        "Live institutional-grade analysis on every USDT pair.\n\n"
        "Tap below to open the app."
    )
    await update.message.reply_text(
        text, parse_mode="Markdown", reply_markup=_start_keyboard()
    )


# ═══════════════════════════════════════════════════════════════
#  OWNER ADMIN COMMANDS
# ═══════════════════════════════════════════════════════════════

PAGE_SIZE_USERS = 20
PAGE_SIZE_WAITLIST = 20


def _check_owner(update: Update) -> bool:
    """Return True if the command sender is an owner. Silently drop otherwise."""
    user = update.effective_user
    if not user:
        return False
    return is_owner(user.id)


def _fmt_ts_relative(ts: Optional[float]) -> str:
    """Format a UNIX timestamp as a short relative string: '3m ago', '2h ago', '5d ago'."""
    if not ts:
        return "—"
    delta = time.time() - ts
    if delta < 60:
        return f"{int(delta)}s ago"
    if delta < 3600:
        return f"{int(delta / 60)}m ago"
    if delta < 86400:
        return f"{int(delta / 3600)}h ago"
    return f"{int(delta / 86400)}d ago"


def _iso(ts: Optional[float]) -> str:
    """UNIX timestamp → ISO 8601 string (UTC). Empty for None."""
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds")


def _fmt_user_row(u: dict) -> str:
    """One line per user for /users output."""
    name = u.get("first_name") or "—"
    uname = f"@{u['username']}" if u.get("username") else "—"
    opens = u.get("open_count") or 0
    src = u.get("first_start_param")
    src_str = f" · src:{src}" if src else ""
    last = _fmt_ts_relative(u.get("last_seen"))
    return f"• {name} ({uname}) · id `{u['tg_id']}` · {opens} opens{src_str} · {last}"


def _fmt_waitlist_row(w: dict) -> str:
    """One line per waitlist entry for /waitlist output."""
    uname = f"@{w['tg_username']}" if w.get("tg_username") else "—"
    when = _fmt_ts_relative(w.get("signed_up_at"))
    return f"• `{w['email']}` · {uname} · {when}"


def _page_keyboard(scope: str, page: int, total: int, page_size: int) -> Optional[InlineKeyboardMarkup]:
    """Build a Prev/Next inline keyboard for paginated admin listings.
    scope: 'users' or 'waitlist'. Returns None if only one page.
    """
    max_page = max(0, (total - 1) // page_size)
    if max_page == 0:
        return None
    row = []
    if page > 0:
        row.append(InlineKeyboardButton("◀ Prev", callback_data=f"admin:{scope}:page:{page - 1}"))
    row.append(InlineKeyboardButton(f"{page + 1}/{max_page + 1}", callback_data="admin:noop"))
    if page < max_page:
        row.append(InlineKeyboardButton("Next ▶", callback_data=f"admin:{scope}:page:{page + 1}"))
    return InlineKeyboardMarkup([row])


async def _render_users_page(message, page: int, edit: bool = False):
    total = get_stats_extended()["users_total"]
    rows = get_recent_users(limit=PAGE_SIZE_USERS, offset=page * PAGE_SIZE_USERS)
    if not rows:
        text = "👥 *Users* — no users yet."
    else:
        header = f"👥 *Users* — {total} total (page {page + 1})\n\n"
        body = "\n".join(_fmt_user_row(u) for u in rows)
        text = header + body
    kb = _page_keyboard("users", page, total, PAGE_SIZE_USERS)
    if edit:
        await message.edit_text(text, parse_mode="Markdown", reply_markup=kb)
    else:
        await message.reply_text(text, parse_mode="Markdown", reply_markup=kb)


async def _render_waitlist_page(message, page: int, edit: bool = False):
    total = get_stats_extended()["waitlist_total"]
    rows = get_waitlist_rows(limit=PAGE_SIZE_WAITLIST, offset=page * PAGE_SIZE_WAITLIST)
    if not rows:
        text = "📝 *Waitlist* — empty."
    else:
        header = f"📝 *Waitlist* — {total} total (page {page + 1})\n\n"
        body = "\n".join(_fmt_waitlist_row(w) for w in rows)
        text = header + body
    kb = _page_keyboard("waitlist", page, total, PAGE_SIZE_WAITLIST)
    if edit:
        await message.edit_text(text, parse_mode="Markdown", reply_markup=kb)
    else:
        await message.reply_text(text, parse_mode="Markdown", reply_markup=kb)


async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _check_owner(update):
        return
    await _render_users_page(update.message, page=0)


async def cmd_waitlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _check_owner(update):
        return
    await _render_waitlist_page(update.message, page=0)


async def cmd_recent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _check_owner(update):
        return
    rows = get_recent_events(limit=30)
    if not rows:
        await update.message.reply_text("📋 *Recent events* — none.", parse_mode="Markdown")
        return
    lines = ["📋 *Recent events* (last 30)\n"]
    for r in rows:
        when = _fmt_ts_relative(r.get("timestamp"))
        who = r.get("first_name") or (f"@{r['username']}" if r.get("username") else f"id:{r['tg_id']}")
        data = r.get("event_data") or ""
        data_excerpt = f" · {data[:40]}" if data else ""
        lines.append(f"• {when} · `{r['event_type']}` · {who}{data_excerpt}")
    text = "\n".join(lines)
    # Telegram caps messages at 4096 chars — truncate defensively
    if len(text) > 4000:
        text = text[:3990] + "\n… (truncated)"
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _check_owner(update):
        return
    s = get_stats_extended()
    lines = [
        "📈 *Brainer Mini Stats*",
        "",
        f"👥 Users: *{s['users_total']}* total · {s['users_24h']} active 24h · {s['users_7d']} active 7d",
        f"📝 Waitlist: *{s['waitlist_total']}* total · {s['waitlist_24h']} in 24h",
        f"🎯 Signals: {s['signals_total']}",
        f"📊 Sessions: {s['sessions_total']}",
    ]
    top = s.get("top_sources") or []
    if top:
        lines.append("")
        lines.append("*Top attribution sources:*")
        for row in top[:5]:
            lines.append(f"  • `{row['source']}` — {row['count']}")
    else:
        lines.append("")
        lines.append("_No tagged-link arrivals yet._")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_export_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _check_owner(update):
        return
    rows = get_recent_users(limit=100_000, offset=0)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "tg_id", "username", "first_name", "last_name", "language",
        "is_premium", "open_count", "first_start_param",
        "first_seen_iso", "last_seen_iso",
    ])
    for r in rows:
        writer.writerow([
            r.get("tg_id"), r.get("username") or "", r.get("first_name") or "",
            r.get("last_name") or "", r.get("language") or "",
            r.get("is_premium") or 0, r.get("open_count") or 0,
            r.get("first_start_param") or "",
            _iso(r.get("first_seen")), _iso(r.get("last_seen")),
        ])
    data = buf.getvalue().encode("utf-8")
    fname = f"brainer_users_{int(time.time())}.csv"
    await update.message.reply_document(
        document=InputFile(io.BytesIO(data), filename=fname),
        caption=f"Users CSV — {len(rows)} rows",
    )


async def cmd_export_waitlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _check_owner(update):
        return
    rows = get_waitlist_rows(limit=100_000, offset=0)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id", "tg_id", "email", "tg_username", "signed_up_iso"])
    for r in rows:
        writer.writerow([
            r.get("id"), r.get("tg_id") or "", r.get("email") or "",
            r.get("tg_username") or "", _iso(r.get("signed_up_at")),
        ])
    data = buf.getvalue().encode("utf-8")
    fname = f"brainer_waitlist_{int(time.time())}.csv"
    await update.message.reply_document(
        document=InputFile(io.BytesIO(data), filename=fname),
        caption=f"Waitlist CSV — {len(rows)} rows",
    )


async def on_admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Prev/Next button taps for paginated admin listings."""
    query = update.callback_query
    if not query:
        return
    # Owner check on callbacks too — defensive against message forwarding
    if not is_owner(query.from_user.id):
        await query.answer()
        return
    await query.answer()
    parts = (query.data or "").split(":")
    # Expected: admin:<scope>:page:<n>
    if len(parts) != 4 or parts[0] != "admin" or parts[2] != "page":
        return
    scope = parts[1]
    try:
        page = int(parts[3])
    except ValueError:
        return
    if scope == "users":
        await _render_users_page(query.message, page=page, edit=True)
    elif scope == "waitlist":
        await _render_waitlist_page(query.message, page=page, edit=True)


# ═══════════════════════════════════════════════════════════════
#  ALERT SENDING (called by webhook handler)
# ═══════════════════════════════════════════════════════════════

async def send_alert(bot, chat_id: int, signal: dict):
    """Send a signal alert to a specific chat."""
    direction = "🟢 BULLISH" if signal.get("direction") == "bull" else "🔴 BEARISH"
    text = (
        f"⚡ *Signal Alert*\n\n"
        f"*{signal.get('type', 'Unknown')}*\n"
        f"Pair: {signal.get('pair', '?')}\n"
        f"Direction: {direction}\n"
        f"Timeframe: {signal.get('timeframe', '?')}\n"
    )
    if signal.get("price"):
        text += f"Price: ${signal['price']:,.2f}\n"

    try:
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="Markdown",
            reply_markup=_open_app_keyboard(),
        )
    except Exception as e:
        logger.error(f"Failed to send alert to {chat_id}: {e}")


async def broadcast_alert(app: Application, signal: dict):
    """Broadcast a signal alert to all users (or specific subscribers)."""
    from database import get_db
    conn = get_db()
    rows = conn.execute("SELECT tg_id FROM users").fetchall()
    conn.close()

    for row in rows:
        await send_alert(app.bot, row["tg_id"], signal)
