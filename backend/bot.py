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
    BotCommand, BotCommandScopeDefault, BotCommandScopeChat,
    InputMediaDocument,
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes,
)

from config import (
    BOT_TOKEN, WEBAPP_URL, BRAINER_URL, COMMUNITY_URL,
    OWNER_CHAT_IDS, is_owner, is_root_admin,
)
from database import (
    upsert_user, store_event,
    get_recent_users, get_waitlist_rows, get_recent_events, get_stats_extended,
    add_admin, remove_admin, list_db_admins, list_db_admin_ids,
    resolve_username_to_tg_id,
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

    # Owner-only admin commands (silently ignored for non-owners).
    # Consolidated April 18 session 2: dropped /waitlist (now embedded in /stats);
    # merged /export_users + /export_waitlist into a single /export that sends
    # both CSVs as a media group.
    app.add_handler(CommandHandler("users", cmd_users))
    app.add_handler(CommandHandler("recent", cmd_recent))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("export", cmd_export))

    # Admin-management commands. /admins (list) is visible to all admins;
    # /addadmin and /removeadmin are root-only (OWNER_CHAT_ID env var) so
    # DB-added admins cannot promote others or evict root. See is_root_admin().
    app.add_handler(CommandHandler("admins", cmd_admins))
    app.add_handler(CommandHandler("addadmin", cmd_addadmin))
    app.add_handler(CommandHandler("removeadmin", cmd_removeadmin))

    # Pagination callback (for /users "Next/Prev" buttons)
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


async def setup_commands(app: Application, demoted_ids: Optional[list] = None):
    """Set the `/` command menu via Bot API `setMyCommands` with scoped lists.

    Public users see only /start. Admins (union of OWNER_CHAT_IDS env var AND
    DB-added admins via /addadmin) get the admin menu including /stats, /users,
    /recent, /export, /admins. Root admins additionally see /addadmin and
    /removeadmin in the list — these commands work for them via the gate check
    regardless, but showing them in the root menu makes discovery easier.

    `demoted_ids`: optional list of tg_ids whose scoped commands should be
    cleared (called by /removeadmin after DB delete). Without this, Telegram
    would keep showing them the admin menu until a manual /empty happens.

    Telegram's scope rule: BotCommandScopeChat(chat_id=<user>) overrides
    BotCommandScopeDefault for that specific user. Deleting a chat scope
    makes the user fall back to default (public) scope.
    """
    public = [
        BotCommand("start", "Open App"),
    ]
    admin = [
        BotCommand("start", "Open App"),
        BotCommand("stats", "Statistics"),
        BotCommand("users", "Recent Users"),
        BotCommand("recent", "Recent Events"),
        BotCommand("export", "Export Data"),
        BotCommand("admins", "List Admins"),
    ]
    # Root admins get the full menu + the admin-management commands.
    root_admin = admin + [
        BotCommand("addadmin", "Promote Admin"),
        BotCommand("removeadmin", "Remove Admin"),
    ]

    try:
        await app.bot.set_my_commands(public, scope=BotCommandScopeDefault())

        # Clear scoped commands for just-demoted users so their admin menu
        # collapses back to the public list on next /start.
        for did in (demoted_ids or []):
            try:
                await app.bot.delete_my_commands(scope=BotCommandScopeChat(chat_id=did))
            except Exception as e:
                logger.warning(f"Failed to clear scoped commands for demoted {did}: {e}")

        # Union of root (env) + DB admins — DB admins get `admin`, root gets `root_admin`.
        db_ids = set(list_db_admin_ids())
        root_ids = set(OWNER_CHAT_IDS)
        # Someone in both env and DB gets treated as root (superset wins).
        db_only = db_ids - root_ids

        for oid in root_ids:
            try:
                await app.bot.set_my_commands(root_admin, scope=BotCommandScopeChat(chat_id=oid))
            except Exception as e:
                logger.warning(f"Failed to set root admin commands for {oid}: {e}")

        for oid in db_only:
            try:
                await app.bot.set_my_commands(admin, scope=BotCommandScopeChat(chat_id=oid))
            except Exception as e:
                logger.warning(f"Failed to set admin commands for DB admin {oid}: {e}")

        logger.info(
            f"Command menus set: public (default), root ({len(root_ids)}), db admins ({len(db_only)})"
        )
    except Exception as e:
        logger.error(f"Failed to set command menus: {e}")


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


def _esc_md(s: Optional[str]) -> str:
    """Escape Telegram Markdown special chars in user-supplied text.
    Required whenever interpolating usernames, first_names, or arbitrary
    strings into a message sent with parse_mode='Markdown'. Without this,
    a single underscore or asterisk in a username (common) makes Telegram
    reject the whole message as BadRequest and the user sees nothing.
    """
    if not s:
        return ""
    for ch in ("_", "*", "[", "]", "`"):
        s = s.replace(ch, "\\" + ch)
    return s


def _fmt_user_row(u: dict) -> str:
    """One line per user for /users output."""
    name = _esc_md(u.get("first_name")) or "—"
    uname = f"@{_esc_md(u['username'])}" if u.get("username") else "—"
    opens = u.get("open_count") or 0
    src = u.get("first_start_param")
    src_str = f" · src:{_esc_md(src)}" if src else ""
    last = _fmt_ts_relative(u.get("last_seen"))
    return f"• {name} ({uname}) · id `{u['tg_id']}` · {opens} opens{src_str} · {last}"


def _fmt_waitlist_row(w: dict) -> str:
    """One line per waitlist entry for /waitlist output."""
    email = _esc_md(w.get("email")) or "—"
    uname = f"@{_esc_md(w['tg_username'])}" if w.get("tg_username") else "—"
    when = _fmt_ts_relative(w.get("signed_up_at"))
    return f"• `{email}` · {uname} · {when}"


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


async def cmd_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _check_owner(update):
        return
    await _render_users_page(update.message, page=0)


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
        who = _esc_md(r.get("first_name")) or (f"@{_esc_md(r['username'])}" if r.get("username") else f"id:{r['tg_id']}")
        data = r.get("event_data") or ""
        data_excerpt = f" · {_esc_md(data[:40])}" if data else ""
        lines.append(f"• {when} · `{r['event_type']}` · {who}{data_excerpt}")
    text = "\n".join(lines)
    # Telegram caps messages at 4096 chars — truncate defensively
    if len(text) > 4000:
        text = text[:3990] + "\n… (truncated)"
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Aggregate counts + top attribution sources + embedded waitlist table.
    Waitlist inline caps at 30 rows; beyond that, user runs /export for the
    full CSV. Telegram message cap is 4096 chars; with 30 rows + other
    sections we stay well under. Escape user-supplied strings with _esc_md.
    """
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
    # Embedded waitlist table — replaces the old /waitlist command.
    wl_rows = get_waitlist_rows(limit=30, offset=0)
    lines.append("")
    if not wl_rows:
        lines.append("_Waitlist empty._")
    else:
        cap = 30
        lines.append(f"📝 *Waitlist* (last {min(len(wl_rows), cap)})")
        for w in wl_rows[:cap]:
            lines.append(_fmt_waitlist_row(w))
        if s["waitlist_total"] > cap:
            remaining = s["waitlist_total"] - cap
            lines.append(f"_… and {remaining} more — use /export for full CSV._")
    text = "\n".join(lines)
    # Defensive truncation — unlikely to trigger given the 30-row cap.
    if len(text) > 4000:
        text = text[:3990] + "\n… (truncated)"
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send users.csv + waitlist.csv as a single Telegram media group message.
    Replaces the old /export_users + /export_waitlist pair. Two attachments
    appear bundled in one chat bubble; each opens independently into the
    user's spreadsheet app.
    """
    if not _check_owner(update):
        return

    # Users CSV
    users_rows = get_recent_users(limit=100_000, offset=0)
    users_buf = io.StringIO()
    uw = csv.writer(users_buf)
    uw.writerow([
        "tg_id", "username", "first_name", "last_name", "language",
        "is_premium", "open_count", "first_start_param",
        "first_seen_iso", "last_seen_iso",
    ])
    for r in users_rows:
        uw.writerow([
            r.get("tg_id"), r.get("username") or "", r.get("first_name") or "",
            r.get("last_name") or "", r.get("language") or "",
            r.get("is_premium") or 0, r.get("open_count") or 0,
            r.get("first_start_param") or "",
            _iso(r.get("first_seen")), _iso(r.get("last_seen")),
        ])
    users_bytes = users_buf.getvalue().encode("utf-8")

    # Waitlist CSV
    wl_rows = get_waitlist_rows(limit=100_000, offset=0)
    wl_buf = io.StringIO()
    ww = csv.writer(wl_buf)
    ww.writerow(["id", "tg_id", "email", "tg_username", "signed_up_iso"])
    for r in wl_rows:
        ww.writerow([
            r.get("id"), r.get("tg_id") or "", r.get("email") or "",
            r.get("tg_username") or "", _iso(r.get("signed_up_at")),
        ])
    wl_bytes = wl_buf.getvalue().encode("utf-8")

    ts = int(time.time())
    media = [
        InputMediaDocument(
            media=InputFile(io.BytesIO(users_bytes), filename=f"brainer_users_{ts}.csv"),
            caption=f"📦 Export · users: {len(users_rows)} rows · waitlist: {len(wl_rows)} rows",
        ),
        InputMediaDocument(
            media=InputFile(io.BytesIO(wl_bytes), filename=f"brainer_waitlist_{ts}.csv"),
        ),
    ]
    try:
        await update.message.reply_media_group(media=media)
    except Exception as e:
        # Fallback: if media group fails (rare), send two separate documents
        logger.warning(f"reply_media_group failed, falling back to two messages: {e}")
        await update.message.reply_document(
            document=InputFile(io.BytesIO(users_bytes), filename=f"brainer_users_{ts}.csv"),
            caption=f"Users CSV — {len(users_rows)} rows",
        )
        await update.message.reply_document(
            document=InputFile(io.BytesIO(wl_bytes), filename=f"brainer_waitlist_{ts}.csv"),
            caption=f"Waitlist CSV — {len(wl_rows)} rows",
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


# ═══════════════════════════════════════════════════════════════
#  ADMIN MANAGEMENT COMMANDS (root-only add/remove, all admins list)
# ═══════════════════════════════════════════════════════════════

def _check_root(update: Update) -> bool:
    """Return True if the command sender is a root admin (env var).
    Silently drops DB admins and everyone else — matches the existing
    non-owner silent-drop pattern for admin commands.
    """
    user = update.effective_user
    if not user:
        return False
    return is_root_admin(user.id)


def _parse_admin_target(raw: str) -> tuple:
    """Resolve an admin command argument (numeric tg_id OR @username) to a tg_id.
    Returns (tg_id, error_msg). On success, error_msg is None. On failure,
    tg_id is None and error_msg is the user-facing explanation.
    """
    raw = raw.strip()
    if not raw:
        return None, "Missing target. Usage: `/addadmin <tg_id_or_@username> [note]`"

    # Numeric path — positive integer only (negative = group/channel, not user)
    if raw.lstrip("-").isdigit():
        tg_id = int(raw)
        if tg_id <= 0:
            return None, f"Telegram user IDs are positive. Got: `{raw}`"
        return tg_id, None

    # Username path — strip @ and validate characters.
    candidate = raw.lstrip("@")
    if not candidate or not candidate.replace("_", "").isalnum():
        return None, f"Invalid input: `{raw}`. Provide a numeric tg_id or @username."

    resolved = resolve_username_to_tg_id(candidate)
    if resolved is None:
        return None, (
            f"I don't know `@{candidate}` yet. Have them tap /start with the bot "
            f"first, or use their numeric ID (get it via @userinfobot)."
        )
    return resolved, None


async def cmd_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all admins — root (env) + DB-added. Visible to any admin."""
    if not _check_owner(update):
        return

    lines = ["👥 *Admins*", ""]

    # Root admins (env var) — always listed first
    lines.append(f"*Root* (env var · {len(OWNER_CHAT_IDS)})")
    if not OWNER_CHAT_IDS:
        lines.append("_none configured_")
    else:
        # Enrich with display names from users table where available.
        from database import get_db
        conn = get_db()
        for rid in OWNER_CHAT_IDS:
            row = conn.execute(
                "SELECT username, first_name FROM users WHERE tg_id = ?", (rid,)
            ).fetchone()
            name = _esc_md(row["first_name"]) if row and row["first_name"] else "—"
            uname = f"@{_esc_md(row['username'])}" if row and row["username"] else "—"
            lines.append(f"• {name} ({uname}) · id `{rid}`")
        conn.close()

    # DB admins
    db_rows = list_db_admins()
    lines.append("")
    lines.append(f"*Added via bot* ({len(db_rows)})")
    if not db_rows:
        lines.append("_No bot-added admins yet._")
    else:
        for r in db_rows:
            name = _esc_md(r.get("first_name")) if r.get("first_name") else "—"
            uname = f"@{_esc_md(r['username'])}" if r.get("username") else "—"
            note = f" · _{_esc_md(r['note'])}_" if r.get("note") else ""
            when = _fmt_ts_relative(r.get("added_at"))
            added_by = r.get("added_by")
            lines.append(
                f"• {name} ({uname}) · id `{r['tg_id']}`{note} · added {when} by `{added_by}`"
            )

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_addadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Promote a user to admin (root-only). Usage: /addadmin <id_or_@username> [note]."""
    if not _check_root(update):
        return  # silent drop — non-root admins and everyone else

    if not context.args:
        await update.message.reply_text(
            "Usage: `/addadmin <tg_id_or_@username> [note]`\n"
            "Example: `/addadmin @alex beta tester`",
            parse_mode="Markdown",
        )
        return

    target_raw = context.args[0]
    note = " ".join(context.args[1:]) if len(context.args) > 1 else None

    target_id, err = _parse_admin_target(target_raw)
    if err:
        await update.message.reply_text(err, parse_mode="Markdown")
        return

    # Can't add a root admin — they already have full access.
    if is_root_admin(target_id):
        await update.message.reply_text(
            f"`{target_id}` is already a root admin (env var). No change.",
            parse_mode="Markdown",
        )
        return

    sender_id = update.effective_user.id
    added = add_admin(target_id, added_by=sender_id, note=note)
    if not added:
        await update.message.reply_text(
            f"`{target_id}` is already an admin. Note not updated.",
            parse_mode="Markdown",
        )
        return

    # Audit trail — surfaces in /recent
    store_event(
        sender_id,
        "admin_added",
        json.dumps({"target": target_id, "note": note}),
    )

    # Refresh scoped menus so the new admin sees the admin `/` menu.
    try:
        await setup_commands(context.application)
    except Exception as e:
        logger.warning(f"Failed to refresh commands after addadmin: {e}")

    # Enrich reply with display name if known
    display = f"`{target_id}`"
    from database import get_db
    conn = get_db()
    row = conn.execute(
        "SELECT username, first_name FROM users WHERE tg_id = ?", (target_id,)
    ).fetchone()
    conn.close()
    if row:
        parts = []
        if row["first_name"]:
            parts.append(_esc_md(row["first_name"]))
        if row["username"]:
            parts.append(f"@{_esc_md(row['username'])}")
        if parts:
            display = f"{' '.join(parts)} (`{target_id}`)"

    note_str = f" · note: _{_esc_md(note)}_" if note else ""
    await update.message.reply_text(
        f"✅ Added {display} as admin{note_str}. They'll see the admin menu on next /start.",
        parse_mode="Markdown",
    )


async def cmd_removeadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Demote a DB admin (root-only). Usage: /removeadmin <id_or_@username>."""
    if not _check_root(update):
        return

    if not context.args:
        await update.message.reply_text(
            "Usage: `/removeadmin <tg_id_or_@username>`",
            parse_mode="Markdown",
        )
        return

    target_id, err = _parse_admin_target(context.args[0])
    if err:
        await update.message.reply_text(err, parse_mode="Markdown")
        return

    # Root admins are managed via env var, not bot.
    if is_root_admin(target_id):
        await update.message.reply_text(
            f"`{target_id}` is a root admin — managed via `OWNER_CHAT_ID` env var on Railway. "
            f"Edit the env var to remove.",
            parse_mode="Markdown",
        )
        return

    removed = remove_admin(target_id)
    if not removed:
        await update.message.reply_text(
            f"`{target_id}` is not an admin. Nothing to remove.",
            parse_mode="Markdown",
        )
        return

    # Audit trail — surfaces in /recent
    sender_id = update.effective_user.id
    store_event(sender_id, "admin_removed", json.dumps({"target": target_id}))

    # Refresh + clear their scoped commands so the admin menu disappears for them.
    try:
        await setup_commands(context.application, demoted_ids=[target_id])
    except Exception as e:
        logger.warning(f"Failed to refresh commands after removeadmin: {e}")

    await update.message.reply_text(
        f"✅ Removed `{target_id}` from admins. Their admin menu will clear on next /start.",
        parse_mode="Markdown",
    )


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
