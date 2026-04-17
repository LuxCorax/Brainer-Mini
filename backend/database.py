"""
SQLite database for Brainer Mini App.
Tables: users, waitlist, signals, sessions, events.
"""
import sqlite3
import time
import logging
from typing import Optional, List, Dict
from config import DB_PATH

logger = logging.getLogger(__name__)


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """Create tables if they don't exist."""
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            tg_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            language TEXT DEFAULT 'en',
            is_premium INTEGER DEFAULT 0,
            photo_url TEXT,
            open_count INTEGER DEFAULT 0,
            first_start_param TEXT,
            first_seen REAL,
            last_seen REAL
        );

        CREATE TABLE IF NOT EXISTS waitlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER,
            email TEXT NOT NULL,
            tg_username TEXT,
            signed_up_at REAL,
            UNIQUE(email)
        );

        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_type TEXT NOT NULL,
            pair TEXT NOT NULL,
            timeframe TEXT,
            direction TEXT,
            price REAL,
            data TEXT,
            timestamp REAL
        );

        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER,
            started_at REAL,
            duration_seconds REAL,
            scroll_depth REAL,
            pairs_viewed TEXT
        );

        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER,
            event_type TEXT NOT NULL,
            event_data TEXT,
            timestamp REAL
        );

        CREATE INDEX IF NOT EXISTS idx_signals_pair ON signals(pair, timestamp);
        CREATE INDEX IF NOT EXISTS idx_signals_type ON signals(signal_type, timestamp);
        CREATE INDEX IF NOT EXISTS idx_events_tg ON events(tg_id, timestamp);
    """)
    _migrate_users_table(conn)
    conn.commit()
    conn.close()
    logger.info("Database initialized")


def _migrate_users_table(conn):
    """Add new columns to users table if missing.
    Idempotent — safe to run on every startup.
    """
    existing = {row["name"] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
    if "photo_url" not in existing:
        conn.execute("ALTER TABLE users ADD COLUMN photo_url TEXT")
    if "open_count" not in existing:
        conn.execute("ALTER TABLE users ADD COLUMN open_count INTEGER DEFAULT 0")
    if "first_start_param" not in existing:
        conn.execute("ALTER TABLE users ADD COLUMN first_start_param TEXT")


# ─── Users ───

def upsert_user(
    tg_id: int,
    username: str = None,
    first_name: str = None,
    last_name: str = None,
    language: str = "en",
    is_premium: bool = False,
    photo_url: str = None,
    increment_opens: bool = False,
    first_start_param: str = None,
):
    """UPSERT a user row.
    - photo_url: COALESCE-merged, new non-None value wins, None preserves existing.
    - first_start_param: FIRST-TOUCH ONLY. Set on INSERT; never overwritten on
      UPDATE. Ensures a user's original attribution source is preserved even if
      they later arrive via a different tagged deep link.
    - increment_opens: pass True only from /api/user/open. Adds 1 to open_count
      (on INSERT: initial row gets open_count=1; on UPDATE: +1 to existing).
      Bot command callers leave this False so /start etc. don't inflate opens.
    """
    now = time.time()
    inc = 1 if increment_opens else 0
    conn = get_db()
    conn.execute("""
        INSERT INTO users (tg_id, username, first_name, last_name, language, is_premium,
                           photo_url, open_count, first_start_param, first_seen, last_seen)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(tg_id) DO UPDATE SET
            username = COALESCE(excluded.username, username),
            first_name = COALESCE(excluded.first_name, first_name),
            last_name = COALESCE(excluded.last_name, last_name),
            language = excluded.language,
            is_premium = excluded.is_premium,
            photo_url = COALESCE(excluded.photo_url, photo_url),
            first_start_param = COALESCE(users.first_start_param, excluded.first_start_param),
            open_count = users.open_count + ?,
            last_seen = excluded.last_seen
    """, (tg_id, username, first_name, last_name, language, int(is_premium),
          photo_url, inc, first_start_param, now, now, inc))
    conn.commit()
    conn.close()


# ─── Waitlist ───

def add_to_waitlist(email: str, tg_username: str = None, tg_id: int = None) -> bool:
    """Add email to waitlist. Returns True if new, False if duplicate."""
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO waitlist (tg_id, email, tg_username, signed_up_at) VALUES (?, ?, ?, ?)",
            (tg_id, email.lower().strip(), tg_username, time.time()),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def get_waitlist_count() -> int:
    conn = get_db()
    row = conn.execute("SELECT COUNT(*) FROM waitlist").fetchone()
    conn.close()
    return row[0]


# ─── Signals ───

def store_signal(
    signal_type: str,
    pair: str,
    timeframe: str = None,
    direction: str = None,
    price: float = None,
    data: str = None,
):
    conn = get_db()
    conn.execute(
        "INSERT INTO signals (signal_type, pair, timeframe, direction, price, data, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (signal_type, pair.upper(), timeframe, direction, price, data, time.time()),
    )
    conn.commit()
    conn.close()


def get_recent_signals(pair: str = None, limit: int = 20) -> List[Dict]:
    conn = get_db()
    if pair:
        rows = conn.execute(
            "SELECT * FROM signals WHERE pair = ? ORDER BY timestamp DESC LIMIT ?",
            (pair.upper(), limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM signals ORDER BY timestamp DESC LIMIT ?", (limit,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─── Events / Sessions ───

def store_event(tg_id: int, event_type: str, event_data: str = None):
    conn = get_db()
    conn.execute(
        "INSERT INTO events (tg_id, event_type, event_data, timestamp) VALUES (?, ?, ?, ?)",
        (tg_id, event_type, event_data, time.time()),
    )
    conn.commit()
    conn.close()


def store_session(
    tg_id: int,
    duration_seconds: float = 0,
    scroll_depth: float = 0,
    pairs_viewed: str = "",
):
    conn = get_db()
    conn.execute(
        "INSERT INTO sessions (tg_id, started_at, duration_seconds, scroll_depth, pairs_viewed) VALUES (?, ?, ?, ?, ?)",
        (tg_id, time.time(), duration_seconds, scroll_depth, pairs_viewed),
    )
    conn.commit()
    conn.close()


# ─── Stats ───

def get_user_count() -> int:
    conn = get_db()
    row = conn.execute("SELECT COUNT(*) FROM users").fetchone()
    conn.close()
    return row[0]


def get_stats() -> Dict:
    conn = get_db()
    users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    waitlist = conn.execute("SELECT COUNT(*) FROM waitlist").fetchone()[0]
    signals = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    sessions = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
    conn.close()
    return {
        "users": users,
        "waitlist": waitlist,
        "signals": signals,
        "sessions": sessions,
    }

# ─── Admin reads (called by bot.py command handlers) ───

def get_recent_users(limit: int = 20, offset: int = 0) -> List[Dict]:
    """Return users ordered by most recent activity (last_seen DESC)."""
    conn = get_db()
    rows = conn.execute(
        """SELECT tg_id, username, first_name, last_name, language, is_premium,
                  open_count, first_seen, last_seen, first_start_param
             FROM users ORDER BY last_seen DESC LIMIT ? OFFSET ?""",
        (limit, offset),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_waitlist_rows(limit: int = 50, offset: int = 0) -> List[Dict]:
    """Return waitlist entries, newest first."""
    conn = get_db()
    rows = conn.execute(
        """SELECT id, tg_id, email, tg_username, signed_up_at
             FROM waitlist ORDER BY signed_up_at DESC LIMIT ? OFFSET ?""",
        (limit, offset),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_recent_events(limit: int = 30) -> List[Dict]:
    """Return recent events joined with user display names."""
    conn = get_db()
    rows = conn.execute(
        """SELECT e.event_type, e.event_data, e.timestamp, e.tg_id,
                  u.username, u.first_name
             FROM events e LEFT JOIN users u ON u.tg_id = e.tg_id
             ORDER BY e.timestamp DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_stats_extended() -> Dict:
    """Richer stats for the bot's /stats command."""
    conn = get_db()
    now = time.time()
    day_ago = now - 86400
    week_ago = now - 7 * 86400
    users_total = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    users_24h = conn.execute("SELECT COUNT(*) FROM users WHERE last_seen >= ?", (day_ago,)).fetchone()[0]
    users_7d = conn.execute("SELECT COUNT(*) FROM users WHERE last_seen >= ?", (week_ago,)).fetchone()[0]
    waitlist_total = conn.execute("SELECT COUNT(*) FROM waitlist").fetchone()[0]
    waitlist_24h = conn.execute("SELECT COUNT(*) FROM waitlist WHERE signed_up_at >= ?", (day_ago,)).fetchone()[0]
    signals_total = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    sessions_total = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
    top_sources = conn.execute(
        """SELECT first_start_param, COUNT(*) AS n
             FROM users
             WHERE first_start_param IS NOT NULL AND first_start_param != ''
             GROUP BY first_start_param
             ORDER BY n DESC LIMIT 10"""
    ).fetchall()
    conn.close()
    return {
        "users_total": users_total,
        "users_24h": users_24h,
        "users_7d": users_7d,
        "waitlist_total": waitlist_total,
        "waitlist_24h": waitlist_24h,
        "signals_total": signals_total,
        "sessions_total": sessions_total,
        "top_sources": [{"source": r["first_start_param"], "count": r["n"]} for r in top_sources],
    }
