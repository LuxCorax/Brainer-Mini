"""
Tests for hybrid admin management (env + DB).
Covers:
  - DB layer: add/remove/is_db_admin/list, username resolution
  - config.py: is_root_admin vs is_owner union semantics
  - bot.py pure helpers: _parse_admin_target input parsing

Does NOT exercise live Telegram handlers (cmd_addadmin etc.) — those require
a running bot + Update object. The handlers' business logic is covered by
testing each piece it composes:
  - DB side effects (add_admin/remove_admin)   — covered below
  - Root gate (is_root_admin)                  — covered below
  - Input parsing (_parse_admin_target)        — covered below

bot.py tests skip cleanly if python-telegram-bot isn't installed (local dev
without requirements.txt). In CI / on Railway, they run.
"""
import os
import sys
import tempfile
import time

# Sandbox DB + env before any project imports
_TEST_DB = tempfile.mktemp(suffix="_brainer_test.db")
os.environ["DB_PATH"] = _TEST_DB
os.environ["OWNER_CHAT_ID"] = "100,200"   # two root admins for test fixture
os.environ["BOT_TOKEN"] = "dummy:dummy"

# Ensure we're importing from this directory (same pattern as other tests)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
import database as db

# Try to import bot — may fail without python-telegram-bot installed.
# When it fails, bot-tests mark themselves skipped via the sentinel.
try:
    import bot as _bot
    _BOT_AVAILABLE = True
except ImportError:
    _bot = None
    _BOT_AVAILABLE = False


class _Skip(Exception):
    """Raised by bot.py tests when the telegram library isn't available."""


def _reset_db():
    """Clean slate — delete file, re-init schema."""
    if os.path.exists(_TEST_DB):
        os.unlink(_TEST_DB)
    db.init_db()


def _require_bot():
    if not _BOT_AVAILABLE:
        raise _Skip("python-telegram-bot not installed")


# ═══════════════════════════════════════════════════════════════
#  DB layer
# ═══════════════════════════════════════════════════════════════

def test_admins_table_schema():
    _reset_db()
    conn = db.get_db()
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(admins)").fetchall()]
    conn.close()
    assert cols == ["tg_id", "added_by", "added_at", "note"], f"bad schema: {cols}"


def test_is_db_admin_empty():
    _reset_db()
    assert db.is_db_admin(999) is False


def test_add_admin_new_returns_true():
    _reset_db()
    assert db.add_admin(999, added_by=100) is True
    assert db.is_db_admin(999) is True


def test_add_admin_duplicate_returns_false():
    _reset_db()
    db.add_admin(999, added_by=100)
    assert db.add_admin(999, added_by=100) is False


def test_add_admin_with_note():
    _reset_db()
    db.add_admin(999, added_by=100, note="beta tester")
    rows = db.list_db_admins()
    assert len(rows) == 1
    assert rows[0]["note"] == "beta tester"


def test_add_admin_without_note():
    _reset_db()
    db.add_admin(999, added_by=100)
    rows = db.list_db_admins()
    assert rows[0]["note"] is None


def test_remove_admin_existing_returns_true():
    _reset_db()
    db.add_admin(999, added_by=100)
    assert db.remove_admin(999) is True
    assert db.is_db_admin(999) is False


def test_remove_admin_missing_returns_false():
    _reset_db()
    assert db.remove_admin(999) is False


def test_list_db_admins_newest_first():
    _reset_db()
    db.add_admin(111, added_by=100)
    time.sleep(0.01)   # ensure distinct added_at timestamps
    db.add_admin(222, added_by=100)
    rows = db.list_db_admins()
    assert [r["tg_id"] for r in rows] == [222, 111]


def test_list_db_admins_joins_user_display():
    _reset_db()
    db.upsert_user(tg_id=555, username="alex", first_name="Alex")
    db.add_admin(555, added_by=100, note="friend")
    rows = db.list_db_admins()
    assert rows[0]["first_name"] == "Alex"
    assert rows[0]["username"] == "alex"
    assert rows[0]["note"] == "friend"


def test_list_db_admins_missing_user_row():
    """Admin added before they ever interacted — users table has no row."""
    _reset_db()
    db.add_admin(888, added_by=100)
    rows = db.list_db_admins()
    assert rows[0]["first_name"] is None
    assert rows[0]["username"] is None
    assert rows[0]["tg_id"] == 888


def test_list_db_admin_ids():
    _reset_db()
    db.add_admin(111, added_by=100)
    db.add_admin(222, added_by=100)
    ids = db.list_db_admin_ids()
    assert set(ids) == {111, 222}


def test_resolve_username_known():
    _reset_db()
    db.upsert_user(tg_id=5555, username="Alex", first_name="Alex")
    assert db.resolve_username_to_tg_id("alex") == 5555
    assert db.resolve_username_to_tg_id("@alex") == 5555
    assert db.resolve_username_to_tg_id("@ALEX") == 5555


def test_resolve_username_unknown():
    _reset_db()
    assert db.resolve_username_to_tg_id("nobody") is None
    assert db.resolve_username_to_tg_id("@nobody") is None


def test_resolve_username_empty():
    _reset_db()
    assert db.resolve_username_to_tg_id("") is None
    assert db.resolve_username_to_tg_id("@") is None


# ═══════════════════════════════════════════════════════════════
#  config.py semantics
# ═══════════════════════════════════════════════════════════════

def test_is_root_admin_env():
    _reset_db()
    assert config.is_root_admin(100) is True
    assert config.is_root_admin(200) is True


def test_is_root_admin_non_env():
    """DB admins are NOT root — they can't add/remove other admins."""
    _reset_db()
    db.add_admin(999, added_by=100)
    assert config.is_root_admin(999) is False


def test_is_root_admin_unknown():
    _reset_db()
    assert config.is_root_admin(9999999) is False


def test_is_owner_env():
    _reset_db()
    assert config.is_owner(100) is True


def test_is_owner_db():
    _reset_db()
    db.add_admin(999, added_by=100)
    assert config.is_owner(999) is True


def test_is_owner_unknown():
    _reset_db()
    assert config.is_owner(12345) is False


def test_is_owner_union_both_paths():
    """Adding a DB admin does not affect env-var checks; both paths independent."""
    _reset_db()
    db.add_admin(999, added_by=100)
    assert config.is_owner(100) is True   # root via env
    assert config.is_owner(999) is True   # via DB
    assert config.is_owner(777) is False  # neither


def test_is_owner_after_remove():
    """Removal via remove_admin strips is_owner for DB admins."""
    _reset_db()
    db.add_admin(999, added_by=100)
    assert config.is_owner(999) is True
    db.remove_admin(999)
    assert config.is_owner(999) is False


def test_is_owner_root_unaffected_by_db():
    """Root admins stay root regardless of DB state."""
    _reset_db()
    assert config.is_owner(100) is True
    db.add_admin(100, added_by=200)   # try to double-add root to DB (won't happen in UI but defensive)
    assert config.is_owner(100) is True
    db.remove_admin(100)
    assert config.is_owner(100) is True   # still root via env


# ═══════════════════════════════════════════════════════════════
#  bot.py: _parse_admin_target
#  Skipped when python-telegram-bot isn't installed.
# ═══════════════════════════════════════════════════════════════

def test_parse_numeric_id():
    _require_bot()
    _reset_db()
    tid, err = _bot._parse_admin_target("123456789")
    assert tid == 123456789 and err is None


def test_parse_negative_rejected():
    _require_bot()
    tid, err = _bot._parse_admin_target("-100")
    assert tid is None and "positive" in err.lower()


def test_parse_zero_rejected():
    _require_bot()
    tid, err = _bot._parse_admin_target("0")
    assert tid is None and "positive" in err.lower()


def test_parse_empty_rejected():
    _require_bot()
    tid, err = _bot._parse_admin_target("")
    assert tid is None and "missing" in err.lower()


def test_parse_whitespace_rejected():
    _require_bot()
    tid, err = _bot._parse_admin_target("   ")
    assert tid is None and "missing" in err.lower()


def test_parse_username_unknown():
    _require_bot()
    _reset_db()
    tid, err = _bot._parse_admin_target("@nobody")
    assert tid is None and "/start" in err


def test_parse_username_known():
    _require_bot()
    _reset_db()
    db.upsert_user(tg_id=5555, username="Alex", first_name="Alex")
    tid, err = _bot._parse_admin_target("@alex")
    assert tid == 5555 and err is None


def test_parse_username_case_insensitive():
    _require_bot()
    _reset_db()
    db.upsert_user(tg_id=5555, username="Alex", first_name="Alex")
    tid, err = _bot._parse_admin_target("@ALEX")
    assert tid == 5555


def test_parse_bare_word_no_at():
    _require_bot()
    _reset_db()
    db.upsert_user(tg_id=5555, username="alex", first_name="Alex")
    tid, err = _bot._parse_admin_target("alex")
    assert tid == 5555


def test_parse_username_with_underscore():
    _require_bot()
    _reset_db()
    db.upsert_user(tg_id=7777, username="goran_che", first_name="Goran")
    tid, err = _bot._parse_admin_target("@goran_che")
    assert tid == 7777


def test_parse_garbage_rejected():
    _require_bot()
    tid, err = _bot._parse_admin_target("!@#$")
    assert tid is None and "invalid" in err.lower()


def test_setup_commands_signature():
    """setup_commands must accept optional demoted_ids param (backward-compat default)."""
    _require_bot()
    import inspect
    sig = inspect.signature(_bot.setup_commands)
    params = sig.parameters
    assert "demoted_ids" in params, f"missing demoted_ids: {list(params)}"
    assert params["demoted_ids"].default is None


def test_create_bot_app_registers_admin_commands():
    """Verify the three new command handlers are wired up."""
    _require_bot()
    app = _bot.create_bot_app()
    cmd_names = set()
    for h in app.handlers[0]:
        if hasattr(h, "commands"):
            cmd_names.update(h.commands)
    expected = {"admins", "addadmin", "removeadmin"}
    missing = expected - cmd_names
    assert not missing, f"missing handlers: {missing}"


# ═══════════════════════════════════════════════════════════════
#  Runner
# ═══════════════════════════════════════════════════════════════

def _run():
    tests = [(name, fn) for name, fn in globals().items()
             if name.startswith("test_") and callable(fn)]
    passed = 0
    failed = 0
    skipped = 0
    for name, fn in tests:
        try:
            fn()
            print(f"  ✓ {name}")
            passed += 1
        except _Skip as e:
            print(f"  ~ {name}  (skipped: {e})")
            skipped += 1
        except AssertionError as e:
            print(f"  ✗ {name}  —  {e}")
            failed += 1
        except Exception as e:
            print(f"  ✗ {name}  —  {type(e).__name__}: {e}")
            failed += 1
    print("\n" + "═" * 50)
    print(f"Results: {passed} passed, {failed} failed, {skipped} skipped")
    # Cleanup temp DB
    if os.path.exists(_TEST_DB):
        os.unlink(_TEST_DB)
    if failed == 0:
        print("ALL TESTS PASSED!" if skipped == 0 else "ALL RUN TESTS PASSED (some skipped)")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(_run())
