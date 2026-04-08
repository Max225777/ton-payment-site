"""
services.py — БД, AI обробка, парсинг, платежі, утиліти
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone, date, timedelta
from typing import Optional, List, Dict, Any, Tuple

import aiosqlite
import httpx
from dotenv import load_dotenv

load_dotenv()

# ─── ENV ──────────────────────────────────────────────────────────────────────
DB_PATH              = os.getenv("DB_PATH", "bot.db")
LOG_LEVEL            = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE             = os.getenv("LOG_FILE", "bot.log")
LOG_MAX_MB           = int(os.getenv("LOG_MAX_MB", "10"))

AI_PROVIDER          = os.getenv("AI_PROVIDER", "groq")
GROQ_API_KEY         = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL           = os.getenv("GROQ_MODEL", "llama3-70b-8192")
OPENAI_API_KEY       = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL         = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
GEMINI_API_KEY       = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL         = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")

AI_TEMPERATURE       = float(os.getenv("AI_TEMPERATURE", "0.3"))
AI_MAX_TOKENS        = int(os.getenv("AI_MAX_TOKENS", "2048"))
AI_MAX_PARALLEL      = int(os.getenv("AI_MAX_PARALLEL", "3"))
AI_DAILY_LIMIT_TRIAL = int(os.getenv("AI_DAILY_LIMIT_TRIAL", "20"))

TELETHON_API_ID      = int(os.getenv("TELETHON_API_ID", "0"))
TELETHON_API_HASH    = os.getenv("TELETHON_API_HASH", "")
TELETHON_SESSION     = os.getenv("TELETHON_SESSION", "1.session")
TELETHON_PHONE       = os.getenv("TELETHON_PHONE", "")

# Session 2
TELETHON_API_ID_2    = int(os.getenv("TELETHON_API_ID_2", "0") or "0")
TELETHON_API_HASH_2  = os.getenv("TELETHON_API_HASH_2", "")
TELETHON_SESSION_2   = os.getenv("TELETHON_SESSION_2", "")

# Session 3
TELETHON_API_ID_3    = int(os.getenv("TELETHON_API_ID_3", "0") or "0")
TELETHON_API_HASH_3  = os.getenv("TELETHON_API_HASH_3", "")
TELETHON_SESSION_3   = os.getenv("TELETHON_SESSION_3", "")

CRYPTO_TESTNET            = os.getenv("CRYPTO_TESTNET", "true").lower() == "true"
CRYPTO_BOT_TOKEN_TEST     = os.getenv("CRYPTO_BOT_TOKEN_TEST", "")
CRYPTO_BOT_TOKEN_LIVE     = os.getenv("CRYPTO_BOT_TOKEN_LIVE", "")

BASE_PRICE           = float(os.getenv("BASE_PRICE", "10.0"))
DISCOUNT_2_CHANNELS  = float(os.getenv("DISCOUNT_2_CHANNELS", "0.1"))
DISCOUNT_3_CHANNELS  = float(os.getenv("DISCOUNT_3_CHANNELS", "0.2"))

TRIAL_DAYS           = int(os.getenv("TRIAL_DAYS", "7"))
TRIAL_DISABLED       = os.getenv("TRIAL_DISABLED", "false").lower() == "true"

REFERRAL_FIRST_PERCENT  = 0.50   # 50% — перша оплата реферала (3 місяці)
REFERRAL_REPEAT_PERCENT = 0.50   # 50% — повторні оплати (3 місяці)
MIN_WITHDRAWAL          = float(os.getenv("MIN_WITHDRAWAL", "5.0"))

PARSE_INTERVAL_MINUTES = int(os.getenv("PARSE_INTERVAL_MINUTES", "30"))
PARSE_POSTS_LIMIT      = int(os.getenv("PARSE_POSTS_LIMIT", "50"))
QUEUE_MAX              = int(os.getenv("QUEUE_MAX", "5"))    # Max pending posts per channel
DOWNLOAD_WORKERS       = int(os.getenv("DOWNLOAD_WORKERS", "4"))  # parallel media downloads
MAX_FILE_MB            = int(os.getenv("MAX_FILE_MB", "50"))   # skip files larger than this

GLOBAL_BLACKLIST     = os.getenv("GLOBAL_BLACKLIST", "")
AD_TEXT              = os.getenv("AD_TEXT", "")

ADMIN_IDS_RAW        = os.getenv("ADMIN_IDS", "")
ADMIN_IDS: List[int] = [int(x.strip()) for x in ADMIN_IDS_RAW.split(",") if x.strip().isdigit()]

BOT_TOKEN            = os.getenv("BOT_TOKEN", "")
BOT_USERNAME         = os.getenv("BOT_USERNAME", "")
BOT_LAUNCH_DATE      = "2026-03-20"
BOT_OWNER_USERNAME   = os.getenv("BOT_OWNER_USERNAME", "")
MANAGER_USERNAME     = os.getenv("MANAGER_USERNAME", "")
# REQUIRED_CHANNELS формат: "Назва|https://t.me/username,Назва2|https://t.me/username2"
def _parse_required_channels() -> list:
    raw = os.getenv("REQUIRED_CHANNELS", "SocialGiant NEWS|https://t.me/SocialGiantNEWS,SocialGiant|https://t.me/SocialGiant")
    result = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if "|" in entry:
            parts = entry.split("|", 1)
            result.append({"name": parts[0].strip(), "url": parts[1].strip()})
        else:
            # fallback: просто username без назви
            result.append({"name": entry.strip("@"), "url": f"https://t.me/{entry.strip('@')}"})
    return result

REQUIRED_CHANNELS: list = _parse_required_channels()

CRYPTO_TOKEN = CRYPTO_BOT_TOKEN_TEST if CRYPTO_TESTNET else CRYPTO_BOT_TOKEN_LIVE
CRYPTO_BASE  = "https://testnet-pay.crypt.bot" if CRYPTO_TESTNET else "https://pay.crypt.bot"

# ─── LOGGING ──────────────────────────────────────────────────────────────────
import logging.handlers

def setup_logging():
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=LOG_MAX_MB * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

log = logging.getLogger(__name__)

# ─── DB ───────────────────────────────────────────────────────────────────────

async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH, timeout=10)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")
    return db


async def _fetchall(db, sql: str, params=()) -> List[dict]:
    """Execute query and return list of dicts."""
    cursor = await db.execute(sql, params)
    rows = await cursor.fetchall()
    if not rows:
        return []
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in rows]


async def _fetchone(db, sql: str, params=()) -> Optional[dict]:
    """Execute query and return single dict or None."""
    cursor = await db.execute(sql, params)
    row = await cursor.fetchone()
    if row is None:
        return None
    cols = [d[0] for d in cursor.description]
    return dict(zip(cols, row))


async def reset_confirm_skipped(channel_id: int):
    """After publish, reset confirm_skipped posts back to pending for this channel."""
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(
            """UPDATE processed_posts SET status='pending'
               WHERE status='confirm_skipped'
               AND raw_post_id IN (SELECT id FROM raw_posts WHERE channel_id=?)""",
            (channel_id,))
        await db.commit()


async def reset_stale_awaiting_confirm():
    """On startup, reset any awaiting_confirm posts back to pending (stale from previous run)."""
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute("UPDATE processed_posts SET status='pending' WHERE status IN ('awaiting_confirm','confirm_skipped')")
        await db.commit()
    log.info("Reset stale awaiting_confirm posts → pending")


async def init_db():
    # Auto-create directory for DB if needed (e.g. /data/bot.db)
    import pathlib
    _db_dir = pathlib.Path(DB_PATH).parent
    if str(_db_dir) not in (".", ""):
        _db_dir.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(DB_PATH, timeout=30) as db:
        # WAL mode: allows concurrent readers + 1 writer → prevents "database is locked"
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.execute("PRAGMA busy_timeout=5000")
        # WAL mode prevents "database is locked" during concurrent reads
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=5000")
        await db.commit()
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE NOT NULL,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            language TEXT DEFAULT 'ru',
            notifications INTEGER DEFAULT 1,
            is_blocked INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            chat_id INTEGER,
            title TEXT,
            username TEXT,
            subscription_status TEXT DEFAULT 'pending',
            subscription_end TEXT,
            trial_used INTEGER DEFAULT 0,
            restricted_mode INTEGER DEFAULT 0,
            category TEXT DEFAULT 'general',
            settings TEXT DEFAULT '{}',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            title TEXT DEFAULT NULL,
            is_active INTEGER DEFAULT 1,
            promo_signature TEXT DEFAULT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(channel_id) REFERENCES channels(id)
        );

        CREATE TABLE IF NOT EXISTS source_patterns (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id  INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
            pattern    TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS raw_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            source_id INTEGER,
            tg_message_id INTEGER,
            grouped_id INTEGER,
            text TEXT,
            media_type TEXT,
            media_file_id TEXT,
            thumbnail_file_id TEXT,
            media_files_json TEXT,
            original_url TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(channel_id) REFERENCES channels(id),
            UNIQUE(channel_id, source_id, tg_message_id)
        );

        CREATE TABLE IF NOT EXISTS processed_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_post_id INTEGER NOT NULL,
            cleaned_text TEXT,
            mode TEXT DEFAULT 'sanitize',
            status TEXT DEFAULT 'pending',
            published_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(raw_post_id) REFERENCES raw_posts(id)
        );

        CREATE TABLE IF NOT EXISTS post_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER,
            source_id INTEGER,
            tg_message_id INTEGER,
            action TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS last_published (
            channel_id INTEGER PRIMARY KEY,
            message_id INTEGER,
            published_at TEXT
        );

        CREATE TABLE IF NOT EXISTS referrals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER,
            referred_id INTEGER,
            channel_id INTEGER,
            status TEXT DEFAULT 'pending',
            bonus_amount REAL DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            paid_at TEXT
        );

        CREATE TABLE IF NOT EXISTS referral_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER UNIQUE,
            code TEXT UNIQUE,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS seen_messages (
            channel_id    INTEGER NOT NULL,
            source_id     INTEGER NOT NULL,
            tg_message_id INTEGER NOT NULL,
            grouped_id    INTEGER,
            seen_at       TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (channel_id, source_id, tg_message_id)
        );
        CREATE INDEX IF NOT EXISTS idx_seen_channel_source
            ON seen_messages(channel_id, source_id);

        CREATE TABLE IF NOT EXISTS user_balances (
            user_id INTEGER PRIMARY KEY,
            balance REAL DEFAULT 0,
            total_earned REAL DEFAULT 0,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            channel_id INTEGER,
            invoice_id TEXT,
            amount REAL,
            currency TEXT DEFAULT 'USDT',
            type TEXT,
            description TEXT,
            referrer_id INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            channel_id INTEGER,
            invoice_id TEXT UNIQUE,
            amount REAL,
            status TEXT DEFAULT 'pending',
            pay_url TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            paid_at TEXT
        );

        CREATE TABLE IF NOT EXISTS ai_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            provider TEXT,
            model TEXT,
            tokens_used INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS withdrawal_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            amount REAL,
            wallet TEXT,
            status TEXT DEFAULT 'pending',
            admin_note TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event TEXT,
            level TEXT DEFAULT 'INFO',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # backward compat ALTER TABLE
        for sql in [
            "ALTER TABLE raw_posts ADD COLUMN media_files_json TEXT",
            "ALTER TABLE raw_posts ADD COLUMN grouped_id INTEGER",
            "ALTER TABLE referrals ADD COLUMN channel_id INTEGER",
            "ALTER TABLE invoices ADD COLUMN pay_url TEXT",
        "ALTER TABLE sources ADD COLUMN ai_mode TEXT DEFAULT ''",
        "ALTER TABLE sources ADD COLUMN filter_level TEXT DEFAULT ''",
        "ALTER TABLE sources ADD COLUMN promo_signature TEXT",
        "ALTER TABLE raw_posts ADD COLUMN thumbnail_file_id TEXT",
        "ALTER TABLE sources ADD COLUMN title TEXT",
        "ALTER TABLE transactions ADD COLUMN description TEXT",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_posts_unique ON raw_posts(channel_id, source_id, tg_message_id)",
        "ALTER TABLE users ADD COLUMN notifications INTEGER DEFAULT 1",
        "ALTER TABLE channels ADD COLUMN category TEXT DEFAULT 'general'",
        # Performance indexes for high volume (1000+ channels)
        "CREATE INDEX IF NOT EXISTS idx_pp_status ON processed_posts(status, raw_post_id)",
        "CREATE INDEX IF NOT EXISTS idx_rp_channel ON raw_posts(channel_id, source_id)",
        "CREATE INDEX IF NOT EXISTS idx_ch_sub ON channels(subscription_status)",
        "CREATE INDEX IF NOT EXISTS idx_src_channel ON sources(channel_id, is_active)",
        "CREATE INDEX IF NOT EXISTS idx_pp_published ON processed_posts(status, published_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_rp_grouped ON raw_posts(grouped_id)",
        "CREATE INDEX IF NOT EXISTS idx_ref_referrer ON referrals(referrer_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_ref_referred ON referrals(referred_id, status)",
        ]:
            try:
                await db.execute(sql)
            except Exception:
                pass

        # Backfill seen_messages from existing raw_posts so legacy data keeps dedup working
        try:
            await db.execute(
                "INSERT OR IGNORE INTO seen_messages(channel_id, source_id, tg_message_id, grouped_id) "
                "SELECT channel_id, source_id, tg_message_id, grouped_id FROM raw_posts "
                "WHERE channel_id IS NOT NULL AND source_id IS NOT NULL AND tg_message_id IS NOT NULL"
            )
        except Exception as _be:
            log.debug(f"seen_messages backfill: {_be}")

        await db.commit()
    log.info("DB initialized")


# ─── USER ─────────────────────────────────────────────────────────────────────

async def get_or_create_user(telegram_id: int, username: str = None,
                              first_name: str = None, last_name: str = None) -> dict:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        row = await _fetchone(db, "SELECT * FROM users WHERE telegram_id=?", (telegram_id,))
        if row:
            await db.execute(
                "UPDATE users SET username=?, first_name=?, last_name=? WHERE telegram_id=?",
                (username, first_name, last_name, telegram_id)
            )
            await db.commit()
            return dict(row)
        await db.execute(
            "INSERT INTO users(telegram_id, username, first_name, last_name, language) VALUES(?,?,?,?,?)",
            (telegram_id, username, first_name, last_name, "ru")
        )
        await db.commit()
        row = await _fetchone(db, "SELECT * FROM users WHERE telegram_id=?", (telegram_id,))
        return dict(row)


async def set_user_language(telegram_id: int, lang: str):
    """Set user interface language (ru/uk)."""
    if lang not in ("ru", "uk"):
        lang = "ru"
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute("UPDATE users SET language=? WHERE telegram_id=?", (lang, telegram_id))
        await db.commit()


async def set_user_notifications(telegram_id: int, enabled: bool):
    """Set user notification preference (balance top-up, referrals only)."""
    val = 1 if enabled else 0
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute("UPDATE users SET notifications=? WHERE telegram_id=?", (val, telegram_id))
        await db.commit()


async def get_user(telegram_id: int) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        return await _fetchone(db, "SELECT * FROM users WHERE telegram_id=?", (telegram_id,))


async def get_user_by_id(user_id: int) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        return await _fetchone(db, "SELECT * FROM users WHERE id=?", (user_id,))


async def set_user_blocked(telegram_id: int, blocked: bool):
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(
            "UPDATE users SET is_blocked=? WHERE telegram_id=?",
            (1 if blocked else 0, telegram_id)
        )
        await db.commit()


async def get_all_users(limit: int = 0) -> List[dict]:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        sql = "SELECT * FROM users ORDER BY created_at DESC"
        if limit > 0:
            sql += f" LIMIT {limit}"
        return await _fetchall(db, sql)


async def search_users(query: str) -> List[dict]:
    """Search users by telegram_id, username or first_name."""
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        if query.isdigit():
            rows = await _fetchall(db, "SELECT * FROM users WHERE telegram_id=?", (int(query),))
        else:
            q = query.lstrip("@").lower()
            rows = await _fetchall(db,
                "SELECT * FROM users WHERE LOWER(username) LIKE ? OR LOWER(first_name) LIKE ?",
                (f"%{q}%", f"%{q}%"))
        return rows


async def check_user_channel_subscriptions(bot, user_id: int) -> dict:
    """Check if user is subscribed to all REQUIRED_CHANNELS.
    Returns {"ok": True} if all subscribed, or {"ok": False, "channels": [...missing...]}.
    """
    if not REQUIRED_CHANNELS:
        return {"ok": True, "channels": []}
    missing = []
    for ch in REQUIRED_CHANNELS:
        url = ch.get("url", "")
        name = ch.get("name", "")
        # Extract username from URL (handle t.me/username/123 message links)
        username = ""
        if "t.me/" in url:
            parts = url.rstrip("/").split("t.me/", 1)[-1].split("/")
            # First part after t.me/ is the username, rest may be message id
            username = parts[0] if parts else ""
        if not username:
            continue
        try:
            member = await bot.get_chat_member(f"@{username}", user_id)
            if member.status in ("left", "kicked"):
                missing.append({"name": name, "url": url})
        except Exception:
            missing.append({"name": name, "url": url})
    if missing:
        return {"ok": False, "channels": missing}
    return {"ok": True, "channels": []}


# ─── CHANNELS ─────────────────────────────────────────────────────────────────

async def get_user_channels(telegram_id: int) -> List[dict]:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        user = await _fetchone(db, "SELECT id FROM users WHERE telegram_id=?", (telegram_id,))
        if not user:
            return []
        rows = await _fetchall(db, "SELECT * FROM channels WHERE user_id=? ORDER BY created_at DESC", (user["id"],))
    # Enrich with subscription_days
    from datetime import datetime as _dt
    result = []
    for r in rows:
        row = dict(r)
        end = row.get("subscription_end")
        if end and row.get("subscription_status") in ("active", "trial"):
            try:
                end_dt = _dt.fromisoformat(end)
                delta = end_dt - _dt.utcnow()
                total_seconds = max(0, int(delta.total_seconds()))
                days = total_seconds // 86400
                hours = (total_seconds % 86400) // 3600
                row["subscription_days"] = days
                row["subscription_hours"] = hours
                row["subscription_total_hours"] = total_seconds // 3600
            except Exception:
                row["subscription_days"] = 0
                row["subscription_hours"] = 0
                row["subscription_total_hours"] = 0
        else:
            row["subscription_days"] = 0
            row["subscription_hours"] = 0
            row["subscription_total_hours"] = 0
        result.append(row)
    return result


async def get_channel(channel_id: int) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        return await _fetchone(db, "SELECT * FROM channels WHERE id=?", (channel_id,))


async def create_channel(user_id: int, chat_id: int, title: str, username: str, category: str = "general") -> int:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        cur = await db.execute(
            "INSERT INTO channels(user_id, chat_id, title, username, category, settings) VALUES(?,?,?,?,?,?)",
            (user_id, chat_id, title, username, category or "general", "{}")
        )
        await db.commit()
        ch_id = cur.lastrowid
        # Count channels to decide if trial applies (query inside same connection)
        row = await _fetchone(db, "SELECT COUNT(*) as c FROM channels WHERE user_id=?", (user_id,))
        total = row["c"] if row else 1
    # Auto-activate trial for first channel
    if not TRIAL_DISABLED and total <= 1:
        await activate_trial(ch_id)
    return ch_id


async def update_channel_settings(channel_id: int, settings: dict):
    async with aiosqlite.connect(DB_PATH, timeout=30) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        # Separate channel-level fields from JSON settings
        channel_fields = {}
        settings_only  = {}
        for k, v in settings.items():
            if k in ("chat_id", "title", "username"):
                channel_fields[k] = v
            else:
                settings_only[k] = v
        if channel_fields:
            sets = ", ".join(f"{k}=?" for k in channel_fields)
            vals = list(channel_fields.values()) + [channel_id]
            await db.execute(f"UPDATE channels SET {sets} WHERE id=?", vals)
        if settings_only:
            # Merge with existing settings JSON
            row = await db.execute("SELECT settings FROM channels WHERE id=?", (channel_id,))
            row = await row.fetchone()
            existing = json.loads((row[0] if row else None) or "{}") if row else {}
            existing.update(settings_only)
            await db.execute("UPDATE channels SET settings=? WHERE id=?",
                             (json.dumps(existing, ensure_ascii=False), channel_id))
        await db.commit()


async def get_channel_settings(channel_id: int) -> dict:
    ch = await get_channel(channel_id)
    if not ch:
        return {}
    try:
        return json.loads(ch.get("settings") or "{}")
    except Exception:
        return {}


async def delete_channel(channel_id: int):
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute("DELETE FROM sources WHERE channel_id=?", (channel_id,))
        await db.execute(
            "UPDATE processed_posts SET status='skipped' WHERE raw_post_id IN "
            "(SELECT id FROM raw_posts WHERE channel_id=?)", (channel_id,)
        )
        await db.execute("DELETE FROM raw_posts WHERE channel_id=?", (channel_id,))
        await db.execute("DELETE FROM channels WHERE id=?", (channel_id,))
        await db.commit()


async def set_channel_status(channel_id: int, status: str, days: int | None = None):
    """Set channel subscription status. If days given, sets subscription_end accordingly.
    When status becomes blocked/restricted — autopost is automatically disabled in settings.
    """
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    if days and status in ("active", "trial"):
        sub_end = (now + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    else:
        sub_end = None

    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        if sub_end:
            await db.execute(
                "UPDATE channels SET subscription_status=?, subscription_end=? WHERE id=?",
                (status, sub_end, channel_id)
            )
        else:
            await db.execute(
                "UPDATE channels SET subscription_status=? WHERE id=?",
                (status, channel_id)
            )
        # If blocked/restricted — disable autopost in settings JSON
        if status in ("blocked", "restricted", "pending"):
            row = await _fetchone(db, "SELECT settings FROM channels WHERE id=?", (channel_id,))
            if row:
                try:
                    import json as _json
                    s = _json.loads(row.get("settings") or "{}")
                    s["autopost_enabled"] = False
                    await db.execute(
                        "UPDATE channels SET settings=? WHERE id=?",
                        (_json.dumps(s, ensure_ascii=False), channel_id)
                    )
                except Exception:
                    pass
        await db.commit()
    log.info(f"Channel {channel_id}: status → {status}" + (f" ({days}d)" if days else ""))


async def check_expired_subscriptions():
    """Auto-expire active/trial channels past their subscription_end date."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        rows = await _fetchall(db,
            """SELECT id, title, username, subscription_status, subscription_end, user_id
               FROM channels
               WHERE subscription_status IN ('active','trial')
               AND subscription_end IS NOT NULL AND subscription_end <= ?""",
            (now,))
        for ch in rows:
            await db.execute(
                "UPDATE channels SET subscription_status='restricted' WHERE id=?",
                (ch["id"],))
            log.info(f"Subscription expired: ch={ch['id']} ({ch.get('title','?')}) → restricted")
        if rows:
            await db.commit()
    return rows  # list of expired channels for notification



async def get_all_channels_with_owners():
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        return await _fetchall(
            db,
            "SELECT c.*, u.telegram_id as owner_tg_id, u.first_name as owner_name, u.username as owner_username "
            "FROM channels c LEFT JOIN users u ON u.id=c.user_id ORDER BY c.id DESC"
        )


async def get_all_channels() -> List[dict]:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        return await _fetchall(db, "SELECT * FROM channels ORDER BY created_at DESC")


# ─── SUBSCRIPTION ─────────────────────────────────────────────────────────────

async def activate_trial(channel_id: int) -> bool:
    ch = await get_channel(channel_id)
    if not ch or ch["trial_used"]:
        return False
    end = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(days=TRIAL_DAYS)).isoformat()
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(
            "UPDATE channels SET subscription_status='trial', subscription_end=?, trial_used=1 WHERE id=?",
            (end, channel_id)
        )
        await db.commit()
    return True


async def activate_subscription(channel_id: int, days: int):
    ch = await get_channel(channel_id)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    if ch and ch["subscription_status"] == "active" and ch.get("subscription_end"):
        try:
            current_end = datetime.fromisoformat(ch["subscription_end"])
            if current_end > now:
                new_end = (current_end + timedelta(days=days)).isoformat()
            else:
                new_end = (now + timedelta(days=days)).isoformat()
        except Exception:
            new_end = (now + timedelta(days=days)).isoformat()
    else:
        new_end = (now + timedelta(days=days)).isoformat()

    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(
            "UPDATE channels SET subscription_status='active', subscription_end=? WHERE id=?",
            (new_end, channel_id)
        )
        await db.commit()


def get_subscription_price(user_channel_count: int = 1, days: int = 30) -> float:
    """Фіксована ціна незалежно від кількості каналів."""
    return round(BASE_PRICE * (days / 30), 2)


def get_status_icon(status: str) -> str:
    return {
        "active":     "🟢",
        "trial":      "🟡",
        "restricted": "🔴",
        "expired":    "⚫",
        "pending":    "⏳",
    }.get(status, "⚫")


# ─── SOURCES ──────────────────────────────────────────────────────────────────

async def get_channel_sources(channel_id: int) -> List[dict]:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        return await _fetchall(db,
            "SELECT * FROM sources WHERE channel_id=? ORDER BY created_at ASC", (channel_id,))


def normalize_source_username(raw: str) -> str:
    """Strip @, https://t.me/ etc and return plain username."""
    raw = raw.strip()
    if "t.me/" in raw:
        raw = raw.split("t.me/")[-1]
    return raw.lstrip("@").rstrip("/")


async def get_source(source_id: int) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH, timeout=30) as db:
        return await _fetchone(db, "SELECT * FROM sources WHERE id=?", (source_id,))



async def add_source(channel_id: int, username_or_url: str) -> int:
    """Accept @username, t.me/username or https://t.me/username.
    Resolves real channel title via Telethon and stores it.
    """
    import re as _re
    raw = username_or_url.strip()
    # Extract username from URL
    url_match = _re.search(r't\.me/([A-Za-z0-9_]+)', raw)
    if url_match:
        username = url_match.group(1)
    else:
        username = raw.lstrip("@").split("/")[0].split("?")[0].strip()
    username = username.lower()

    # Try to resolve real title via Telethon
    title = None
    try:
        client, _ = await _get_telethon_client()
        if client:
            try:
                entity = await client.get_entity(f"@{username}")
                title = getattr(entity, "title", None) or getattr(entity, "username", None)
            except Exception:
                pass
            finally:
                try: await client.disconnect()
                except Exception: pass
    except Exception:
        pass

    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        row = await _fetchone(db, "SELECT id FROM sources WHERE channel_id=? AND username=?", (channel_id, username))
        if row:
            # Update title if we got one
            if title:
                await db.execute("UPDATE sources SET title=? WHERE id=?", (title, row["id"]))
                await db.commit()
            return row["id"]
        cur = await db.execute(
            "INSERT INTO sources(channel_id, username, title) VALUES(?,?,?)", (channel_id, username, title)
        )
        await db.commit()
        return cur.lastrowid


async def toggle_source(source_id: int):
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(
            "UPDATE sources SET is_active = CASE WHEN is_active=1 THEN 0 ELSE 1 END WHERE id=?",
            (source_id,)
        )
        await db.commit()


async def delete_source(source_id: int):
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute("DELETE FROM sources WHERE id=?", (source_id,))
        await db.commit()


# ─── POSTS ────────────────────────────────────────────────────────────────────

async def get_pending_posts(channel_id: int) -> List[dict]:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        rows = await _fetchall(db, """
            SELECT pp.id, pp.raw_post_id, pp.cleaned_text, pp.status, pp.created_at,
                   rp.media_type, rp.media_file_id, rp.thumbnail_file_id, rp.media_files_json,
                   rp.original_url, rp.created_at as raw_created_at,
                   s.username as source_username, s.title as source_title, rp.tg_message_id
            FROM processed_posts pp
            JOIN raw_posts rp ON rp.id = pp.raw_post_id
            LEFT JOIN sources s ON s.id = rp.source_id
            WHERE rp.channel_id=? AND pp.status IN ('pending','awaiting_confirm','confirm_skipped')
            ORDER BY
                ROW_NUMBER() OVER (PARTITION BY rp.source_id ORDER BY pp.created_at) ASC,
                rp.source_id ASC
        """, (channel_id,))
        # Filter: skip posts with no text AND no media (orphaned)
        result = []
        for r in rows:
            has_text = bool((r.get("cleaned_text") or "").strip())
            has_media = bool(r.get("media_file_id") or r.get("media_files_json"))
            if not has_text and not has_media:
                # Auto-skip orphaned posts silently
                await db.execute("UPDATE processed_posts SET status='skipped' WHERE id=?", (r["id"],))
                continue
            result.append(r)
        await db.commit()
        return result


async def load_media_for_post(post: dict) -> dict:
    """Lazy-load media file_id from raw_posts if not cached."""
    if not post.get("media_type") or post.get("media_file_id"):
        return post
    post = dict(post)
    raw_id = post.get("raw_post_id")
    if raw_id:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            row = await _fetchone(db,
                "SELECT media_file_id, media_files_json FROM raw_posts WHERE id=?",
                (raw_id,))
            if row:
                post["media_file_id"]    = row["media_file_id"]
                post["media_files_json"] = row["media_files_json"]
    return post


async def count_pending_posts(channel_id: int) -> int:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        row = await _fetchone(db, "SELECT COUNT(*) as cnt FROM processed_posts pp "
            "JOIN raw_posts rp ON rp.id=pp.raw_post_id "
            "WHERE rp.channel_id=? AND pp.status IN ('pending','awaiting_confirm','confirm_skipped') "
            "AND (COALESCE(pp.cleaned_text,'')!='' OR COALESCE(rp.media_file_id,'')!='' OR COALESCE(rp.media_files_json,'')!='')", (channel_id,))
        return row["cnt"] if row else 0


async def count_published_posts(channel_id: int) -> int:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        row = await _fetchone(db, "SELECT COUNT(*) as cnt FROM processed_posts pp "
            "JOIN raw_posts rp ON rp.id=pp.raw_post_id "
            "WHERE rp.channel_id=? AND pp.status='published'", (channel_id,))
        return row["cnt"] if row else 0


async def update_post_status(post_id: int, status: str, only_if_pending: bool = False) -> bool:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        pub = datetime.now(timezone.utc).replace(tzinfo=None).isoformat() if status == "published" else None
        if only_if_pending:
            cur = await db.execute(
                "UPDATE processed_posts SET status=?, published_at=? WHERE id=? AND status IN ('pending','awaiting_confirm','confirm_skipped')",
                (status, pub, post_id)
            )
        else:
            cur = await db.execute(
                "UPDATE processed_posts SET status=?, published_at=? WHERE id=?",
                (status, pub, post_id)
            )
        await db.commit()
        return cur.rowcount > 0


async def mark_confirm_sent(post_id: int):
    """Mark post as awaiting manual confirmation — prevents re-sending confirm spam."""
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(
            "UPDATE processed_posts SET status='awaiting_confirm' WHERE id=? AND status='pending'",
            (post_id,)
        )
        await db.commit()


async def update_post_text(post_id: int, text: str):
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(
            "UPDATE processed_posts SET cleaned_text=? WHERE id=?", (text, post_id)
        )
        await db.commit()


async def clear_post_queue(channel_id: int):
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(
            "UPDATE processed_posts SET status='skipped' "
            "WHERE status='pending' AND raw_post_id IN "
            "(SELECT id FROM raw_posts WHERE channel_id=?)",
            (channel_id,)
        )
        await db.commit()


async def save_last_published(channel_id: int, message_id: int):
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(
            "INSERT OR REPLACE INTO last_published(channel_id, message_id, published_at) VALUES(?,?,?)",
            (channel_id, message_id, datetime.now(timezone.utc).replace(tzinfo=None).isoformat())
        )
        await db.commit()


async def get_last_published(channel_id: int) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        row = await _fetchone(db, "SELECT * FROM last_published WHERE channel_id=?", (channel_id,))
        return row if row else None


async def post_already_parsed(channel_id: int, source_id: int, tg_message_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        row = await _fetchone(db, "SELECT 1 FROM seen_messages WHERE channel_id=? AND source_id=? AND tg_message_id=?", (channel_id, source_id, tg_message_id))
        if row:
            return True
        row = await _fetchone(db, "SELECT id FROM raw_posts WHERE channel_id=? AND source_id=? AND tg_message_id=?", (channel_id, source_id, tg_message_id))
        return row is not None


async def mark_messages_seen(channel_id: int, source_id: int, items: list) -> None:
    """Persistently record parsed tg_message_ids so they never re-appear in the queue.

    items: iterable of (tg_message_id, grouped_id_or_None)
    """
    if not items:
        return
    rows = [(channel_id, source_id, int(mid), (int(gid) if gid else None)) for mid, gid in items if mid]
    if not rows:
        return
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.executemany(
            "INSERT OR IGNORE INTO seen_messages(channel_id, source_id, tg_message_id, grouped_id) VALUES(?,?,?,?)",
            rows,
        )
        await db.commit()


async def load_seen_message_ids(channel_id: int, source_id: int) -> tuple:
    """Return (set of tg_message_ids, set of grouped_ids) already seen for this channel+source."""
    async with aiosqlite.connect(DB_PATH, timeout=30) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        cur = await db.execute(
            "SELECT tg_message_id, grouped_id FROM seen_messages WHERE channel_id=? AND source_id=?",
            (channel_id, source_id),
        )
        rows = await cur.fetchall()
        # Also include any ids still live in raw_posts (legacy rows from before seen_messages existed)
        cur2 = await db.execute(
            "SELECT tg_message_id, grouped_id FROM raw_posts WHERE channel_id=? AND source_id=?",
            (channel_id, source_id),
        )
        rows2 = await cur2.fetchall()
    ids: set = set()
    gids: set = set()
    for r in list(rows) + list(rows2):
        if r[0] is not None:
            ids.add(int(r[0]))
        if r[1] is not None:
            gids.add(int(r[1]))
    return ids, gids


async def save_raw_post(channel_id: int, source_id: int, tg_message_id: int,
                         text: str, media_type: str = None, media_file_id: str = None,
                         media_files_json: str = None, original_url: str = None,
                         thumbnail_file_id: str = "", grouped_id: int = None) -> int:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        cur = await db.execute(
            "INSERT OR IGNORE INTO raw_posts(channel_id, source_id, tg_message_id, grouped_id, text, media_type, "
            "media_file_id, thumbnail_file_id, media_files_json, original_url) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (channel_id, source_id, tg_message_id, grouped_id, text, media_type,
             media_file_id, thumbnail_file_id, media_files_json, original_url)
        )
        # Persistent dedup record (survives midnight queue reset)
        await db.execute(
            "INSERT OR IGNORE INTO seen_messages(channel_id, source_id, tg_message_id, grouped_id) VALUES(?,?,?,?)",
            (channel_id, source_id, tg_message_id, grouped_id)
        )
        await db.commit()
        if cur.lastrowid:
            return cur.lastrowid
        return 0  # duplicate ignored


async def save_processed_post(raw_post_id: int, cleaned_text: str, mode: str = "sanitize") -> int:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        # Check if already exists (initial sanitize pass) → update, not insert
        existing = await db.execute(
            "SELECT id FROM processed_posts WHERE raw_post_id=?", (raw_post_id,)
        )
        row = await existing.fetchone()
        if row:
            await db.execute(
                "UPDATE processed_posts SET cleaned_text=?, mode=? WHERE raw_post_id=?",
                (cleaned_text, mode, raw_post_id)
            )
            await db.commit()
            return row[0]
        cur = await db.execute(
            "INSERT INTO processed_posts(raw_post_id, cleaned_text, mode) VALUES(?,?,?)",
            (raw_post_id, cleaned_text, mode)
        )
        await db.commit()
        return cur.lastrowid


# ─── TEXT UTILITIES ───────────────────────────────────────────────────────────

_BLACKLIST_CACHE: List[str] = []

def get_blacklist_words() -> List[str]:
    global _BLACKLIST_CACHE
    if not _BLACKLIST_CACHE:
        raw = GLOBAL_BLACKLIST.strip()
        _BLACKLIST_CACHE = [w.strip().lower() for w in raw.split(",") if w.strip()] if raw else []
    return _BLACKLIST_CACHE




def contains_blacklisted(text: str, extra: List[str] = None) -> bool:
    words = get_blacklist_words()
    if extra:
        words = words + [w.lower() for w in extra]
    lower = text.lower()
    return any(w in lower for w in words)


def sanitize_html_for_telegram(text: str) -> str:
    """
    Prepare HTML for Telegram:
    - Convert markdown **bold** / *italic* to HTML tags
    - Keep <a href> tags INTACT (URLs cleaned separately by _clean_links)
    - Strip unsupported tags but keep their inner text
    - Do NOT remove URLs here — greedy regex destroys link text
    """
    if not text:
        return ""

    # Strip markdown links [text](url) → keep text, handle broken ]( patterns
    import re as _re
    # Full proper [text](url) → keep text
    text = _re.sub(r'\[([^\]\n]*?)\]\([^\)\n]*?\)', r'\1', text)
    # Broken ]( without closing ) — strip ]( and the URL word after it
    text = _re.sub(r'\]\(\S*', '', text)
    # Remaining lone ] and [
    text = _re.sub(r'\]', '', text)
    text = _re.sub(r'\[', '', text)

    # Convert markdown → HTML (before HTML parsing)
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text, flags=re.DOTALL)
    text = re.sub(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"<i>\1</i>", text, flags=re.DOTALL)
    text = re.sub(r"__(.+?)__", r"<u>\1</u>", text, flags=re.DOTALL)
    text = re.sub(r"(?<!_)_(?!_)(.+?)(?<!_)_(?!_)", r"<i>\1</i>", text, flags=re.DOTALL)
    text = re.sub(r"`(.+?)`", r"<code>\1</code>", text)

    ALLOWED = {"b", "i", "u", "s", "code", "pre", "tg-spoiler"}

    from html.parser import HTMLParser

    ALLOWED_WITH_A = ALLOWED | {"a"}

    class TGHTMLCleaner(HTMLParser):
        def __init__(self):
            super().__init__()
            self.result = []
            self.stack = []  # stack of open tags — enforces proper nesting

        def handle_starttag(self, tag, attrs):
            if tag in ALLOWED:
                self.result.append(f"<{tag}>")
                self.stack.append(tag)
            elif tag == "a":
                attr_dict = dict(attrs)
                href = attr_dict.get("href", "")
                if href:
                    self.result.append(f'<a href="{href}">')
                    self.stack.append("a")

        def handle_endtag(self, tag):
            if tag not in ALLOWED_WITH_A:
                return  # ignore unsupported closing tags completely
            if tag not in self.stack:
                return  # tag was never opened — ignore stray close tag
            # Close tags down to (and including) the matching open tag
            # This handles mismatched nesting by auto-closing inner tags
            while self.stack:
                top = self.stack.pop()
                self.result.append(f"</{top}>")
                if top == tag:
                    break

        def handle_data(self, data):
            self.result.append(data)

        def handle_entityref(self, name):
            self.result.append(f"&{name};")

        def handle_charref(self, name):
            self.result.append(f"&#{name};")

        def get_result(self):
            # Close any remaining open tags
            for tag in reversed(self.stack):
                self.result.append(f"</{tag}>")
            return "".join(self.result)

    try:
        cleaner = TGHTMLCleaner()
        cleaner.feed(text)
        text = cleaner.get_result()
    except Exception:
        text = re.sub(r"<[^>]+>", "", text)

    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _truncate_html(text: str, byte_limit: int) -> str:
    """Truncate HTML text to byte_limit, cutting at last sentence or word boundary."""
    import re as _r
    if len(text.encode("utf-8")) <= byte_limit:
        return text
    # Try to keep HTML tags intact by tracking open/close
    # Fallback: find a good cut point in raw text
    cut = byte_limit - 3
    truncated = text.encode("utf-8")[:cut].decode("utf-8", errors="ignore")
    # Cut at last sentence end (.!?) or newline
    for sep in ['. ', '! ', '? ', '.\n', '!\n', '?\n', '\n\n', '\n']:
        idx = truncated.rfind(sep)
        if idx > len(truncated) * 0.5:
            truncated = truncated[:idx + len(sep)].rstrip()
            break
    else:
        # Cut at last space to avoid broken words
        idx = truncated.rfind(' ')
        if idx > len(truncated) * 0.3:
            truncated = truncated[:idx]
    # Close any open HTML tags
    open_tags = _r.findall(r'<(b|i|u|s|code|pre|a)\b[^>]*>', truncated, _r.I)
    close_tags = _r.findall(r'</(b|i|u|s|code|pre|a)>', truncated, _r.I)
    for tag in reversed(open_tags[len(close_tags):]):
        truncated += f'</{tag}>'
    return truncated


def _safe_caption(text: str, limit: int = 1024) -> str:
    """Truncate caption to Telegram caption limit (1024 bytes).
    Preserves postbtn link at end. Cuts at sentence/word boundary."""
    import re as _r
    if not text:
        return ""
    if len(text.encode("utf-8")) <= limit:
        return text
    # Extract postbtn link from end
    link_match = _r.search(r'(\n\n<b><a href="[^"]{1,500}">[^<]{1,200}</a></b>)\s*$', text, _r.DOTALL)
    link_suffix = link_match.group(1) if link_match else ""
    body = text[:link_match.start()] if link_match else text
    suffix_bytes = len(link_suffix.encode("utf-8"))
    body_limit = limit - suffix_bytes - 1
    return _truncate_html(body, body_limit) + link_suffix


def _safe_text(text: str, limit: int = 4096) -> str:
    """Truncate message text to Telegram message limit (4096 bytes).
    Preserves postbtn link at end. Cuts at sentence/word boundary."""
    import re as _r
    if not text:
        return ""
    if len(text.encode("utf-8")) <= limit:
        return text
    # Extract postbtn link from end
    link_match = _r.search(r'(\n\n<b><a href="[^"]{1,500}">[^<]{1,200}</a></b>)\s*$', text, _r.DOTALL)
    link_suffix = link_match.group(1) if link_match else ""
    body = text[:link_match.start()] if link_match else text
    suffix_bytes = len(link_suffix.encode("utf-8"))
    body_limit = limit - suffix_bytes - 1
    return _truncate_html(body, body_limit) + link_suffix


def build_footer(settings: dict, has_media: bool = False) -> str:
    """Build footer text. If URL set — make it a clickable link."""
    if not settings.get("footer_enabled"):
        return ""
    ft   = settings.get("footer_text", "")
    url  = settings.get("footer_url", "")
    if not ft:
        return ""
    if url and url.startswith("http"):
        ft = f'<b><a href="{url}">{ft}</a></b>'
    else:
        ft = f"<b>{ft}</b>"
    return "\n\n" + ft


# ─── AI ───────────────────────────────────────────────────────────────────────

_ai_semaphore = asyncio.Semaphore(AI_MAX_PARALLEL)


async def log_ai_usage(user_id: int, tokens: int = 0):
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(
            "INSERT INTO ai_usage(user_id, provider, model, tokens_used) VALUES(?,?,?,?)",
            (user_id, AI_PROVIDER, GROQ_MODEL if AI_PROVIDER == "groq" else
             OPENAI_MODEL if AI_PROVIDER == "openai" else GEMINI_MODEL, tokens)
        )
        await db.commit()


# ─── PARSING (TELETHON) ───────────────────────────────────────────────────────

# Global bot instance (set by main.py via set_bot_instance)
_bot_instance = None


def set_bot_instance(bot):
    """Called from main.py to share the bot instance with services."""
    global _bot_instance
    _bot_instance = bot


# ─── MEDIA (DOWNLOAD VIA TELETHON + UPLOAD VIA BOT) ─────────────────────────

import tempfile as _tempfile
MEDIA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "media_cache")
os.makedirs(MEDIA_DIR, exist_ok=True)

async def _download_and_get_file_id(client, msg, media_type: str) -> Optional[str]:
    """Download media via Telethon → upload via Bot API → return file_id. No admin chat used."""
    local_path = None
    try:
        # Quick size check — skip files that are too large before downloading
        try:
            from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
            if isinstance(getattr(msg, "media", None), MessageMediaDocument):
                doc_size = getattr(msg.media.document, "size", 0) or 0
                if doc_size > MAX_FILE_MB * 1024 * 1024:
                    log.info(f"  skip msg {msg.id}: file too large ({doc_size//1024//1024}MB > {MAX_FILE_MB}MB)")
                    return None
        except Exception:
            pass
        # Download to local media_cache folder
        local_path = await client.download_media(msg, file=MEDIA_DIR)
        if not local_path or not os.path.exists(local_path):
            log.warning(f"  download failed for msg {msg.id}")
            return None
        log.info(f"  downloaded {os.path.basename(local_path)} ({os.path.getsize(local_path)//1024}KB)")

        if not _bot_instance or not ADMIN_IDS:
            log.warning("  upload skipped: no bot instance or ADMIN_IDS")
            return None

        from aiogram.types import FSInputFile
        admin_id = ADMIN_IDS[0]
        f = FSInputFile(local_path)
        sent = None
        thumb_fid = None

        # Generate thumbnail for video using ffmpeg
        if media_type in ("video", "animation") and local_path:
            try:
                import subprocess as _sp
                thumb_path = local_path + "_thumb.jpg"
                _sp.run([
                    "ffmpeg", "-y", "-i", local_path,
                    "-ss", "00:00:01", "-vframes", "1",
                    "-vf", "scale=320:-1",
                    thumb_path
                ], capture_output=True, timeout=30)
                if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 100:
                    tf = FSInputFile(thumb_path)
                    tsent = await _bot_instance.send_photo(admin_id, photo=tf)
                    thumb_fid = tsent.photo[-1].file_id
                    await _bot_instance.delete_message(admin_id, tsent.message_id)
                    os.remove(thumb_path)
                    log.info(f"  thumbnail generated and uploaded")
            except Exception as _te:
                log.debug(f"  thumbnail generation failed: {_te}")

        try:
            if media_type == "photo":
                sent = await _bot_instance.send_photo(admin_id, photo=f)
                fid  = sent.photo[-1].file_id
            elif media_type == "video":
                sent = await _bot_instance.send_video(admin_id, video=f)
                fid  = sent.video.file_id
            elif media_type == "animation":
                sent = await _bot_instance.send_animation(admin_id, animation=f)
                fid  = sent.animation.file_id
            else:
                sent = await _bot_instance.send_document(admin_id, document=f)
                fid  = sent.document.file_id
        except Exception as e:
            log.warning(f"  upload as {media_type} failed: {e}, retrying as document")
            f2   = FSInputFile(local_path)
            sent = await _bot_instance.send_document(admin_id, document=f2)
            fid  = sent.document.file_id

        # Delete the helper upload message silently
        if sent:
            try:
                await _bot_instance.delete_message(admin_id, sent.message_id)
            except Exception:
                pass
        # Return (file_id, thumbnail_file_id) tuple
        return (fid, thumb_fid) if thumb_fid else fid

    except Exception as e:
        log.warning(f"  _download_and_get_file_id error msg {getattr(msg,'id','?')}: {e}")
        return None
    finally:
        # Always clean up local file
        if local_path:
            try:
                os.remove(local_path)
            except Exception:
                pass


# ─── AI ─────────────────────────────────────────────────────────────────────

async def _call_ai(system_prompt: str, user_text: str) -> str:
    """Call configured AI provider (Groq / OpenAI / Gemini)."""
    provider = AI_PROVIDER.lower()
    if provider == "groq":
        import groq
        client = groq.AsyncGroq(api_key=GROQ_API_KEY)
        resp = await client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "system", "content": system_prompt},
                      {"role": "user",   "content": user_text}],
            temperature=AI_TEMPERATURE,
            max_tokens=AI_MAX_TOKENS,
        )
        return resp.choices[0].message.content.strip()
    elif provider == "openai":
        import openai
        client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY)
        resp = await client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "system", "content": system_prompt},
                      {"role": "user",   "content": user_text}],
            temperature=AI_TEMPERATURE,
            max_tokens=AI_MAX_TOKENS,
        )
        return resp.choices[0].message.content.strip()
    elif provider == "gemini":
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel(GEMINI_MODEL)
        resp = await asyncio.to_thread(
            model.generate_content, f"{system_prompt}\n\n{user_text}"
        )
        return resp.text.strip()
    else:
        return sanitize_html_for_telegram(user_text)


TRANSLATE_SUFFIX = {
    "uk": "Ukrainian", "ru": "Russian", "en": "English",
    "de": "German",    "pl": "Polish",  "es": "Spanish",
    "fr": "French",    "tr": "Turkish",
}



# ─── Classify prompt — works on pre-cleaned body (footer already stripped) ──
AD_CLASSIFY_PROMPT = (
    "You are a spam filter for Telegram news channels.\n"
    "Classify as AD ONLY if the post has NO real news content — its sole purpose is advertising.\n"
    "Classify as AD if:\n"
    "  - The post contains ZERO real information — only calls to subscribe/join/follow\n"
    "  - Pure product/course/giveaway advertisement with no news\n"
    "  - Sponsored post with no actual information value\n"
    "Classify as OK if:\n"
    "  - Post has ANY real news: war, politics, crime, accidents, sports, weather, economy\n"
    "  - Post has facts, names, dates, locations, numbers — even brief ones\n"
    "  - Post has a channel mention or link at the end BUT also has real content\n"
    "  - ANY post with actual information, even if short\n"
    "Default is OK — only mark AD if you are certain it is pure advertising with no news.\n"
    "Reply with EXACTLY one word: AD or OK"
)


# Patterns that are ALWAYS ads regardless of content
import re as _re_ad
_HARD_AD_PATTERNS = [
    # Only very explicit ad constructions with @username
    _re_ad.compile(r'подпиш\w+\s+(на|в)\s+@', _re_ad.I),
    _re_ad.compile(r'підпиш\w+\s+(на|в)\s+@', _re_ad.I),
    _re_ad.compile(r'(жми|тисни|переходи|заходи)\s+@', _re_ad.I),
    # paid labels
    _re_ad.compile(r'(партнерский пост|партнерська публікац)', _re_ad.I),
]

def _hard_ad_check(text: str) -> bool:
    """Returns True if text is obviously an ad (hard keyword match)."""
    for pat in _HARD_AD_PATTERNS:
        if pat.search(text):
            return True
    return False


def _strip_footer_lines(text: str) -> str:
    """Remove promo/link lines from the end of post.
    Removes:
    - Any line containing t.me/ link (including with surrounding text like "Читай в @channel")
    - Any line that is only @username
    - Any bare https:// URL line
    - Short lines (<=60 chars) after a link-line was already removed
    """
    import re as _r
    lines = text.strip().splitlines()
    removed_link = False
    while lines:
        s = _r.sub(r'<[^>]+>', '', lines[-1]).strip()
        raw_line = lines[-1]
        is_link_line = (
            not s
            or _r.search(r't\.me/', raw_line)        # contains t.me/ link (even in HTML)
            or _r.search(r't\.me/', s)               # plain text t.me/
            or _r.fullmatch(r'@[A-Za-z0-9_]{3,}', s) # only @username
            or _r.fullmatch(r'https?://\S+', s)      # only URL
        )
        # Also remove short line after a link line was removed (e.g. "Більше новин 👇")
        is_short_after_link = removed_link and len(s) <= 60
        if is_link_line or is_short_after_link:
            lines.pop()
            if is_link_line:
                removed_link = True
        else:
            break
    return "\n".join(lines).strip()




async def detect_and_save_signature_for_new_source(source_id: int, username: str) -> str:
    import re as _re
    def strip_html(t):
        return _re.sub(r'<[^>]+>', '', t or '').strip()
    client, _num = await _get_telethon_client()
    if not client:
        return ''
    try:
        entity = await client.get_entity('@' + username.lstrip('@'))
        # fetch only text — no media download
        messages = await client.get_messages(entity, limit=25)
        # strip media references so Telethon won't auto-download anything
        for _m in messages:
            try: _m.media = None
            except: pass
        await client.disconnect()
    except Exception as _e:
        log.warning(f'detect_signature @{username}: {_e}')
        try: await client.disconnect()
        except: pass
        return ''
    texts = []
    for _i, _msg in enumerate(messages):
        _raw = (getattr(_msg, 'message', '') or getattr(_msg, 'text', '') or '').strip()
        _plain = strip_html(_raw)
        if len(_plain) > 20:
            texts.append(f'[{_i+1}] {_plain}')
    if len(texts) < 4:
        return ''
    _posts = '\n\n'.join(texts[:25])
    # Statistical pre-check: find lines appearing in 50%+ of posts
    import re as _re2
    _line_counts: dict = {}
    for _t in texts:
        _lines = [l.strip() for l in _t.splitlines() if len(l.strip()) > 5]
        for _line in _lines[-3:]:  # only last 3 lines of each post
            _line_counts[_line] = _line_counts.get(_line, 0) + 1
    _threshold = max(3, int(len(texts) * 0.5))
    _stat_hint = [l for l, c in sorted(_line_counts.items(), key=lambda x:-x[1]) if c >= _threshold]
    _hint_str = ', '.join(repr(x[:60]) for x in _stat_hint[:5]) if _stat_hint else 'не знайдено статистично'
    _sys = ('Ти аналізатор Telegram-каналів для бота автопостингу. '
            'Знайди рекламний підпис/footer що ПОВТОРЮЄТЬСЯ як мінімум у половині (50%+) постів. '
            'ВАЖЛИВО: підпис має бути ОДНАКОВИМ або МАЙЖЕ ОДНАКОВИМ текстом у кінці різних постів. '
            'НЕ обирай текст що зустрічається лише в 1-2 постах. '
            'Може бути: @username посилання, t.me/ посилання, '
            'Instagram/TikTok/YouTube/Discord посилання, заклик підписатись/перейти/слідкувати. '
            'Відповідай ТІЛЬКИ точним текстом підпису (як є у постах). Якщо не знайдено — NONE.')
    _usr = (f'Ось {len(texts)} постів каналу @{username}.\n'
            f'Статистичний аналіз (рядки що зустрічаються у 50%+ постів): {_hint_str}\n\n'
            f'Перевір і знайди точний рекламний підпис що зустрічається у більшості постів:\n\n{_posts}')
    try:
        _res = (await _call_ai(_sys, _usr)).strip()
        _clean = _res.strip('"').strip("'")
        if _clean.upper() == 'NONE' or len(_clean) < 3:
            log.info(f'detect_signature @{username}: no pattern')
            return ''
        # Validate: any signature line must appear in at least 40% of posts
        _sig_check_lines = [l.strip() for l in _clean.splitlines() if len(l.strip()) >= 4]
        _check_line = _sig_check_lines[0][:60] if _sig_check_lines else _clean.strip()[:60]
        _appear_count = sum(1 for _t in texts if _check_line.lower() in _t.lower())
        # If first line doesn't match enough, try other lines
        if _appear_count < max(2, int(len(texts) * 0.4)) and len(_sig_check_lines) > 1:
            for _sl in _sig_check_lines[1:]:
                _cnt = sum(1 for _t in texts if _sl[:60].lower() in _t.lower())
                if _cnt > _appear_count:
                    _appear_count = _cnt
        if _appear_count < max(2, int(len(texts) * 0.4)):
            log.info(f'detect_signature @{username}: AI result appears only {_appear_count}/{len(texts)} times — rejected')
            return ''
        log.info(f'detect_signature @{username}: found: {repr(_clean[:80])}')
        async with aiosqlite.connect(DB_PATH, timeout=10) as _db:
            await _db.execute('UPDATE sources SET promo_signature=? WHERE id=?', (_clean, source_id))
            # Get channel_id for this source
            _src_row = await _fetchone(_db, 'SELECT channel_id FROM sources WHERE id=?', (source_id,))
            await _db.commit()
        # Re-clean any already-queued posts from this source
        if _src_row:
            await reprocess_pending_with_signature(_src_row['channel_id'], source_id, _clean)
        return _clean
    except Exception as _ae:
        log.warning(f'detect_signature @{username}: AI error: {_ae}')
        return ''



async def _detect_source_signature(source_id: int, messages: list) -> str:
    """
    Analyze last N messages from a source to find promo footer.
    Detects: @username links, t.me/ links, social CTAs, "follow us", "join our" etc.
    Uses both statistical analysis AND AI verification.
    Saves result to sources.promo_signature.
    """
    import re as _r

    def strip_html(t):
        return _r.sub(r'<[^>]+>', '', t or '').strip()

    def get_last_lines(text: str, n: int = 4) -> list:
        lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
        return lines[-n:] if len(lines) >= n else lines

    # Promo keyword patterns (any language)
    PROMO_RE = _r.compile(
        r'(@[A-Za-z0-9_]{3,}|t\.me/|telegram\.me/|instagram\.com/|tiktok\.com/|youtube\.com/|'
        r'twitter\.com/|x\.com/|facebook\.com/|vk\.com/|discord\.gg/|'
        r'підпис|підпишись|підписуйся|подписывайся|підписатись|subscribe|follow us|join us|'
        r'наш канал|наш discord|наш telegram|читай тут|читайте на|більше на|'
        r'more at|check out|перейди|перейдіть|переходь|заходь|заходьте|жми|натисни)',
        _r.IGNORECASE
    )

    samples = []
    for msg in messages[:40]:
        raw = getattr(msg, 'message', '') or getattr(msg, 'text', '') or ''
        plain = strip_html(raw).strip()
        if len(plain) > 30:
            samples.append(plain)

    if len(samples) < 4:
        return ''

    threshold = max(3, int(len(samples) * 0.5))

    # Statistical: find repeating last lines
    candidate_counts: dict = {}
    for s in samples:
        last = get_last_lines(s, 3)
        for line in last:
            if len(line) >= 6:
                candidate_counts[line] = candidate_counts.get(line, 0) + 1

    best = ''
    best_count = 0
    for candidate, count in sorted(candidate_counts.items(), key=lambda x: -x[1]):
        if count >= threshold:
            # Prefer promo lines even if slightly less common
            is_promo = bool(PROMO_RE.search(candidate))
            score = count + (3 if is_promo else 0)
            if score > best_count and len(candidate) > 4:
                best = candidate
                best_count = score

    # Try 2-line combos (strip URLs before comparing for better matching)
    def _strip_urls(s):
        s = _r.sub(r'https?://\S+', '', s)
        s = _r.sub(r'\([^)]*\)', '', s)  # (link) → empty
        return _r.sub(r'\s+', ' ', s).strip()

    for s in samples:
        last2 = get_last_lines(s, 2)
        if len(last2) == 2:
            combo = '\n'.join(last2)
            combo_clean = _strip_urls(combo)
            if len(combo_clean) >= 8:
                cnt = sum(1 for ss in samples if _strip_urls('\n'.join(get_last_lines(ss, 2))) == combo_clean)
                is_promo = bool(PROMO_RE.search(combo))
                if cnt >= threshold - 1 and (is_promo or cnt >= threshold):
                    if len(combo) > len(best) or (is_promo and cnt >= threshold - 1):
                        best = combo

    if len(best) < 6:
        # AI fallback: ask AI to find promo footer
        try:
            sample_texts = '\n---\n'.join(s[-200:] for s in samples[:8])
            ai_prompt = (
                f"Analyze these {len(samples[:8])} Telegram post endings and find the REPEATING promotional footer/signature "
                f"(call to action, channel link, social media CTA, subscribe request, etc.).\n\n"
                f"Posts endings:\n{sample_texts}\n\n"
                f"Reply with ONLY the exact footer text that repeats, or 'NONE' if no clear pattern."
            )
            ai_result = await _call_ai(
                "You analyze Telegram posts to find repeating promo footers. Reply with ONLY the exact repeating footer text, or 'NONE'.",
                ai_prompt
            )
            if ai_result and ai_result.strip().upper() != 'NONE' and len(ai_result.strip()) > 4:
                best = ai_result.strip()
                log.info(f"  source {source_id}: AI detected signature: {repr(best[:60])}")
        except Exception as _ae:
            log.debug(f"  source {source_id}: AI signature detection failed: {_ae}")

    if len(best) < 4:
        return ''

    log.info(f"  source {source_id}: detected promo signature: {repr(best[:60])}")

    # Save to DB
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute(
                "UPDATE sources SET promo_signature=? WHERE id=?",
                (best.strip(), source_id)
            )
            await db.commit()
    except Exception as _e:
        log.warning(f"  source {source_id}: could not save promo_signature: {_e}")
        return ''
    log.info(f"  source {source_id}: detected promo signature: {repr(best.strip())}")
    return best.strip()


async def _get_source_signature(source_id: int) -> str:
    """Load saved promo_signature for a source. Safe if column missing."""
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            row = await _fetchone(db, "SELECT promo_signature FROM sources WHERE id=?", (source_id,))
        return (row.get('promo_signature') or '') if row else ''
    except Exception:
        return ''



def _cut_source_signature(text: str, signature: str) -> str:
    """Remove promo signature from text. Line-by-line matching + promo cleanup.
    Strips HTML tags before comparing (signature is plain text, post has HTML)."""
    if not signature or not text:
        return text
    import re as _r

    # Normalize whitespace
    def _ws(s):
        return s.replace('\xa0', ' ').replace('\u200b', '')

    # Strip HTML tags + URLs + @mentions + junk chars for comparison
    def _plain(s):
        s = _r.sub(r'<a[^>]+>.*?</a>', '', s, flags=_r.DOTALL)  # remove full <a> tags
        s = _r.sub(r'<[^>]+>', '', s)
        s = _r.sub(r'https?://\S+', '', s)
        s = _r.sub(r'@[A-Za-z0-9_]{3,}', '', s)
        s = _r.sub(r'[()|\-]', ' ', s)  # parens, pipes, dashes → space
        s = _r.sub(r'\s+', ' ', s)
        return s.strip()

    # Check if a line looks like promo (not real content)
    _PROMO_WORDS = {'подписаться', 'подписывайся', 'подписывайтесь', 'подписка',
                    'subscribe', 'follow', 'join', 'жесть', 'бункер', '18+',
                    'подписатись', 'підписатись', 'підписуйся', 'канал', 'channel'}
    def _is_promo_line(line_text):
        """Check if line is promotional (short, emoji-heavy, promo words)."""
        plain = _plain(line_text).lower()
        if not plain or len(plain) < 3:
            return True  # empty = remove
        if len(plain) > 120:
            return False  # long line = real content
        # Check for promo words
        words = set(_r.findall(r'[\w]+', plain))
        if words & _PROMO_WORDS:
            return True
        # Line is very short and starts with emoji
        if len(plain) < 60 and _r.match(r'[^\w\s]', plain):
            return True
        return False

    text = _ws(text)
    sig = _ws(signature).strip()

    # Build set of signature lines (cleaned, lowered)
    sig_lines = [l.strip().lower() for l in sig.splitlines() if len(l.strip()) >= 3]
    sig_lines_clean = [_plain(sl).strip() for sl in sig_lines]
    if not sig_lines_clean:
        return text

    # Work line-by-line from the bottom: find trailing lines that match sig
    text_lines = text.splitlines()
    cut_from = len(text_lines)
    found_sig = False

    for i in range(len(text_lines) - 1, -1, -1):
        line_plain = _plain(text_lines[i]).strip().lower()
        if not line_plain:
            cut_from = i  # skip empty lines
            continue
        # Check if this line matches any signature line
        matched = False
        for sl in sig_lines_clean:
            if sl in line_plain or line_plain in sl:
                matched = True
                break
        if matched:
            cut_from = i
            found_sig = True
        else:
            break

    # After removing sig lines, also remove adjacent promo lines above
    if found_sig and cut_from > 0:
        for i in range(cut_from - 1, max(-1, cut_from - 5), -1):
            if _is_promo_line(text_lines[i]):
                cut_from = i
            else:
                break

    if cut_from < len(text_lines):
        result = '\n'.join(text_lines[:cut_from]).strip()
        if len(result) > 5:
            return result

    # Fallback: try full sig match on plain text, then cut from that position
    text_plain = _plain(text).lower()
    sig_plain = _plain(sig).lower()

    # Try full block match
    idx = text_plain.rfind(sig_plain)
    if idx != -1 and idx > len(text_plain) * 0.05:
        prefix_newlines = text_plain[:idx].count('\n')
        if prefix_newlines < len(text_lines):
            result = '\n'.join(text_lines[:prefix_newlines]).strip()
            if len(result) > 5:
                return result

    # Try first sig line match
    if sig_lines_clean:
        idx = text_plain.rfind(sig_lines_clean[0])
        if idx != -1 and idx > len(text_plain) * 0.05:
            prefix_newlines = text_plain[:idx].count('\n')
            if prefix_newlines < len(text_lines):
                result = '\n'.join(text_lines[:prefix_newlines]).strip()
                if len(result) > 5:
                    return result

    return text

def _clean_links(text, source_signature: str = ""):
    """
    Step 0: Cut source-specific signature FIRST (while text still has full links intact).
    Step 1: Remove ENTIRE lines that contain t.me/, @username, https://.
            Also remove the line BEFORE if it ends with colon (intro label).
    Step 2: Remove remaining inline promo <a href> tags.
    Step 3: Remove remaining bare https:// and @usernames inline.
    Step 4: Final signature cleanup pass.
    """
    import re as _r

    PROMO_DOMAINS = (
        "t.me/", "telegram.me/", "telegram.dog/",
        "instagram.com/", "tiktok.com/", "youtube.com/", "youtu.be/",
        "twitter.com/", "x.com/", "vk.com/", "vk.ru/",
        "facebook.com/", "fb.com/", "fb.me/",
        "discord.gg/", "discord.com/invite/",
        "whatsapp.com/", "wa.me/", "linktr.ee/", "taplink.cc/",
    )

    # Step 0: cut source-specific signature BEFORE link removal (exact match while text is intact)
    if source_signature:
        text = _cut_source_signature(text, source_signature)

    # Step 1: remove promo lines
    # Delete: lines that START with @username/link, or where removing promo leaves < 15 chars
    # Keep: lines with real content that happen to mention someone inline
    promo_re   = _r.compile(r't\.me/|telegram\.me/|@[A-Za-z0-9_]{3,}|https?://', _r.I)
    start_promo= _r.compile(r'^\s*(?:@[A-Za-z0-9_]{3,}|https?://|t\.me/|telegram\.me/)', _r.I)
    _strip_tag = lambda s: _r.sub(r'<[^>]+>', '', s).strip()
    _rm_promo  = lambda s: _r.sub(r'(?:<a[^>]+>.*?</a>|https?://\S+|@[A-Za-z0-9_]{3,})', '', s).strip()
    lines_in = text.splitlines()
    keep = [True] * len(lines_in)
    for i, line in enumerate(lines_in):
        if not promo_re.search(line):
            continue
        plain = _strip_tag(line).strip()
        if not plain:
            keep[i] = False
            continue
        # Line starts with promo token → always delete (it's a promo line)
        if start_promo.match(plain):
            keep[i] = False
            continue
        # Line has promo inline but real content before it → keep
        without_promo = _rm_promo(plain)
        if len(without_promo) < 15:
            keep[i] = False
    text = "\n".join(l for k, l in enumerate(lines_in) if keep[k])

    # Step 2: handle remaining <a> tags
    def handle_link(m):
        href  = m.group(1)
        inner = m.group(2).strip()
        if not inner: return ""
        if any(d in href for d in PROMO_DOMAINS): return ""
        return inner

    text = _r.sub(r'<a[^>]+href=.([^>]{1,400}?).[^>]*>(.*?)</a>',
                  handle_link, text, flags=_r.DOTALL)

    # Step 3: remove remaining inline links/usernames
    text = _r.sub(r'https?://\S+', '', text)
    text = _r.sub(r'@[A-Za-z0-9_]{3,}', '', text)
    text = _r.sub(r'[ \t]{2,}', ' ', text)
    text = _r.sub(r'\n{3,}', '\n\n', text)
    text = _strip_footer_lines(text).strip()

    return text


REPHRASE_PROMPT = (
    "You are a Telegram channel editor.\n"
    "Rules:\n"
    "1. Rephrase MINIMALLY — change only a few words, keep same structure and length.\n"
    "2. NEVER add links, @usernames, URLs or any info not in the input text.\n"
    "3. NEVER add subscribe/follow/join calls or channel promotions.\n"
    "4. Keep ALL facts, numbers, names, dates exactly as-is.\n"
    "5. Keep ALL emojis exactly as they are.\n"
    "6. Optionally wrap the headline in <b></b>. Max 1 bold tag. No other HTML.\n"
    "Output ONLY the rephrased text. Nothing else."
)

# Phrases AI returns when it refuses to process content
_AI_REFUSAL_PHRASES = (
    "i cannot", "i can't", "i'm unable", "i am unable",
    "i won't", "i will not", "i refuse", "i'm not able",
    "sorry, i", "i apologize", "as an ai", "as a language model",
    "i don't feel comfortable", "i'm not comfortable",
    "this content", "this request", "against my", "violates",
    "не можу", "не буду", "не здатний", "не в змозі",
    "не могу", "не буду выполнять", "отказываюсь",
    "unable to process", "cannot process", "cannot assist",
)

def _is_ai_refusal(text: str) -> bool:
    """Check if AI returned a refusal/censorship message instead of processing the text."""
    if not text:
        return False
    t = text.strip().lower()[:300]
    return any(phrase in t for phrase in _AI_REFUSAL_PHRASES)


async def process_text_ai(text: str, mode: str, settings: dict, source_signature: str = "") -> str:
    """
    Step 1: Sanitize HTML + cut signature + remove promo links
    Step 2: If < 15 chars → skip
    Step 3: Hard AD check
    Step 4: AI classify (AD → skip)
    Step 5: AI rephrase (only if ai_mode='on')
    Step 6: Translate (only if channel_lang != 'off')
    Logs each step result for debugging.
    """
    if not text:
        return text

    import re as _re_chk
    sanitized = sanitize_html_for_telegram(text)
    _plain = lambda t: _re_chk.sub(r'<[^>]+>', '', t or '').strip()

    _raw_plain = _plain(sanitized)
    log.info(f"  [S0] raw END: {repr(_raw_plain[-150:])}")
    if source_signature:
        log.info(f"  [S0] signature: {repr(source_signature[:80])}")

    # Step 1: clean links + cut signature
    cleaned = _clean_links(sanitized, source_signature)
    cleaned = _cut_source_signature(cleaned, source_signature)
    _cl_plain = _plain(cleaned)
    log.info(f"  [S1] after clean+sig END: {repr(_cl_plain[-150:])}")
    log.info(f"  [S1] sig removed: {source_signature[:30] not in _cl_plain if source_signature else True}")

    # Step 2: skip if nothing left
    if len(_plain(cleaned)) < 15:
        log.info("  [S2] empty after cleaning → skip")
        return ""

    # Step 3: hard pattern check
    body_for_check = _strip_footer_lines(cleaned)
    if _hard_ad_check(body_for_check):
        log.info("  [S3] hard AD pattern → skip")
        return ""
    log.info(f"  [S3] hard check passed")

    ai_mode = settings.get("ai_mode", "on")
    channel_lang = settings.get("channel_lang", "off")

    # Build content filter list from settings
    _content_filters = []
    if settings.get("filter_violence"): _content_filters.append("violence, gore, graphic violence")
    if settings.get("filter_sexual"): _content_filters.append("sexual content, pornography, 18+ content")
    if settings.get("filter_gambling"): _content_filters.append("gambling, casino, betting, slots")
    if settings.get("filter_drugs"): _content_filters.append("drugs, narcotics, drug use")

    async with _ai_semaphore:
        try:
            # Step 4: AI classify (ad + content filters)
            classify_prompt = AD_CLASSIFY_PROMPT
            if _content_filters:
                classify_prompt += (
                    "\n\nADDITIONALLY, also classify as AD (reject) if the post contains: "
                    + "; ".join(_content_filters)
                    + ".\nThese content types must be filtered out."
                )
            verdict = await _call_ai(classify_prompt, body_for_check)
            await asyncio.sleep(0.2)
            log.info(f"  [S4] classify verdict: {repr(verdict.strip()[:60])}")
            if verdict.strip().upper().startswith("AD"):
                log.info("  [S4] AI classified as AD → skip")
                return ""

            # Step 5: AI rephrase
            if ai_mode == "on":
                # Build rephrase prompt with explicit signature removal instruction
                _rephrase_prompt = REPHRASE_PROMPT
                if source_signature:
                    import re as _re_sig
                    # Build clean version of sig for AI (strip URLs/HTML to show just the words)
                    _sig_clean = _re_sig.sub(r'<a[^>]+>.*?</a>', '', source_signature)
                    _sig_clean = _re_sig.sub(r'<[^>]+>', '', _sig_clean)
                    _sig_clean = _re_sig.sub(r'https?://\S+', '', _sig_clean)
                    _sig_clean = _re_sig.sub(r'@[A-Za-z0-9_]{3,}', '', _sig_clean)
                    _sig_clean = _re_sig.sub(r'\s+', ' ', _sig_clean).strip()
                    _rephrase_prompt = (
                        REPHRASE_PROMPT + "\n"
                        f"7. CRITICAL: DELETE the following promo/signature lines from the text COMPLETELY. "
                        f"Do NOT rephrase them, do NOT include them — just DELETE them from output:\n"
                        f"\"{_sig_clean}\""
                    )
                rephrased = await _call_ai(_rephrase_prompt, cleaned)
                log.info(f"  [S5] rephrase END: {repr(_plain(rephrased)[-120:])}")
                if _is_ai_refusal(rephrased):
                    log.info("  [S5] AI refused/censored → skip post")
                    return ""
                if rephrased and rephrased.strip() and len(rephrased) > 10:
                    # Strip any links/URLs that AI might have added
                    rephrased = _re_chk.sub(r'https?://\S+', '', rephrased)
                    rephrased = _re_chk.sub(r'<a\s+href=[^>]+>.*?</a>', '', rephrased, flags=_re_chk.DOTALL)
                    rephrased = _re_chk.sub(r'@[A-Za-z0-9_]{3,}', '', rephrased)
                    cleaned = rephrased
            else:
                log.info("  [S5] rephrase disabled (ai_mode=off)")

            # Cut signature again after AI (catches anything added back)
            cleaned = _clean_links(cleaned, source_signature)
            cleaned = _cut_source_signature(cleaned, source_signature)
            log.info(f"  [S5b] after post-rephrase sig cut END: {repr(_plain(cleaned)[-120:])}")

            # Step 6: Translate
            if channel_lang and channel_lang != "off":
                lang_name = TRANSLATE_SUFFIX.get(channel_lang, channel_lang)
                log.info(f"  [S6] translating to {lang_name}...")
                _sig_note = (
                    f"\nMANDATORY: Remove this promo signature completely from output: {source_signature.strip()}"
                    if source_signature else ""
                )
                translate_prompt = (
                    f"Translate the following Telegram post to {lang_name}.\n"
                    "Keep ALL HTML tags exactly as-is.\n"
                    "Keep all facts, numbers, names unchanged.\n"
                    "Do NOT add any links, URLs, @usernames or info not in the original.\n"
                    "Output ONLY the translated text. Nothing else."
                    + _sig_note
                )
                result = await _call_ai(translate_prompt, cleaned)
                if _is_ai_refusal(result):
                    log.info("  [S6] AI refused translation → skip post")
                    return ""
                if result and result.strip():
                    # Strip any links/URLs that AI might have added
                    result = _re_chk.sub(r'https?://\S+', '', result)
                    result = _re_chk.sub(r'<a\s+href=[^>]+>.*?</a>', '', result, flags=_re_chk.DOTALL)
                    result = _re_chk.sub(r'@[A-Za-z0-9_]{3,}', '', result)
                    result = _cut_source_signature(result, source_signature)
                    log.info(f"  [S6] translated OK: {repr(_plain(result)[:120])}")
                    return result
                log.warning("  [S6] translate returned empty, using untranslated")
            else:
                log.info("  [S6] no translation (lang=off)")

            log.info(f"  [DONE] final: {repr(_plain(cleaned)[:120])}")
            return cleaned

        except Exception as e:
            log.warning(f"  AI error ({AI_PROVIDER}): {e} — fallback to clean only")
            fb = _clean_links(sanitized, source_signature)
            return _cut_source_signature(fb, source_signature)


async def _get_post_status(raw_post_id: int) -> str:
    """Get current status of processed post by raw_post_id."""
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            row = await _fetchone(db,
                "SELECT status FROM processed_posts WHERE raw_post_id=? ORDER BY id DESC LIMIT 1",
                (raw_post_id,))
            return row["status"] if row else "unknown"
    except Exception:
        return "unknown"

async def reprocess_all_pending_for_channel(channel_id: int):
    """Reprocess ALL pending posts for channel with current signatures - for cleaning existing queue."""
    try:
        sources = await get_channel_sources(channel_id)
        settings = await get_channel_settings(channel_id)
        for src in sources:
            sig = await _get_source_signature(src["id"])
            if not sig:
                continue
            async with aiosqlite.connect(DB_PATH, timeout=30) as db:
                rows = await _fetchall(db,
                    """SELECT pp.id, pp.raw_post_id, rp.text as raw_text
                       FROM processed_posts pp
                       JOIN raw_posts rp ON rp.id=pp.raw_post_id
                       WHERE rp.source_id=? AND rp.channel_id=?
                       AND pp.status IN ('pending','awaiting_confirm')""",
                    (src["id"], channel_id)
                )
            for row in rows:
                try:
                    await _ai_process_post(row["raw_post_id"], row.get("raw_text") or "",
                                           settings, source_signature=sig)
                except Exception as _e:
                    log.debug(f"reprocess_all post {row.get('raw_post_id')}: {_e}")
        log.info(f"reprocess_all_pending_for_channel {channel_id}: done")
    except Exception as e:
        log.error(f"reprocess_all_pending error: {e}")

async def reprocess_pending_with_signature(channel_id: int, source_id: int, signature: str):
    """Cut signature from ALL pending posts for this source. Fast - no AI calls."""
    if not signature:
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=30) as db:
            rows = await _fetchall(db,
                """SELECT pp.id, pp.cleaned_text
                   FROM processed_posts pp
                   JOIN raw_posts rp ON rp.id = pp.raw_post_id
                   WHERE rp.source_id=? AND rp.channel_id=?
                   AND pp.status IN ('pending','awaiting_confirm')""",
                (source_id, channel_id)
            )
        if not rows:
            log.info(f"reprocess: no pending posts for source {source_id}")
            return
        log.info(f"reprocess: cutting sig from {len(rows)} posts, sig={repr(signature[:50])}")
        updated = 0
        async with aiosqlite.connect(DB_PATH, timeout=30) as db:
            for row in rows:
                old_text = row.get("cleaned_text") or ""
                # Apply all cleaning: links + signature cut
                new_text = _clean_links(old_text, signature)
                new_text = _cut_source_signature(new_text, signature)
                if new_text != old_text:
                    await db.execute(
                        "UPDATE processed_posts SET cleaned_text=? WHERE id=?",
                        (new_text, row["id"])
                    )
                    updated += 1
            await db.commit()
        log.info(f"reprocess done: updated {updated}/{len(rows)} posts for source {source_id}")
    except Exception as e:
        log.error(f"reprocess_pending error: {e}")


async def _ai_process_post(raw_id: int, text: str, settings: dict, source_signature: str = ""):
    """Classify + clean. AD → mark skipped. OK → save cleaned text.
    text = raw original HTML (for classify context)
    source_signature = already-known promo suffix to cut
    """
    result = await process_text_ai(text, "on", settings, source_signature=source_signature)
    async with aiosqlite.connect(DB_PATH, timeout=30) as db:
        if not result:
            # Text was classified as ad or fully removed — skip the post
            # Even if it has media, don't send media-only posts when text was ad
            await db.execute(
                "UPDATE processed_posts SET status='skipped' WHERE raw_post_id=?",
                (raw_id,)
            )
            await db.commit()
            log.info(f"  post raw_id={raw_id} marked skipped (ad/empty text removed)")
            return
        await db.execute(
            "UPDATE processed_posts SET cleaned_text=?, status='pending' WHERE raw_post_id=?",
            (result, raw_id)
        )
        await db.commit()
    log.info(f"  post raw_id={raw_id} rephrased OK")


async def cleanup_orphaned_media():
    """Delete media_cache files older than 2 hours that have no pending post."""
    import os, glob, time
    now = time.time()
    cutoff = 2 * 3600  # 2 hours
    deleted = 0
    try:
        for fpath in glob.glob(os.path.join(MEDIA_DIR, "*")):
            try:
                age = now - os.path.getmtime(fpath)
                if age > cutoff:
                    os.remove(fpath)
                    deleted += 1
            except Exception:
                pass
        if deleted:
            log.info(f"Media cleanup: deleted {deleted} orphaned files")
    except Exception as e:
        log.warning(f"Media cleanup error: {e}")


async def cleanup_published_media(post_id: int):
    """Delete local media files after post is published."""
    import os, glob
    try:
        for f in glob.glob(os.path.join(MEDIA_DIR, f"*")):
            try: os.remove(f)
            except Exception: pass
    except Exception:
        pass


async def daily_midnight_cleanup():
    """
    Called every day at 00:00:
    1. Delete ALL pending + skipped posts (queue resets nightly for fresh content)
    2. Keep published/awaiting_confirm posts intact
    3. Clear media_cache directory
    """
    import os, glob
    log.info("Midnight cleanup: resetting queue for fresh content...")
    async with aiosqlite.connect(DB_PATH, timeout=30) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        # Clear processed_posts that are pending/skipped
        await db.execute(
            "DELETE FROM processed_posts WHERE status IN ('pending', 'skipped')"
        )
        # Clear raw_posts that have no published/awaiting_confirm processed post
        await db.execute(
            "DELETE FROM raw_posts WHERE id NOT IN ("
            "  SELECT raw_post_id FROM processed_posts "
            "  WHERE status IN ('published', 'awaiting_confirm')"
            ")"
        )
        await db.commit()
    try:
        for f in glob.glob(os.path.join(MEDIA_DIR, "*")):
            try: os.remove(f)
            except Exception: pass
        log.info("Midnight cleanup: media_cache cleared")
    except Exception as e:
        log.warning(f"Midnight cleanup error: {e}")
    log.info("Midnight cleanup done — queue cleared, ready for fresh parse")
    await cleanup_orphaned_media()

_SESSION_STATUS: dict = {}

def _init_session_status():
    """Scan project dir for N.session files and init status dict."""
    import glob, os
    found = []
    for f in sorted(glob.glob("*.session")):
        name = os.path.splitext(os.path.basename(f))[0]
        if name.isdigit():
            found.append(name)
    if not found:
        # fallback: check TELETHON_SESSION env
        if TELETHON_SESSION:
            sess = os.path.splitext(os.path.basename(TELETHON_SESSION))[0]
            found = [sess if sess.isdigit() else "1"]
    for n in found:
        _SESSION_STATUS[n] = {"ok": True, "error": None, "last_ok": None, "last_fail": None, "fail_count": 0}
    log.info(f"Sessions discovered: {found}")

_init_session_status()

def _mark_session_ok(num: str):
    from datetime import datetime
    if str(num) not in _SESSION_STATUS:
        _SESSION_STATUS[str(num)] = {"ok": True, "error": None, "last_ok": None, "last_fail": None, "fail_count": 0}
    s = _SESSION_STATUS[str(num)]
    s["ok"] = True
    s["error"] = None
    s["last_ok"] = datetime.now().strftime("%H:%M:%S")

def _mark_session_fail(num: str, err: str = ""):
    from datetime import datetime
    if str(num) not in _SESSION_STATUS:
        _SESSION_STATUS[str(num)] = {"ok": True, "error": None, "last_ok": None, "last_fail": None, "fail_count": 0}
    s = _SESSION_STATUS[str(num)]
    s["ok"] = False
    s["error"] = str(err)
    s["last_fail"] = datetime.now().strftime("%H:%M:%S")
    s["fail_count"] = s.get("fail_count", 0) + 1

_notified_dead_sessions: set = set()  # avoid spam to admin


def get_session_status() -> dict:
    result = {n: dict(s) for n, s in _SESSION_STATUS.items()}
    return result


_session_rr_index: int = 0
_session_last_switch: float = 0.0  # timestamp of last hourly switch
SESSION_ROTATE_HOURS: float = float(os.getenv("SESSION_ROTATE_HOURS", "1"))

async def _get_telethon_client():
    """Auto-discover N.session files. Rotates every SESSION_ROTATE_HOURS hours.
    Falls back to next if current is unavailable or errors.
    Returns (client, session_num) or (None, None).
    """
    global _session_rr_index, _session_last_switch
    import glob, os as _os, time as _time
    from telethon import TelegramClient

    # Discover all N.session files
    session_files = sorted(
        [(int(_os.path.splitext(_os.path.basename(f))[0]), f)
         for f in glob.glob("*.session")
         if _os.path.splitext(_os.path.basename(f))[0].isdigit()],
        key=lambda x: x[0]
    )
    if not session_files and TELETHON_SESSION:
        sess_path = TELETHON_SESSION if TELETHON_SESSION.endswith(".session") else TELETHON_SESSION + ".session"
        if _os.path.exists(sess_path):
            session_files = [(1, sess_path)]

    if not session_files:
        log.error("No .session files found in project directory")
        return None, None

    for num, _ in session_files:
        if str(num) not in _SESSION_STATUS:
            _SESSION_STATUS[str(num)] = {"ok": True, "error": None, "last_ok": None, "last_fail": None, "fail_count": 0}

    api_id   = TELETHON_API_ID
    api_hash = TELETHON_API_HASH
    if not api_id or not api_hash:
        log.error("TELETHON_API_ID / TELETHON_API_HASH not set in .env")
        return None, None

    configured = [(api_id, api_hash, sess_file, str(num)) for num, sess_file in session_files]

    # Hourly rotation: switch to next session every N hours
    now_ts = _time.time()
    rotate_secs = SESSION_ROTATE_HOURS * 3600
    if now_ts - _session_last_switch >= rotate_secs:
        _session_rr_index = (_session_rr_index + 1) % len(configured)
        _session_last_switch = now_ts
        log.info(f"Session hourly rotate → session {configured[_session_rr_index][3]}")

    async def _try(api_id, api_hash, sess_file, num):
        """Connect using existing .session file only. Never prompts for phone/code."""
        import os
        # Check if session file exists — skip if not
        sess_path = sess_file if sess_file.endswith(".session") else sess_file + ".session"
        if not os.path.exists(sess_path):
            log.warning(f"Session {num}: file '{sess_path}' not found — skipping")
            _mark_session_fail(num)
            return None
        try:
            c = TelegramClient(
                sess_file, api_id, api_hash,
                device_model="AutoPostBot", system_version="1.0",
                app_version="1.0", lang_code="uk", system_lang_code="uk"
            )
            # No interactive auth — connect only, never ask for phone/code
            await c.connect()
            if not await c.is_user_authorized():
                log.warning(f"Session {num}: not authorized (re-login required)")
                _mark_session_fail(num, "unauthorized")
                await c.disconnect()
                if num not in _notified_dead_sessions and _bot_instance and ADMIN_IDS:
                    _notified_dead_sessions.add(num)
                    try:
                        await _bot_instance.send_message(
                            ADMIN_IDS[0],
                            f"⚠️ <b>Telethon сесія #{num} не авторизована!</b>\n\n"
                            f"Файл <code>{sess_file}.session</code> потребує повторної авторизації.\n"
                            f"Бот переключився на інші сесії.",
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass
                return None
            # Quick test — detect any error (not just banned)
            try:
                from telethon.tl.functions.users import GetUsersRequest as _GUR
                from telethon.tl.types import InputUserSelf as _IUS
                await c(_GUR([_IUS()]))
            except Exception as _te:
                err_lower = str(_te).lower()
                if any(x in err_lower for x in ("banned", "blocked", "deactivated", "account_banned", "user_deactivated")):
                    log.warning(f"Session {num}: ACCOUNT BANNED — {_te}")
                    _mark_session_fail(num, f"banned: {_te}")
                    await c.disconnect()
                    if num not in _notified_dead_sessions and _bot_instance and ADMIN_IDS:
                        _notified_dead_sessions.add(num)
                        try:
                            await _bot_instance.send_message(
                                ADMIN_IDS[0],
                                f"🚫 <b>Telethon сесія #{num} ЗАБЛОКОВАНА!</b>\n\n"
                                f"Акаунт заблокований Telegram. Потрібен новий акаунт.\n"
                                f"Помилка: <code>{_te}</code>",
                                parse_mode="HTML"
                            )
                        except Exception: pass
                    return None
                else:
                    # Any other error (flood, network etc) — mark as invalid and rotate
                    log.warning(f"Session {num} test request failed — marking invalid: {_te}")
                    _mark_session_fail(num, str(_te))
                    await c.disconnect()
                    return None
            _mark_session_ok(num)
            return c
        except Exception as e:
            log.warning(f"Session {num} error: {e}")
            _mark_session_fail(num, str(e))
            return None

    # Start from current hourly-rotated index, try each in order
    start = _session_rr_index % len(configured)
    order = configured[start:] + configured[:start]
    for api_id, api_hash, sess_file, num in order:
        c = await _try(api_id, api_hash, sess_file, num)
        if c:
            log.info(f"Telethon: using session {num}")
            return c, num

    log.error("All Telethon sessions unavailable")
    return None, None

async def _parse_source(channel_id: int, source: dict, max_add: int = QUEUE_MAX) -> int:
    """
    Parse one source channel:
    - Download media via Telethon (works even on protected channels)
    - Upload to bot to get file_id
    - Run AI: classify then rephrase
    Only posts WITH media are saved.
    """
    try:
        from telethon import TelegramClient
        from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
    except ImportError:
        log.error("Telethon not installed")
        return 0

    added    = 0
    client, _sess_num = await _get_telethon_client()
    if not client:
        log.error("No Telethon session available")
        return 0
    try:
        TARGET_NEW    = QUEUE_MAX      # fill up to queue max per parse
        FETCH_BATCH   = 50             # скільки брати за один запит
        MAX_FETCH     = 5000           # ліміт — йдемо глибоко поки не наберемо нових

        try:
            un = source['username'].lstrip('@')
            entity = await client.get_entity(f"@{source['username']}")
        except Exception as e:
            err_str = str(e).lower()
            if "private" in err_str or "invite" in err_str or "join" in err_str or "forbidden" in err_str:
                log.warning(f"Source @{source['username']}: private/restricted — skipping")
            elif any(x in err_str for x in ("banned", "blocked", "deactivated", "account_banned", "flood")):
                log.error(f"  Session error during parse: {e} — account may be banned/flooded")
                if _bot_instance and ADMIN_IDS:
                    try:
                        await _bot_instance.send_message(
                            ADMIN_IDS[0],
                            f"⚠️ <b>Помилка парсингу!</b>\n\n"
                            f"Джерело: @{source['username']}\n"
                            f"Помилка: <code>{e}</code>\n\n"
                            f"Можливо акаунт Telethon заблокований або є FloodWait.",
                            parse_mode="HTML"
                        )
                    except Exception: pass
            else:
                log.warning(f"  _collect @{source['username']}: get_entity failed: {e}")
            await client.disconnect()
            return 0
        settings = await get_channel_settings(channel_id)
        blacklist_extra = settings.get("blacklist", [])

        # Перевіряємо чи є вже пости в черзі для цього джерела
        pending_count = await count_pending_posts(channel_id)

        # Загружаємо вже парсовані ID з persistent seen_messages (не чиститься на півночі)
        parsed_ids, parsed_grouped_ids = await load_seen_message_ids(channel_id, source["id"])
        log.info(f"  already parsed: {len(parsed_ids)} for @{source['username']}")

        # Завантажуємо від НАЙНОВІШИХ до старіших (дефолтний порядок Telegram)
        # Спочатку беремо найсвіжіші пости, потім йдемо вглиб якщо нових мало
        all_messages  = []
        fetched_total = 0
        offset_id     = 0  # 0 = з найновішого

        while fetched_total < MAX_FETCH:
            try:
                batch = await client.get_messages(entity, limit=FETCH_BATCH, offset_id=offset_id)
            except Exception as _fe:
                err_str = str(_fe).lower()
                if 'flood' in err_str or 'floodwait' in err_str:
                    import re as _re
                    wait_sec = int((_re.search(r'([0-9]+)', str(_fe)) or type('',(),{'group':lambda s,x:'60'})()).group(1))
                    log.warning(f"FloodWait on @{source['username']}: sleeping {wait_sec}s")
                    await asyncio.sleep(min(wait_sec, 120))
                    continue
                log.error(f"get_messages error @{source['username']}: {_fe}")
                break
            if not batch:
                break
            fetched_total += len(batch)
            offset_id = batch[-1].id  # наступний батч іде ще глибше (старіші)

            new_in_batch = [m for m in batch if m.id not in parsed_ids]
            all_messages.extend(new_in_batch)

            truly_new = len(all_messages)
            log.info(f"  fetched {fetched_total}, truly new: {truly_new}")

            if truly_new >= TARGET_NEW * 3:
                break  # вистачає кандидатів з запасом на AI-відсів (3x)
            if len(batch) < FETCH_BATCH:
                if truly_new == 0:
                    log.info("  no new posts — end of history")
                break

        messages = all_messages
        log.info(f"Parsing @{source['username']}: fetched {len(messages)} msgs total")

        # Group albums
        # Build initial groups from fetched messages
        albums: Dict[int, list] = {}
        for msg in messages:
            if msg.grouped_id:
                albums.setdefault(msg.grouped_id, []).append(msg)

        # For albums with only 1 message, fetch surrounding messages to get all parts
        for gid, grp in list(albums.items()):
            if len(grp) < 2:
                anchor_id = grp[0].id
                try:
                    # Fetch messages around the anchor (±5 messages)
                    nearby = await client.get_messages(entity, limit=10, offset_id=anchor_id + 5, min_id=anchor_id - 5)
                    for nm in nearby:
                        if nm.grouped_id == gid and nm.id not in {m.id for m in grp}:
                            grp.append(nm)
                            # Also add to messages list so it can be processed
                            if nm.id not in parsed_ids:
                                messages.append(nm)
                    albums[gid] = sorted(grp, key=lambda m: m.id)
                    if len(albums[gid]) > 1:
                        log.info(f"  album {gid}: fetched {len(albums[gid])} parts")
                except Exception as _ae:
                    log.warning(f"  album fetch error gid={gid}: {_ae}")

        processed_groups: set  = set()

        # Load already-saved grouped_ids to prevent album duplicates
        async with aiosqlite.connect(DB_PATH, timeout=30) as _gdb:
            await _gdb.execute("PRAGMA journal_mode=WAL")
            _gcur = await _gdb.execute(
                "SELECT rp.tg_message_id FROM raw_posts rp "
                "JOIN sources s ON s.id=rp.source_id "
                "WHERE rp.channel_id=? AND s.username=?",
                (channel_id, source["username"])
            )
            _grows = await _gcur.fetchall()
        saved_msg_ids: set = {r[0] for r in _grows}
        # Mark grouped_ids of already-saved albums as processed
        for msg in messages:
            if msg.grouped_id and msg.id in saved_msg_ids:
                processed_groups.add(msg.grouped_id)

        from telethon.extensions import html as tl_html

        def msg_to_html(m) -> str:
            try:
                return tl_html.unparse(
                    getattr(m, "text", "") or "",
                    getattr(m, "entities", None) or []
                ).strip()
            except Exception:
                return (getattr(m, "text", None) or "").strip()

        # Auto-detect promo signature from messages
        existing_sig = await _get_source_signature(source["id"])
        if len(messages) >= 5:
            new_sig = await _detect_source_signature(source["id"], messages)
            if new_sig:
                existing_sig = new_sig
            elif not existing_sig:
                existing_sig = await _get_source_signature(source["id"])
        source_signature = existing_sig

        if not source_signature:
            log.info(f"  @{source['username']}: no promo signature detected — proceeding without signature filter")

        log.info(f"Parsing @{source['username']}: {len(messages)} msgs → collecting candidates")

        from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument

        # ── PASS 1: classify all messages into candidates ─────────────
        already_in_q = await count_pending_posts(channel_id)
        slots_left = max(0, QUEUE_MAX - already_in_q)
        if slots_left == 0:
            log.info(f"  queue full ({already_in_q}/{QUEUE_MAX}), skip collect")
            return added
        media_only = settings.get("media_only", False)
        text_only = settings.get("text_only", False)
        candidates = []
        # Build set of all message IDs that belong to albums for fast lookup
        album_msg_ids: set = set()
        for gid, grp in albums.items():
            for m in grp:
                album_msg_ids.add(m.id)

        for msg in reversed(messages):
            try:
                has_text = bool((getattr(msg, "text", None) or "").strip())
                has_media = bool(msg.media)
                if not has_media and not has_text:
                    continue
                # media_only filter: skip text-only posts
                if media_only and not has_media:
                    continue
                # text_only filter: skip posts with media
                if text_only and has_media:
                    continue
                # Skip album members — they are handled when we encounter the group
                if msg.grouped_id and msg.grouped_id in processed_groups:
                    continue
                if msg.id in parsed_ids:
                    # If this is an album member, mark group as processed
                    if msg.grouped_id:
                        processed_groups.add(msg.grouped_id)
                    continue
                if len(candidates) >= slots_left:
                    break
                text = ""
                if msg.grouped_id:
                    processed_groups.add(msg.grouped_id)
                    album_msgs = sorted(albums.get(msg.grouped_id, [msg]), key=lambda m: m.id)
                    for am in album_msgs:
                        t = msg_to_html(am)
                        if t: text = t; break
                    if text and contains_blacklisted(text, blacklist_extra):
                        continue
                    candidates.append((msg, text, True, album_msgs))
                elif msg.id not in album_msg_ids:
                    # Only process as single post if it's NOT part of any album
                    text = msg_to_html(msg)
                    if text and contains_blacklisted(text, blacklist_extra):
                        log.debug(f"  skip {msg.id}: blacklisted word")
                        continue
                    # Skip non-album posts with no media AND no text
                    if not msg.media and not text.strip():
                        continue
                    candidates.append((msg, text, False, None))
            except Exception as e:
                log.warning(f"  classify {getattr(msg,'id','?')}: {e}")

        log.info(f"  {len(candidates)} candidates to download | queue={already_in_q}/{QUEUE_MAX}")

        # ── PASS 2: download+save in parallel batches ─────────────────
        async def _process_one(cand):
            msg, text, is_album, album_msgs = cand
            try:
                media_type = media_file_id = media_files_json = None
                if is_album:
                    album_files = []
                    for am in album_msgs:
                        if not am.media: continue
                        if isinstance(am.media, MessageMediaPhoto):
                            fid = await _download_and_get_file_id(client, am, "photo")
                            mtype = "photo"
                        elif isinstance(am.media, MessageMediaDocument):
                            mime = getattr(am.media.document, "mime_type", "")
                            if any(x in mime for x in ("audio","ogg","opus","voice","webp","tgsticker")): continue
                            mtype = "video" if "video" in mime else ("animation" if "gif" in mime else "document")
                            fid = await _download_and_get_file_id(client, am, mtype)
                        else: continue
                        if fid: album_files.append({"type": mtype, "file_id": fid})
                    if not album_files: return None
                    media_files_json = json.dumps(album_files)
                    media_type = "album"
                    media_file_id = album_files[0]["file_id"]
                elif msg.media:
                    if isinstance(msg.media, MessageMediaPhoto):
                        media_type = "photo"
                        media_file_id = await _download_and_get_file_id(client, msg, "photo")
                    elif isinstance(msg.media, MessageMediaDocument):
                        mime = getattr(msg.media.document, "mime_type", "")
                        if any(x in mime for x in ("audio","ogg","opus","voice","webp","tgsticker")): return None
                        media_type = "video" if "video" in mime else ("animation" if "gif" in mime else "document")
                        media_file_id = await _download_and_get_file_id(client, msg, media_type)
                    else:
                        return None
                    if not media_file_id:
                        try:
                            await save_raw_post(channel_id=channel_id, source_id=source["id"],
                                tg_message_id=msg.id, text=text or "", media_type="failed",
                                media_file_id=None, media_files_json=None,
                                original_url=f"https://t.me/{source['username']}/{msg.id}",
                    thumbnail_file_id="")
                        except Exception: pass
                        return None
                if not text and not media_file_id:
                    return None
                return (msg.id, text, media_type, media_file_id, media_files_json,
                        f"https://t.me/{source['username']}/{msg.id}",
                        getattr(msg, "grouped_id", None))
            except Exception as e:
                log.warning(f"  Error {getattr(msg,'id','?')}: {e}")
                return None

        for i in range(0, len(candidates), DOWNLOAD_WORKERS):
            if added >= max_add: break
            # Stop if queue already full
            current_q = await count_pending_posts(channel_id)
            if current_q >= QUEUE_MAX:
                log.info(f"  queue full ({current_q}/{QUEUE_MAX}), stopping download")
                break
            batch = candidates[i:i + DOWNLOAD_WORKERS]
            results = await asyncio.gather(*[_process_one(c) for c in batch])
            for res in results:
                if res is None or added >= max_add: continue
                msg_id, text, media_type, media_file_id, media_files_json, orig_url, g_id = res
                raw_id = await save_raw_post(
                    channel_id=channel_id, source_id=source["id"],
                    tg_message_id=msg_id, text=text, media_type=media_type,
                    media_file_id=media_file_id, media_files_json=media_files_json,
                    original_url=orig_url,
                    thumbnail_file_id="", grouped_id=g_id)
                # Persistent dedup record — survives midnight cleanup
                await mark_messages_seen(channel_id, source["id"], [(msg_id, g_id)])
                # For media-only posts (no text): save directly as pending, skip AI
                if not (text or "").strip():
                    await save_processed_post(raw_id, "", mode="sanitize")
                else:
                    cleaned = _clean_links(sanitize_html_for_telegram(text), source_signature)
                    await save_processed_post(raw_id, cleaned, mode="sanitize")
                    # AI: classify (ad filter) + rephrase + translate
                    try:
                        await _ai_process_post(raw_id, text, settings, source_signature=source_signature)
                    except Exception as _ae:
                        import logging as _l; _l.getLogger(__name__).warning(f"AI err: {_ae}")
                added += 1
                log.info(f"  ✓ saved {msg_id} media={media_type} len={len(text or '')}")
        # end parallel processing
        # Mark ALL fetched messages as seen — even filtered/skipped ones — so we never
        # re-check them on subsequent parses (prevents duplicates after midnight reset).
        try:
            _seen_batch = [(m.id, getattr(m, 'grouped_id', None)) for m in messages if getattr(m, 'id', None)]
            await mark_messages_seen(channel_id, source["id"], _seen_batch)
        except Exception as _se:
            log.debug(f"  mark_messages_seen (bulk): {_se}")
    except Exception as e:
        log.error(f"Parse @{source['username']} error: {e}")
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass

    log.info(f"  @{source['username']}: {added} new posts saved")
    return added


async def _parse_source_collect(channel_id: int, source: dict, limit: int) -> list:
    """
    Like _parse_source but ONLY collects and downloads — does NOT save to DB.
    Returns list of tuples: (msg_id, text, media_type, media_file_id, media_files_json, orig_url)
    """
    from telethon import TelegramClient
    from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
    from telethon.extensions import html as tl_html
    import json as _json

    client, session_num = await _get_telethon_client()
    if not client:
        return []

    results = []
    try:
        try:
            _un2 = source['username'].lstrip('@')
            entity = None
            entity = await client.get_entity(f"@{source['username']}")
        except Exception as e:
            log.warning(f"  _collect @{source['username']}: get_entity failed: {e}")
            return []

        settings = await get_channel_settings(channel_id)
        blacklist_extra = settings.get("blacklist", [])

        # Persistent dedup via seen_messages (survives midnight cleanup)
        parsed_ids, parsed_grouped_ids = await load_seen_message_ids(channel_id, source["id"])

        TARGET_NEW  = limit
        FETCH_BATCH = 50
        all_messages = []
        fetched_total = 0
        offset_id = 0

        while fetched_total < 5000:
            try:
                batch = await client.get_messages(entity, limit=FETCH_BATCH, offset_id=offset_id)
            except Exception as _fe:
                log.warning(f"  _collect get_messages @{source['username']}: {_fe}")
                break
            if not batch:
                break
            fetched_total += len(batch)
            offset_id = batch[-1].id
            new_in_batch = [m for m in batch if m.id not in parsed_ids]
            all_messages.extend(new_in_batch)
            if len(all_messages) >= TARGET_NEW * 3:
                break
            if len(batch) < FETCH_BATCH:
                break

        # Group albums
        albums: dict = {}
        for msg in all_messages:
            if msg.grouped_id:
                albums.setdefault(msg.grouped_id, []).append(msg)
        for gid, grp in list(albums.items()):
            if len(grp) < 2:
                anchor_id = grp[0].id
                try:
                    nearby = await client.get_messages(entity, limit=10, offset_id=anchor_id + 5, min_id=anchor_id - 5)
                    for nm in nearby:
                        if nm.grouped_id == gid and nm.id not in {m.id for m in grp}:
                            grp.append(nm)
                    albums[gid] = sorted(grp, key=lambda m: m.id)
                except Exception:
                    pass

        def msg_to_html(m) -> str:
            try:
                return tl_html.unparse(
                    getattr(m, "text", "") or "",
                    getattr(m, "entities", None) or []
                ).strip()
            except Exception:
                return (getattr(m, "text", None) or "").strip()

        processed_groups: set = set()
        candidates = []
        media_only = settings.get("media_only", False)
        for msg in reversed(all_messages):
            try:
                has_text = bool((getattr(msg, "text", None) or "").strip())
                has_media = bool(msg.media)
                if not has_media and not has_text:
                    continue
                if media_only and not has_media:
                    continue
                if msg.grouped_id and msg.grouped_id in processed_groups:
                    continue
                # Skip entire album if already parsed (prevents re-downloading album members)
                if msg.grouped_id and msg.grouped_id in parsed_grouped_ids:
                    processed_groups.add(msg.grouped_id)
                    continue
                if len(candidates) >= limit:
                    break
                text = ""
                if msg.grouped_id and msg.grouped_id not in processed_groups:
                    processed_groups.add(msg.grouped_id)
                    album_msgs = sorted(albums.get(msg.grouped_id, [msg]), key=lambda m: m.id)
                    for am in album_msgs:
                        t = msg_to_html(am)
                        if t: text = t; break
                    if text and contains_blacklisted(text, blacklist_extra):
                        continue
                    candidates.append((msg, text, True, album_msgs))
                else:
                    text = msg_to_html(msg)
                    if text and contains_blacklisted(text, blacklist_extra):
                        continue
                    candidates.append((msg, text, False, None))
            except Exception as e:
                log.warning(f"  classify {getattr(msg,'id','?')}: {e}")

        # Download media for each candidate
        async def _dl_one(cand):
            msg, text, is_album, album_msgs = cand
            try:
                media_type = media_file_id = media_files_json = None
                if is_album:
                    album_files = []
                    for am in album_msgs:
                        if not am.media: continue
                        if isinstance(am.media, MessageMediaPhoto):
                            fid = await _download_and_get_file_id(client, am, "photo")
                            mtype = "photo"
                        elif isinstance(am.media, MessageMediaDocument):
                            mime = getattr(am.media.document, "mime_type", "")
                            if any(x in mime for x in ("audio","ogg","opus","voice","webp","tgsticker")): continue
                            mtype = "video" if "video" in mime else ("animation" if "gif" in mime else "document")
                            fid = await _download_and_get_file_id(client, am, mtype)
                        else: continue
                        if fid: album_files.append({"type": mtype, "file_id": fid})
                    if not album_files: return None
                    media_files_json = _json.dumps(album_files)
                    media_type = "album"
                    media_file_id = album_files[0]["file_id"]
                elif msg.media:
                    if isinstance(msg.media, MessageMediaPhoto):
                        media_type = "photo"
                        media_file_id = await _download_and_get_file_id(client, msg, "photo")
                    elif isinstance(msg.media, MessageMediaDocument):
                        mime = getattr(msg.media.document, "mime_type", "")
                        if any(x in mime for x in ("audio","ogg","opus","voice","webp","tgsticker")): return None
                        media_type = "video" if "video" in mime else ("animation" if "gif" in mime else "document")
                        media_file_id = await _download_and_get_file_id(client, msg, media_type)
                    else:
                        return None
                    if not media_file_id: return None
                if not text and not media_file_id:
                    return None
                return (msg.id, text, media_type, media_file_id, media_files_json,
                        f"https://t.me/{source['username']}/{msg.id}",
                        getattr(msg, "grouped_id", None))
            except Exception as e:
                log.warning(f"  _dl_one {getattr(msg,'id','?')}: {e}")
                return None

        for i in range(0, len(candidates), DOWNLOAD_WORKERS):
            batch = candidates[i:i + DOWNLOAD_WORKERS]
            batch_results = await asyncio.gather(*[_dl_one(c) for c in batch])
            for r in batch_results:
                if r is not None:
                    results.append(r)

        # Mark every fetched message as seen so we never reparse it (persists past midnight cleanup)
        try:
            _seen_batch = [(m.id, getattr(m, 'grouped_id', None)) for m in all_messages if getattr(m, 'id', None)]
            await mark_messages_seen(channel_id, source["id"], _seen_batch)
        except Exception as _se:
            log.debug(f"  mark_messages_seen (collect bulk): {_se}")

    except Exception as e:
        log.error(f"_parse_source_collect @{source['username']}: {e}")
    finally:
        try: await client.disconnect()
        except Exception: pass

    return results


async def _parse_source_deep_collect(channel_id: int, source: dict, limit: int) -> list:
    """Deep collect: fetch posts OLDER than what's already in DB."""
    from telethon import TelegramClient
    from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
    from telethon.extensions import html as tl_html
    import json as _json

    async with aiosqlite.connect(DB_PATH, timeout=30) as _db:
        _cur = await _db.execute(
            "SELECT MIN(tg_message_id) FROM seen_messages WHERE channel_id=? AND source_id=?",
            (channel_id, source["id"])
        )
        row = await _cur.fetchone()
    oldest_id = row[0] if row and row[0] else 0
    # Full persistent dedup set
    published_ids, _ = await load_seen_message_ids(channel_id, source["id"])

    if oldest_id == 0:
        return []

    client, _ = await _get_telethon_client()
    if not client:
        return []

    results = []
    try:
        try:
            entity = await client.get_entity(f"@{source['username']}")
        except Exception as e:
            log.warning(f"  deep_collect @{source['username']}: {e}")
            return []

        log.info(f"  deep: fetching posts older than id={oldest_id} for @{source['username']}")
        batch = await client.get_messages(entity, limit=30, offset_id=oldest_id)
        if not batch:
            return []

        log.info(f"  deep: got {len(batch)} older posts for @{source['username']}")

        def msg_to_html(m) -> str:
            try:
                return tl_html.unparse(getattr(m,"text","") or "", getattr(m,"entities",None) or []).strip()
            except Exception:
                return (getattr(m,"text",None) or "").strip()

        albums: dict = {}
        for msg in batch:
            if msg.grouped_id:
                albums.setdefault(msg.grouped_id, []).append(msg)

        processed_groups: set = set()
        settings = await get_channel_settings(channel_id)
        blacklist_extra = settings.get("blacklist", [])
        candidates = []
        for msg in reversed(batch):
            if msg.id in published_ids:
                continue
            has_text = bool((getattr(msg, "text", None) or "").strip())
            if not msg.media and not has_text:
                continue
            if msg.grouped_id and msg.grouped_id in processed_groups:
                continue
            if len(candidates) >= limit:
                break
            text = ""
            if msg.grouped_id and msg.grouped_id not in processed_groups:
                processed_groups.add(msg.grouped_id)
                album_msgs = sorted(albums.get(msg.grouped_id, [msg]), key=lambda m: m.id)
                for am in album_msgs:
                    t = msg_to_html(am)
                    if t: text = t; break
                if text and contains_blacklisted(text, blacklist_extra):
                    continue
                candidates.append((msg, text, True, album_msgs))
            else:
                text = msg_to_html(msg)
                if text and contains_blacklisted(text, blacklist_extra):
                    continue
                candidates.append((msg, text, False, None))

        async def _dl(cand):
            msg, text, is_album, album_msgs = cand
            try:
                media_type = media_file_id = media_files_json = None
                if is_album:
                    album_files = []
                    for am in album_msgs:
                        if not am.media: continue
                        if isinstance(am.media, MessageMediaPhoto):
                            _ar = await _download_and_get_file_id(client, am, "photo"); mtype = "photo"
                            fid = _ar[0] if isinstance(_ar, tuple) else _ar
                            album_files.append({"type": mtype, "file_id": fid}) if fid else None
                        elif isinstance(am.media, MessageMediaDocument):
                            mime = getattr(am.media.document, "mime_type", "")
                            if any(x in mime for x in ("audio","ogg","opus","voice","webp","tgsticker")): continue
                            mtype = "video" if "video" in mime else ("animation" if "gif" in mime else "document")
                            _ar = await _download_and_get_file_id(client, am, mtype)
                            if isinstance(_ar, tuple):
                                fid, _athumb = _ar[0], _ar[1]
                                af_entry = {"type": mtype, "file_id": fid}
                                if _athumb: af_entry["thumb_fid"] = _athumb
                            else:
                                fid = _ar
                                af_entry = {"type": mtype, "file_id": fid}
                            if fid: album_files.append(af_entry)
                        else: continue
                    if not album_files: return None
                    media_files_json = _json.dumps(album_files)
                    media_type = "album"; media_file_id = album_files[0]["file_id"]
                elif msg.media:
                    if isinstance(msg.media, MessageMediaPhoto):
                        media_type = "photo"
                        _r = await _download_and_get_file_id(client, msg, "photo")
                        media_file_id = _r[0] if isinstance(_r, tuple) else _r
                    elif isinstance(msg.media, MessageMediaDocument):
                        mime = getattr(msg.media.document, "mime_type", "")
                        if any(x in mime for x in ("audio","ogg","opus","voice","webp","tgsticker")): return None
                        media_type = "video" if "video" in mime else ("animation" if "gif" in mime else "document")
                        _r = await _download_and_get_file_id(client, msg, media_type)
                        if isinstance(_r, tuple):
                            media_file_id, _thumb_fid = _r[0], _r[1]
                        else:
                            media_file_id, _thumb_fid = _r, None
                    else: return None
                    if not media_file_id: return None
                if not text and not media_file_id: return None
                return (msg.id, text, media_type, media_file_id, media_files_json,
                        f"https://t.me/{source['username']}/{msg.id}")
            except Exception as e:
                log.warning(f"  deep_dl {getattr(msg,'id','?')}: {e}")
                return None

        for i in range(0, len(candidates), DOWNLOAD_WORKERS):
            b = candidates[i:i + DOWNLOAD_WORKERS]
            br = await asyncio.gather(*[_dl(c) for c in b])
            for r in br:
                if r is not None:
                    results.append(r)
        log.info(f"  deep: collected {len(results)} from @{source['username']}")

    except Exception as e:
        log.error(f"deep_collect @{source['username']}: {e}")
    finally:
        try: await client.disconnect()
        except Exception: pass

    return results


# Per-channel parse locks to prevent concurrent runs
_parse_locks: dict = {}

async def parse_channel_sources(channel_id: int) -> int:
    """Parse all active sources. If all give 0 — dig deeper into history."""
    # Prevent concurrent parses for same channel
    if channel_id not in _parse_locks:
        _parse_locks[channel_id] = asyncio.Lock()
    if _parse_locks[channel_id].locked():
        log.info(f"Channel {channel_id}: parse already running, waiting...")
        # Wait for existing parse to finish, then return actual queue count
        async with _parse_locks[channel_id]:
            pass  # just wait for lock release
        q = await count_pending_posts(channel_id)
        log.info(f"Channel {channel_id}: waited for parse, queue now={q}")
        return q if q > 0 else 0
    async with _parse_locks[channel_id]:
        return await _parse_channel_sources_inner(channel_id)


async def _parse_channel_sources_inner(channel_id: int) -> int:
    """Inner parse logic. Called with lock held."""
    # Skip if queue already full
    current_queue = await count_pending_posts(channel_id)
    if current_queue >= QUEUE_MAX:
        log.info(f"Channel {channel_id}: queue={current_queue}>={QUEUE_MAX}, skip parse")
        return -1  # -1 = queue full, not "0 found"
    sources = await get_channel_sources(channel_id)
    active  = [s for s in sources if s.get("is_active", 1)]
    if not active:
        return 0
    import math
    n_sources = len(active)
    per_source = max(1, math.ceil(QUEUE_MAX / n_sources))

    # Step 1: collect candidates from each source (no saving yet)
    src_items: dict = {}  # source_id → list of ready tuples
    for src in active:
        try:
            items = await _parse_source_collect(channel_id, src, limit=per_source * 2)
            src_items[src["id"]] = items
            log.info(f"  @{src['username']}: {len(items)} candidates collected")
        except Exception as e:
            log.error(f"parse_channel_sources collect @{src.get('username')}: {e}")
            src_items[src["id"]] = []

    # Step 2: interleave round-robin: src1[0], src2[0], src1[1], src2[1]...
    keys = [s["id"] for s in active]
    interleaved = []
    while any(src_items[k] for k in keys):
        for k in keys:
            if src_items[k]:
                src = next(s for s in active if s["id"] == k)
                interleaved.append((src, src_items[k].pop(0)))

    # Step 3: save interleaved up to QUEUE_MAX
    settings = await get_channel_settings(channel_id)
    media_only = settings.get("media_only", False)
    total = await count_pending_posts(channel_id)
    log.info(f"Channel {channel_id}: queue before save = {total}/{QUEUE_MAX}")
    for src, item in interleaved:
        if total >= QUEUE_MAX:
            break
        try:
            msg_id, text, media_type, media_file_id, media_files_json, orig_url, g_id = (*item, None)[:7] if len(item) == 6 else item
            # media_only: skip text-only posts (no media at all, or album with no files)
            has_real_media = bool(
                (media_type == "album" and media_files_json) or
                (media_type in ("photo","video","animation","document") and media_file_id)
            )
            if media_only and not has_real_media:
                log.debug(f"  skip {msg_id}: media_only=True, no real media")
                continue
            _sig = await _get_source_signature(src["id"])
            _thumb_id = ""
            raw_id = await save_raw_post(
                channel_id=channel_id, source_id=src["id"],
                tg_message_id=msg_id, text=text, media_type=media_type,
                media_file_id=media_file_id, media_files_json=media_files_json,
                original_url=orig_url, thumbnail_file_id=_thumb_id, grouped_id=g_id)
            if not raw_id:
                log.debug(f"  skip dup {msg_id} src=@{src['username']}")
                continue
            # Clean signature BEFORE saving (fast path)
            _html = sanitize_html_for_telegram(text)
            cleaned = _clean_links(_html, _sig)
            cleaned = _cut_source_signature(cleaned, _sig)
            await save_processed_post(raw_id, cleaned, mode="sanitize")
            try:
                await _ai_process_post(raw_id, text, settings, source_signature=_sig)
            except Exception as _ae:
                log.warning(f"AI err: {_ae}")
            # Only count post if it ended up as pending (not skipped as AD)
            post_status = await _get_post_status(raw_id)
            if post_status == 'pending':
                total += 1
            log.info(f"  ✓ saved {msg_id} src=@{src['username']} media={media_type} status={post_status}")
        except Exception as e:
            log.error(f"save @{src.get('username')}: {e}")

    # If nothing new found — dig deeper
    if total == 0:
        log.info(f"Channel {channel_id}: 0 new — digging deeper into history")
        deep_items: dict = {}
        for src in active:
            try:
                items = await _parse_source_deep_collect(channel_id, src, limit=per_source * 2)
                deep_items[src["id"]] = items
            except Exception as e:
                log.error(f"deep collect @{src.get('username')}: {e}")
                deep_items[src["id"]] = []

        interleaved_deep = []
        while any(deep_items[k] for k in keys):
            for k in keys:
                if deep_items[k]:
                    src = next(s for s in active if s["id"] == k)
                    interleaved_deep.append((src, deep_items[k].pop(0)))

        total = await count_pending_posts(channel_id)  # recheck after first pass
        for src, item in interleaved_deep:
            if total >= QUEUE_MAX:
                break
            try:
                msg_id, text, media_type, media_file_id, media_files_json, orig_url, g_id = (*item, None)[:7] if len(item) == 6 else item
                has_real_media = bool(
                    (media_type == "album" and media_files_json) or
                    (media_type in ("photo","video","animation","document") and media_file_id)
                )
                if media_only and not has_real_media:
                    log.debug(f"  deep skip {msg_id}: media_only=True")
                    continue
                _sig = await _get_source_signature(src["id"])
                raw_id = await save_raw_post(
                    channel_id=channel_id, source_id=src["id"],
                    tg_message_id=msg_id, text=text, media_type=media_type,
                    media_file_id=media_file_id, media_files_json=media_files_json,
                    original_url=orig_url,
                    thumbnail_file_id="", grouped_id=g_id)
                if not raw_id:
                    log.debug(f"  deep skip dup {msg_id}")
                    continue
                cleaned = _clean_links(sanitize_html_for_telegram(text), _sig)
                await save_processed_post(raw_id, cleaned, mode="sanitize")
                try:
                    await _ai_process_post(raw_id, text, settings, source_signature=_sig)
                except Exception as _ae:
                    log.warning(f"AI err deep: {_ae}")
                total += 1
                log.info(f"  deep ✓ saved {msg_id} src=@{src['username']}")
            except Exception as e:
                log.error(f"deep save @{src.get('username')}: {e}")

    log.info(f"Channel {channel_id}: parsed {total} new posts")
    return total



async def _parse_source_deep(channel_id: int, source: dict, max_add: int = QUEUE_MAX) -> int:
    """
    Fetch posts OLDER than what's already in DB.
    Used when recent posts are all already parsed.
    Returns number of posts added.
    """
    try:
        from telethon import TelegramClient
    except ImportError:
        return 0

    # Find the oldest message ID we already have for this source
    async with aiosqlite.connect(DB_PATH, timeout=30) as _db:
        _cur = await _db.execute(
            "SELECT MIN(tg_message_id) FROM raw_posts WHERE channel_id=? AND source_id=?",
            (channel_id, source["id"])
        )
        row = await _cur.fetchone()
        _cur2 = await _db.execute(
            "SELECT rp.tg_message_id FROM raw_posts rp "
            "WHERE rp.channel_id=? AND rp.source_id=?",
            (channel_id, source["id"])
        )
        _rows2 = await _cur2.fetchall()
    published_ids_deep: set = {r[0] for r in _rows2}  # all already-parsed message IDs
    oldest_id = row[0] if row and row[0] else 0

    if oldest_id == 0:
        log.info(f"  deep: no existing posts for @{source['username']}, skip")
        return 0

    log.info(f"  deep: fetching posts older than id={oldest_id} for @{source['username']}")

    # Temporarily patch source to fetch older messages
    # We do this by fetching with offset_id = oldest_id we have
    deep_source = dict(source)

    added = 0
    client, _sess_num = await _get_telethon_client()
    if not client:
        return 0
    try:
        entity = await client.get_entity(f"@{source['username']}")
        # Fetch OLDER messages (offset_id = oldest we have → returns messages before it)
        batch = await client.get_messages(entity, limit=30, offset_id=oldest_id)
        if not batch:
            log.info(f"  deep: no older posts found for @{source['username']}")
            return 0

        log.info(f"  deep: got {len(batch)} older posts for @{source['username']}")

        # Re-use _parse_source logic by temporarily patching parsed_ids
        # Simpler: just call _parse_source which already handles everything,
        # but we pass the messages directly to avoid re-fetching
        settings = await get_channel_settings(channel_id)
        blacklist_extra = settings.get("blacklist", [])
    
        from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
        from telethon.extensions import html as tl_html

        def msg_to_html(m) -> str:
            try:
                return tl_html.unparse(
                    getattr(m, "text", "") or "",
                    getattr(m, "entities", None) or []
                ).strip()
            except Exception:
                return (getattr(m, "text", None) or "").strip()

        albums: dict = {}
        for msg in batch:
            if msg.grouped_id:
                albums.setdefault(msg.grouped_id, []).append(msg)
        processed_groups: set = set()

        for msg in reversed(batch):
            if not msg.media and not (getattr(msg, "text", None) or "").strip():
                continue
            if msg.id in published_ids_deep:
                continue  # already published
            if msg.grouped_id and msg.grouped_id in processed_groups:
                continue

            try:
                text = msg_to_html(msg)
                if text and contains_blacklisted(text, blacklist_extra):
                    continue

                media_type = None
                media_file_id = None
                media_files_json = None

                if msg.media:
                    if isinstance(msg.media, MessageMediaPhoto):
                        media_type = "photo"
                        media_file_id = await _download_and_get_file_id(client, msg, "photo")
                    elif isinstance(msg.media, MessageMediaDocument):
                        mime = getattr(msg.media.document, "mime_type", "")
                        if any(x in mime for x in ("audio", "ogg", "opus", "voice", "webp", "tgsticker")):
                            continue
                        media_type = "video" if "video" in mime else ("animation" if "gif" in mime else "document")
                        media_file_id = await _download_and_get_file_id(client, msg, media_type)
                    if msg.media and not media_file_id:
                        continue

                if msg.grouped_id:
                    processed_groups.add(msg.grouped_id)

                raw_id = await save_raw_post(
                    channel_id=channel_id, source_id=source["id"],
                    tg_message_id=msg.id, text=text,
                    media_type=media_type, media_file_id=media_file_id,
                    media_files_json=media_files_json,
                    original_url=f"https://t.me/{source['username']}/{msg.id}",
                    thumbnail_file_id="")
                cleaned = sanitize_html_for_telegram(text)
                await save_processed_post(raw_id, cleaned, mode="sanitize")
                added += 1
                log.info(f"  deep ✓ saved {msg.id} media={media_type}")

            except Exception as e:
                log.warning(f"  deep error msg {msg.id}: {e}")

    except Exception as e:
        log.error(f"_parse_source_deep @{source['username']}: {e}")
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass

    # AI tasks


    log.info(f"  deep: added {added} old posts from @{source['username']}")
    return added


_parse_channel_semaphore = asyncio.Semaphore(6)  # Max 6 channels parsed concurrently

async def parse_all_channels():
    """Run parsing for all active channels (parallel with semaphore)."""
    channels = await get_all_channels()
    active = [ch for ch in channels if ch["subscription_status"] in ("active", "trial", "restricted")]

    async def _parse_one(ch_id):
        async with _parse_channel_semaphore:
            try:
                await parse_channel_sources(ch_id)
            except Exception as e:
                log.error(f"parse channel {ch_id} error: {e}")

    if active:
        await asyncio.gather(*[_parse_one(ch["id"]) for ch in active], return_exceptions=True)


# ─── PAYMENTS (CRYPTO BOT) ────────────────────────────────────────────────────

async def create_invoice(amount: float, channel_id: int, days: int, user_id: int) -> Optional[dict]:
    """Create CryptoBot invoice. Returns {invoice_id, pay_url}."""
    if not CRYPTO_TOKEN:
        log.error("No CryptoBot token")
        return None
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{CRYPTO_BASE}/api/createInvoice",
                headers={"Crypto-Pay-API-Token": CRYPTO_TOKEN},
                json={
                    "asset": "USDT",
                    "amount": str(amount),
                    "description": f"Subscription {days}d for channel #{channel_id}",
                    "payload": f"{user_id}:{channel_id}:{days}",
                    "expires_in": 3600,
                },
                timeout=15
            )
        data = resp.json()
        if data.get("ok"):
            inv = data["result"]
            invoice_id = str(inv["invoice_id"])
            pay_url = inv["pay_url"]
            async with aiosqlite.connect(DB_PATH, timeout=10) as db:
                await db.execute(
                    "INSERT OR IGNORE INTO invoices(user_id, channel_id, invoice_id, amount, pay_url) "
                    "VALUES(?,?,?,?,?)",
                    (user_id, channel_id, invoice_id, amount, pay_url)
                )
                await db.commit()
            return {"invoice_id": invoice_id, "pay_url": pay_url}
    except Exception as e:
        log.error(f"create_invoice error: {e}")
    return None


async def check_invoice_status(invoice_id: str) -> str:
    """Check invoice status. If paid → call handle_payment_webhook. Returns status string."""
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        row = await _fetchone(db, "SELECT * FROM invoices WHERE invoice_id=?", (invoice_id,))
        if row and dict(row).get("status") == "paid":
            return "paid"

    if not CRYPTO_TOKEN:
        return "pending"

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{CRYPTO_BASE}/api/getInvoices",
                headers={"Crypto-Pay-API-Token": CRYPTO_TOKEN},
                params={"invoice_ids": invoice_id},
                timeout=15
            )
        data = resp.json()
        if data.get("ok"):
            items = data["result"].get("items", [])
            if items and items[0]["status"] == "paid":
                await handle_payment_webhook(invoice_id)
                return "paid"
    except Exception as e:
        log.error(f"check_invoice_status error: {e}")

    return "pending"


async def handle_payment_webhook(invoice_id: str) -> Optional[dict]:
    """Process a paid invoice: activate subscription, referral bonus."""
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        inv_row = await _fetchone(db, "SELECT * FROM invoices WHERE invoice_id=?", (invoice_id,))
        if not inv_row:
            return None
        inv = dict(inv_row)
        if inv["status"] == "paid":
            return inv  # already processed

        await db.execute(
            "UPDATE invoices SET status='paid', paid_at=CURRENT_TIMESTAMP WHERE invoice_id=?",
            (invoice_id,)
        )
        await db.commit()

    channel_id = inv["channel_id"]
    user_id    = inv["user_id"]
    amount     = inv["amount"]

    if channel_id and int(channel_id) != 0:
        # Activate subscription (30 days per BASE_PRICE unit)
        days = round(30 * (amount / BASE_PRICE))
        if days < 1:
            days = 30
        await activate_subscription(int(channel_id), days)
    else:
        # Balance top-up — credit user's wallet directly
        user_data = await get_user_by_id(user_id)
        if user_data:
            await adjust_user_balance(user_data["telegram_id"], amount)
            log.info(f"Balance top-up: user_id={user_id} tg={user_data['telegram_id']} +${amount}")
        else:
            log.warning(f"Balance top-up: user_id={user_id} not found")

    # Record transaction
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(
            "INSERT INTO transactions(user_id, channel_id, invoice_id, amount, type) VALUES(?,?,?,?,'subscription')",
            (user_id, channel_id, invoice_id, amount)
        )
        await db.commit()

    # Referral bonus — only for actual channel subscriptions, not balance top-ups
    ref_info = None
    if channel_id and int(channel_id) != 0:
        # user_id here is DB id, need telegram_id for process_referral_bonus
        _payer = await get_user_by_id(user_id)
        if _payer:
            ref_info = await process_referral_bonus(_payer["telegram_id"], int(channel_id), amount)
    return {"inv": inv, "ref_info": ref_info, "user_id": user_id, "channel_id": channel_id, "amount": amount}


# ─── REFERRALS ────────────────────────────────────────────────────────────────

import random
import string

def gen_referral_code(length: int = 8) -> str:
    chars = string.ascii_letters + string.digits
    return "".join(random.choices(chars, k=length))


async def get_or_create_referral_code(telegram_id: int) -> str:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        user = await _fetchone(db, "SELECT id FROM users WHERE telegram_id=?", (telegram_id,))
        if not user:
            return ""
        user_id = user["id"]
        row = await _fetchone(db, "SELECT code FROM referral_codes WHERE user_id=?", (user_id,))
        if row:
            return row["code"]
        code = gen_referral_code()
        while True:
            exists = await _fetchone(db, "SELECT id FROM referral_codes WHERE code=?", (code,))
            if not exists:
                break
            code = gen_referral_code()
        await db.execute(
            "INSERT INTO referral_codes(user_id, code) VALUES(?,?)", (user_id, code)
        )
        await db.commit()
        return code


async def get_referral_code_owner(code: str) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        row = await _fetchone(db, "SELECT u.* FROM users u JOIN referral_codes rc ON rc.user_id=u.id WHERE rc.code=?", (code,))
        return row if row else None


async def register_referral(referrer_tg_id: int, referred_tg_id: int) -> Optional[dict]:
    """Register referral relationship. Returns referrer info dict if new referral registered, else None."""
    if referrer_tg_id == referred_tg_id:
        return None
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        ref_r = await _fetchone(db, "SELECT * FROM users WHERE telegram_id=?", (referrer_tg_id,))
        ref_d = await _fetchone(db, "SELECT * FROM users WHERE telegram_id=?", (referred_tg_id,))
        if not ref_r or not ref_d:
            log.warning(f"register_referral: user not found referrer={referrer_tg_id} referred={referred_tg_id}")
            return None
        # Check not already referred (by anyone)
        existing = await _fetchone(db,
            "SELECT id FROM referrals WHERE referred_id=? AND channel_id IS NULL",
            (ref_d["id"],))
        if existing:
            log.debug(f"register_referral: {referred_tg_id} already has a referrer")
            return None
        await db.execute(
            "INSERT INTO referrals(referrer_id, referred_id) VALUES(?,?)",
            (ref_r["id"], ref_d["id"])
        )
        await db.commit()
    log.info(f"New referral registered: referrer={referrer_tg_id} → referred={referred_tg_id}")
    return {
        "referrer_tg_id":  referrer_tg_id,
        "referrer_name":   ref_r.get("first_name") or ref_r.get("username") or str(referrer_tg_id),
        "referred_tg_id":  referred_tg_id,
        "referred_name":   ref_d.get("first_name") or ref_d.get("username") or str(referred_tg_id),
    }


async def process_referral_bonus(referred_tg_id: int, channel_id: int, amount: float) -> Optional[dict]:
    """
    Calculate and credit referral bonus.
    Returns rich dict with bonus info, or None if no referral.
    50% flat from all payments within 90 days of referral registration.
    """
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        ref_d = await _fetchone(db, "SELECT * FROM users WHERE telegram_id=?", (referred_tg_id,))
        if not ref_d:
            return None

        referred_id = ref_d["id"]

        # Find referrer (pending row without channel_id)
        ref_row = await _fetchone(
            db,
            "SELECT r.*, u.telegram_id as referrer_tg_id, u.first_name as referrer_name "
            "FROM referrals r JOIN users u ON u.id=r.referrer_id "
            "WHERE r.referred_id=? AND r.channel_id IS NULL AND r.status='pending'",
            (referred_id,)
        )
        if not ref_row:
            return None

        ref = dict(ref_row)
        referrer_db_id = ref["referrer_id"]

        # Check 3-month window (90 days from referral registration date)
        ref_created_at = ref.get("created_at")
        if ref_created_at:
            try:
                reg_dt = datetime.fromisoformat(str(ref_created_at))
                elapsed_days = (datetime.now(timezone.utc).replace(tzinfo=None) - reg_dt).days
                if elapsed_days > 90:
                    log.info(f"Referral bonus skipped: 3-month window expired ({elapsed_days}d) for referred={referred_tg_id}")
                    return None
            except Exception:
                pass

        # Count how many PAID bonuses already exist for this referred+channel
        prev = await _fetchone(
            db,
            "SELECT COUNT(*) as cnt FROM referrals WHERE referred_id=? AND channel_id=? AND status='paid'",
            (referred_id, channel_id)
        )
        prev_count = (prev["cnt"] if prev else 0)
        is_first   = (prev_count == 0)
        # Flat 50% for all payments within 3-month window
        percent    = REFERRAL_FIRST_PERCENT
        bonus      = round(amount * percent, 2)

        # Insert new paid row
        await db.execute(
            "INSERT INTO referrals(referrer_id, referred_id, channel_id, status, bonus_amount, paid_at) "
            "VALUES(?,?,?,'paid',?,CURRENT_TIMESTAMP)",
            (referrer_db_id, referred_id, channel_id, bonus)
        )

        # Credit referrer balance
        await db.execute(
            "INSERT INTO user_balances(user_id, balance, total_earned, updated_at) VALUES(?,?,?,CURRENT_TIMESTAMP) "
            "ON CONFLICT(user_id) DO UPDATE SET "
            "balance=balance+excluded.balance, total_earned=total_earned+excluded.total_earned, updated_at=CURRENT_TIMESTAMP",
            (referrer_db_id, bonus, bonus)
        )
        await db.commit()

        referrer_user = await _fetchone(db, "SELECT * FROM users WHERE id=?", (referrer_db_id,))
        ch = await _fetchone(db, "SELECT title, username FROM channels WHERE id=?", (channel_id,))

        return {
            "referrer_tg_id":  ref["referrer_tg_id"],
            "referrer_name":   ref["referrer_name"] or str(ref["referrer_tg_id"]),
            "referred_tg_id":  referred_tg_id,
            "referred_name":   ref_d.get("first_name") or str(referred_tg_id),
            "ch_title":        ch["title"] if ch else str(channel_id),
            "ch_uname":        (ch["username"] if ch else None) or "",
            "bonus":           bonus,
            "percent":         int(percent * 100),
            "is_first":        is_first,
        }


async def check_autorenew_subscriptions() -> list:
    """
    Check channels with autorenew_enabled=True that expire within 1 hour.
    Deducts from user balance and renews for 30 days.
    Called every 30 minutes by scheduler.
    """
    from datetime import datetime, timezone, timedelta
    import os as _os
    slot_price = float(_os.getenv("SLOT_PRICE", "2.0"))
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    cutoff = (now + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        rows = await _fetchall(db, """
            SELECT c.*, u.telegram_id as owner_tg_id
            FROM channels c
            JOIN users u ON u.id = c.user_id
            WHERE c.subscription_status IN ('active', 'trial')
            AND c.subscription_end IS NOT NULL
            AND c.subscription_end <= ?
        """, (cutoff,))

    renewed = []
    for ch in rows:
        try:
            ch_settings = json.loads(ch.get("settings") or "{}")
            if not ch_settings.get("autorenew_enabled"):
                continue  # Only process channels with auto-renew enabled

            tg_id = ch["owner_tg_id"]
            balance = await get_user_balance(tg_id)

            if balance < slot_price:
                log.info(f"Autorenew ch={ch['id']}: insufficient balance ${balance:.2f} < ${slot_price:.2f}")
                # Notify user about insufficient funds
                if _bot_instance:
                    try:
                        await _bot_instance.send_message(
                            tg_id,
                            f"⚠️ <b>Автопродовження підписки</b>\n\nКанал: @{ch.get('username') or ch['id']}\n\n"
                            f"На балансі <b>${balance:.2f}</b>, потрібно <b>${slot_price:.2f}</b>.\n"
                            f"Поповніть баланс у Mini App, щоб уникнути зупинки автопостингу.",
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass
                continue

            # Deduct price from balance
            new_bal = await adjust_user_balance(tg_id, -slot_price)

            # Log transaction
            async with aiosqlite.connect(DB_PATH, timeout=10) as db:
                user_row = await _fetchone(db, "SELECT id FROM users WHERE telegram_id=?", (tg_id,))
                if user_row:
                    await db.execute(
                        "INSERT INTO transactions(user_id, channel_id, amount, type, description) VALUES(?,?,?,?,?)",
                        (user_row["id"], ch["id"], -slot_price, "subscription",
                         f"Автопродовження @{ch.get('username') or ch['id']} · 30д")
                    )
                    await db.commit()

            # Renew subscription
            await activate_subscription(ch["id"], 30)
            renewed.append(ch["id"])
            log.info(f"Autorenew ch={ch['id']}: renewed 30d, balance now ${new_bal:.2f}")

            # Trigger referral bonus
            try:
                await process_referral_bonus(tg_id, ch["id"], slot_price)
            except Exception:
                pass

            # Notify user about successful renewal
            if _bot_instance:
                try:
                    await _bot_instance.send_message(
                        tg_id,
                        f"✅ <b>Підписку автоматично продовжено!</b>\n\n"
                        f"Канал: @{ch.get('username') or ch['id']}\n"
                        f"Списано: <b>${slot_price:.2f}</b>\n"
                        f"Залишок балансу: <b>${new_bal:.2f}</b>",
                        parse_mode="HTML"
                    )
                except Exception:
                    pass

        except Exception as e:
            log.error(f"Autorenew ch={ch.get('id')}: {e}")

    return renewed


async def get_referral_stats(telegram_id: int) -> dict:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        user = await _fetchone(db, "SELECT id FROM users WHERE telegram_id=?", (telegram_id,))
        if not user:
            return {"total": 0, "paid": 0, "earned": 0.0, "balance": 0.0}
        user_id = user["id"]

        total = (await _fetchone(db, "SELECT COUNT(*) as c FROM referrals WHERE referrer_id=?", (user_id,)))["c"]

        paid_cnt = (await _fetchone(db, "SELECT COUNT(*) as c FROM referrals WHERE referrer_id=? AND status='paid'", (user_id,)))["c"]

        earned_row = await _fetchone(db, "SELECT COALESCE(SUM(bonus_amount),0) as s FROM referrals WHERE referrer_id=? AND status='paid'", (user_id,))
        earned = earned_row["s"] if earned_row else 0.0

        bal_row = await _fetchone(db, "SELECT balance FROM user_balances WHERE user_id=?", (user_id,))
        balance = bal_row["balance"] if bal_row else 0.0

        return {"total": total, "paid": paid_cnt, "earned": earned, "balance": balance}


# ─── MARKETPLACE ──────────────────────────────────────────────────────────────

async def get_marketplace_channels() -> list:
    """Get all channels that opted into the marketplace catalog."""
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        rows = await _fetchall(db, """
            SELECT c.id, c.title, c.username, c.category,
                   c.subscription_status, c.settings, c.chat_id
            FROM channels c
            WHERE c.subscription_status IN ('active', 'trial')
            ORDER BY c.created_at DESC
        """)
    result = []
    now_ts = datetime.now(timezone.utc).isoformat()
    for r in rows:
        s = json.loads(r.get("settings") or "{}")
        if not s.get("is_listed"):
            continue
        subs_count = s.get("subscribers_count") or 0
        photo_url = None
        chat_id = r.get("chat_id")
        need_refresh = False
        last_check = s.get("subs_checked_at") or ""
        if last_check:
            try:
                elapsed = (datetime.now(timezone.utc) - datetime.fromisoformat(last_check)).total_seconds()
                if elapsed > 86400:
                    need_refresh = True
            except Exception:
                need_refresh = True
        else:
            need_refresh = True
        if _bot_instance and chat_id:
            if need_refresh:
                try:
                    subs_count = await _bot_instance.get_chat_member_count(chat_id)
                    # Save to DB so we don't re-fetch for 24h
                    s["subscribers_count"] = subs_count
                    s["subs_checked_at"] = now_ts
                    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
                        await db.execute("UPDATE channels SET settings=? WHERE id=?",
                                         (json.dumps(s), r["id"]))
                        await db.commit()
                except Exception:
                    pass
            try:
                chat = await _bot_instance.get_chat(chat_id)
                if chat.photo:
                    photo_url = f"/api/marketplace/photo/{r['id']}"
            except Exception:
                pass
        result.append({
            "id": r["id"],
            "title": r.get("title") or "",
            "username": r.get("username") or "",
            "category": r.get("category") or "general",
            "channel_lang": s.get("channel_lang") or "",
            "subscribers_count": subs_count,
            "price_per_post": s.get("price_per_post") or 0,
            "accept_paid_ads": bool(s.get("accept_paid_ads")),
            "accept_crosspromo": bool(s.get("accept_crosspromo")),
            "contact_username": s.get("contact_username") or "",
            "channel_link": s.get("channel_link") or "",
            "monetization_info": s.get("monetization_info") or "",
            "photo_url": photo_url,
        })
    return result


# ─── ADMIN ────────────────────────────────────────────────────────────────────

async def get_admin_stats() -> dict:
    # Calculate days_running outside try block so it always works
    try:
        days = (date.today() - date.fromisoformat(BOT_LAUNCH_DATE)).days if BOT_LAUNCH_DATE else 0
    except Exception:
        days = 0
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            total_users = (await _fetchone(db, "SELECT COUNT(*) as c FROM users"))["c"]
            today = date.today().isoformat()
            new_today = (await _fetchone(db, "SELECT COUNT(*) as c FROM users WHERE DATE(created_at)=?", (today,)))["c"]

            total_channels = (await _fetchone(db, "SELECT COUNT(*) as c FROM channels"))["c"]

            statuses_rows = await _fetchall(db, "SELECT subscription_status, COUNT(*) as c FROM channels GROUP BY subscription_status")
            channels_by_status = {r["subscription_status"]: r["c"] for r in statuses_rows}

            total_published = (await _fetchone(db, "SELECT COUNT(*) as c FROM processed_posts WHERE status='published'"))["c"]

            try:
                total_revenue_row = await _fetchone(db, "SELECT COALESCE(SUM(amount),0) as s FROM transactions WHERE type='subscription'")
                total_revenue = total_revenue_row["s"] if total_revenue_row else 0.0
            except Exception:
                total_revenue = 0.0

            try:
                total_referral_paid_row = await _fetchone(db, "SELECT COALESCE(SUM(bonus_amount),0) as s FROM referrals WHERE status='paid'")
                total_referral_paid = total_referral_paid_row["s"] if total_referral_paid_row else 0.0
            except Exception:
                total_referral_paid = 0.0

            try:
                total_ai_tokens_row = await _fetchone(db, "SELECT COALESCE(SUM(tokens_used),0) as s FROM ai_usage")
                total_ai_tokens = total_ai_tokens_row["s"] if total_ai_tokens_row else 0
            except Exception:
                total_ai_tokens = 0

            return {
                "total_users":        total_users,
                "new_today":          new_today,
                "total_channels":     total_channels,
                "channels_by_status": channels_by_status,
                "total_published":    total_published,
                "total_revenue":      total_revenue,
                "total_referral_paid": total_referral_paid,
                "total_ai_tokens":    total_ai_tokens,
                "ai_provider":        AI_PROVIDER,
                "ai_model":           GROQ_MODEL if AI_PROVIDER == "groq" else
                                      OPENAI_MODEL if AI_PROVIDER == "openai" else GEMINI_MODEL,
                "crypto_mode":        "testnet" if CRYPTO_TESTNET else "live",
                "days_running":       days,
            }
    except Exception as e:
        log.error(f"get_admin_stats failed: {e}")
        return {
            "total_users": 0, "new_today": 0, "total_channels": 0,
            "channels_by_status": {}, "total_published": 0,
            "total_revenue": 0.0, "total_referral_paid": 0.0,
            "total_ai_tokens": 0, "ai_provider": AI_PROVIDER,
            "ai_model": "", "crypto_mode": "testnet", "days_running": days,
        }


async def get_user_balance(telegram_id: int) -> float:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        user = await _fetchone(db, "SELECT id FROM users WHERE telegram_id=?", (telegram_id,))
        if not user:
            return 0.0
        row = await _fetchone(db, "SELECT balance FROM user_balances WHERE user_id=?", (user["id"],))
        return row["balance"] if row else 0.0


async def adjust_user_balance(telegram_id: int, delta: float) -> float:
    """Adjust user balance by delta (positive or negative). Returns new balance."""
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        user = await _fetchone(db, "SELECT id FROM users WHERE telegram_id=?", (telegram_id,))
        if not user:
            return 0.0
        user_id = user["id"]
        await db.execute(
            "INSERT INTO user_balances(user_id, balance, total_earned, updated_at) VALUES(?,MAX(0,?),MAX(0,?),CURRENT_TIMESTAMP) "
            "ON CONFLICT(user_id) DO UPDATE SET "
            "balance=MAX(0, balance+?), "
            "total_earned=CASE WHEN ? > 0 THEN total_earned+? ELSE total_earned END, "
            "updated_at=CURRENT_TIMESTAMP",
            (user_id, delta, delta if delta > 0 else 0, delta, delta, delta)
        )
        await db.commit()
        row = await _fetchone(db, "SELECT balance FROM user_balances WHERE user_id=?", (user_id,))
        return row["balance"] if row else 0.0


async def get_pending_withdrawals() -> List[dict]:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        rows = await _fetchall(db, "SELECT wr.*, u.telegram_id, u.username FROM withdrawal_requests wr JOIN users u ON u.id=wr.user_id WHERE wr.status='pending' ORDER BY wr.created_at ASC")
        return rows


async def process_withdrawal(wid: int, approved: bool, admin_note: str = ""):
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        status = "approved" if approved else "rejected"
        await db.execute(
            "UPDATE withdrawal_requests SET status=?, admin_note=? WHERE id=?",
            (status, admin_note, wid)
        )
        await db.commit()


async def log_event(event: str, level: str = "INFO"):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute(
                "INSERT INTO logs(event, level) VALUES(?,?)", (event, level)
            )
            await db.commit()
    except Exception:
        pass


# ─── AUTOPOST ─────────────────────────────────────────────────────────────────

async def get_channels_for_autopost() -> List[dict]:
    """Get channels with autopost enabled and active subscription."""
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        rows = await _fetchall(db, "SELECT * FROM channels WHERE subscription_status IN ('active','trial')")
        result = []
        for r in rows:
            ch = dict(r)
            try:
                settings = json.loads(ch.get("settings") or "{}")
            except Exception:
                settings = {}
            if settings.get("autopost_enabled"):
                ch["_settings"] = settings
                result.append(ch)
        return result


async def should_autopost_now(channel: dict) -> bool:
    """Check if current time is within autopost window and interval elapsed."""
    status = channel.get("subscription_status", "")
    if status not in ("active", "trial"):
        log.debug(f"  ch={channel['id']}: autopost blocked — status={status}")
        return False
    settings = channel.get("_settings", {})
    ppd = int(settings.get("autopost_ppd", 1))
    time_from = settings.get("autopost_from", "00:00")
    time_to   = settings.get("autopost_to",   "23:59")

    from datetime import timezone
    import os
    tz_offset = int(os.getenv("TIMEZONE_OFFSET", "0"))
    now_utc = datetime.now(timezone.utc)
    now = now_utc + __import__('datetime').timedelta(hours=tz_offset)
    now_str = now.strftime("%H:%M")

    # Normalize HH:MM — fix string comparison bug ("9:00" vs "14:20")
    def _nt(t: str) -> str:
        p = t.strip().split(":")
        return f"{int(p[0]):02d}:{p[1] if len(p)>1 else '00'}"

    if _nt(now_str) < _nt(time_from) or _nt(now_str) > _nt(time_to):
        log.info(f"Autopost ch={channel['id']}: outside window {time_from}-{time_to} (now={now_str})")
        return False

    # Check interval
    pending = await count_pending_posts(channel["id"])
    if pending == 0:
        log.info(f"Autopost ch={channel['id']}: no pending posts in queue")
        return False

    # Calculate interval in minutes based on ppd spread across window
    from_h, from_m = map(int, time_from.split(":"))
    to_h,   to_m   = map(int, time_to.split(":"))
    window_min = (to_h * 60 + to_m) - (from_h * 60 + from_m)
    if window_min <= 0:
        window_min = 60
    interval_min = max(1, window_min // max(ppd, 1))

    lp = await get_last_published(channel["id"])
    if lp:
        try:
            last_time = datetime.fromisoformat(lp["published_at"])
            now_for_elapsed = datetime.now(timezone.utc).replace(tzinfo=None)
            elapsed = (now_for_elapsed - last_time).total_seconds() / 60
            if elapsed < interval_min:
                log.info(f"Autopost ch={channel['id']}: interval not elapsed ({elapsed:.0f}m/{interval_min}m), next in {interval_min - elapsed:.0f}m")
                return False
        except Exception:
            pass

    return True


# ─── PUBLIC STATS ─────────────────────────────────────────────────────────────

async def get_public_stats() -> dict:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        total_users = (await _fetchone(db, "SELECT COUNT(*) as c FROM users"))["c"]
        total_channels = (await _fetchone(db, "SELECT COUNT(*) as c FROM channels"))["c"]
        active_subs = (await _fetchone(db, "SELECT COUNT(*) as c FROM channels WHERE subscription_status='active'"))["c"]
        total_published = (await _fetchone(db, "SELECT COUNT(*) as c FROM processed_posts WHERE status='published'"))["c"]

    try:
        launch = date.fromisoformat(BOT_LAUNCH_DATE)
        days_running = (date.today() - launch).days
    except Exception:
        days_running = 0

    return {
        "days_running":    days_running,
        "total_users":     total_users,
        "total_channels":  total_channels,
        "active_subs":     active_subs,
        "total_published": total_published,
    }