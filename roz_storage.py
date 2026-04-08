"""roz parser — SQLite persistence + CSV/JSON export."""
from __future__ import annotations

import csv
import json
import logging
import sqlite3
from typing import Iterable

from roz_config import DB_PATH

log = logging.getLogger("roz.storage")


SCHEMA = """
CREATE TABLE IF NOT EXISTS channels (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    tgstat_url    TEXT UNIQUE NOT NULL,
    username      TEXT,
    title         TEXT,
    subscribers   INTEGER DEFAULT 0,
    category      TEXT,
    about         TEXT,
    contacts_json TEXT,
    updated_at    TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_channels_username ON channels(username);
CREATE INDEX IF NOT EXISTS idx_channels_subs     ON channels(subscribers);
"""


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def init_db() -> None:
    with _conn() as c:
        c.executescript(SCHEMA)


def upsert_channel(card) -> None:
    with _conn() as c:
        c.execute(
            """
            INSERT INTO channels(tgstat_url, username, title, subscribers, category, about, contacts_json)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(tgstat_url) DO UPDATE SET
                username      = excluded.username,
                title         = excluded.title,
                subscribers   = excluded.subscribers,
                category      = excluded.category,
                about         = excluded.about,
                contacts_json = excluded.contacts_json,
                updated_at    = CURRENT_TIMESTAMP
            """,
            (
                card.tgstat_url,
                card.username,
                card.title,
                int(card.subscribers or 0),
                card.category or "",
                card.about or "",
                json.dumps(card.extracted_contacts or [], ensure_ascii=False),
            ),
        )


def save_all(cards: Iterable) -> int:
    n = 0
    for card in cards:
        try:
            upsert_channel(card)
            n += 1
        except Exception as e:
            log.warning("upsert failed for %s: %s", getattr(card, "tgstat_url", "?"), e)
    return n


def export_csv(path: str = "roz_results.csv") -> int:
    with _conn() as c, open(path, "w", encoding="utf-8", newline="") as f:
        rows = c.execute(
            "SELECT tgstat_url, username, title, subscribers, category, about, contacts_json "
            "FROM channels ORDER BY subscribers DESC"
        ).fetchall()
        w = csv.writer(f)
        w.writerow(["tgstat_url", "username", "title", "subscribers", "category", "about", "contacts"])
        for r in rows:
            w.writerow([
                r["tgstat_url"], r["username"], r["title"], r["subscribers"],
                r["category"], r["about"], r["contacts_json"],
            ])
        return len(rows)


def export_json(path: str = "roz_results.json") -> int:
    with _conn() as c:
        rows = [dict(r) for r in c.execute("SELECT * FROM channels").fetchall()]
    for r in rows:
        try:
            r["contacts"] = json.loads(r.pop("contacts_json") or "[]")
        except Exception:
            r["contacts"] = []
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    return len(rows)
