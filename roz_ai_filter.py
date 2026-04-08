"""roz parser — AI-backed contact extractor.

Given channel description text, returns a list of Telegram usernames or
contact links that likely belong to the owner / admin. Combines:

1. fast regex sweep (catches obvious @username, t.me/xxx, https://t.me/+invite)
2. AI fallback — feeds the cleaned text to a Groq/OpenAI-compatible chat
   completion and asks it to return ONLY the JSON array of contacts.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Iterable

import httpx

from roz_config import AI_API_KEY, AI_BASE_URL, AI_MODEL, HTTP_TIMEOUT

log = logging.getLogger("roz.ai")

# ── Regex pass ───────────────────────────────────────────────────────────────
_USERNAME_RE = re.compile(r"(?<![A-Za-z0-9_@/])@([A-Za-z][A-Za-z0-9_]{3,31})")
_TME_RE      = re.compile(r"(?:https?://)?t\.me/(\+?[A-Za-z0-9_\-]+)", re.IGNORECASE)
_URL_TG_RE   = re.compile(r"(?:https?://)?telegram\.(?:me|dog)/(\+?[A-Za-z0-9_\-]+)", re.IGNORECASE)

# Usernames that are obviously NOT contacts (channels themselves, bots, etc.)
_BLACKLIST = {
    "telegram", "telegramchannel", "durov", "tgstat", "tgstat_bot",
    "share", "joinchat", "addstickers", "proxy",
}


def regex_contacts(text: str) -> list[str]:
    if not text:
        return []
    found: list[str] = []
    for m in _USERNAME_RE.finditer(text):
        u = m.group(1).lower()
        if u not in _BLACKLIST:
            found.append("@" + u)
    for m in _TME_RE.finditer(text):
        u = m.group(1)
        if u.lower() in _BLACKLIST:
            continue
        # invite links kept as-is, plain usernames normalized to @u
        if u.startswith("+"):
            found.append("t.me/" + u)
        else:
            found.append("@" + u.lower())
    for m in _URL_TG_RE.finditer(text):
        u = m.group(1)
        if u.lower() in _BLACKLIST:
            continue
        found.append("@" + u.lower().lstrip("+"))
    # Dedup preserving order
    seen = set()
    out = []
    for c in found:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


# ── AI pass ──────────────────────────────────────────────────────────────────
_SYS = (
    "You extract owner/admin contacts from a Telegram channel description. "
    "Return ONLY a JSON array of strings with Telegram usernames (format @name) "
    "and/or invite links (t.me/+...). Ignore generic promo links, sponsors, and "
    "third-party references. If nothing fits, return []."
)


async def ai_contacts(text: str, model: str = AI_MODEL) -> list[str]:
    if not AI_API_KEY:
        log.debug("AI_API_KEY not set, skipping AI pass")
        return []
    if not text or len(text.strip()) < 10:
        return []
    payload = {
        "model": model,
        "temperature": 0,
        "max_tokens": 256,
        "messages": [
            {"role": "system", "content": _SYS},
            {"role": "user",   "content": text[:4000]},
        ],
    }
    headers = {
        "Authorization": f"Bearer {AI_API_KEY}",
        "Content-Type":  "application/json",
    }
    url = AI_BASE_URL.rstrip("/") + "/chat/completions"
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            r = await client.post(url, json=payload, headers=headers)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        log.warning("AI call failed: %s", e)
        return []
    try:
        content = data["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError):
        return []
    # Model sometimes wraps JSON in markdown fences
    content = re.sub(r"^```(?:json)?", "", content).rstrip("`").strip()
    try:
        arr = json.loads(content)
    except Exception:
        # Fallback: pull tokens that look like contacts from the raw reply
        return regex_contacts(content)
    if not isinstance(arr, list):
        return []
    return [str(x).strip() for x in arr if isinstance(x, (str, int))]


def merge_contacts(*lists: Iterable[str]) -> list[str]:
    seen = set()
    out: list[str] = []
    for lst in lists:
        for c in lst or []:
            k = c.lower().lstrip("@")
            if k in seen or not k:
                continue
            seen.add(k)
            out.append(c)
    return out


async def extract_contacts(text: str) -> list[str]:
    rx = regex_contacts(text)
    ai = await ai_contacts(text)
    return merge_contacts(rx, ai)
