"""roz parser — configuration.

Environment variables:
  ROZ_AI_API_KEY      — Groq / OpenAI-compatible API key
  ROZ_AI_BASE_URL     — base URL, default https://api.groq.com/openai/v1
  ROZ_AI_MODEL        — model name, default llama-3.3-70b-versatile
  ROZ_DB_PATH         — sqlite db path for results, default roz_results.db
  ROZ_USER_AGENT      — browser UA string
  ROZ_REQUEST_DELAY   — seconds between requests (politeness), default 1.5
  ROZ_MAX_PAGES       — how many list pages to crawl, default 5
  ROZ_CATEGORIES      — comma-separated tgstat categories (slugs), empty = all
  ROZ_COUNTRIES       — comma-separated tgstat countries, default ru
  ROZ_PROXY           — optional http/https proxy url
"""
import os

AI_API_KEY     = os.getenv("ROZ_AI_API_KEY", "")
AI_BASE_URL    = os.getenv("ROZ_AI_BASE_URL", "https://api.groq.com/openai/v1")
AI_MODEL       = os.getenv("ROZ_AI_MODEL", "llama-3.3-70b-versatile")

DB_PATH        = os.getenv("ROZ_DB_PATH", "roz_results.db")

USER_AGENT     = os.getenv(
    "ROZ_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
)
REQUEST_DELAY  = float(os.getenv("ROZ_REQUEST_DELAY", "1.5"))
MAX_PAGES      = int(os.getenv("ROZ_MAX_PAGES", "5"))

CATEGORIES     = [c.strip() for c in os.getenv("ROZ_CATEGORIES", "").split(",") if c.strip()]
COUNTRIES      = [c.strip() for c in os.getenv("ROZ_COUNTRIES", "ru").split(",") if c.strip()]

PROXY          = os.getenv("ROZ_PROXY", "") or None

TGSTAT_BASE    = "https://tgstat.ru"
SEARCH_URL     = TGSTAT_BASE + "/channels/search"

# Pacing / limits
CHANNEL_DETAIL_DELAY = float(os.getenv("ROZ_DETAIL_DELAY", "1.0"))
HTTP_TIMEOUT         = float(os.getenv("ROZ_HTTP_TIMEOUT", "20"))
