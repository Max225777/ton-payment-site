"""roz parser — tgstat.ru scraper.

Scrapes channel listings from https://tgstat.ru/channels/search and extracts
per-channel description/about text which usually contains the owner's contact.

Notes:
- tgstat uses a server-rendered listing plus an XHR "load more" endpoint.
- This scraper is polite: it sleeps REQUEST_DELAY between pages and uses a
  real browser User-Agent. Still, tgstat may rate-limit — use a proxy if so.
"""
from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from typing import Iterable, Optional
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from roz_config import (
    SEARCH_URL, TGSTAT_BASE, USER_AGENT, REQUEST_DELAY,
    CHANNEL_DETAIL_DELAY, HTTP_TIMEOUT, MAX_PAGES, PROXY,
    CATEGORIES, COUNTRIES,
)

log = logging.getLogger("roz.scraper")


@dataclass
class ChannelCard:
    tgstat_url: str
    username: str = ""
    title: str = ""
    subscribers: int = 0
    category: str = ""
    about: str = ""
    raw_html_snippet: str = ""
    extracted_contacts: list = field(default_factory=list)


def _headers() -> dict:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru,en;q=0.8,uk;q=0.6",
        "Referer": TGSTAT_BASE + "/",
    }


def _make_client() -> httpx.AsyncClient:
    kwargs = dict(
        headers=_headers(),
        timeout=HTTP_TIMEOUT,
        follow_redirects=True,
    )
    if PROXY:
        kwargs["proxy"] = PROXY
    return httpx.AsyncClient(**kwargs)


_SUBS_RE = re.compile(r"([\d\s.,]+)\s*([KkКкMmМм]?)")


def _parse_subs(txt: str) -> int:
    if not txt:
        return 0
    txt = txt.strip().replace("\xa0", " ")
    m = _SUBS_RE.search(txt)
    if not m:
        return 0
    num_raw = m.group(1).replace(" ", "").replace(",", ".")
    suf = m.group(2).lower()
    try:
        n = float(num_raw)
    except ValueError:
        return 0
    if suf in ("k", "к"):
        n *= 1_000
    elif suf in ("m", "м"):
        n *= 1_000_000
    return int(n)


def _parse_listing_html(html: str) -> list[ChannelCard]:
    soup = BeautifulSoup(html, "html.parser")
    cards: list[ChannelCard] = []
    # tgstat listing cards live inside .card.peer-item or similar — scan all
    # links pointing to /channel/@username (their canonical format).
    seen = set()
    for a in soup.select("a[href*='/channel/']"):
        href = a.get("href") or ""
        if not href.startswith("/channel/") and "tgstat.ru/channel/" not in href:
            continue
        full = urljoin(TGSTAT_BASE, href.split("?")[0].rstrip("/"))
        if full in seen:
            continue
        seen.add(full)
        uname = ""
        m = re.search(r"/channel/(@?[\w\-+]+)", full)
        if m:
            uname = m.group(1).lstrip("@")
        # Walk up to the card container for richer extraction
        card_el = a.find_parent(class_=re.compile(r"(card|peer-item|channel-item)"))
        title = ""
        subs  = 0
        about = ""
        if card_el:
            t_el = card_el.find(class_=re.compile(r"(title|channel-name|peer-name)"))
            if t_el:
                title = t_el.get_text(" ", strip=True)
            s_el = card_el.find(class_=re.compile(r"(subscribers|sub-count|participants)"))
            if s_el:
                subs = _parse_subs(s_el.get_text(" ", strip=True))
            d_el = card_el.find(class_=re.compile(r"(text|desc|about|bio)"))
            if d_el:
                about = d_el.get_text(" ", strip=True)
        if not title:
            title = a.get_text(" ", strip=True)
        cards.append(ChannelCard(
            tgstat_url=full,
            username=uname,
            title=title,
            subscribers=subs,
            about=about,
            raw_html_snippet=str(card_el)[:4000] if card_el else "",
        ))
    return cards


async def _fetch(client: httpx.AsyncClient, url: str, params: Optional[dict] = None) -> str:
    log.debug("GET %s params=%s", url, params)
    r = await client.get(url, params=params)
    if r.status_code == 429:
        log.warning("rate-limited by tgstat (429), sleeping 30s")
        await asyncio.sleep(30)
        r = await client.get(url, params=params)
    r.raise_for_status()
    return r.text


async def fetch_listing(category: str = "", country: str = "ru",
                        max_pages: int = MAX_PAGES) -> list[ChannelCard]:
    """Fetch channel listing pages for a single (category, country) combo."""
    out: list[ChannelCard] = []
    async with _make_client() as client:
        for page in range(max_pages):
            params = {"country": country, "page": page}
            if category:
                params["category"] = category
            try:
                html = await _fetch(client, SEARCH_URL, params)
            except Exception as e:
                log.warning("listing page %s failed: %s", page, e)
                break
            batch = _parse_listing_html(html)
            if not batch:
                log.info("no more cards at page=%s (cat=%s country=%s)", page, category, country)
                break
            out.extend(batch)
            log.info("page %s: %s cards (total %s)", page, len(batch), len(out))
            await asyncio.sleep(REQUEST_DELAY)
    return out


async def fetch_channel_about(client: httpx.AsyncClient, card: ChannelCard) -> str:
    """Fetch full channel page and return description/about text."""
    try:
        html = await _fetch(client, card.tgstat_url)
    except Exception as e:
        log.warning("detail fetch failed for %s: %s", card.tgstat_url, e)
        return ""
    soup = BeautifulSoup(html, "html.parser")
    # Canonical description blocks on tgstat
    candidates = []
    for sel in [
        "div.channel-description",
        "div.channel-description-text",
        "div.about",
        "div.card .text",
        "meta[name='description']",
        "meta[property='og:description']",
    ]:
        for el in soup.select(sel):
            if el.name == "meta":
                txt = el.get("content") or ""
            else:
                txt = el.get_text(" ", strip=True)
            if txt:
                candidates.append(txt.strip())
    # Dedup while preserving order
    seen = set()
    merged = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            merged.append(c)
    return "\n\n".join(merged).strip()


async def enrich_with_about(cards: Iterable[ChannelCard]) -> list[ChannelCard]:
    cards = list(cards)
    async with _make_client() as client:
        for i, card in enumerate(cards):
            if card.about and len(card.about) > 40:
                continue  # listing already gave us enough
            txt = await fetch_channel_about(client, card)
            if txt:
                card.about = (card.about + "\n\n" + txt).strip() if card.about else txt
            await asyncio.sleep(CHANNEL_DETAIL_DELAY)
            if (i + 1) % 10 == 0:
                log.info("enriched %s/%s", i + 1, len(cards))
    return cards


async def crawl_all() -> list[ChannelCard]:
    categories = CATEGORIES or [""]  # "" = all categories
    all_cards: list[ChannelCard] = []
    dedup: set = set()
    for cat in categories:
        for country in COUNTRIES or ["ru"]:
            batch = await fetch_listing(category=cat, country=country)
            for c in batch:
                if c.tgstat_url in dedup:
                    continue
                dedup.add(c.tgstat_url)
                all_cards.append(c)
    log.info("total unique cards: %s", len(all_cards))
    return all_cards
