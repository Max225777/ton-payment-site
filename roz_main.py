"""roz parser — orchestrator.

Usage:
    python roz_main.py                # full pipeline: crawl → enrich → extract → save
    python roz_main.py --no-ai        # skip AI, regex only
    python roz_main.py --export csv   # export existing DB to CSV and exit
    python roz_main.py --export json  # export to JSON and exit
    python roz_main.py --dry-run      # print results without saving
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys

import roz_storage as storage
from roz_config import REQUEST_DELAY
from roz_scraper import crawl_all, enrich_with_about
from roz_ai_filter import extract_contacts, regex_contacts


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


async def run(use_ai: bool, dry_run: bool) -> int:
    storage.init_db()
    log = logging.getLogger("roz.main")

    cards = await crawl_all()
    if not cards:
        log.warning("no cards scraped — check anti-bot / network")
        return 0

    cards = await enrich_with_about(cards)

    total_contacts = 0
    for i, card in enumerate(cards):
        if not card.about:
            continue
        if use_ai:
            card.extracted_contacts = await extract_contacts(card.about)
        else:
            card.extracted_contacts = regex_contacts(card.about)
        total_contacts += len(card.extracted_contacts)
        if (i + 1) % 25 == 0:
            log.info("processed %s/%s cards", i + 1, len(cards))

    log.info("extracted %s contacts across %s channels", total_contacts, len(cards))

    if dry_run:
        for c in cards:
            if c.extracted_contacts:
                print(f"{c.title or c.username} ({c.tgstat_url}): {c.extracted_contacts}")
        return len(cards)

    saved = storage.save_all(cards)
    log.info("saved %s rows to %s", saved, storage.DB_PATH)
    return saved


def main() -> int:
    p = argparse.ArgumentParser(prog="roz_main")
    p.add_argument("--no-ai", action="store_true", help="skip AI, regex only")
    p.add_argument("--dry-run", action="store_true", help="do not write to DB")
    p.add_argument("--export", choices=["csv", "json"], help="export DB and exit")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    _setup_logging(args.verbose)

    if args.export:
        storage.init_db()
        if args.export == "csv":
            n = storage.export_csv()
            print(f"exported {n} rows → roz_results.csv")
        else:
            n = storage.export_json()
            print(f"exported {n} rows → roz_results.json")
        return 0

    return asyncio.run(run(use_ai=not args.no_ai, dry_run=args.dry_run))


if __name__ == "__main__":
    sys.exit(main() and 0)
