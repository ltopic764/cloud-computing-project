# Using Algolia API

import logging
import time
import os
from datetime import datetime, timezone, timedelta

import requests

logger = logging.getLogger(__name__)

# Base URL address Algolia HN API
ALGOLIA_BASE = "https://hn.algolia.com/api/v1/search_by_date"

# Maximum number of results by page
HITS_PER_PAGE = 1000

# Delay between requests
REQUESTS_DELAY = 0.2

# Item types that we want to receive
ITEM_TYPES = ["story", "ask_hn", "comment", "job", "poll"]

def get_yesterday_timestamps() -> tuple[int, int]:
    """
    Returns Unix timestamp for beginning and end of yesterdays day
    """
    todays_utc = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # yesterday
    yesterday_utc = todays_utc - timedelta(days=1)

    starts_ts = int(yesterday_utc.timestamp())
    end_ts = int(todays_utc.timestamp()) - 1

    logger.info(f"Fetching for period: {yesterday_utc.date()} ({starts_ts} - {end_ts})")

    return starts_ts, end_ts

def fetch_page(item_type: str, start_ts: int, end_ts: int, page: int, session: requests.Session) -> dict | None:
    """
    Returns result page for a specific item_type and time period
    """
    params = {
        # tags filters by item type
        "tags": item_type,

        # numericFilters filters by Uninx timestamp of creation
        "numericFilters": f"created_at_i>={start_ts},created_at_i<={end_ts}",

        "hitsPerPage": HITS_PER_PAGE,

        # begin from page 0
        "page": page,
    }

    try:
        # GET request with timeout, otherwise lambda waits forever
        response = session.get(ALGOLIA_BASE, params=params, timeout=10)

        # If status code 4xx,5xx error
        response.raise_for_status()

        return response.join()
    
    except requests.exceptions.Timeout:
        logger.warning(f"Timeout for {item_type}, page {page}, skipping")
        return None
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching {item_type} page {page}: {e}")
        return None
    
def fetch_items_for_type(item_type: str, session: requests.Session) -> list[dict]:
    """
    Return all items specific item_type published yesterday on all pages
    """
    start_ts, end_ts = get_yesterday_timestamps()

    all_items = [] # all items through pages
    page = 0

    while True:
        logger.info(f"Fetching {item_type}: page {page}")

        # delay between requests
        if page > 0:
            time.sleep(REQUESTS_DELAY)
        
        response = fetch_page(item_type, start_ts, end_ts, page, session)

        if response is None:
            logger.warning(f"Stopping pagination for {item_type} on page {page}")
            break

        # Get list from response
        hits = response.get("hits", [])

        # If the page is empty we made it to the end
        if not hits:
            logger.info(f"No more items for {item_type}, ending on page {page}")
            break

        all_items.extend(hits)

        total_pages = response.get("nbPages", 1)

        # If on last page, stop
        if page >= total_pages - 1:
            logger.info(f"Last page for {item_type}: {page+1}/{total_pages}")
            break

        # Next page
        page += 1
    
    logger.info(f"Got {item_type}: {len(all_items)} items")
    return all_items

def fetch_all(session: requests.Session) -> dict[str, list[dict]]:
    """
    Main function that GETs all item types for yesterdays date
    """
    results = {}

    for item_type in ITEM_TYPES:
        logger.info(f"Starting by fetching type: {item_type}")
        results[item_type] = fetch_items_for_type(item_type, session)

    # Log summary
    total = sum(len(items) for items in results.values())
    logger.info(f"Completed fetching. Total items: {total}")
    for item_type, items in results.items():
        logger.info(f"  {item_type}: {len(items)}")

    return results
