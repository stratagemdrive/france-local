"""
france_news.py
Fetches France-focused news from English-language RSS feeds, categorizes
stories, and writes them to docs/france_news.json — capped at 20 per
category, max age 7 days, oldest entries replaced first.
No external APIs are used. All sources publish in English.
"""

import json
import os
import re
import time
import logging
from datetime import datetime, timezone, timedelta
from dateutil import parser as dateparser
import feedparser

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OUTPUT_DIR = "docs"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "france_news.json")
MAX_PER_CATEGORY = 20
MAX_AGE_DAYS = 7
CATEGORIES = ["Diplomacy", "Military", "Energy", "Economy", "Local Events"]

# RSS feeds — all free, English-language, France-focused, no APIs
FEEDS = [
    # France 24 English — France's international broadcaster
    {"source": "France 24", "url": "https://www.france24.com/en/rss"},
    {"source": "France 24", "url": "https://www.france24.com/en/france/rss"},
    {"source": "France 24", "url": "https://www.france24.com/en/business-tech/rss"},
    {"source": "France 24", "url": "https://www.france24.com/en/europe/rss"},
    # RFI English — Radio France Internationale English service
    {"source": "RFI English", "url": "https://www.rfi.fr/en/france/rss"},
    {"source": "RFI English", "url": "https://www.rfi.fr/en/rss"},
    # The Guardian France section
    {"source": "The Guardian", "url": "https://www.theguardian.com/world/france/rss"},
    {"source": "The Guardian", "url": "https://www.theguardian.com/world/europe-news/rss"},
    # The Local France — English-language journalism from Paris
    {"source": "The Local France", "url": "https://feeds.thelocal.com/rss/fr"},
    # BBC News France topic feed
    {"source": "BBC News", "url": "https://feeds.bbci.co.uk/news/topics/c302m85qx9xt/rss.xml"},
    {"source": "BBC News", "url": "https://feeds.bbci.co.uk/news/world/europe/rss.xml"},
]

# ---------------------------------------------------------------------------
# Category keyword mapping (France-contextualised)
# ---------------------------------------------------------------------------

CATEGORY_KEYWORDS = {
    "Diplomacy": [
        "diplomacy", "diplomatic", "foreign policy", "embassy", "ambassador",
        "treaty", "bilateral", "multilateral", "nato", "united nations",
        "foreign minister", "foreign affairs", "summit", "sanctions",
        "international relations", "geopolitical", "european union", "eu",
        "trade deal", "g7", "g20", "macron", "barrot", "elysee",
        "quai d'orsay", "accord", "alliance", "envoy", "consul",
        "france and", "paris agreement", "french president", "french government",
        "france's role", "france's position", "french foreign",
    ],
    "Military": [
        "military", "army", "navy", "air force", "defence", "defense",
        "troops", "soldier", "weapons", "missile", "nuclear",
        "armed forces", "war", "combat", "deployment", "conflict",
        "nato", "intelligence", "spy", "dgse", "french military",
        "legion", "gendarmerie", "ukraine", "bomb", "airbase",
        "aircraft carrier", "charles de gaulle", "submarine",
        "french troops", "french army", "french forces",
    ],
    "Energy": [
        "energy", "nuclear power", "nuclear plant", "edf", "oil", "gas",
        "renewable", "solar", "wind", "electricity", "power grid",
        "net zero", "carbon", "climate", "fossil fuel", "emissions",
        "cop", "green energy", "energy transition", "energy price",
        "energy bill", "total energies", "totalenergies", "hydrogen",
        "battery", "electric vehicle", "decarboni", "power station",
        "energy security", "french electricity", "réacteur",
    ],
    "Economy": [
        "economy", "economic", "gdp", "inflation", "interest rate",
        "banque de france", "budget", "finance minister", "treasury",
        "tax", "unemployment", "jobs", "recession", "growth", "trade",
        "euro", "cac 40", "cac40", "fiscal", "spending", "debt",
        "deficit", "wage", "cost of living", "investment", "business",
        "exports", "imports", "productivity", "retail", "manufacturing",
        "french economy", "french budget", "le maire", "attal",
        "bayrou", "french finance", "austerity", "pension", "retraite",
    ],
    "Local Events": [
        "local", "region", "department", "mayor", "municipality",
        "city", "town", "community", "hospital", "school", "crime",
        "police", "court", "flood", "fire", "transport", "strike",
        "protest", "social care", "housing", "paris", "marseille",
        "lyon", "toulouse", "nice", "nantes", "strasbourg", "bordeaux",
        "lille", "montpellier", "rennes", "reims", "riot", "violence",
        "french court", "french police", "seine", "french election",
        "french parliament", "assemblée", "sénat", "corsica", "overseas",
        "french overseas", "guadeloupe", "martinique", "reunion",
        "immigration", "migrant", "far right", "rassemblement national",
        "le pen", "left wing", "right wing", "french politics",
    ],
}


def classify(title: str, description: str):
    """Return the best-matching category for a story, or None if no match."""
    text = (title + " " + (description or "")).lower()
    scores = {cat: 0 for cat in CATEGORIES}
    for cat, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if re.search(r'\b' + re.escape(kw) + r'\b', text):
                scores[cat] += 1
    best_cat = max(scores, key=scores.get)
    return best_cat if scores[best_cat] > 0 else None


def strip_html(text: str) -> str:
    """Remove HTML tags from a string."""
    return re.sub(r"<[^>]+>", "", text or "").strip()


def parse_date(entry):
    """Parse a feed entry's published date into a UTC-aware datetime."""
    raw = entry.get("published") or entry.get("updated") or entry.get("created")
    if not raw:
        struct = entry.get("published_parsed") or entry.get("updated_parsed")
        if struct:
            return datetime(*struct[:6], tzinfo=timezone.utc)
        return None
    try:
        dt = dateparser.parse(raw)
        if dt and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc) if dt else None
    except Exception:
        return None


def fetch_feed(feed_cfg: dict) -> list:
    """Fetch a single RSS feed and return a list of story dicts."""
    source = feed_cfg["source"]
    url = feed_cfg["url"]
    stories = []
    try:
        parsed = feedparser.parse(url)
        if parsed.bozo and not parsed.entries:
            log.warning("Bozo feed (%s): %s", source, url)
            return stories
        cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)
        for entry in parsed.entries:
            pub_date = parse_date(entry)
            if pub_date and pub_date < cutoff:
                continue
            title = strip_html(entry.get("title", "")).strip()
            desc = strip_html(entry.get("summary", "")).strip()
            if not title:
                continue
            category = classify(title, desc)
            if not category:
                continue
            story = {
                "title": title,
                "source": source,
                "url": entry.get("link", ""),
                "published_date": pub_date.isoformat() if pub_date else None,
                "category": category,
            }
            stories.append(story)
    except Exception as exc:
        log.error("Failed to fetch %s (%s): %s", source, url, exc)
    return stories


def load_existing() -> dict:
    """Load the current JSON file, grouped by category."""
    if not os.path.exists(OUTPUT_FILE):
        return {cat: [] for cat in CATEGORIES}
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, OSError):
        return {cat: [] for cat in CATEGORIES}

    grouped = {cat: [] for cat in CATEGORIES}
    stories = data.get("stories", data) if isinstance(data, dict) else data
    if isinstance(stories, list):
        for story in stories:
            cat = story.get("category")
            if cat in grouped:
                grouped[cat].append(story)
    return grouped


def merge(existing: dict, fresh: list) -> dict:
    """
    Merge fresh stories into the existing pool.
    - De-duplicate by URL.
    - Discard stories older than MAX_AGE_DAYS.
    - Replace oldest entries first when over MAX_PER_CATEGORY.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)

    existing_urls = set()
    for stories in existing.values():
        for s in stories:
            if s.get("url"):
                existing_urls.add(s["url"])

    for story in fresh:
        cat = story.get("category")
        if cat not in existing:
            continue
        if story["url"] in existing_urls:
            continue
        existing[cat].append(story)
        existing_urls.add(story["url"])

    for cat in CATEGORIES:
        pool = existing[cat]
        # Drop expired stories
        pool = [
            s for s in pool
            if s.get("published_date") and
               dateparser.parse(s["published_date"]).astimezone(timezone.utc) >= cutoff
        ]
        # Sort newest-first, cap at limit (oldest replaced first)
        pool.sort(key=lambda s: s.get("published_date") or "", reverse=True)
        existing[cat] = pool[:MAX_PER_CATEGORY]

    return existing


def write_output(grouped: dict) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    flat = []
    for stories in grouped.values():
        flat.extend(stories)
    output = {
        "country": "France",
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "story_count": len(flat),
        "stories": flat,
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
        json.dump(output, fh, ensure_ascii=False, indent=2)
    log.info("Wrote %d stories to %s", len(flat), OUTPUT_FILE)


def main():
    log.info("Loading existing data ...")
    existing = load_existing()

    log.info("Fetching %d RSS feeds ...", len(FEEDS))
    fresh = []
    for cfg in FEEDS:
        results = fetch_feed(cfg)
        log.info("  %s — %d stories from %s", cfg["source"], len(results), cfg["url"])
        fresh.extend(results)
        time.sleep(0.5)  # polite crawl delay

    log.info("Merging %d fresh stories ...", len(fresh))
    merged = merge(existing, fresh)

    counts = {cat: len(merged[cat]) for cat in CATEGORIES}
    log.info("Category totals: %s", counts)

    write_output(merged)


if __name__ == "__main__":
    main()
