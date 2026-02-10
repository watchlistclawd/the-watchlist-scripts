"""
Shared utilities for the data pipeline.
"""
import re
import os
import json
import time
import unicodedata


def slugify(text: str) -> str:
    """
    Generate a URL-safe slug from text.
    'My Dress-Up Darling' -> 'my-dress-up-darling'
    'Shingeki no Kyojin' -> 'shingeki-no-kyojin'
    """
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower().strip()
    text = re.sub(r"[/\\]", "-", text)  # slash → hyphen before stripping
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "-", text)
    text = text.strip("-")
    return text


def sort_title(title: str) -> str | None:
    """
    Generate sort title by moving leading articles to end.
    'The Godfather' -> 'Godfather, The'
    Returns None if no change needed.
    """
    articles = ["the", "a", "an"]
    words = title.split()
    if len(words) > 1 and words[0].lower() in articles:
        return f"{' '.join(words[1:])}, {words[0]}"
    return None


def ensure_dir(path: str):
    """Create directory if it doesn't exist."""
    os.makedirs(path, exist_ok=True)


def save_json(filepath: str, data: dict | list):
    """Save data as formatted JSON."""
    ensure_dir(os.path.dirname(filepath))
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_json(filepath: str) -> dict | list:
    """Load JSON from file."""
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def rate_limit(last_call_time: float, min_interval: float = 1.0) -> float:
    """
    Simple rate limiter. Returns current time after waiting if needed.
    
    Args:
        last_call_time: timestamp of last API call (0 for first call)
        min_interval: minimum seconds between calls
    
    Returns:
        Current timestamp after any necessary wait
    """
    if last_call_time > 0:
        elapsed = time.time() - last_call_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
    return time.time()


# Data directory root (sibling folder)
DATA_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "the-watchlist-data")

def franchise_dir(slug: str) -> str:
    return os.path.join(DATA_ROOT, "franchises", slug)

def entry_dir(franchise_slug: str, entry_slug: str) -> str:
    return os.path.join(DATA_ROOT, "franchises", franchise_slug, "entries", entry_slug)

def character_dir(franchise_slug: str, char_slug: str) -> str:
    return os.path.join(DATA_ROOT, "franchises", franchise_slug, "characters", char_slug)

def creator_dir(slug: str) -> str:
    return os.path.join(DATA_ROOT, "creators", slug)

def company_dir(slug: str) -> str:
    return os.path.join(DATA_ROOT, "companies", slug)
