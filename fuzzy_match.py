#!/usr/bin/env python3
"""
Fuzzy name matching for cross-referencing IDs between data sources.

Uses difflib (built-in) for string similarity. No external dependencies.
Optional: install rapidfuzz for 10x faster matching on large sets.

Usage:
    from fuzzy_match import fuzzy_lookup, build_name_index
    
    index = build_name_index(jikan_characters, key="name", id_key="mal_id")
    mal_id = fuzzy_lookup("Tanjiro Kamado", index, threshold=80)
"""

import re
import difflib
from typing import Any

# Try rapidfuzz first (much faster), fall back to difflib
try:
    from rapidfuzz import fuzz as _rfuzz
    def _ratio(a: str, b: str) -> float:
        return _rfuzz.token_sort_ratio(a, b)
    FUZZY_ENGINE = "rapidfuzz"
except ImportError:
    def _ratio(a: str, b: str) -> float:
        na = _normalize(a)
        nb = _normalize(b)
        return difflib.SequenceMatcher(None, na, nb).ratio() * 100
    FUZZY_ENGINE = "difflib"


def _normalize(s: str) -> str:
    """Normalize a name for comparison: lowercase, remove punctuation, sort words."""
    s = s.lower()
    s = re.sub(r'[^\w\s]', '', s)
    # Sort words to handle "Last First" vs "First Last"
    return ' '.join(sorted(s.split()))


def _normalize_unsorted(s: str) -> str:
    """Normalize without sorting — preserves word order for exact prefix matching."""
    s = s.lower()
    s = re.sub(r'[^\w\s]', '', s)
    return ' '.join(s.split())


# ── Name Index ────────────────────────────────────────────────────────────

class NameIndex:
    """
    Index of names for fast fuzzy lookup.
    
    Stores multiple normalized forms per entry for exact-first matching:
    - Original name
    - "Last, First" → "First Last" reversal
    - Sorted words (for token_sort matching)
    """
    
    def __init__(self):
        self.exact = {}      # normalized_name → id
        self.entries = []     # [(normalized, original, id), ...] for fuzzy search
    
    def add(self, name: str, id_value: Any):
        """Add a name→id mapping with multiple normalized forms."""
        if not name or id_value is None:
            return
        
        # Store exact normalized form
        norm = _normalize_unsorted(name)
        self.exact[norm] = id_value
        
        # Store reversed form: "Last, First" → "First Last"
        if ", " in name:
            parts = name.split(", ", 1)
            reversed_name = f"{parts[1]} {parts[0]}"
            norm_rev = _normalize_unsorted(reversed_name)
            self.exact[norm_rev] = id_value
        
        # Store sorted form for fuzzy
        self.entries.append((_normalize(name), name, id_value))
    
    def lookup(self, query: str, threshold: int = 80) -> Any:
        """
        Look up a name, trying exact match first, then fuzzy.
        
        Returns id_value if match found above threshold, else None.
        threshold: 0-100 similarity score required for fuzzy match.
        """
        if not query:
            return None
        
        # Try exact match first (fast)
        norm = _normalize_unsorted(query)
        if norm in self.exact:
            return self.exact[norm]
        
        # Try fuzzy match
        best_score = 0
        best_id = None
        
        for entry_norm, entry_orig, entry_id in self.entries:
            score = _ratio(query, entry_orig)
            if score > best_score:
                best_score = score
                best_id = entry_id
        
        if best_score >= threshold:
            return best_id
        
        return None
    
    def __len__(self):
        return len(self.entries)


def build_name_index(items: list[dict], name_key: str = "name", 
                     id_key: str = "mal_id", sub_key: str = None) -> NameIndex:
    """
    Build a NameIndex from a list of dicts.
    
    items: list of dicts from Jikan API
    name_key: key for the name field (or nested like "character.name")
    id_key: key for the ID field
    sub_key: if set, items are nested (e.g., each item has item[sub_key][name_key])
    """
    index = NameIndex()
    for item in items:
        if sub_key:
            obj = item.get(sub_key, {})
        else:
            obj = item
        
        name = obj.get(name_key, "")
        id_val = obj.get(id_key)
        if name and id_val:
            index.add(name, id_val)
    
    return index


# ── Date-based matching ───────────────────────────────────────────────────

def dates_match(date1: str | None, date2: str | None, tolerance_days: int = 30) -> bool:
    """
    Check if two date strings (YYYY-MM-DD) are within tolerance_days of each other.
    Useful as a secondary signal when names are ambiguous.
    """
    if not date1 or not date2:
        return False
    
    try:
        from datetime import datetime
        d1 = datetime.strptime(date1[:10], "%Y-%m-%d")
        d2 = datetime.strptime(date2[:10], "%Y-%m-%d")
        return abs((d1 - d2).days) <= tolerance_days
    except (ValueError, TypeError):
        return False


def match_by_name_and_date(query_name: str, query_date: str | None,
                           candidates: list[dict],
                           name_key: str = "name", date_key: str = "start_date",
                           id_key: str = "mal_id",
                           name_threshold: int = 75,
                           date_tolerance: int = 30) -> Any:
    """
    Match by name first, with date as tiebreaker/boost.
    
    If name score is above threshold and dates match → strong match.
    If name score is high enough (>90) → match even without date.
    If name score is borderline (75-90) → require date match to confirm.
    """
    if not query_name:
        return None
    
    best_score = 0
    best_id = None
    best_date_match = False
    
    for candidate in candidates:
        name = candidate.get(name_key, "")
        cand_date = candidate.get(date_key, "")
        cand_id = candidate.get(id_key)
        
        if not name or cand_id is None:
            continue
        
        score = _ratio(query_name, name)
        date_ok = dates_match(query_date, cand_date, date_tolerance) if query_date else False
        
        # Boost score by 10 if dates also match
        effective_score = score + (10 if date_ok else 0)
        
        if effective_score > best_score:
            best_score = effective_score
            best_id = cand_id
            best_date_match = date_ok
    
    # High confidence name match (>90) → accept without date
    if best_score >= 90:
        return best_id
    
    # Borderline name match (75-90) → only accept if dates also match
    if best_score >= name_threshold and best_date_match:
        return best_id
    
    return None


# ── Stats helper ──────────────────────────────────────────────────────────

def match_stats(total: int, matched: int) -> str:
    """Format a match rate string."""
    pct = (matched / total * 100) if total > 0 else 0
    return f"{matched}/{total} ({pct:.0f}%)"


if __name__ == "__main__":
    # Quick self-test
    print(f"Fuzzy engine: {FUZZY_ENGINE}")
    
    idx = NameIndex()
    idx.add("Kamado, Tanjiro", 1)
    idx.add("Hashibira, Inosuke", 2)
    idx.add("Agatsuma, Zenitsu", 3)
    
    tests = [
        ("Tanjiro Kamado", 1),
        ("Kamado Tanjiro", 1),
        ("Inosuke Hashibira", 2),
        ("Zenitsu Agatsuma", 3),
        ("Muzan Kibutsuji", None),  # not in index
    ]
    
    print("\nLookup tests:")
    for name, expected in tests:
        result = idx.lookup(name, threshold=80)
        status = "✓" if result == expected else "✗"
        print(f"  {status} '{name}' → {result} (expected {expected})")
    
    # Date matching
    print("\nDate tests:")
    print(f"  Same day: {dates_match('2020-01-15', '2020-01-15')}")
    print(f"  Within 30d: {dates_match('2020-01-15', '2020-02-10')}")
    print(f"  Outside 30d: {dates_match('2020-01-15', '2020-03-15')}")
