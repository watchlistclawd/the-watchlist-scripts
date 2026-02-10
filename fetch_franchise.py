#!/usr/bin/env python3
"""
Master franchise fetcher — orchestrates the full data pipeline.

Flow:
1. Search MAL/AniList for franchise → get base info + air date
2. Search TVDB → match by air date + country → get canonical season structure
3. Fetch full data from all sources per season
4. Download all photos

Usage:
    python3 fetch_franchise.py <franchise-slug> --title "Search Title"
    python3 fetch_franchise.py demon-slayer --title "Kimetsu no Yaiba"
    python3 fetch_franchise.py attack-on-titan --title "Attack on Titan"

    # Override TVDB match:
    python3 fetch_franchise.py demon-slayer --title "Kimetsu no Yaiba" --tvdb-id 348545
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from utils import slugify, ensure_dir, franchise_dir, entry_dir, save_json, load_json
import fetch_anilist_anime as anilist
import fetch_anilist_media as anilist_media
import fetch_jikan_anime as jikan
import fetch_tvdb_anime as tvdb
import download_photos
import shutil

# Relation types worth following as separate entries
FOLLOW_RELATIONS = {
    "ADAPTATION",    # manga ↔ anime
    "SEQUEL",        # sequel movies/seasons (already in TVDB but good for movies)
    "PREQUEL",       # prequel entries
    "SIDE_STORY",    # OVAs, specials
    "SPIN_OFF",      # spin-off series
}

# Formats worth creating separate entries for
ENTRY_FORMATS = {
    "MANGA", "MOVIE", "OVA", "ONA", "SPECIAL", "TV", "TV_SHORT",
    "NOVEL", "ONE_SHOT",
}


def _load_index(franchise_slug: str) -> dict:
    """
    Load franchise-index.json. Maps source IDs to entry slugs.
    
    Structure:
    {
        "entries": {
            "anilist:101922": {
                "slug": "demon-slayer-kimetsu-no-yaiba",
                "type": "ANIME",
                "format": "TV"
            },
            "anilist:87216": {
                "slug": "demon-slayer-kimetsu-no-yaiba-manga",
                "type": "MANGA",
                "format": "MANGA"
            }
        },
        "tvdb_id": 348545,
        "last_updated": "2026-02-09T19:30:00Z"
    }
    """
    index_path = os.path.join(franchise_dir(franchise_slug), "franchise-index.json")
    if os.path.exists(index_path):
        return load_json(index_path)
    return {"entries": {}}


def _save_index(franchise_slug: str, index: dict):
    """Save franchise-index.json."""
    from datetime import timezone
    index["last_updated"] = datetime.now(timezone.utc).isoformat()
    index_path = os.path.join(franchise_dir(franchise_slug), "franchise-index.json")
    ensure_dir(os.path.dirname(index_path))
    save_json(index_path, index)


def _resolve_slug(franchise_slug: str, index: dict, anilist_id: int, desired_slug: str,
                  media_type: str = "", media_format: str = "") -> str:
    """
    Resolve the correct slug for an entry, handling renames.
    
    - If anilist_id exists in index → use existing slug (or rename if different)
    - If new → register desired_slug in index
    
    Returns the final slug to use.
    """
    key = f"anilist:{anilist_id}"
    entries_path = os.path.join(franchise_dir(franchise_slug), "entries")
    
    if key in index["entries"]:
        existing_slug = index["entries"][key]["slug"]
        
        if existing_slug != desired_slug:
            # Slug changed (e.g. English title was added to AniList)
            old_path = os.path.join(entries_path, existing_slug)
            new_path = os.path.join(entries_path, desired_slug)
            
            if os.path.exists(old_path) and not os.path.exists(new_path):
                shutil.move(old_path, new_path)
                print(f"  🔄 Renamed: {existing_slug} → {desired_slug}")
            elif os.path.exists(old_path) and os.path.exists(new_path):
                # Both exist somehow — keep the old one
                print(f"  ⚠ Both {existing_slug} and {desired_slug} exist — keeping {existing_slug}")
                desired_slug = existing_slug
            
            # Update index
            index["entries"][key]["slug"] = desired_slug
        
        return desired_slug
    else:
        # New entry — register it
        index["entries"][key] = {
            "slug": desired_slug,
            "type": media_type,
            "format": media_format,
        }
        return desired_slug


def _parse_date(date_str: str) -> datetime | None:
    """Parse various date formats to datetime."""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S+00:00", "%Y-%m-%dT%H:%M:%S.000Z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(date_str.split("T")[0], "%Y-%m-%d")
        except (ValueError, IndexError):
            continue
    return None


def _days_apart(d1: datetime | None, d2: datetime | None) -> int:
    """Days between two dates. Returns 99999 if either is None."""
    if not d1 or not d2:
        return 99999
    return abs((d1 - d2).days)


def match_tvdb_series(title: str, expected_air_date: str, expected_country: str = "jpn") -> dict | None:
    """
    Search TVDB and find the best match using air date cross-reference.
    
    Strategy:
    1. Search TVDB for title
    2. Prefer results from expected country (jpn for anime)
    3. Pick the one whose first_air_time is closest to expected_air_date
    4. Reject if > 365 days apart (probably wrong show)
    """
    results = tvdb.search_series(title)
    if not results:
        print("  ⚠ No TVDB results found")
        return None

    expected_dt = _parse_date(expected_air_date)
    
    scored = []
    for r in results:
        air_date = r.get("first_air_time") or r.get("year", "")
        r_dt = _parse_date(air_date)
        days = _days_apart(expected_dt, r_dt)
        
        # Country bonus: prefer Japanese origin for anime
        country = (r.get("country") or "").lower()
        country_match = 1 if country == expected_country else 0
        
        # Primary language bonus
        lang = (r.get("primary_language") or "").lower()
        lang_match = 1 if lang == "jpn" else 0
        
        # Score: lower is better. Country/lang mismatch adds 1000 penalty
        score = days - (country_match * 1000) - (lang_match * 500)
        
        scored.append({
            "result": r,
            "days_apart": days,
            "country": country,
            "lang": lang,
            "score": score,
        })
    
    scored.sort(key=lambda x: x["score"])
    
    # Show candidates
    print(f"\n  TVDB candidates (matched against air date {expected_air_date}):")
    for i, s in enumerate(scored[:5]):
        r = s["result"]
        marker = " ★" if i == 0 else ""
        print(f"    [{i}] {r.get('name')} ({r.get('year', '?')}) country={s['country']} days_off={s['days_apart']}{marker}")
    
    best = scored[0]
    if best["days_apart"] > 365 and best["country"] != expected_country:
        print(f"  ⚠ Best match is {best['days_apart']} days off and wrong country — skipping TVDB")
        return None
    
    return best["result"]


def fetch_franchise(franchise_slug: str, title: str, tvdb_id: int | None = None):
    """
    Full franchise fetch pipeline.
    """
    print(f"\n{'='*60}")
    print(f"  FRANCHISE: {franchise_slug}")
    print(f"  TITLE: {title}")
    print(f"{'='*60}")

    # Load franchise index (ID → slug mapping)
    index = _load_index(franchise_slug)

    # --- Step 1: AniList ---
    print(f"\n📡 Step 1: Fetching from AniList...")
    anilist_results = anilist.search_anime(title, per_page=3)
    if not anilist_results:
        print("  ⚠ No AniList results — aborting")
        return
    
    al_media = anilist_results[0]
    al_title = al_media.get("title", {})
    al_name = al_title.get("english") or al_title.get("romaji") or title
    al_start = al_media.get("startDate", {})
    al_air_date = None
    if al_start and al_start.get("year"):
        al_air_date = f"{al_start['year']:04d}-{(al_start.get('month') or 1):02d}-{(al_start.get('day') or 1):02d}"
    
    print(f"  Found: {al_name} (AniList ID: {al_media['id']}, Air: {al_air_date})")
    
    # Resolve slug via index (handles renames)
    desired_slug = slugify(al_name)
    entry_slug = _resolve_slug(franchise_slug, index, al_media["id"], desired_slug,
                               al_media.get("type", ""), al_media.get("format", ""))
    anilist.save_anime_source(al_media, franchise_slug, entry_slug)

    # --- Step 2: Jikan/MAL ---
    print(f"\n📡 Step 2: Fetching from MAL/Jikan...")
    time.sleep(0.5)
    
    mal_id = al_media.get("idMal")
    if mal_id:
        print(f"  Using MAL ID {mal_id} from AniList cross-reference")
        time.sleep(0.4)
        anime = jikan.get_anime_full(mal_id)
        time.sleep(0.4)
        characters = jikan.get_anime_characters(mal_id)
        time.sleep(0.4)
        staff = jikan.get_anime_staff(mal_id)
        jikan.save_anime_source(anime, characters, staff, franchise_slug, entry_slug)
    else:
        print(f"  No MAL ID found, searching...")
        jikan_results = jikan.search_anime(title, limit=3)
        if jikan_results:
            anime = jikan_results[0]
            mal_id = anime["mal_id"]
            time.sleep(0.4)
            anime = jikan.get_anime_full(mal_id)
            time.sleep(0.4)
            characters = jikan.get_anime_characters(mal_id)
            time.sleep(0.4)
            staff = jikan.get_anime_staff(mal_id)
            jikan.save_anime_source(anime, characters, staff, franchise_slug, entry_slug)

    # --- Step 3: TVDB (season normalizer) ---
    print(f"\n📡 Step 3: Fetching from TVDB (season normalizer)...")
    
    if tvdb_id:
        print(f"  Using provided TVDB ID: {tvdb_id}")
    else:
        # Cross-reference by air date
        matched = match_tvdb_series(title, al_air_date or "")
        if matched:
            tvdb_id = int(matched.get("tvdb_id", matched.get("id", "").replace("series-", "")))
            print(f"\n  ★ Matched: {matched.get('name')} (TVDB ID: {tvdb_id})")
        else:
            print("  ⚠ Could not match TVDB series")
    
    if tvdb_id:
        series = tvdb.get_series_extended(tvdb_id)
        if series:
            tvdb.save_tvdb_source(series, franchise_slug, entry_slug)
            
            # Print season summary
            seasons = series.get("seasons", [])
            default_seasons = [s for s in seasons if s.get("type", {}).get("id") == 1]
            if default_seasons:
                print(f"\n  📺 TVDB Season Structure:")
                for s in default_seasons:
                    sn = s.get("number", "?")
                    name = s.get("name") or "(unnamed)"
                    print(f"    S{sn}: {name}")

    # --- Step 4: Follow relations recursively (manga, movies, etc.) ---
    print(f"\n🔗 Step 4: Following relations (recursive)...")
    related_entries = []
    seen_ids = {al_media["id"]}  # track visited AniList IDs
    tv_seasons_skipped = []  # TV sequels covered by TVDB
    
    def _collect_relations(anilist_id: int, depth: int = 0):
        """Recursively walk sequel/prequel chains to find all related entries."""
        if depth > 10:  # safety limit
            return
        
        # Fetch this media's relations
        time.sleep(0.7)
        try:
            media = anilist_media.get_media_by_id(anilist_id)
        except Exception as e:
            print(f"  {'  '*depth}⚠ Failed to fetch ID {anilist_id}: {e}")
            return
        if not media:
            return
        
        relations = media.get("relations", {}).get("edges", [])
        for rel in relations:
            rel_type = rel.get("relationType", "")
            node = rel.get("node", {})
            rel_id = node.get("id")
            
            if rel_id in seen_ids:
                continue
            seen_ids.add(rel_id)
            
            fmt = node.get("format") or ""
            media_type = node.get("type") or ""
            rel_title = node.get("title", {})
            rel_name = rel_title.get("english") or rel_title.get("romaji") or ""
            
            if rel_type not in FOLLOW_RELATIONS:
                print(f"  {'  '*depth}⏭ {rel_name} (relation: {rel_type})")
                continue
            if fmt not in ENTRY_FORMATS:
                print(f"  {'  '*depth}⏭ {rel_name} (format: {fmt})")
                continue
            
            # TV sequels/prequels are covered by TVDB seasons — don't make separate entries
            if fmt == "TV" and media_type == "ANIME" and rel_type in ("SEQUEL", "PREQUEL"):
                print(f"  {'  '*depth}📺 {rel_name} (TV {rel_type} → covered by TVDB seasons)")
                tv_seasons_skipped.append(rel_name)
                # But keep walking the chain to find movies/OVAs further down
                _collect_relations(rel_id, depth + 1)
                continue
            
            # This is a genuine separate entry (movie, manga, OVA, etc.)
            base_slug = slugify(rel_name) if rel_name else f"{franchise_slug}-{rel_id}"
            fmt_suffix = fmt.lower().replace("_", "-") if fmt else media_type.lower()
            desired = f"{base_slug}-{fmt_suffix}" if base_slug == entry_slug else base_slug
            rel_slug = _resolve_slug(franchise_slug, index, rel_id, desired, media_type, fmt)
            
            print(f"  {'  '*depth}→ {rel_type}: {rel_name} ({media_type}/{fmt}) → {rel_slug}")
            related_entries.append({
                "anilist_id": rel_id,
                "mal_id": node.get("idMal"),
                "slug": rel_slug,
                "name": rel_name,
                "type": media_type,
                "format": fmt,
                "relation": rel_type,
            })
            
            # Keep walking sequel chains from movies too (e.g. movie trilogies)
            if rel_type in ("SEQUEL", "PREQUEL"):
                _collect_relations(rel_id, depth + 1)
    
    # Start from the main entry
    _collect_relations(al_media["id"])
    
    if tv_seasons_skipped:
        print(f"\n  📺 TV seasons covered by TVDB (not separate entries):")
        for name in tv_seasons_skipped:
            print(f"     • {name}")
    
    # Fetch each related entry
    for rel in related_entries:
        rel_entry_dir = entry_dir(franchise_slug, rel["slug"])
        sources_dir = os.path.join(rel_entry_dir, "sources")
        
        # Skip if already fetched
        if os.path.exists(os.path.join(sources_dir, "anilist_raw.json")):
            print(f"  ✓ {rel['slug']} already fetched, skipping")
            continue
        
        print(f"\n  📡 Fetching {rel['name']}...")
        time.sleep(0.7)  # AniList rate limit
        
        try:
            media = anilist_media.get_media_by_id(rel["anilist_id"])
            if media:
                anilist_media.save_media_source(media, franchise_slug, rel["slug"])
                
                # Also fetch from Jikan if it's anime and has a MAL ID
                if rel["type"] == "ANIME" and rel.get("mal_id"):
                    time.sleep(0.5)
                    try:
                        anime_data = jikan.get_anime_full(rel["mal_id"])
                        time.sleep(0.4)
                        chars = jikan.get_anime_characters(rel["mal_id"])
                        time.sleep(0.4)
                        staff_data = jikan.get_anime_staff(rel["mal_id"])
                        jikan.save_anime_source(anime_data, chars, staff_data, franchise_slug, rel["slug"])
                    except Exception as e:
                        print(f"    ⚠ Jikan failed for {rel['name']}: {e}")
        except Exception as e:
            print(f"    ⚠ Failed to fetch {rel['name']}: {e}")

    # --- Step 5: Download photos for ALL entries ---
    print(f"\n📸 Step 5: Downloading photos...")
    # Main entry
    download_photos.download_entry_photos(franchise_slug, entry_slug)
    # Related entries
    for rel in related_entries:
        rel_sources = os.path.join(entry_dir(franchise_slug, rel["slug"]), "sources")
        if os.path.exists(os.path.join(rel_sources, "anilist_raw.json")):
            print(f"\n  📸 Photos for {rel['slug']}...")
            download_photos.download_entry_photos(franchise_slug, rel["slug"])

    # Save franchise index
    if tvdb_id:
        index["tvdb_id"] = tvdb_id
    _save_index(franchise_slug, index)

    # --- Summary ---
    fdir = franchise_dir(franchise_slug)
    images_dir = os.path.join(fdir, "images")
    img_count = len(os.listdir(images_dir)) - 1 if os.path.exists(images_dir) else 0  # -1 for manifest
    entries_path = os.path.join(fdir, "entries")
    entry_count = len(os.listdir(entries_path)) if os.path.exists(entries_path) else 0
    
    print(f"\n{'='*60}")
    print(f"  ✅ FRANCHISE COMPLETE: {franchise_slug}")
    print(f"  📁 {fdir}")
    print(f"  📂 {entry_count} entries")
    if related_entries:
        for rel in related_entries:
            print(f"     → {rel['slug']} ({rel['type']}/{rel['format']}, {rel['relation']})")
    print(f"  🖼️  {img_count} photos")
    if tvdb_id:
        print(f"  📺 TVDB ID: {tvdb_id}")
    print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(description="Master franchise data fetcher")
    parser.add_argument("franchise_slug", help="Franchise slug")
    parser.add_argument("--title", "-t", required=True, help="Search title")
    parser.add_argument("--tvdb-id", "-i", type=int, help="Override TVDB series ID")
    args = parser.parse_args()

    fetch_franchise(args.franchise_slug, args.title, args.tvdb_id)


if __name__ == "__main__":
    main()
