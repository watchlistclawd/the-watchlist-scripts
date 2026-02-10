#!/usr/bin/env python3
"""
Fetch anime series data from TVDB v4 API.
TVDB is the season normalizer — their season structure is canonical.

Usage:
    python3 fetch_tvdb_anime.py <franchise-slug> [--title "Search Title"]
    python3 fetch_tvdb_anime.py demon-slayer --title "Demon Slayer"
    python3 fetch_tvdb_anime.py demon-slayer --tvdb-id 348545

API: https://api4.thetvdb.com/v4
Auth: API key → login → JWT bearer token

Saves:
    franchises/{slug}/entries/{entry-slug}/sources/tvdb_raw.json
    franchises/{slug}/entries/{entry-slug}/sources/tvdb.toon
    franchises/{slug}/entries/{entry-slug}/sources/tvdb_seasons.toon
    franchises/{slug}/entries/{entry-slug}/sources/tvdb_episodes.toon
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.parse

sys.path.insert(0, os.path.dirname(__file__))
import toon
from utils import slugify, ensure_dir, franchise_dir, entry_dir, save_json

TVDB_BASE = "https://api4.thetvdb.com/v4"
API_KEY = "4a1eb83b-bacd-4589-9401-7d524233d30b"

# Cache token in module
_token = None
_token_time = 0


def _login() -> str:
    """Login to TVDB and get JWT token. Cached for 20 hours."""
    global _token, _token_time
    if _token and (time.time() - _token_time) < 72000:  # 20h
        return _token

    req = urllib.request.Request(
        f"{TVDB_BASE}/login",
        data=json.dumps({"apikey": API_KEY}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read())
    
    _token = data["data"]["token"]
    _token_time = time.time()
    print(f"  ✓ TVDB login successful")
    return _token


def _get(path: str, params: dict = None) -> dict:
    """Make authenticated TVDB API request."""
    token = _login()
    url = f"{TVDB_BASE}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)

    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def search_series(query: str, series_type: str = "series") -> list[dict]:
    """Search TVDB for a series by name."""
    resp = _get("/search", {"query": query, "type": series_type})
    return resp.get("data", [])


def get_series_extended(tvdb_id: int) -> dict:
    """Get extended series info including artwork, seasons, etc."""
    resp = _get(f"/series/{tvdb_id}/extended", {"meta": "episodes"})
    return resp.get("data", {})


def get_series_episodes(tvdb_id: int, season_type: str = "default", page: int = 0) -> dict:
    """Get episodes for a series, paginated."""
    resp = _get(f"/series/{tvdb_id}/episodes/{season_type}", {"page": page})
    return resp.get("data", {})


def get_season_extended(season_id: int) -> dict:
    """Get extended season info."""
    resp = _get(f"/seasons/{season_id}/extended")
    return resp.get("data", {})


def save_tvdb_source(series: dict, franchise_slug: str, entry_slug: str):
    """
    Save TVDB series data as raw JSON + TOON files.
    """
    base = entry_dir(franchise_slug, entry_slug)
    sources = os.path.join(base, "sources")
    ensure_dir(sources)

    # Save full raw JSON dump first
    save_json(os.path.join(sources, "tvdb_raw.json"), series)
    print(f"  ✓ TVDB raw JSON saved")

    # Series-level TOON
    aliases = series.get("aliases", [])
    alias_names = [a.get("name", "") for a in aliases] if isinstance(aliases, list) else []
    
    series_data = [{
        "tvdb_id": series.get("id", ""),
        "name": series.get("name", ""),
        "slug": series.get("slug", ""),
        "status": (series.get("status", {}) or {}).get("name", "") if isinstance(series.get("status"), dict) else series.get("status", ""),
        "first_aired": series.get("firstAired", ""),
        "last_aired": series.get("lastAired", ""),
        "next_aired": series.get("nextAired", ""),
        "overview": (series.get("overview") or "")[:500],
        "original_language": series.get("originalLanguage", ""),
        "original_country": series.get("originalCountry", ""),
        "original_network": _get_network_name(series),
        "avg_runtime": series.get("averageRuntime", ""),
        "aliases": json.dumps(alias_names, ensure_ascii=False),
        "genres": json.dumps([g.get("name", "") for g in (series.get("genres") or [])], ensure_ascii=False),
        "image": series.get("image", ""),
        "score": series.get("score", ""),
    }]
    toon.save(os.path.join(sources, "tvdb.toon"), "tvdb_series", series_data)

    # Seasons TOON
    seasons = series.get("seasons", [])
    if seasons:
        # Filter to default season type (type.id == 1 is "Aired Order")
        default_seasons = [s for s in seasons if s.get("type", {}).get("id") == 1]
        if not default_seasons:
            default_seasons = seasons  # fallback to all

        season_data = []
        for s in default_seasons:
            s_type = s.get("type", {})
            season_data.append({
                "tvdb_season_id": s.get("id", ""),
                "season_number": s.get("number", ""),
                "name": s.get("name") or "",
                "type": s_type.get("name", "") if isinstance(s_type, dict) else "",
                "image": s.get("image") or "",
                "year": s.get("year") or "",
                "episode_count": len(s.get("episodes", [])) if s.get("episodes") else "",
                "last_updated": s.get("lastUpdated", ""),
            })
        toon.save(os.path.join(sources, "tvdb_seasons.toon"), "seasons", season_data)
        print(f"  ✓ {len(season_data)} seasons saved")

    # Episodes TOON (from the episodes nested in the series extended response)
    episodes = series.get("episodes", [])
    if episodes:
        ep_data = []
        for ep in episodes:
            ep_data.append({
                "tvdb_episode_id": ep.get("id", ""),
                "season_number": ep.get("seasonNumber", ""),
                "episode_number": ep.get("number", ""),
                "name": ep.get("name") or "",
                "overview": (ep.get("overview") or "")[:200],
                "aired": ep.get("aired") or "",
                "runtime": ep.get("runtime") or "",
                "image": ep.get("image") or "",
                "is_movie": ep.get("isMovie", 0),
                "year": ep.get("year") or "",
                "last_updated": ep.get("lastUpdated", ""),
            })
        toon.save(os.path.join(sources, "tvdb_episodes.toon"), "episodes", ep_data)
        print(f"  ✓ {len(ep_data)} episodes saved")

    # Artwork (useful for photo downloading later)
    artworks = series.get("artworks", [])
    if artworks:
        art_data = []
        for art in artworks:
            art_data.append({
                "id": art.get("id", ""),
                "type": art.get("type", ""),
                "image": art.get("image") or "",
                "thumbnail": art.get("thumbnail") or "",
                "language": art.get("language") or "",
                "score": art.get("score", ""),
                "width": art.get("width", ""),
                "height": art.get("height", ""),
                "season_id": art.get("seasonId") or "",
            })
        toon.save(os.path.join(sources, "tvdb_artworks.toon"), "artworks", art_data)
        print(f"  ✓ {len(art_data)} artworks cataloged")

    print(f"  ✓ TVDB data saved to {sources}/")


def _get_network_name(series: dict) -> str:
    """Extract original network name from series data."""
    network = series.get("originalNetwork")
    if isinstance(network, dict):
        return network.get("name", "")
    networks = series.get("latestNetwork")
    if isinstance(networks, dict):
        return networks.get("name", "")
    return ""


def main():
    parser = argparse.ArgumentParser(description="Fetch anime data from TVDB")
    parser.add_argument("franchise_slug", nargs="?", help="Franchise slug")
    parser.add_argument("--title", "-t", help="Search title")
    parser.add_argument("--tvdb-id", "-i", type=int, help="Fetch by TVDB series ID directly")
    parser.add_argument("--entry-slug", "-e", help="Entry slug (defaults to franchise slug)")
    args = parser.parse_args()

    if not args.franchise_slug and not args.tvdb_id:
        parser.error("Need franchise_slug or --tvdb-id")

    if args.tvdb_id:
        tvdb_id = args.tvdb_id
        print(f"Fetching TVDB series ID {tvdb_id}...")
    else:
        search_title = args.title or args.franchise_slug.replace("-", " ")
        print(f"Searching TVDB for: {search_title}")
        results = search_series(search_title)
        if not results:
            print("No results found.")
            return

        # Show top results for disambiguation
        for i, r in enumerate(results[:5]):
            print(f"  [{i}] {r.get('name')} ({r.get('year', '?')}) - {r.get('tvdb_id')} - {r.get('status', '?')}")

        # Take top result
        top = results[0]
        tvdb_id = int(top.get("tvdb_id", top.get("id", "").replace("series-", "")))
        print(f"\n  Using: {top.get('name')} (TVDB ID: {tvdb_id})")

    # Get extended series data with episodes
    series = get_series_extended(tvdb_id)
    if not series:
        print("Failed to fetch series data.")
        return

    franchise_slug = args.franchise_slug or slugify(series.get("name", str(tvdb_id)))
    entry_slug = args.entry_slug or franchise_slug

    print(f"  Series: {series.get('name')}")
    print(f"  Status: {(series.get('status', {}) or {}).get('name', '?')}")
    print(f"  Seasons: {len(series.get('seasons', []))}")
    print(f"  Episodes: {len(series.get('episodes', []))}")

    save_tvdb_source(series, franchise_slug, entry_slug)


if __name__ == "__main__":
    main()
