#!/usr/bin/env python3
"""
Download and catalog photos from MAL/AniList source files.
Renames to slug-based convention and tracks provenance in images-manifest.json.

Usage:
    python3 download_photos.py <franchise-slug> [--entry-slug <slug>]
    python3 download_photos.py demon-slayer
    python3 download_photos.py demon-slayer --entry-slug demon-slayer-s1

Naming convention:
    entry-{franchise}-{entry}.jpg          (poster/cover)
    banner-{franchise}-{entry}.jpg         (banner image)
    character-{franchise}-{char-name}.jpg  (character portrait)
    staff-{franchise}-{person-name}.jpg    (staff photo)
    va-{franchise}-{va-name}.jpg           (voice actor photo)

Manifest tracks source URL, fetch time, type, subject for takedown compliance.
"""

import argparse
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))
import toon
from utils import slugify, ensure_dir, franchise_dir, entry_dir, load_json

# Timeout + user agent for image downloads
_HEADERS = {"User-Agent": "TheWatchlist/1.0 (data pipeline)"}
_DOWNLOAD_DELAY = 0.3  # seconds between downloads


def _download_image(url: str, dest_path: str) -> bool:
    """Download an image. Returns True on success."""
    if not url or url.strip() == "":
        return False
    try:
        req = urllib.request.Request(url, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=15) as resp:
            content_type = resp.headers.get("Content-Type", "")
            data = resp.read()
            if len(data) < 100:  # too small, probably error
                return False
            with open(dest_path, "wb") as f:
                f.write(data)
            return True
    except Exception as e:
        print(f"    ⚠ Failed to download {url}: {e}")
        return False


def _get_extension(url: str) -> str:
    """Guess file extension from URL."""
    url_lower = url.lower().split("?")[0]
    if url_lower.endswith(".png"):
        return ".png"
    elif url_lower.endswith(".webp"):
        return ".webp"
    return ".jpg"  # default


def _load_manifest(manifest_path: str) -> dict:
    """Load existing manifest or return empty dict."""
    if os.path.exists(manifest_path):
        with open(manifest_path, "r") as f:
            return json.load(f)
    return {}


def _save_manifest(manifest_path: str, manifest: dict):
    """Save manifest to disk."""
    ensure_dir(os.path.dirname(manifest_path))
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)


def download_entry_photos(franchise_slug: str, entry_slug: str):
    """
    Download all photos for a franchise entry from available source files.
    Reads AniList and Jikan TOON/raw files to find image URLs.
    """
    base = entry_dir(franchise_slug, entry_slug)
    sources = os.path.join(base, "sources")
    images_dir = os.path.join(franchise_dir(franchise_slug), "images")
    ensure_dir(images_dir)
    manifest_path = os.path.join(images_dir, "images-manifest.json")
    manifest = _load_manifest(manifest_path)

    downloaded = 0
    skipped = 0

    # --- Entry poster/cover from AniList ---
    anilist_raw_path = os.path.join(sources, "anilist_raw.json")
    if os.path.exists(anilist_raw_path):
        anilist = load_json(anilist_raw_path)

        # Cover image
        cover = (anilist.get("coverImage") or {})
        cover_url = cover.get("extraLarge") or cover.get("large") or ""
        if cover_url:
            ext = _get_extension(cover_url)
            filename = f"entry-{franchise_slug}-{entry_slug}{ext}"
            dest = os.path.join(images_dir, filename)
            if filename not in manifest:
                if _download_image(cover_url, dest):
                    manifest[filename] = {
                        "source": "anilist",
                        "sourceUrl": cover_url,
                        "fetchedAt": datetime.now(timezone.utc).isoformat(),
                        "type": "entry-poster",
                        "subjectSlug": entry_slug,
                    }
                    downloaded += 1
                    print(f"    ✓ {filename}")
                    time.sleep(_DOWNLOAD_DELAY)
            else:
                skipped += 1

        # Banner image
        banner_url = anilist.get("bannerImage") or ""
        if banner_url:
            ext = _get_extension(banner_url)
            filename = f"banner-{franchise_slug}-{entry_slug}{ext}"
            dest = os.path.join(images_dir, filename)
            if filename not in manifest:
                if _download_image(banner_url, dest):
                    manifest[filename] = {
                        "source": "anilist",
                        "sourceUrl": banner_url,
                        "fetchedAt": datetime.now(timezone.utc).isoformat(),
                        "type": "entry-banner",
                        "subjectSlug": entry_slug,
                    }
                    downloaded += 1
                    print(f"    ✓ {filename}")
                    time.sleep(_DOWNLOAD_DELAY)
            else:
                skipped += 1

        # Characters from AniList
        char_edges = anilist.get("characters", {}).get("edges", [])
        for edge in char_edges:
            node = edge.get("node", {})
            name = (node.get("name", {}).get("full") or "").strip()
            if not name:
                continue
            char_slug = slugify(name)
            image_url = (node.get("image") or {}).get("large", "")
            if image_url:
                ext = _get_extension(image_url)
                filename = f"character-{franchise_slug}-{char_slug}{ext}"
                dest = os.path.join(images_dir, filename)
                if filename not in manifest:
                    if _download_image(image_url, dest):
                        manifest[filename] = {
                            "source": "anilist",
                            "sourceUrl": image_url,
                            "fetchedAt": datetime.now(timezone.utc).isoformat(),
                            "type": "character",
                            "subjectSlug": char_slug,
                            "characterName": name,
                        }
                        downloaded += 1
                        print(f"    ✓ {filename}")
                        time.sleep(_DOWNLOAD_DELAY)
                else:
                    skipped += 1

            # Voice actors from AniList
            for va in edge.get("voiceActors", []):
                va_name = (va.get("name", {}).get("full") or "").strip()
                va_image = (va.get("image") or {}).get("large", "")
                if va_name and va_image:
                    va_slug = slugify(va_name)
                    ext = _get_extension(va_image)
                    filename = f"va-{franchise_slug}-{va_slug}{ext}"
                    dest = os.path.join(images_dir, filename)
                    if filename not in manifest:
                        if _download_image(va_image, dest):
                            manifest[filename] = {
                                "source": "anilist",
                                "sourceUrl": va_image,
                                "fetchedAt": datetime.now(timezone.utc).isoformat(),
                                "type": "voice-actor",
                                "subjectSlug": va_slug,
                                "personName": va_name,
                            }
                            downloaded += 1
                            print(f"    ✓ {filename}")
                            time.sleep(_DOWNLOAD_DELAY)
                    else:
                        skipped += 1

        # Staff from AniList
        staff_edges = anilist.get("staff", {}).get("edges", [])
        for edge in staff_edges:
            node = edge.get("node", {})
            name = (node.get("name", {}).get("full") or "").strip()
            image_url = (node.get("image") or {}).get("large", "")
            if name and image_url:
                person_slug = slugify(name)
                ext = _get_extension(image_url)
                filename = f"staff-{franchise_slug}-{person_slug}{ext}"
                dest = os.path.join(images_dir, filename)
                if filename not in manifest:
                    if _download_image(image_url, dest):
                        manifest[filename] = {
                            "source": "anilist",
                            "sourceUrl": image_url,
                            "fetchedAt": datetime.now(timezone.utc).isoformat(),
                            "type": "staff",
                            "subjectSlug": person_slug,
                            "personName": name,
                            "role": edge.get("role", ""),
                        }
                        downloaded += 1
                        print(f"    ✓ {filename}")
                        time.sleep(_DOWNLOAD_DELAY)
                else:
                    skipped += 1

    # --- Jikan/MAL fallbacks (only for items not already downloaded) ---
    jikan_raw_path = os.path.join(sources, "jikan_raw.json")
    if os.path.exists(jikan_raw_path):
        jikan = load_json(jikan_raw_path)
        anime = jikan.get("anime", jikan) if isinstance(jikan, dict) else {}

        # Entry poster from MAL (if not already from AniList)
        mal_image = anime.get("images", {}).get("jpg", {}).get("large_image_url", "")
        if mal_image:
            ext = _get_extension(mal_image)
            filename = f"entry-{franchise_slug}-{entry_slug}-mal{ext}"
            dest = os.path.join(images_dir, filename)
            if filename not in manifest:
                if _download_image(mal_image, dest):
                    manifest[filename] = {
                        "source": "myanimelist",
                        "sourceUrl": mal_image,
                        "fetchedAt": datetime.now(timezone.utc).isoformat(),
                        "type": "entry-poster",
                        "subjectSlug": entry_slug,
                    }
                    downloaded += 1
                    print(f"    ✓ {filename} (MAL)")
                    time.sleep(_DOWNLOAD_DELAY)
            else:
                skipped += 1

        # Characters from Jikan (fill gaps not covered by AniList)
        for c in jikan.get("characters", []):
            char = c.get("character", {})
            name = (char.get("name") or "").strip()
            if not name:
                continue
            # Jikan names are "Last, First" — normalize
            if ", " in name:
                parts = name.split(", ", 1)
                name = f"{parts[1]} {parts[0]}"
            char_slug = slugify(name)
            existing_key = f"character-{franchise_slug}-{char_slug}.jpg"
            if existing_key in manifest:
                skipped += 1
                continue

            image_url = char.get("images", {}).get("jpg", {}).get("image_url", "")
            if image_url:
                ext = _get_extension(image_url)
                filename = f"character-{franchise_slug}-{char_slug}{ext}"
                dest = os.path.join(images_dir, filename)
                if filename not in manifest:
                    if _download_image(image_url, dest):
                        manifest[filename] = {
                            "source": "myanimelist",
                            "sourceUrl": image_url,
                            "fetchedAt": datetime.now(timezone.utc).isoformat(),
                            "type": "character",
                            "subjectSlug": char_slug,
                            "characterName": name,
                        }
                        downloaded += 1
                        print(f"    ✓ {filename} (MAL)")
                        time.sleep(_DOWNLOAD_DELAY)
                else:
                    skipped += 1

    # Save manifest
    _save_manifest(manifest_path, manifest)
    print(f"\n  📸 Photos: {downloaded} downloaded, {skipped} skipped (already in manifest)")
    print(f"  📋 Manifest: {manifest_path}")


def main():
    parser = argparse.ArgumentParser(description="Download photos from source data")
    parser.add_argument("franchise_slug", help="Franchise slug")
    parser.add_argument("--entry-slug", "-e", help="Entry slug (defaults to first entry found)")
    args = parser.parse_args()

    # Find entries if no entry slug given
    franchise_path = franchise_dir(args.franchise_slug)
    entries_path = os.path.join(franchise_path, "entries")
    
    if args.entry_slug:
        entry_slugs = [args.entry_slug]
    elif os.path.exists(entries_path):
        entry_slugs = sorted(os.listdir(entries_path))
    else:
        print(f"No entries found at {entries_path}")
        return

    for es in entry_slugs:
        print(f"\n📥 Downloading photos for {args.franchise_slug}/{es}...")
        download_entry_photos(args.franchise_slug, es)


if __name__ == "__main__":
    main()
