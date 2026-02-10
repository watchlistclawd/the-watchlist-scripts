#!/usr/bin/env python3
"""
Download images from API sources and build manifest for later enrichment.
"""
import hashlib
import json
import os
import re
import urllib.request
import urllib.error
from typing import Optional
from pathlib import Path

# Where images are stored relative to data root
IMAGES_DIR = "images"

# Preference order for selecting images during enrichment
IMAGE_PREFERENCE = {
    "creators": ["anilist", "mal"],
    "entries": ["mal", "anilist", "tvdb"],
    "characters": ["anilist", "mal"],
    "companies": ["anilist", "mal"],
}


def slugify(text: str) -> str:
    """Generate URL-safe slug from text."""
    if not text:
        return "unknown"
    text = text.lower().strip()
    text = re.sub(r"[/\\]", "-", text)
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "-", text)
    return text.strip("-")[:50]  # Limit length


def get_extension(url: str, content_type: str = None) -> str:
    """Extract file extension from URL or content-type."""
    # Try URL first
    if "." in url.split("/")[-1]:
        ext = url.split(".")[-1].split("?")[0].lower()
        if ext in ("jpg", "jpeg", "png", "gif", "webp"):
            return ext if ext != "jpeg" else "jpg"
    
    # Fall back to content-type
    if content_type:
        if "jpeg" in content_type or "jpg" in content_type:
            return "jpg"
        if "png" in content_type:
            return "png"
        if "gif" in content_type:
            return "gif"
        if "webp" in content_type:
            return "webp"
    
    return "jpg"  # Default


def download_image(url: str, save_path: str, timeout: int = 10) -> bool:
    """Download image from URL to save_path. Returns True on success."""
    if not url or not url.startswith("http"):
        return False
    
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "TheWatchlist/1.0"
        })
        with urllib.request.urlopen(req, timeout=timeout) as response:
            content_type = response.headers.get("Content-Type", "")
            data = response.read()
            
            # Verify it's actually an image
            if len(data) < 1000:  # Too small, probably error page
                return False
            
            # Create directory if needed
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            
            with open(save_path, "wb") as f:
                f.write(data)
            
            return True
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as e:
        print(f"    Failed to download {url}: {e}")
        return False


class ImageManifest:
    """Tracks downloaded images for later enrichment."""
    
    def __init__(self, data_root: str, franchise_slug: str):
        self.data_root = Path(data_root)
        self.franchise_slug = franchise_slug
        self.manifest_path = self.data_root / "sources" / franchise_slug / "image_manifest.json"
        self.images_dir = self.data_root / IMAGES_DIR / franchise_slug
        
        # Load existing manifest or create new
        if self.manifest_path.exists():
            with open(self.manifest_path) as f:
                self.data = json.load(f)
        else:
            self.data = {
                "franchise": franchise_slug,
                "creators": {},
                "entries": {},
                "characters": {},
                "companies": {},
            }
    
    def save(self):
        """Write manifest to disk."""
        os.makedirs(self.manifest_path.parent, exist_ok=True)
        with open(self.manifest_path, "w") as f:
            json.dump(self.data, f, indent=2)
    
    def add_image(
        self,
        entity_type: str,  # "creators", "entries", "characters", "companies"
        source: str,       # "anilist", "mal", "tvdb"
        source_id: str,    # ID in that source
        name: str,         # Entity name for filename
        image_url: str,    # URL to download
    ) -> Optional[str]:
        """Download image and add to manifest. Returns relative path or None."""
        if not image_url:
            return None
        
        # Build filename
        slug = slugify(name)
        ext = get_extension(image_url)
        filename = f"{source}-{source_id}-{slug}.{ext}"
        
        # Paths
        rel_path = f"{IMAGES_DIR}/{self.franchise_slug}/{entity_type}/{filename}"
        abs_path = self.data_root / rel_path
        
        # Download if not already exists
        if not abs_path.exists():
            print(f"    Downloading {entity_type} image: {filename}")
            if not download_image(image_url, str(abs_path)):
                return None
        
        # Add to manifest
        key = f"{source}:{source_id}"
        if entity_type not in self.data:
            self.data[entity_type] = {}
        self.data[entity_type][key] = rel_path
        
        return rel_path
    
    def get_best_image(self, entity_type: str, external_ids: dict) -> Optional[str]:
        """Get best available image path based on preference hierarchy."""
        prefs = IMAGE_PREFERENCE.get(entity_type, ["anilist", "mal"])
        
        for source in prefs:
            source_id = external_ids.get(f"{source}_id")
            if source_id:
                key = f"{source}:{source_id}"
                if key in self.data.get(entity_type, {}):
                    return self.data[entity_type][key]
        
        return None


def extract_and_download_images(
    manifest: ImageManifest,
    source: str,
    data: dict,
) -> None:
    """Extract image URLs from source data and download them."""
    
    if source == "anilist":
        _download_anilist_images(manifest, data)
    elif source == "mal":
        _download_mal_images(manifest, data)
    elif source == "tvdb":
        _download_tvdb_images(manifest, data)


def _download_anilist_images(manifest: ImageManifest, data: dict) -> None:
    """Download images from AniList data."""
    anilist_id = str(data.get("id", ""))
    
    # Entry cover
    cover = data.get("coverImage", {}).get("large")
    if cover:
        title = data.get("title", {}).get("romaji") or data.get("title", {}).get("english") or "unknown"
        manifest.add_image("entries", "anilist", anilist_id, title, cover)
    
    # Staff images
    for edge in data.get("staff", {}).get("edges", []):
        node = edge.get("node", {})
        staff_id = str(node.get("id", ""))
        name = node.get("name", {}).get("full", "unknown")
        image = node.get("image", {}).get("large")
        if staff_id and image:
            manifest.add_image("creators", "anilist", staff_id, name, image)
    
    # Character images
    for edge in data.get("characters", {}).get("edges", []):
        node = edge.get("node", {})
        char_id = str(node.get("id", ""))
        name = node.get("name", {}).get("full", "unknown")
        image = node.get("image", {}).get("large")
        if char_id and image:
            manifest.add_image("characters", "anilist", char_id, name, image)
        
        # Voice actor images
        for va in edge.get("voiceActors", []):
            va_id = str(va.get("id", ""))
            va_name = va.get("name", {}).get("full", "unknown")
            va_image = va.get("image", {}).get("large")
            if va_id and va_image:
                manifest.add_image("creators", "anilist", va_id, va_name, va_image)


def _download_mal_images(manifest: ImageManifest, data: dict) -> None:
    """Download images from MAL/Jikan data."""
    anime = data.get("anime", {})
    mal_id = str(anime.get("mal_id", ""))
    
    # Entry cover
    images = anime.get("images", {}).get("jpg", {})
    cover = images.get("large_image_url") or images.get("image_url")
    if cover:
        title = anime.get("title", "unknown")
        manifest.add_image("entries", "mal", mal_id, title, cover)
    
    # Staff images
    for s in data.get("staff", []):
        person = s.get("person", {})
        person_id = str(person.get("mal_id", ""))
        name = person.get("name", "unknown")
        image = person.get("images", {}).get("jpg", {}).get("image_url")
        if person_id and image:
            manifest.add_image("creators", "mal", person_id, name, image)
    
    # Character images
    for c in data.get("characters", []):
        char = c.get("character", {})
        char_id = str(char.get("mal_id", ""))
        name = char.get("name", "unknown")
        image = char.get("images", {}).get("jpg", {}).get("image_url")
        if char_id and image:
            manifest.add_image("characters", "mal", char_id, name, image)
        
        # Voice actor images
        for va in c.get("voice_actors", []):
            person = va.get("person", {})
            va_id = str(person.get("mal_id", ""))
            va_name = person.get("name", "unknown")
            va_image = person.get("images", {}).get("jpg", {}).get("image_url")
            if va_id and va_image:
                manifest.add_image("creators", "mal", va_id, va_name, va_image)


def _download_tvdb_images(manifest: ImageManifest, data: dict) -> None:
    """Download images from TVDB data."""
    tvdb_id = str(data.get("id", ""))
    
    # Series poster
    image = data.get("image")
    if image and image.startswith("http"):
        name = data.get("name") or "unknown"
        manifest.add_image("entries", "tvdb", tvdb_id, name, image)


if __name__ == "__main__":
    # Test
    import sys
    
    data_root = os.path.join(os.path.dirname(__file__), "..", "the-watchlist-data")
    franchise = sys.argv[1] if len(sys.argv) > 1 else "sentenced-to-be-a-hero"
    
    manifest = ImageManifest(data_root, franchise)
    sources_dir = os.path.join(data_root, "sources", franchise)
    
    # Process AniList
    al_dir = os.path.join(sources_dir, "anilist")
    if os.path.exists(al_dir):
        for f in os.listdir(al_dir):
            if f.endswith(".json"):
                print(f"Processing AniList {f}...")
                with open(os.path.join(al_dir, f)) as fh:
                    data = json.load(fh)
                extract_and_download_images(manifest, "anilist", data)
    
    # Process MAL
    mal_dir = os.path.join(sources_dir, "mal")
    if os.path.exists(mal_dir):
        for f in os.listdir(mal_dir):
            if f.endswith(".json"):
                print(f"Processing MAL {f}...")
                with open(os.path.join(mal_dir, f)) as fh:
                    data = json.load(fh)
                extract_and_download_images(manifest, "mal", data)
    
    # Process TVDB
    tvdb_dir = os.path.join(sources_dir, "tvdb")
    if os.path.exists(tvdb_dir):
        for f in os.listdir(tvdb_dir):
            if f.endswith(".json"):
                print(f"Processing TVDB {f}...")
                with open(os.path.join(tvdb_dir, f)) as fh:
                    data = json.load(fh)
                extract_and_download_images(manifest, "tvdb", data)
    
    manifest.save()
    print(f"\nManifest saved: {manifest.manifest_path}")
    print(f"Entries: {len(manifest.data.get('entries', {}))}")
    print(f"Creators: {len(manifest.data.get('creators', {}))}")
    print(f"Characters: {len(manifest.data.get('characters', {}))}")
