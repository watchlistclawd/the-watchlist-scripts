#!/usr/bin/env python3
"""
Build a manifest of all images across entities and entries.
Tracks images for download and local storage.
"""
import os
import sys
from pathlib import Path
from utils import load_json, save_json, DATA_ROOT


def scan_character_images() -> list:
    """Scan all characters and collect their images."""
    images = []
    chars_dir = os.path.join(DATA_ROOT, 'characters')
    
    if not os.path.exists(chars_dir):
        return images
    
    for char_slug in os.listdir(chars_dir):
        char_path = os.path.join(chars_dir, char_slug)
        if not os.path.isdir(char_path):
            continue
        
        char_json_path = os.path.join(char_path, 'character.json')
        if not os.path.exists(char_json_path):
            continue
        
        char_data = load_json(char_json_path)
        
        # Character portrait from AniList
        if char_data.get('images', {}).get('anilist'):
            images.append({
                'type': 'character_portrait',
                'entity_type': 'character',
                'entity_slug': char_slug,
                'entity_name': char_data.get('name', ''),
                'source': 'anilist',
                'url': char_data['images']['anilist'],
                'local_path': None,
                'downloaded': False
            })
        
        # Character portrait from MAL
        if char_data.get('images', {}).get('mal'):
            images.append({
                'type': 'character_portrait',
                'entity_type': 'character',
                'entity_slug': char_slug,
                'entity_name': char_data.get('name', ''),
                'source': 'mal',
                'url': char_data['images']['mal'],
                'local_path': None,
                'downloaded': False
            })
        
        # Voice actor images
        for va in char_data.get('voice_actors', []):
            if va.get('anilist_image'):
                images.append({
                    'type': 'voice_actor_portrait',
                    'entity_type': 'character',
                    'entity_slug': char_slug,
                    'entity_name': char_data.get('name', ''),
                    'va_name': va.get('name', ''),
                    'source': 'anilist',
                    'url': va['anilist_image'],
                    'local_path': None,
                    'downloaded': False
                })
            
            if va.get('mal_image'):
                images.append({
                    'type': 'voice_actor_portrait',
                    'entity_type': 'character',
                    'entity_slug': char_slug,
                    'entity_name': char_data.get('name', ''),
                    'va_name': va.get('name', ''),
                    'source': 'mal',
                    'url': va['mal_image'],
                    'local_path': None,
                    'downloaded': False
                })
    
    return images


def scan_creator_images() -> list:
    """Scan all creators and collect their images."""
    images = []
    creators_dir = os.path.join(DATA_ROOT, 'creators')
    
    if not os.path.exists(creators_dir):
        return images
    
    for creator_slug in os.listdir(creators_dir):
        creator_path = os.path.join(creators_dir, creator_slug)
        if not os.path.isdir(creator_path):
            continue
        
        creator_json_path = os.path.join(creator_path, 'creator.json')
        if not os.path.exists(creator_json_path):
            continue
        
        creator_data = load_json(creator_json_path)
        
        # Creator portrait from AniList
        if creator_data.get('images', {}).get('anilist'):
            images.append({
                'type': 'creator_portrait',
                'entity_type': 'creator',
                'entity_slug': creator_slug,
                'entity_name': creator_data.get('name', ''),
                'source': 'anilist',
                'url': creator_data['images']['anilist'],
                'local_path': None,
                'downloaded': False
            })
        
        # Creator portrait from MAL
        if creator_data.get('images', {}).get('mal'):
            images.append({
                'type': 'creator_portrait',
                'entity_type': 'creator',
                'entity_slug': creator_slug,
                'entity_name': creator_data.get('name', ''),
                'source': 'mal',
                'url': creator_data['images']['mal'],
                'local_path': None,
                'downloaded': False
            })
    
    return images


def scan_entry_images() -> list:
    """Scan all franchise entries and collect their images (posters, etc.)."""
    images = []
    franchises_dir = os.path.join(DATA_ROOT, 'franchises')
    
    if not os.path.exists(franchises_dir):
        return images
    
    for franchise_slug in os.listdir(franchises_dir):
        franchise_path = os.path.join(franchises_dir, franchise_slug)
        if not os.path.isdir(franchise_path):
            continue
        
        entries_dir = os.path.join(franchise_path, 'entries')
        if not os.path.exists(entries_dir):
            continue
        
        for entry_slug in os.listdir(entries_dir):
            entry_path = os.path.join(entries_dir, entry_slug)
            if not os.path.isdir(entry_path):
                continue
            
            # Check for entry metadata
            entry_json_path = os.path.join(entry_path, 'entry.json')
            if os.path.exists(entry_json_path):
                entry_data = load_json(entry_json_path)
                
                # Entry cover images
                if entry_data.get('images', {}).get('anilist'):
                    images.append({
                        'type': 'entry_cover',
                        'entity_type': 'entry',
                        'franchise_slug': franchise_slug,
                        'entry_slug': entry_slug,
                        'entry_name': entry_data.get('name', ''),
                        'source': 'anilist',
                        'url': entry_data['images']['anilist'],
                        'local_path': None,
                        'downloaded': False
                    })
                
                if entry_data.get('images', {}).get('mal'):
                    images.append({
                        'type': 'entry_cover',
                        'entity_type': 'entry',
                        'franchise_slug': franchise_slug,
                        'entry_slug': entry_slug,
                        'entry_name': entry_data.get('name', ''),
                        'source': 'mal',
                        'url': entry_data['images']['mal'],
                        'local_path': None,
                        'downloaded': False
                    })
    
    return images


def build_manifest():
    """Build the complete image manifest."""
    print("Scanning for images...")
    
    all_images = []
    
    # Scan characters
    print("  Scanning characters...")
    char_images = scan_character_images()
    all_images.extend(char_images)
    print(f"    Found {len(char_images)} images")
    
    # Scan creators
    print("  Scanning creators...")
    creator_images = scan_creator_images()
    all_images.extend(creator_images)
    print(f"    Found {len(creator_images)} images")
    
    # Scan entries
    print("  Scanning entries...")
    entry_images = scan_entry_images()
    all_images.extend(entry_images)
    print(f"    Found {len(entry_images)} images")
    
    # Build manifest
    manifest = {
        'version': '1.0',
        'total_images': len(all_images),
        'images': all_images
    }
    
    # Save manifest
    manifest_path = os.path.join(DATA_ROOT, 'image_manifest.json')
    save_json(manifest_path, manifest)
    
    print(f"\n✓ Image manifest created!")
    print(f"  Total images: {len(all_images)}")
    print(f"  Character portraits: {len([i for i in all_images if i['type'] == 'character_portrait'])}")
    print(f"  Voice actor portraits: {len([i for i in all_images if i['type'] == 'voice_actor_portrait'])}")
    print(f"  Creator portraits: {len([i for i in all_images if i['type'] == 'creator_portrait'])}")
    print(f"  Entry covers: {len([i for i in all_images if i['type'] == 'entry_cover'])}")
    print(f"\nSaved to: {manifest_path}")


def main():
    print("Building image manifest...")
    print("Scanning all entities and entries for images\n")
    
    build_manifest()


if __name__ == '__main__':
    main()
