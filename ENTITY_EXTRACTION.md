# Entity Extraction System

## Overview

This system extracts, cross-references, and populates entities (characters, creators, companies) from franchise raw data. The critical rule: **entities must appear in BOTH AniList AND MAL to be included.**

## Scripts

### 1. extract_entities.py

Extracts entities from a single franchise's raw data.

**Usage:**
```bash
python3 extract_entities.py <franchise-slug>

# Example
python3 extract_entities.py attack-on-titan
```

**What it does:**
- Reads raw AniList and Jikan/MAL data from franchise entries
- Cross-references characters by ID (AniList ID == MAL ID)
- Cross-references staff/creators by normalized name
- Cross-references companies/studios by normalized name
- Outputs to `franchises/{slug}/extracted_entities/`

**Output:**
- `characters.json` - All qualifying characters with VA info
- `creators.json` - All qualifying staff/creators with roles
- `companies.json` - All qualifying studios/producers

### 2. populate_entities.py

Deduplicates and populates top-level entity folders from all franchises.

**Usage:**
```bash
python3 populate_entities.py
```

**What it does:**
- Scans all franchises for extracted_entities/
- Deduplicates entities across franchises
- Creates top-level folders: characters/, creators/, companies/
- Each entity gets:
  - Main JSON file (character.json, creator.json, or company.json)
  - sources/anilist.json
  - sources/mal.json

**Output structure:**
```
the-watchlist-data/
├── characters/
│   └── eren-yeager/
│       ├── character.json
│       └── sources/
│           ├── anilist.json
│           └── mal.json
├── creators/
│   └── hajime-isayama/
│       ├── creator.json
│       └── sources/
│           ├── anilist.json
│           └── mal.json
└── companies/
    └── wit-studio/
        ├── company.json
        └── sources/
            ├── anilist.json
            └── mal.json
```

### 3. build_image_manifest.py

Creates a comprehensive manifest of all images for tracking and future download.

**Usage:**
```bash
python3 build_image_manifest.py
```

**What it does:**
- Scans all characters, creators, and entries
- Collects all image URLs
- Tracks download status and local paths
- Outputs to `image_manifest.json`

**Output:**
```json
{
  "version": "1.0",
  "total_images": 1114,
  "images": [
    {
      "type": "character_portrait",
      "entity_slug": "eren-yeager",
      "entity_name": "Eren Yeager",
      "source": "anilist",
      "url": "https://...",
      "local_path": null,
      "downloaded": false
    }
  ]
}
```

## Workflow

**Extract entities for all franchises:**
```bash
for franchise in attack-on-titan demon-slayer little-witch-academia lupin-iii; do
  python3 extract_entities.py $franchise
done
```

**Populate top-level folders:**
```bash
python3 populate_entities.py
```

**Build image manifest:**
```bash
python3 build_image_manifest.py
```

## Cross-Reference Logic

### Characters
- **Match by ID:** AniList character.id must equal MAL character.mal_id
- **Result:** Only characters with exact ID matches are included

### Staff/Creators
- **Match by normalized name:** Handles "Last, First" format from MAL
- Normalization: lowercase, remove accents, strip punctuation
- **Result:** Only staff appearing in both sources with matching names are included

### Companies/Studios
- **Match by normalized name:** Compares AniList studios with MAL studios + producers
- **Result:** Only companies appearing in both sources with matching names are included

### Voice Actors
- Extracted as part of characters
- Japanese voice actors only (for cross-reference)
- **Match by normalized name** between AniList and MAL
- Handles name format differences

## Validation

**Check extraction results:**
```bash
cd ~/projects/the-watchlist-data/franchises/attack-on-titan/extracted_entities
cat characters.json | jq 'keys'
cat creators.json | jq 'keys'
cat companies.json | jq 'keys'
```

**Check populated entities:**
```bash
cd ~/projects/the-watchlist-data
ls characters/ | head -10
ls creators/ | head -10
ls companies/
```

**Verify specific entity:**
```bash
cat characters/eren-yeager/character.json | jq .
cat creators/hajime-isayama/creator.json | jq .
cat companies/wit-studio/company.json | jq .
```

## Current Stats

- **Characters:** 116 unique across all franchises
- **Creators:** 335 unique across all franchises
- **Companies:** 7 unique across all franchises
- **Images tracked:** 1,114 (characters, VAs, creators)

## Notes

- Background/minor characters are automatically filtered out by the cross-reference rule
- Only Japanese voice actors are cross-referenced (English/Korean/etc. VAs are excluded)
- Entities can appear in multiple franchises - the system tracks all appearances
- Studios vs. Producers: both are extracted as "companies" with type differentiation
