# TODO: Data Slimming Blacklists

## Decided
- [ ] Tags: keep only rank >= 70 (top 30%)
- [x] Images: download during fetch, save as `{source}-{sourceId}-{slug}.{ext}`, write manifest.json
  - `image_downloader.py` extracts URLs from source JSONs and downloads to `images/{franchise}/{type}/`
  - `enrich_images.py` generates SQL UPDATE statements matching via external IDs in `details` JSONB
  - Preference hierarchy:
    - creators: anilist > mal
    - entries: mal > anilist > tvdb  
    - characters: anilist > mal
  - Tested: 1,424 images across 2 franchises
- [ ] TVDB aliases: keep only eng/jpn/kor
- [ ] MAL relations: blacklist Summary/Character/Other
- [ ] MAL theme songs: strip openings/endings
- [ ] Titles: keep only eng/jpn/kor/romaji
- [ ] TVDB episodes: keep number, title (eng/jpn/kor/romaji), aired date, runtime. Strip translations arrays, airsBeforeX, images
- [ ] Voice actors: keep only eng/jpn/kor
- [ ] Slug conventions (for SEO, not primary matching):
  
  | Table | Pattern | Example |
  |-------|---------|---------|
  | entries | `{title}-{medium}-{year}` | `attack-on-titan-anime-2013` |
  | creators | `{name}-{birthyear}` or `{name}` if no birthyear | `tetsurou-araki-1976` |
  | characters | `{name}-{franchise}` | `levi-attack-on-titan` |
  | products | `{character}-{type}-{company}-{year}` | `marin-scale-good-smile-2024` |
  | companies | `{name}` | `wit-studio` |
  | franchises | `{name}` | `attack-on-titan` |
  | retailers | `{name}` | `amiami` |
  
  **Matching strategy:** Use `wikidata_id` + external IDs in `details` JSONB for deduplication.
  Slug is for SEO/URLs only.
  
- [ ] Fetch creator birthyear from AniList (`dateOfBirth { year }`) for slug generation

## Resolved
- MAL external/streaming links → KEEP (useful for end users)
