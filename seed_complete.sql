-- ============================================================================
-- COMPLETE SEED FILE FOR THE WATCHLIST
-- ============================================================================
-- Run on fresh DB or after wipe
-- Covers: locales, countries, media_types, company_roles, creator_roles,
--         relationship_types, genres, tags
-- ============================================================================

-- ----------------------------------------------------------------------------
-- LOCALES (ISO 639-1)
-- ----------------------------------------------------------------------------
INSERT INTO locales (code, name, native_name) VALUES
    ('en', 'English', 'English'),
    ('ja', 'Japanese', '日本語'),
    ('ko', 'Korean', '한국어'),
    ('zh', 'Chinese', '中文'),
    ('zh-TW', 'Traditional Chinese', '繁體中文'),
    ('es', 'Spanish', 'Español'),
    ('fr', 'French', 'Français'),
    ('de', 'German', 'Deutsch'),
    ('pt', 'Portuguese', 'Português'),
    ('pt-BR', 'Brazilian Portuguese', 'Português Brasileiro'),
    ('it', 'Italian', 'Italiano'),
    ('ru', 'Russian', 'Русский'),
    ('ar', 'Arabic', 'العربية'),
    ('th', 'Thai', 'ไทย'),
    ('vi', 'Vietnamese', 'Tiếng Việt'),
    ('id', 'Indonesian', 'Bahasa Indonesia'),
    ('ms', 'Malay', 'Bahasa Melayu'),
    ('tl', 'Filipino', 'Filipino'),
    ('pl', 'Polish', 'Polski'),
    ('nl', 'Dutch', 'Nederlands'),
    ('tr', 'Turkish', 'Türkçe'),
    ('hi', 'Hindi', 'हिन्दी')
ON CONFLICT (code) DO NOTHING;

-- ----------------------------------------------------------------------------
-- COUNTRIES (ISO 3166-1 alpha-2) - Major markets
-- ----------------------------------------------------------------------------
INSERT INTO countries (code, name) VALUES
    ('JP', 'Japan'),
    ('US', 'United States'),
    ('KR', 'South Korea'),
    ('CN', 'China'),
    ('TW', 'Taiwan'),
    ('GB', 'United Kingdom'),
    ('CA', 'Canada'),
    ('AU', 'Australia'),
    ('NZ', 'New Zealand'),
    ('FR', 'France'),
    ('DE', 'Germany'),
    ('IT', 'Italy'),
    ('ES', 'Spain'),
    ('MX', 'Mexico'),
    ('BR', 'Brazil'),
    ('AR', 'Argentina'),
    ('RU', 'Russia'),
    ('PL', 'Poland'),
    ('NL', 'Netherlands'),
    ('SE', 'Sweden'),
    ('NO', 'Norway'),
    ('DK', 'Denmark'),
    ('FI', 'Finland'),
    ('TH', 'Thailand'),
    ('VN', 'Vietnam'),
    ('ID', 'Indonesia'),
    ('MY', 'Malaysia'),
    ('SG', 'Singapore'),
    ('PH', 'Philippines'),
    ('IN', 'India')
ON CONFLICT (code) DO NOTHING;

-- ----------------------------------------------------------------------------
-- MEDIA TYPES
-- ----------------------------------------------------------------------------
INSERT INTO media_types (id, name, display_name, description) VALUES
    (gen_random_uuid(), 'anime', 'Anime', 'Japanese animated series and films'),
    (gen_random_uuid(), 'manga', 'Manga', 'Japanese comics and graphic novels'),
    (gen_random_uuid(), 'light_novel', 'Light Novel', 'Japanese young adult novels'),
    (gen_random_uuid(), 'movie', 'Movie', 'Feature films'),
    (gen_random_uuid(), 'tv', 'TV Series', 'Live-action television series'),
    (gen_random_uuid(), 'ova', 'OVA', 'Original Video Animation'),
    (gen_random_uuid(), 'ona', 'ONA', 'Original Net Animation'),
    (gen_random_uuid(), 'special', 'Special', 'TV specials and one-offs'),
    (gen_random_uuid(), 'music', 'Music', 'Albums, singles, soundtracks'),
    (gen_random_uuid(), 'game', 'Video Game', 'Video games'),
    (gen_random_uuid(), 'novel', 'Novel', 'Books and novels'),
    (gen_random_uuid(), 'comic', 'Comic', 'Western comics'),
    (gen_random_uuid(), 'manhwa', 'Manhwa', 'Korean comics'),
    (gen_random_uuid(), 'manhua', 'Manhua', 'Chinese comics'),
    (gen_random_uuid(), 'doujinshi', 'Doujinshi', 'Self-published works')
ON CONFLICT (name) DO NOTHING;

-- ----------------------------------------------------------------------------
-- COMPANY ROLES
-- ----------------------------------------------------------------------------
INSERT INTO company_roles (id, name, display_name, description) VALUES
    (gen_random_uuid(), 'animation_studio', 'Animation Studio', 'Primary animation production'),
    (gen_random_uuid(), 'studio', 'Studio', 'Production studio'),
    (gen_random_uuid(), 'producer', 'Producer', 'Production company or production committee member'),
    (gen_random_uuid(), 'distributor', 'Distributor', 'Distribution company'),
    (gen_random_uuid(), 'publisher', 'Publisher', 'Publishing company (manga, light novels, books)'),
    (gen_random_uuid(), 'licensor', 'Licensor', 'Licensing rights holder'),
    (gen_random_uuid(), 'broadcaster', 'Broadcaster', 'TV network or streaming platform'),
    (gen_random_uuid(), 'developer', 'Developer', 'Game/software development studio'),
    (gen_random_uuid(), 'record_label', 'Record Label', 'Music label'),
    (gen_random_uuid(), 'manufacturer', 'Manufacturer', 'Product manufacturer'),
    (gen_random_uuid(), 'serialization', 'Serialization', 'Magazine or platform where manga is serialized')
ON CONFLICT (name) DO NOTHING;

-- ----------------------------------------------------------------------------
-- CREATOR ROLES
-- ----------------------------------------------------------------------------
-- Direction
INSERT INTO creator_roles (id, name, display_name, category, description) VALUES
    (gen_random_uuid(), 'director', 'Director', 'direction', 'Series or film director'),
    (gen_random_uuid(), 'chief_director', 'Chief Director', 'direction', 'Overall series director (supervises episode directors)'),
    (gen_random_uuid(), 'episode_director', 'Episode Director', 'direction', 'Individual episode director'),
    (gen_random_uuid(), 'assistant_director', 'Assistant Director', 'direction', 'Assistant to director'),
    (gen_random_uuid(), 'unit_director', 'Unit Director', 'direction', 'Director of specific production unit')
ON CONFLICT (name) DO NOTHING;

-- Story/Writing
INSERT INTO creator_roles (id, name, display_name, category, description) VALUES
    (gen_random_uuid(), 'original_creator', 'Original Creator', 'story', 'Creator of the original source material'),
    (gen_random_uuid(), 'original_story', 'Original Story', 'story', 'Original story creator (when different from source)'),
    (gen_random_uuid(), 'original_character_design', 'Original Character Design', 'story', 'Character designer for source material'),
    (gen_random_uuid(), 'series_composition', 'Series Composition', 'writing', 'Overall story structure and script supervision'),
    (gen_random_uuid(), 'screenplay', 'Screenplay', 'writing', 'Script/screenplay writer'),
    (gen_random_uuid(), 'script', 'Script', 'writing', 'Episode script writer'),
    (gen_random_uuid(), 'storyboard', 'Storyboard', 'visual', 'Storyboard artist')
ON CONFLICT (name) DO NOTHING;

-- Visual/Animation
INSERT INTO creator_roles (id, name, display_name, category, description) VALUES
    (gen_random_uuid(), 'character_design', 'Character Design', 'visual', 'Anime character designer'),
    (gen_random_uuid(), 'chief_animation_director', 'Chief Animation Director', 'animation', 'Overall animation quality supervisor'),
    (gen_random_uuid(), 'animation_director', 'Animation Director', 'animation', 'Animation supervision per episode'),
    (gen_random_uuid(), 'key_animation', 'Key Animation', 'animation', 'Key animator'),
    (gen_random_uuid(), 'second_key_animation', 'Second Key Animation', 'animation', 'Second key animator'),
    (gen_random_uuid(), 'in_between', 'In-Between Animation', 'animation', 'In-between animator'),
    (gen_random_uuid(), 'art_director', 'Art Director', 'visual', 'Background and setting design'),
    (gen_random_uuid(), 'art_design', 'Art Design', 'visual', 'Art design'),
    (gen_random_uuid(), 'color_design', 'Color Design', 'visual', 'Color palette design'),
    (gen_random_uuid(), 'compositing', 'Compositing', 'visual', 'Digital compositing'),
    (gen_random_uuid(), 'cgi_director', 'CGI Director', 'visual', '3D/CG direction'),
    (gen_random_uuid(), 'photography_director', 'Director of Photography', 'visual', 'Cinematography/photography direction'),
    (gen_random_uuid(), 'editing', 'Editing', 'visual', 'Video editing')
ON CONFLICT (name) DO NOTHING;

-- Audio
INSERT INTO creator_roles (id, name, display_name, category, description) VALUES
    (gen_random_uuid(), 'music', 'Music', 'audio', 'Music composer'),
    (gen_random_uuid(), 'music_production', 'Music Production', 'audio', 'Music production'),
    (gen_random_uuid(), 'sound_director', 'Sound Director', 'audio', 'Sound design and direction'),
    (gen_random_uuid(), 'sound_effects', 'Sound Effects', 'audio', 'Sound effects creation'),
    (gen_random_uuid(), 'sound_production', 'Sound Production', 'audio', 'Sound production')
ON CONFLICT (name) DO NOTHING;

-- Production
INSERT INTO creator_roles (id, name, display_name, category, description) VALUES
    (gen_random_uuid(), 'producer', 'Producer', 'production', 'Production management'),
    (gen_random_uuid(), 'executive_producer', 'Executive Producer', 'production', 'Executive production oversight'),
    (gen_random_uuid(), 'planning', 'Planning', 'production', 'Project planning'),
    (gen_random_uuid(), 'production_manager', 'Production Manager', 'production', 'Production management'),
    (gen_random_uuid(), 'editor', 'Editor', 'production', 'Editorial work (manga/publishing)')
ON CONFLICT (name) DO NOTHING;

-- Cast
INSERT INTO creator_roles (id, name, display_name, category, description) VALUES
    (gen_random_uuid(), 'voice_actor', 'Voice Actor', 'cast', 'Voice acting'),
    (gen_random_uuid(), 'narrator', 'Narrator', 'cast', 'Narration'),
    (gen_random_uuid(), 'adr_director', 'ADR Director', 'cast', 'Automated Dialogue Replacement direction')
ON CONFLICT (name) DO NOTHING;

-- Music Performance
INSERT INTO creator_roles (id, name, display_name, category, description) VALUES
    (gen_random_uuid(), 'theme_song_performance', 'Theme Song Performance', 'music', 'OP/ED performance'),
    (gen_random_uuid(), 'theme_song_composition', 'Theme Song Composition', 'music', 'OP/ED composition'),
    (gen_random_uuid(), 'theme_song_lyrics', 'Theme Song Lyrics', 'music', 'OP/ED lyrics'),
    (gen_random_uuid(), 'theme_song_arrangement', 'Theme Song Arrangement', 'music', 'OP/ED arrangement'),
    (gen_random_uuid(), 'insert_song_performance', 'Insert Song Performance', 'music', 'Insert song performance')
ON CONFLICT (name) DO NOTHING;

-- Literary
INSERT INTO creator_roles (id, name, display_name, category, description) VALUES
    (gen_random_uuid(), 'author', 'Author', 'story', 'Book/manga/light novel author'),
    (gen_random_uuid(), 'artist', 'Artist', 'visual', 'Manga/comic artist'),
    (gen_random_uuid(), 'illustrator', 'Illustrator', 'visual', 'Light novel/book illustrator')
ON CONFLICT (name) DO NOTHING;

-- ----------------------------------------------------------------------------
-- RELATIONSHIP TYPES
-- ----------------------------------------------------------------------------
INSERT INTO relationship_types (id, name, display_name, inverse_name, inverse_display_name, is_directional, description) VALUES
    (gen_random_uuid(), 'sequel', 'Sequel', 'prequel', 'Prequel', true, 'Chronological sequel'),
    (gen_random_uuid(), 'prequel', 'Prequel', 'sequel', 'Sequel', true, 'Chronological prequel'),
    (gen_random_uuid(), 'parent', 'Parent Story', 'spinoff', 'Spin-off', true, 'Main story that spinoffs derive from'),
    (gen_random_uuid(), 'spinoff', 'Spin-off', 'parent', 'Parent Story', true, 'Derived work focusing on side elements'),
    (gen_random_uuid(), 'adaptation', 'Adaptation', 'source', 'Source Material', true, 'Adapted from another medium'),
    (gen_random_uuid(), 'source', 'Source Material', 'adaptation', 'Adaptation', true, 'Original source material'),
    (gen_random_uuid(), 'alternative', 'Alternative Version', 'alternative', 'Alternative Version', false, 'Alternative retelling or version'),
    (gen_random_uuid(), 'side_story', 'Side Story', 'main_story', 'Main Story', true, 'Related side story'),
    (gen_random_uuid(), 'main_story', 'Main Story', 'side_story', 'Side Story', true, 'Main story'),
    (gen_random_uuid(), 'summary', 'Summary', 'full', 'Full Version', true, 'Condensed recap version'),
    (gen_random_uuid(), 'full', 'Full Version', 'summary', 'Summary', true, 'Full version'),
    (gen_random_uuid(), 'other', 'Other', 'other', 'Other', false, 'Other related work')
ON CONFLICT (name) DO NOTHING;

-- ----------------------------------------------------------------------------
-- GENRES (Universal + Anime-specific)
-- ----------------------------------------------------------------------------
-- Universal genres (media_type_id = NULL means applies to all)
INSERT INTO genres (id, name, display_name, description) VALUES
    (gen_random_uuid(), 'action', 'Action', 'Emphasizes physical feats, combat, and excitement'),
    (gen_random_uuid(), 'adventure', 'Adventure', 'Focuses on journeys, exploration, and quests'),
    (gen_random_uuid(), 'comedy', 'Comedy', 'Intended to be humorous or amusing'),
    (gen_random_uuid(), 'drama', 'Drama', 'Focuses on realistic emotional themes'),
    (gen_random_uuid(), 'fantasy', 'Fantasy', 'Features magical or supernatural elements'),
    (gen_random_uuid(), 'horror', 'Horror', 'Intended to frighten or disturb'),
    (gen_random_uuid(), 'mystery', 'Mystery', 'Centers on solving crimes or puzzles'),
    (gen_random_uuid(), 'romance', 'Romance', 'Focuses on romantic relationships'),
    (gen_random_uuid(), 'sci_fi', 'Sci-Fi', 'Features futuristic or scientific concepts'),
    (gen_random_uuid(), 'slice_of_life', 'Slice of Life', 'Depicts everyday experiences'),
    (gen_random_uuid(), 'sports', 'Sports', 'Centers on athletic competition'),
    (gen_random_uuid(), 'supernatural', 'Supernatural', 'Features ghosts, spirits, or paranormal'),
    (gen_random_uuid(), 'thriller', 'Thriller', 'Emphasizes tension and suspense'),
    (gen_random_uuid(), 'psychological', 'Psychological', 'Focuses on mental states and mind games');

-- Anime demographics (not genres but commonly categorized as such)
INSERT INTO genres (id, name, display_name, description) VALUES
    (gen_random_uuid(), 'shounen', 'Shounen', 'Targeted at young male audience (12-18)'),
    (gen_random_uuid(), 'shoujo', 'Shoujo', 'Targeted at young female audience (12-18)'),
    (gen_random_uuid(), 'seinen', 'Seinen', 'Targeted at adult male audience (18+)'),
    (gen_random_uuid(), 'josei', 'Josei', 'Targeted at adult female audience (18+)'),
    (gen_random_uuid(), 'kids', 'Kids', 'Targeted at children');

-- Anime-specific genres
INSERT INTO genres (id, name, display_name, description) VALUES
    (gen_random_uuid(), 'mecha', 'Mecha', 'Features giant robots or mechanical suits'),
    (gen_random_uuid(), 'isekai', 'Isekai', 'Protagonist transported to another world'),
    (gen_random_uuid(), 'ecchi', 'Ecchi', 'Mildly sexual content'),
    (gen_random_uuid(), 'harem', 'Harem', 'One protagonist with multiple romantic interests'),
    (gen_random_uuid(), 'reverse_harem', 'Reverse Harem', 'Female protagonist with multiple male interests'),
    (gen_random_uuid(), 'mahou_shoujo', 'Magical Girl', 'Features girls with magical powers'),
    (gen_random_uuid(), 'martial_arts', 'Martial Arts', 'Emphasizes hand-to-hand combat techniques'),
    (gen_random_uuid(), 'music_genre', 'Music', 'Centers on musicians or music industry'),
    (gen_random_uuid(), 'parody', 'Parody', 'Satirizes or parodies other works/genres'),
    (gen_random_uuid(), 'school', 'School', 'Set primarily in a school environment'),
    (gen_random_uuid(), 'space', 'Space', 'Set in outer space'),
    (gen_random_uuid(), 'vampire', 'Vampire', 'Features vampires prominently'),
    (gen_random_uuid(), 'military', 'Military', 'Features military themes or settings'),
    (gen_random_uuid(), 'historical', 'Historical', 'Set in a historical period'),
    (gen_random_uuid(), 'samurai', 'Samurai', 'Features samurai warriors'),
    (gen_random_uuid(), 'super_power', 'Super Power', 'Characters with superhuman abilities'),
    (gen_random_uuid(), 'demons', 'Demons', 'Features demons prominently'),
    (gen_random_uuid(), 'game_genre', 'Game', 'Centers on games or game-like settings'),
    (gen_random_uuid(), 'gourmet', 'Gourmet', 'Focuses on food and cooking'),
    (gen_random_uuid(), 'idol', 'Idol', 'Features idol performers'),
    (gen_random_uuid(), 'cgdct', 'CGDCT', 'Cute Girls Doing Cute Things'),
    (gen_random_uuid(), 'boys_love', 'Boys Love', 'Male/male romantic relationships'),
    (gen_random_uuid(), 'girls_love', 'Girls Love', 'Female/female romantic relationships');

-- ----------------------------------------------------------------------------
-- TAGS (Themes, Moods, Content Warnings, Tropes)
-- ----------------------------------------------------------------------------
-- Themes
INSERT INTO tags (id, name, display_name, category, description) VALUES
    (gen_random_uuid(), 'anti_hero', 'Anti-Hero', 'theme', 'Protagonist with morally ambiguous traits'),
    (gen_random_uuid(), 'revenge', 'Revenge', 'theme', 'Plot driven by vengeance'),
    (gen_random_uuid(), 'conspiracy', 'Conspiracy', 'theme', 'Features hidden plots and schemes'),
    (gen_random_uuid(), 'time_travel', 'Time Travel', 'theme', 'Involves traveling through time'),
    (gen_random_uuid(), 'coming_of_age', 'Coming of Age', 'theme', 'Focus on personal growth and maturity'),
    (gen_random_uuid(), 'found_family', 'Found Family', 'theme', 'Non-blood relatives forming family bonds'),
    (gen_random_uuid(), 'survival', 'Survival', 'theme', 'Characters struggling to survive'),
    (gen_random_uuid(), 'war', 'War', 'theme', 'Set during wartime'),
    (gen_random_uuid(), 'politics', 'Politics', 'theme', 'Features political intrigue'),
    (gen_random_uuid(), 'reincarnation', 'Reincarnation', 'theme', 'Character reborn into new life'),
    (gen_random_uuid(), 'death_game', 'Death Game', 'theme', 'Characters compete in lethal games'),
    (gen_random_uuid(), 'post_apocalyptic', 'Post-Apocalyptic', 'theme', 'Set after civilization collapse'),
    (gen_random_uuid(), 'dystopia', 'Dystopia', 'theme', 'Set in oppressive society'),
    (gen_random_uuid(), 'amnesia', 'Amnesia', 'theme', 'Character with memory loss'),
    (gen_random_uuid(), 'body_swap', 'Body Swap', 'theme', 'Characters exchange bodies'),
    (gen_random_uuid(), 'time_loop', 'Time Loop', 'theme', 'Characters repeat same time period'),
    (gen_random_uuid(), 'tournament', 'Tournament', 'theme', 'Features competitive tournament arc'),
    (gen_random_uuid(), 'dungeon', 'Dungeon', 'theme', 'Features dungeon exploration'),
    (gen_random_uuid(), 'gods', 'Gods', 'theme', 'Features deities or divine beings'),
    (gen_random_uuid(), 'crime', 'Crime', 'theme', 'Features criminal activities')
ON CONFLICT (name) DO NOTHING;

-- Settings
INSERT INTO tags (id, name, display_name, category, description) VALUES
    (gen_random_uuid(), 'medieval', 'Medieval', 'setting', 'Medieval European setting'),
    (gen_random_uuid(), 'modern', 'Modern', 'setting', 'Contemporary setting'),
    (gen_random_uuid(), 'futuristic', 'Futuristic', 'setting', 'Far future setting'),
    (gen_random_uuid(), 'urban', 'Urban', 'setting', 'City setting'),
    (gen_random_uuid(), 'rural', 'Rural', 'setting', 'Countryside setting'),
    (gen_random_uuid(), 'cyberpunk', 'Cyberpunk', 'setting', 'High-tech low-life setting'),
    (gen_random_uuid(), 'steampunk', 'Steampunk', 'setting', 'Victorian-era technology setting')
ON CONFLICT (name) DO NOTHING;

-- Moods
INSERT INTO tags (id, name, display_name, category, description) VALUES
    (gen_random_uuid(), 'dark', 'Dark', 'mood', 'Grim, dark tone'),
    (gen_random_uuid(), 'lighthearted', 'Lighthearted', 'mood', 'Light, fun tone'),
    (gen_random_uuid(), 'cozy', 'Cozy', 'mood', 'Warm, comfortable feeling'),
    (gen_random_uuid(), 'melancholic', 'Melancholic', 'mood', 'Sad, reflective tone'),
    (gen_random_uuid(), 'intense', 'Intense', 'mood', 'High tension throughout')
ON CONFLICT (name) DO NOTHING;

-- Content warnings
INSERT INTO tags (id, name, display_name, category, description) VALUES
    (gen_random_uuid(), 'gore', 'Gore', 'content_warning', 'Graphic violence and blood'),
    (gen_random_uuid(), 'violence', 'Violence', 'content_warning', 'Physical violence'),
    (gen_random_uuid(), 'sexual_content', 'Sexual Content', 'content_warning', 'Sexual themes or scenes'),
    (gen_random_uuid(), 'nudity', 'Nudity', 'content_warning', 'Nude scenes'),
    (gen_random_uuid(), 'substance_abuse', 'Substance Abuse', 'content_warning', 'Drug or alcohol abuse'),
    (gen_random_uuid(), 'suicide_themes', 'Suicide Themes', 'content_warning', 'Depictions of suicide'),
    (gen_random_uuid(), 'child_death', 'Child Death', 'content_warning', 'Death of minors')
ON CONFLICT (name) DO NOTHING;

-- Character archetypes/tropes
INSERT INTO tags (id, name, display_name, category, description) VALUES
    (gen_random_uuid(), 'male_protagonist', 'Male Protagonist', 'trope', 'Male main character'),
    (gen_random_uuid(), 'female_protagonist', 'Female Protagonist', 'trope', 'Female main character'),
    (gen_random_uuid(), 'ensemble_cast', 'Ensemble Cast', 'trope', 'Multiple main characters'),
    (gen_random_uuid(), 'overpowered', 'Overpowered', 'trope', 'Extremely powerful protagonist'),
    (gen_random_uuid(), 'underdog', 'Underdog', 'trope', 'Weak protagonist who grows'),
    (gen_random_uuid(), 'genius', 'Genius', 'trope', 'Highly intelligent character'),
    (gen_random_uuid(), 'tsundere', 'Tsundere', 'trope', 'Cold exterior, warm interior'),
    (gen_random_uuid(), 'kuudere', 'Kuudere', 'trope', 'Calm and emotionless exterior'),
    (gen_random_uuid(), 'yandere', 'Yandere', 'trope', 'Obsessive love to dangerous degree')
ON CONFLICT (name) DO NOTHING;

-- Summary
SELECT 
    (SELECT COUNT(*) FROM locales) as locales,
    (SELECT COUNT(*) FROM countries) as countries,
    (SELECT COUNT(*) FROM media_types) as media_types,
    (SELECT COUNT(*) FROM company_roles) as company_roles,
    (SELECT COUNT(*) FROM creator_roles) as creator_roles,
    (SELECT COUNT(*) FROM relationship_types) as relationship_types,
    (SELECT COUNT(*) FROM genres) as genres,
    (SELECT COUNT(*) FROM tags) as tags;
