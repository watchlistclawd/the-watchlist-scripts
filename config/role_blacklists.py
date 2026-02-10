"""
Role blacklists for filtering low-value staff/company roles.
Using blacklist (not whitelist) so new roles are included by default.
"""

# Substrings to match — case-insensitive, partial match
CREATOR_ROLE_BLACKLIST = [
    # Production admin
    "assistant producer",
    "associate producer",
    "planning producer",
    "planning",  # but not "planning" as part of another role
    "production manager",
    "production assistant",
    "production coordination",
    
    # Low-level animation
    "2nd key animation",
    "in-between animation",
    "layout",
    
    # Art/color/photography (not Art Director)
    "art design",
    "background art",
    "color design",
    "color setting",
    "photography",
    
    # Music sub-roles (keep "Theme Song Performance", "Music")
    "composition",  # Theme Song Composition
    "arrangement",  # Theme Song Arrangement  
    "lyrics",       # Theme Song Lyrics
    
    # Audio technical
    "sound effects",
    "recording engineer",
    
    # Localization
    "adr script",
    
    # Misc technical
    "cg animation",
    "prop design",
    "special effects",
    "publicity",
    "editing",
    "finishing",
    "endcard",
    "talent coordination",
]

COMPANY_ROLE_BLACKLIST = [
    "other",      # AniList "Producer/Other"
    "licensor",   # Regional distribution rights
]


def is_creator_role_blocked(role: str) -> bool:
    """Check if a creator role should be filtered out."""
    role_lower = role.lower()
    return any(blocked in role_lower for blocked in CREATOR_ROLE_BLACKLIST)


def is_company_role_blocked(role: str) -> bool:
    """Check if a company role should be filtered out."""
    role_lower = role.lower()
    return any(blocked in role_lower for blocked in COMPANY_ROLE_BLACKLIST)
