#!/usr/bin/env python3
"""
Centralized rate limiter for API calls.

Token bucket per domain with retry-on-429 wrapper.
Thread-safe for future parallelism.

Usage:
    from rate_limiter import anilist_request, jikan_request
    
    data = anilist_request(query, variables)  # GraphQL POST
    data = jikan_request("/anime/1")          # REST GET
"""

import json
import time
import threading
import urllib.request
import urllib.error
import urllib.parse

# ── Token Bucket ──────────────────────────────────────────────────────────

class TokenBucket:
    """Rate limiter using token bucket algorithm."""
    
    def __init__(self, rate_per_sec: float, burst: int = 1, min_gap_sec: float = 0.0):
        """
        rate_per_sec: sustained request rate
        burst: max tokens (allows small bursts)
        min_gap_sec: minimum time between any two requests
        """
        self.rate = rate_per_sec
        self.burst = burst
        self.min_gap = min_gap_sec
        self.tokens = float(burst)
        self.last_refill = time.monotonic()
        self.last_request = 0.0
        self.lock = threading.Lock()
        self.total_wait = 0.0
        self.total_requests = 0
    
    def acquire(self):
        """Block until a token is available."""
        with self.lock:
            now = time.monotonic()
            
            # Refill tokens based on elapsed time
            elapsed = now - self.last_refill
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
            self.last_refill = now
            
            # Wait for token
            if self.tokens < 1.0:
                wait = (1.0 - self.tokens) / self.rate
                self.total_wait += wait
                time.sleep(wait)
                self.tokens = 0.0
                self.last_refill = time.monotonic()
            else:
                self.tokens -= 1.0
            
            # Enforce minimum gap
            gap_remaining = self.min_gap - (time.monotonic() - self.last_request)
            if gap_remaining > 0:
                self.total_wait += gap_remaining
                time.sleep(gap_remaining)
            
            self.last_request = time.monotonic()
            self.total_requests += 1
    
    def stats(self) -> dict:
        return {
            "total_requests": self.total_requests,
            "total_wait_seconds": round(self.total_wait, 1),
        }


# ── Per-API Buckets ───────────────────────────────────────────────────────

# AniList: 90 req/min officially. We use 60 to NEVER get pulled over.
# Min gap of 1.0s ensures steady pacing, no bursts.
# We'd rather be slow than banned.
anilist_bucket = TokenBucket(rate_per_sec=60/60, burst=1, min_gap_sec=1.0)

# Jikan: 3 req/sec, 60 req/min. We use 40 req/min with 1.5s min gap.
# Jikan is community-run — be extra respectful.
jikan_bucket = TokenBucket(rate_per_sec=40/60, burst=1, min_gap_sec=1.5)


# ── Request Helpers ───────────────────────────────────────────────────────

def _retry_request(make_request, max_retries: int = 4, label: str = ""):
    """Execute a request function with retry on 429."""
    for attempt in range(max_retries):
        try:
            return make_request()
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < max_retries - 1:
                # Use Retry-After header if present, else exponential backoff
                retry_after = e.headers.get("Retry-After")
                if retry_after:
                    wait = int(retry_after) + 1
                else:
                    wait = (attempt + 1) * 20  # 20s, 40s, 60s
                print(f"    ⏳ 429 rate limited{f' ({label})' if label else ''}, "
                      f"waiting {wait}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
            elif e.code == 500 and attempt < max_retries - 1:
                wait = (attempt + 1) * 5
                print(f"    ⚠ 500 server error{f' ({label})' if label else ''}, "
                      f"retrying in {wait}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
            else:
                raise


ANILIST_URL = "https://graphql.anilist.co"

def anilist_request(query: str, variables: dict, label: str = "") -> dict:
    """Make rate-limited AniList GraphQL request with retry."""
    def make_request():
        anilist_bucket.acquire()
        req = urllib.request.Request(
            ANILIST_URL,
            data=json.dumps({"query": query, "variables": variables}).encode(),
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "TheWatchlist/1.0 (data pipeline)",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    
    return _retry_request(make_request, label=label or "AniList")


JIKAN_BASE = "https://api.jikan.moe/v4"

def jikan_request(path: str, params: dict = None, label: str = "") -> dict:
    """Make rate-limited Jikan REST request with retry."""
    def make_request():
        jikan_bucket.acquire()
        url = f"{JIKAN_BASE}{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={
            "Accept": "application/json",
            "User-Agent": "TheWatchlist/1.0 (data pipeline)",
        })
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    
    return _retry_request(make_request, label=label or "Jikan")


def print_stats():
    """Print rate limiter statistics."""
    a = anilist_bucket.stats()
    j = jikan_bucket.stats()
    print(f"\n📊 Rate limiter stats:")
    print(f"   AniList: {a['total_requests']} requests, {a['total_wait_seconds']}s total wait")
    print(f"   Jikan:   {j['total_requests']} requests, {j['total_wait_seconds']}s total wait")
