"""
hunter_email.py — Find decision-maker emails via Hunter.io API.

Two-phase lookup:
  1. Domain Search: query by company name, filter for decision-maker titles
  2. Email Finder (fallback): look up a specific person if administrator name exists

Caches results to .tmp/hunter_cache.json to avoid re-querying on re-runs.
"""

import json
import os
import re
import time
import requests
from tools.enrichment_plugins.base import EnrichmentPlugin

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
HUNTER_CACHE_FILE = os.path.join(PROJECT_ROOT, ".tmp", "hunter_cache.json")

HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "")

# Load from .env if not in environment
if not HUNTER_API_KEY:
    env_path = os.path.join(PROJECT_ROOT, ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("HUNTER_API_KEY=") and not line.startswith("#"):
                    HUNTER_API_KEY = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break

DOMAIN_SEARCH_URL = "https://api.hunter.io/v2/domain-search"
EMAIL_FINDER_URL = "https://api.hunter.io/v2/email-finder"

# Decision-maker titles to keep (case-insensitive substring match)
DECISION_MAKER_TITLES = [
    "administrator",
    "director of operations",
    "ceo",
    "executive director",
    "office manager",
]

# Suffixes to strip from company names before querying
COMPANY_SUFFIXES = re.compile(
    r",?\s*\b(LLC|Inc|Corp|Corporation|PA|PLLC|PC|LP|LLP|Ltd|Co|MD|DDS|DMD|DO|DPM|DVM|PhD)\b\.?\s*$",
    re.IGNORECASE,
)


def clean_company_name(name):
    """Strip legal suffixes and extra whitespace from a company name."""
    if not name:
        return ""
    cleaned = COMPANY_SUFFIXES.sub("", name).strip()
    # Remove trailing commas/periods left over
    cleaned = cleaned.rstrip(",. ")
    return cleaned


class HunterEmailEnricher(EnrichmentPlugin):
    name = "hunter_email"
    description = "Find decision-maker emails via Hunter.io"

    def __init__(self):
        self._cache = None
        self._cache_dirty = False
        self._last_request_time = 0
        self._api_disabled = False  # Set True on auth errors to stop all calls

    # ── Cache ──────────────────────────────────────────────────

    def _load_cache(self):
        if self._cache is not None:
            return
        if os.path.exists(HUNTER_CACHE_FILE):
            try:
                with open(HUNTER_CACHE_FILE) as f:
                    self._cache = json.load(f)
            except (json.JSONDecodeError, IOError):
                self._cache = {}
        else:
            self._cache = {}

    def _save_cache(self):
        if not self._cache_dirty or self._cache is None:
            return
        os.makedirs(os.path.dirname(HUNTER_CACHE_FILE), exist_ok=True)
        with open(HUNTER_CACHE_FILE, "w") as f:
            json.dump(self._cache, f)
        self._cache_dirty = False

    def flush_cache(self):
        """Force save cache to disk. Called after batch processing."""
        self._save_cache()

    # ── Rate limiting ──────────────────────────────────────────

    def _throttle(self):
        """Wait at least 0.15s between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < 0.15:
            time.sleep(0.15 - elapsed)

    # ── API calls ──────────────────────────────────────────────

    def _api_get(self, url, params):
        """Make a Hunter.io API GET request with error handling."""
        self._throttle()
        params["api_key"] = HUNTER_API_KEY

        try:
            resp = requests.get(url, params=params, timeout=15)
            self._last_request_time = time.time()

            if resp.status_code in (401, 403):
                print(f"  [hunter_email] Auth error ({resp.status_code}) — disabling further API calls")
                self._api_disabled = True
                return None

            if resp.status_code == 429:
                # Rate limited — back off and retry once
                retry_after = int(resp.headers.get("Retry-After", 10))
                print(f"  [hunter_email] Rate limited, waiting {retry_after}s...")
                time.sleep(retry_after)
                resp = requests.get(url, params=params, timeout=15)
                self._last_request_time = time.time()
                if resp.status_code != 200:
                    return None

            if resp.status_code == 200:
                return resp.json()

        except requests.RequestException as e:
            self._last_request_time = time.time()
            # Network error — skip this lead, don't disable
            return None

        return None

    def _domain_search(self, company_name):
        """Search Hunter.io by company name for decision-maker emails."""
        result = self._api_get(DOMAIN_SEARCH_URL, {"company": company_name, "limit": 10})
        if not result:
            return None

        emails = result.get("data", {}).get("emails", [])
        if not emails:
            return None

        # Filter for decision-maker titles
        for email_entry in emails:
            position = (email_entry.get("position") or "").lower()
            if any(title in position for title in DECISION_MAKER_TITLES):
                first = email_entry.get("first_name") or ""
                last = email_entry.get("last_name") or ""
                name = f"{first} {last}".strip()
                return {
                    "contact_email": email_entry.get("value", ""),
                    "contact_name": name,
                    "contact_title": email_entry.get("position", ""),
                    "email_confidence": email_entry.get("confidence", 0),
                    "email_source": "hunter_domain",
                }

        # No decision-maker title found — take the first email as fallback
        best = emails[0]
        first = best.get("first_name") or ""
        last = best.get("last_name") or ""
        name = f"{first} {last}".strip()
        return {
            "contact_email": best.get("value", ""),
            "contact_name": name,
            "contact_title": best.get("position", ""),
            "email_confidence": best.get("confidence", 0),
            "email_source": "hunter_domain",
        }

    def _email_finder(self, company_name, full_name):
        """Try to find a specific person's email via Hunter.io Email Finder."""
        parts = full_name.strip().split()
        if len(parts) < 2:
            return None

        first_name = parts[0]
        last_name = parts[-1]

        result = self._api_get(EMAIL_FINDER_URL, {
            "company": company_name,
            "first_name": first_name,
            "last_name": last_name,
        })
        if not result:
            return None

        data = result.get("data", {})
        email = data.get("email")
        if not email:
            return None

        return {
            "contact_email": email,
            "contact_name": full_name.strip(),
            "contact_title": data.get("position") or "",
            "email_confidence": data.get("confidence", 0),
            "email_source": "hunter_finder",
        }

    # ── Plugin interface ───────────────────────────────────────

    def can_enrich(self, lead: dict) -> bool:
        if self._api_disabled:
            return False
        if not HUNTER_API_KEY:
            return False
        if not (lead.get("facility_name") or "").strip():
            return False
        # Skip if already has a contact email
        if (lead.get("contact_email") or "").strip():
            return False
        return True

    def enrich(self, lead: dict) -> dict:
        self._load_cache()

        facility_name = (lead.get("facility_name") or lead.get("name") or "").strip()
        company = clean_company_name(facility_name)
        if not company:
            return {}

        cache_key = company.upper()

        # Check cache
        if cache_key in self._cache:
            cached = self._cache[cache_key]
            if cached is None:
                # Cached miss
                return {}
            return dict(cached)

        # Phase 1: Domain Search
        result = self._domain_search(company)

        # Phase 2: Email Finder fallback
        if not result:
            administrator = (lead.get("administrator") or "").strip()
            if administrator:
                result = self._email_finder(company, administrator)

        # Cache the result (or miss)
        self._cache[cache_key] = result
        self._cache_dirty = True

        # Periodically save cache
        if len(self._cache) % 100 == 0:
            self._save_cache()

        return result if result else {}
