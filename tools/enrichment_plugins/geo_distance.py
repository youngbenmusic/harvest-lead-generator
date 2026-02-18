"""
geo_distance.py — Geocode lead addresses and calculate distance from Birmingham, AL.

Uses Nominatim (OpenStreetMap) for geocoding with aggressive caching.
Falls back to ZIP centroid table if geocoding fails.
Respects Nominatim usage policy: 1 req/sec, custom User-Agent.
"""

import json
import math
import os
import time
import requests
from tools.enrichment_plugins.base import EnrichmentPlugin

# Birmingham, AL coordinates
BIRMINGHAM_LAT = 33.5207
BIRMINGHAM_LON = -86.8025

# Nominatim API
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {
    "User-Agent": "HarvestMedWaste/1.0 (contact@harvestmedwaste.com)",
}

# Cache file for geocoded coordinates
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GEOCODE_CACHE_FILE = os.path.join(PROJECT_ROOT, ".tmp", "geocode_cache.json")

# Alabama ZIP code centroids (fallback when geocoding fails)
AL_ZIP_CENTROIDS = {
    "350": (33.52, -86.80),   # Birmingham
    "351": (33.52, -86.80),   # Birmingham
    "352": (33.45, -86.90),   # Birmingham suburbs
    "353": (33.52, -86.80),   # Birmingham
    "354": (33.20, -87.55),   # Tuscaloosa
    "355": (33.20, -87.55),   # Tuscaloosa
    "356": (33.45, -86.05),   # Talladega / Anniston
    "357": (34.73, -87.68),   # Florence / Muscle Shoals
    "358": (34.73, -86.59),   # Huntsville / Decatur
    "359": (34.73, -86.59),   # Huntsville
    "360": (32.38, -86.30),   # Montgomery
    "361": (32.38, -86.30),   # Montgomery
    "362": (31.55, -87.88),   # Thomasville
    "363": (31.22, -85.39),   # Dothan
    "364": (31.22, -85.39),   # Dothan
    "365": (30.69, -88.05),   # Mobile
    "366": (30.69, -88.05),   # Mobile
    "367": (33.99, -85.99),   # Gadsden / Albertville
    "368": (31.05, -87.07),   # Evergreen
    "369": (32.10, -87.57),   # Selma
}

# Service zone thresholds (miles from Birmingham)
SERVICE_ZONES = [
    (30,  "Zone 1 - Metro"),
    (60,  "Zone 2 - Regional"),
    (100, "Zone 3 - Extended"),
    (150, "Zone 4 - Statewide"),
    (999, "Zone 5 - Out of Area"),
]


def haversine(lat1, lon1, lat2, lon2):
    """Calculate great-circle distance in miles between two points."""
    R = 3959  # Earth radius in miles

    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))

    return R * c


def get_zip_coords(zip5):
    """Look up approximate coordinates for an Alabama ZIP code (fallback)."""
    if not zip5 or len(zip5) < 3:
        return None, None

    prefix = zip5[:3]
    if prefix in AL_ZIP_CENTROIDS:
        return AL_ZIP_CENTROIDS[prefix]

    return None, None


class GeoDistanceCalculator(EnrichmentPlugin):
    name = "geo_distance"
    description = "Geocode addresses and calculate distance from Birmingham, AL"

    def __init__(self):
        self._cache = None
        self._cache_dirty = False
        self._last_request_time = 0

    def _load_cache(self):
        """Load geocode cache from disk."""
        if self._cache is not None:
            return

        if os.path.exists(GEOCODE_CACHE_FILE):
            try:
                with open(GEOCODE_CACHE_FILE) as f:
                    self._cache = json.load(f)
            except (json.JSONDecodeError, IOError):
                self._cache = {}
        else:
            self._cache = {}

    def _save_cache(self):
        """Persist geocode cache to disk."""
        if not self._cache_dirty or self._cache is None:
            return
        os.makedirs(os.path.dirname(GEOCODE_CACHE_FILE), exist_ok=True)
        with open(GEOCODE_CACHE_FILE, "w") as f:
            json.dump(self._cache, f)
        self._cache_dirty = False

    def _build_address_string(self, lead):
        """Build a full address string for geocoding."""
        parts = []
        addr = (lead.get("address_line1") or "").strip()
        if addr:
            parts.append(addr)
        city = (lead.get("city") or "").strip()
        if city:
            parts.append(city)
        state = (lead.get("state") or "AL").strip()
        zip5 = (lead.get("zip5") or "").strip()
        parts.append(f"{state} {zip5}".strip())
        return ", ".join(parts)

    def _geocode(self, address_string):
        """Geocode an address using Nominatim. Returns (lat, lon) or (None, None)."""
        # Rate limit: 1 request per second
        elapsed = time.time() - self._last_request_time
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)

        try:
            resp = requests.get(
                NOMINATIM_URL,
                params={"q": address_string, "format": "json", "limit": 1},
                headers=NOMINATIM_HEADERS,
                timeout=10,
            )
            self._last_request_time = time.time()

            if resp.status_code == 200:
                results = resp.json()
                if results:
                    return float(results[0]["lat"]), float(results[0]["lon"])
        except (requests.RequestException, ValueError, KeyError, IndexError):
            self._last_request_time = time.time()

        return None, None

    def can_enrich(self, lead: dict) -> bool:
        # Can enrich if we have an address or a ZIP code
        has_address = bool((lead.get("address_line1") or "").strip())
        has_zip = bool((lead.get("zip5") or lead.get("zip") or "").strip())
        return has_address or has_zip

    def enrich(self, lead: dict) -> dict:
        self._load_cache()

        lat = lead.get("latitude")
        lon = lead.get("longitude")

        # If lead already has coordinates, skip geocoding
        if lat is not None and lon is not None:
            distance = haversine(BIRMINGHAM_LAT, BIRMINGHAM_LON, lat, lon)
            return self._build_result(lat, lon, distance)

        # Build address for geocoding and cache lookup
        address_string = self._build_address_string(lead)
        cache_key = address_string.upper().strip()

        # Check cache
        if cache_key in self._cache:
            cached = self._cache[cache_key]
            lat, lon = cached.get("lat"), cached.get("lon")
            if lat is not None and lon is not None:
                distance = haversine(BIRMINGHAM_LAT, BIRMINGHAM_LON, lat, lon)
                return self._build_result(lat, lon, distance)

        # Try Nominatim geocoding if we have a real address
        if (lead.get("address_line1") or "").strip():
            lat, lon = self._geocode(address_string)
            if lat is not None:
                self._cache[cache_key] = {"lat": lat, "lon": lon}
                self._cache_dirty = True
                # Periodically save cache (every 100 new entries)
                if len(self._cache) % 100 == 0:
                    self._save_cache()
                distance = haversine(BIRMINGHAM_LAT, BIRMINGHAM_LON, lat, lon)
                return self._build_result(lat, lon, distance)

            # Cache the failure too so we don't retry
            self._cache[cache_key] = {"lat": None, "lon": None}
            self._cache_dirty = True

        # Fall back to ZIP centroid
        zip5 = (lead.get("zip5") or lead.get("zip") or "").strip()
        fallback_lat, fallback_lon = get_zip_coords(zip5)
        if fallback_lat is not None:
            distance = haversine(BIRMINGHAM_LAT, BIRMINGHAM_LON, fallback_lat, fallback_lon)
            # Don't set lat/lon on the lead for ZIP centroids — they're not accurate
            return {
                "distance_from_birmingham": round(distance, 1),
                "service_zone": self._get_service_zone(distance),
            }

        return {}

    def _build_result(self, lat, lon, distance):
        """Build the enrichment result dict."""
        return {
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "distance_from_birmingham": round(distance, 1),
            "service_zone": self._get_service_zone(distance),
        }

    def _get_service_zone(self, distance):
        """Map distance to service zone."""
        for threshold, zone in SERVICE_ZONES:
            if distance <= threshold:
                return zone
        return "Zone 5 - Out of Area"

    def flush_cache(self):
        """Force save the cache to disk. Call after batch processing."""
        self._save_cache()
