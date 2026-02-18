"""
geo_distance.py — Calculate distance from Birmingham, AL using Haversine formula.

Uses a ZIP code centroid lookup for distance calculation.
No external API needed — uses a built-in table of Alabama ZIP centroids.
"""

import math
from tools.enrichment_plugins.base import EnrichmentPlugin

# Birmingham, AL coordinates
BIRMINGHAM_LAT = 33.5207
BIRMINGHAM_LON = -86.8025

# Alabama ZIP code centroids (major ZIPs — covers ~80% of leads)
# Format: ZIP prefix (3 digits) → (lat, lon)
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
    """Look up approximate coordinates for an Alabama ZIP code."""
    if not zip5 or len(zip5) < 3:
        return None, None

    prefix = zip5[:3]
    if prefix in AL_ZIP_CENTROIDS:
        return AL_ZIP_CENTROIDS[prefix]

    return None, None


class GeoDistanceCalculator(EnrichmentPlugin):
    name = "geo_distance"
    description = "Calculate distance from Birmingham, AL"

    def can_enrich(self, lead: dict) -> bool:
        zip5 = lead.get("zip5", "") or lead.get("zip", "")
        return bool(zip5) and len(zip5) >= 3

    def enrich(self, lead: dict) -> dict:
        zip5 = lead.get("zip5", "") or lead.get("zip", "")
        lat, lon = get_zip_coords(zip5)

        if lat is None:
            return {}

        distance = haversine(BIRMINGHAM_LAT, BIRMINGHAM_LON, lat, lon)

        # Determine service zone
        service_zone = "Unknown"
        for threshold, zone in SERVICE_ZONES:
            if distance <= threshold:
                service_zone = zone
                break

        return {
            "distance_from_birmingham": round(distance, 1),
            "service_zone": service_zone,
        }
