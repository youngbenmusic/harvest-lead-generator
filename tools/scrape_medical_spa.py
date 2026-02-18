"""
scrape_medical_spa.py — Scrape medical spa leads via Google Places API (New).

Medical spas have no NPI taxonomy code, so we use Google Places Text Search
to find them across Alabama cities.

Uses the Places API (New) endpoint:
  POST https://places.googleapis.com/v1/places:searchText

Searches 6 cities x 3 search terms, follows pagination, deduplicates by
place_id, and filters to AL only. Phone numbers are returned in the same
request via field masks (no separate Details call needed).

Writes results to .tmp/medspa_results.json.

Usage:
    python tools/scrape_medical_spa.py
    python tools/scrape_medical_spa.py --json-only   # Same behavior (no DB table)
"""

import json
import os
import re
import sys
import time
import argparse

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

try:
    import requests
except ImportError:
    print("ERROR: Install requests first: pip install requests")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
except ImportError:
    pass

from tools.normalize import normalize_name

TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
OUTPUT_FILE = os.path.join(PROJECT_ROOT, ".tmp", "medspa_results.json")

# Fields to request — Basic (id, displayName, formattedAddress, location)
# plus Contact (nationalPhoneNumber). This avoids a separate Details call.
FIELD_MASK = ",".join([
    "places.id",
    "places.displayName",
    "places.formattedAddress",
    "places.location",
    "places.nationalPhoneNumber",
])

SEARCH_TERMS = ["medical spa", "medspa", "aesthetic clinic"]
SEARCH_CITIES = ["Birmingham", "Huntsville", "Montgomery", "Mobile", "Tuscaloosa", "Dothan"]

PAGE_DELAY = 2.0   # Delay between paginated requests
QUERY_DELAY = 1.0  # Delay between different queries


def get_api_key():
    """Get the Google Places API key from environment."""
    key = os.environ.get("GOOGLE_PLACES_API_KEY", "")
    if not key:
        print("ERROR: GOOGLE_PLACES_API_KEY not set in .env")
        print("  Add it to your .env file: GOOGLE_PLACES_API_KEY=your_key_here")
        sys.exit(1)
    return key


def parse_formatted_address(formatted_address):
    """Parse Google's formatted_address into components.

    Input:  "123 Main St, Birmingham, AL 35203, USA"
    Output: {"address": "123 Main St", "city": "Birmingham", "state": "AL", "zip": "35203"}
    """
    parts = [p.strip() for p in formatted_address.split(",")]
    result = {"address": "", "city": "", "state": "", "zip": ""}

    if len(parts) >= 1:
        result["address"] = parts[0]
    if len(parts) >= 2:
        result["city"] = parts[1]
    if len(parts) >= 3:
        # "AL 35203" or just "AL"
        state_zip = parts[2].strip()
        match = re.match(r"([A-Z]{2})\s*(\d{5})?", state_zip)
        if match:
            result["state"] = match.group(1)
            result["zip"] = match.group(2) or ""
    return result


def text_search(api_key, query, page_token=None):
    """Execute a Google Places Text Search (New) request.

    Uses POST to places.googleapis.com/v1/places:searchText with
    API key and field mask in headers.

    Returns:
        (list of place dicts, next_page_token or None)
    """
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": FIELD_MASK,
    }

    body = {
        "textQuery": query,
        "pageSize": 20,
    }
    if page_token:
        body["pageToken"] = page_token

    resp = requests.post(TEXT_SEARCH_URL, headers=headers, json=body, timeout=30)

    if resp.status_code != 200:
        try:
            err = resp.json()
            msg = err.get("error", {}).get("message", resp.text[:200])
        except Exception:
            msg = resp.text[:200]
        print(f"  API error (HTTP {resp.status_code}): {msg}")
        return [], None

    data = resp.json()
    places = data.get("places", [])
    next_token = data.get("nextPageToken")
    return places, next_token


def build_existing_keys():
    """Build a set of (normalized_name, CITY) keys from existing leads for cross-source dedup."""
    leads_file = os.path.join(PROJECT_ROOT, "data", "alabama_leads.json")
    keys = set()
    if os.path.exists(leads_file):
        try:
            with open(leads_file) as f:
                leads = json.load(f)
            for lead in leads:
                name = lead.get("name", "") or lead.get("facility_name", "")
                city = lead.get("city", "")
                if name and city:
                    keys.add((normalize_name(name), city.upper()))
        except Exception as e:
            print(f"  Warning: Could not load existing leads for dedup: {e}")
    return keys


def scrape_medical_spas(json_only=False):
    """Scrape medical spa leads from Google Places API (New).

    Args:
        json_only: Accepted for interface consistency, but has no effect
                   (medspa results always go to JSON, no DB staging table).

    Returns:
        List of result dicts written to .tmp/medspa_results.json.
    """
    print("Harvest Med Waste — Medical Spa Scraper (Google Places API New)")
    print(f"Search terms: {SEARCH_TERMS}")
    print(f"Cities: {SEARCH_CITIES}")
    print()

    api_key = get_api_key()
    existing_keys = build_existing_keys()
    print(f"Existing leads loaded for dedup: {len(existing_keys)} keys")

    seen_place_ids = set()
    all_results = []
    total_queries = len(SEARCH_TERMS) * len(SEARCH_CITIES)
    query_num = 0

    for term in SEARCH_TERMS:
        for city in SEARCH_CITIES:
            query_num += 1
            query = f"{term} in {city}, Alabama"
            print(f"[{query_num}/{total_queries}] Searching: {query}", flush=True)

            page = 1
            next_token = None
            while True:
                places, next_token = text_search(api_key, query, page_token=next_token)

                new_count = 0
                for place in places:
                    place_id = place.get("id", "")
                    if not place_id or place_id in seen_place_ids:
                        continue
                    seen_place_ids.add(place_id)

                    # Parse address
                    formatted = place.get("formattedAddress", "")
                    addr = parse_formatted_address(formatted)

                    # Filter: only Alabama results
                    if addr["state"] != "AL":
                        continue

                    # Cross-source dedup check
                    display_name = place.get("displayName", {}).get("text", "")
                    norm_key = (normalize_name(display_name), addr["city"].upper())
                    if norm_key in existing_keys:
                        continue

                    location = place.get("location", {})
                    phone = place.get("nationalPhoneNumber", "")

                    record = {
                        "source": "google_places",
                        "place_id": place_id,
                        "facility_type": "Medical Spa",
                        "facility_name": display_name,
                        "address": addr["address"],
                        "city": addr["city"],
                        "state": "AL",
                        "zip": addr["zip"],
                        "county": "",
                        "phone": phone,
                        "latitude": location.get("latitude"),
                        "longitude": location.get("longitude"),
                    }
                    all_results.append(record)
                    new_count += 1

                if page == 1 and new_count > 0:
                    print(f"  Page {page}: {len(places)} results, {new_count} new AL matches")
                elif page > 1:
                    print(f"  Page {page}: {len(places)} results, {new_count} new AL matches")

                if not next_token:
                    break

                page += 1
                time.sleep(PAGE_DELAY)

            time.sleep(QUERY_DELAY)

    print(f"\nUnique AL medical spas found: {len(all_results)}")

    # Save to JSON
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nSaved {len(all_results)} medical spas to {OUTPUT_FILE}")

    # Summary by city
    if all_results:
        print("\n--- Summary by City ---")
        city_counts = {}
        for r in all_results:
            c = r.get("city", "Unknown")
            city_counts[c] = city_counts.get(c, 0) + 1
        for c, count in sorted(city_counts.items(), key=lambda x: -x[1]):
            print(f"  {c}: {count}")

    return all_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape medical spas via Google Places API")
    parser.add_argument("--json-only", action="store_true",
                        help="JSON output only (default behavior, no DB staging)")
    args = parser.parse_args()
    scrape_medical_spas(json_only=args.json_only)
