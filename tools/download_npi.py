"""
download_npi.py — Query the NPPES API for Alabama healthcare providers.

Uses the NPPES API with taxonomy_description queries to target
specific provider types that generate medical waste. Paginates
through results 200 at a time.

No API key required. Free public API.

Usage:
    python tools/download_npi.py
"""

import json
import os
import sys
import time
import urllib.request
import urllib.parse

API_BASE = "https://npiregistry.cms.hhs.gov/api/"
STATE = "AL"
LIMIT = 200
DELAY = 0.5  # seconds between requests

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, ".tmp")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "npi_raw.json")

# Taxonomy descriptions that map to medical waste-generating facilities.
# The API does partial matching on these descriptions.
TAXONOMY_QUERIES = [
    "dentist",
    "dental hygienist",
    "dental laboratory",
    "veterinar",
    "hospital",
    "surgery",
    "surgical",
    "urgent care",
    "laboratory",
    "nursing facility",
    "skilled nursing",
    "custodial care",
    "hospice",
    "dermatology",
    "family medicine",
    "internal medicine",
    "pediatric",
    "obstetrics",
    "gynecology",
    "orthopedic",
    "ophthalmology",
    "urology",
    "gastroenterology",
    "cardiology",
    "neurology",
    "oncology",
    "pulmonary",
    "radiology",
    "pathology",
    "anesthesiology",
    "allergy",
    "clinic",
    "rehabilitation",
    "psychiatric",
    "dialysis",
    "home health",
    "pharmacy",
    "chiropract",
    "optometr",
    "podiatr",
    "nurse practitioner",
    "physician assistant",
    "physical therap",
    "occupational therap",
    "speech",
    "pain medicine",
    "emergency medicine",
    "ambulance",
]


def fetch_page(taxonomy_desc, skip):
    """Fetch one page of results from the NPPES API."""
    params = {
        "version": "2.1",
        "state": STATE,
        "taxonomy_description": taxonomy_desc,
        "limit": str(LIMIT),
        "skip": str(skip),
    }
    url = API_BASE + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/json")
    req.add_header("User-Agent", "HarvestMedWaste-LeadGen/1.0")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"    Error: {e}", flush=True)
        # One retry
        time.sleep(2)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception:
            return None


MAX_PAGES = 25  # Cap at 5000 results per taxonomy query


def paginate_query(taxonomy_desc, seen_npis):
    """Paginate through all results for a taxonomy description."""
    new_records = []
    skip = 0
    pages = 0

    while pages < MAX_PAGES:
        data = fetch_page(taxonomy_desc, skip)
        if data is None or not data.get("results"):
            break

        results = data["results"]
        for r in results:
            npi = r.get("number")
            if npi and npi not in seen_npis:
                seen_npis.add(npi)
                new_records.append(r)

        if len(results) < LIMIT:
            break

        skip += LIMIT
        pages += 1
        time.sleep(DELAY)

    return new_records


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Harvest Med Waste — NPI Data Download", flush=True)
    print(f"Querying NPPES API for {STATE} providers", flush=True)
    start = time.time()

    all_records = []
    seen_npis = set()

    for i, taxonomy in enumerate(TAXONOMY_QUERIES):
        new = paginate_query(taxonomy, seen_npis)
        if new:
            all_records.extend(new)
            print(f"  [{i+1}/{len(TAXONOMY_QUERIES)}] {taxonomy}: "
                  f"+{len(new)} (total: {len(all_records)})", flush=True)
        else:
            print(f"  [{i+1}/{len(TAXONOMY_QUERIES)}] {taxonomy}: 0 new", flush=True)
        time.sleep(DELAY)

    elapsed = time.time() - start

    if not all_records:
        print("ERROR: No records downloaded.", flush=True)
        sys.exit(1)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(all_records, f, indent=2)

    mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f"\nSaved {len(all_records)} records to {OUTPUT_FILE} ({mb:.1f} MB)", flush=True)
    print(f"Done in {elapsed:.0f}s", flush=True)


if __name__ == "__main__":
    main()
