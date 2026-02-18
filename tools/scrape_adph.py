"""
scrape_adph.py — Scrape Alabama Department of Public Health facility directory.

Targets: https://dph1.adph.state.al.us/FacilitiesDirectory/
Extracts facility data across all categories (hospitals, nursing homes,
ASCs, labs, home health, hospices, rehab, rural health clinics, etc.)

Writes results to staging_adph table or .tmp/adph_results.json as fallback.

Usage:
    python tools/scrape_adph.py
    python tools/scrape_adph.py --json-only   # Skip DB, write JSON only
"""

import json
import os
import sys
import time
import re
import argparse

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: Install dependencies first: pip install requests beautifulsoup4")
    sys.exit(1)

BASE_URL = "https://dph1.adph.state.al.us/FacilitiesDirectory/"
DELAY = 1.5  # seconds between requests
CACHE_DIR = os.path.join(PROJECT_ROOT, ".tmp", "adph_raw")
OUTPUT_FILE = os.path.join(PROJECT_ROOT, ".tmp", "adph_results.json")

# ADPH facility categories to scrape
FACILITY_CATEGORIES = [
    "Hospitals",
    "Nursing Homes",
    "Assisted Living Facilities",
    "Ambulatory Surgical Centers",
    "End Stage Renal Disease Facilities",
    "Home Health Agencies",
    "Hospices",
    "Intermediate Care Facilities",
    "Clinical Laboratories",
    "Rehabilitation Centers",
    "Rural Health Clinics",
    "Portable X-Ray Suppliers",
    "Psychiatric Residential Treatment Facilities",
    "Comprehensive Outpatient Rehabilitation Facilities",
    "Community Mental Health Centers",
    "Organ Procurement Organizations",
    "Religious Nonmedical Health Care Institutions",
    "Outpatient Physical Therapy",
    "Critical Access Hospitals",
]

# Map ADPH categories to our unified facility types
ADPH_TO_FACILITY_TYPE = {
    "Hospitals": "Hospital",
    "Critical Access Hospitals": "Hospital",
    "Nursing Homes": "Nursing Home",
    "Assisted Living Facilities": "Nursing Home",
    "Intermediate Care Facilities": "Nursing Home",
    "Ambulatory Surgical Centers": "Surgery Center",
    "Clinical Laboratories": "Lab",
    "Home Health Agencies": "Other",
    "Hospices": "Other",
    "End Stage Renal Disease Facilities": "Other",
    "Rehabilitation Centers": "Other",
    "Rural Health Clinics": "Medical Practice",
    "Portable X-Ray Suppliers": "Other",
    "Psychiatric Residential Treatment Facilities": "Other",
    "Comprehensive Outpatient Rehabilitation Facilities": "Other",
    "Community Mental Health Centers": "Other",
    "Organ Procurement Organizations": "Other",
    "Religious Nonmedical Health Care Institutions": "Other",
    "Outpatient Physical Therapy": "Medical Practice",
}


def get_session():
    """Create a requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "HarvestMedWaste-LeadGen/1.0 (research)",
        "Accept": "text/html,application/xhtml+xml",
    })
    return session


def fetch_page(session, url, cache_name=None):
    """Fetch a page, optionally caching the raw HTML."""
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        html = resp.text

        if cache_name:
            os.makedirs(CACHE_DIR, exist_ok=True)
            cache_path = os.path.join(CACHE_DIR, cache_name)
            with open(cache_path, "w", encoding="utf-8") as f:
                f.write(html)

        return html
    except requests.RequestException as e:
        print(f"  Error fetching {url}: {e}")
        return None


def parse_facility_list(html, category):
    """Parse facility records from an ADPH directory page."""
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    facilities = []

    # ADPH directory pages typically use tables or div-based layouts
    # Try multiple parsing strategies

    # Strategy 1: Look for table rows with facility data
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        for row in rows[1:]:  # skip header
            cells = row.find_all(["td", "th"])
            if len(cells) >= 3:
                facility = parse_table_row(cells, category)
                if facility and facility.get("facility_name"):
                    facilities.append(facility)

    # Strategy 2: Look for div-based card layouts
    if not facilities:
        cards = soup.find_all("div", class_=re.compile(r"facility|card|item|result", re.I))
        for card in cards:
            facility = parse_card(card, category)
            if facility and facility.get("facility_name"):
                facilities.append(facility)

    # Strategy 3: Look for definition lists or labeled data
    if not facilities:
        for dl in soup.find_all("dl"):
            facility = parse_definition_list(dl, category)
            if facility and facility.get("facility_name"):
                facilities.append(facility)

    return facilities


def parse_table_row(cells, category):
    """Extract facility info from a table row."""
    texts = [c.get_text(strip=True) for c in cells]

    facility = {
        "source": "adph",
        "adph_category": category,
        "facility_type": ADPH_TO_FACILITY_TYPE.get(category, "Other"),
        "facility_name": "",
        "address": "",
        "city": "",
        "state": "AL",
        "zip": "",
        "county": "",
        "phone": "",
        "administrator": "",
        "license_number": "",
    }

    # Common ADPH table layouts
    if len(texts) >= 6:
        facility["facility_name"] = texts[0]
        facility["address"] = texts[1]
        facility["city"] = texts[2]
        facility["county"] = texts[3] if not texts[3].isdigit() else ""
        facility["phone"] = clean_phone_raw(texts[4] if len(texts) > 4 else "")
        facility["administrator"] = texts[5] if len(texts) > 5 else ""
    elif len(texts) >= 3:
        facility["facility_name"] = texts[0]
        # Try to extract address, city from combined fields
        addr_parts = texts[1].split(",") if len(texts) > 1 else []
        if addr_parts:
            facility["address"] = addr_parts[0].strip()
            if len(addr_parts) > 1:
                facility["city"] = addr_parts[1].strip()
        facility["phone"] = clean_phone_raw(texts[2] if len(texts) > 2 else "")

    # Extract license number from any cell
    for text in texts:
        lic_match = re.search(r"(?:License|LIC|#)\s*[:# ]?\s*(\w[\w-]+)", text, re.I)
        if lic_match:
            facility["license_number"] = lic_match.group(1)
            break

    # Extract ZIP from address or city field
    for text in texts:
        zip_match = re.search(r"\b(\d{5})(?:-\d{4})?\b", text)
        if zip_match:
            facility["zip"] = zip_match.group(1)
            break

    return facility


def parse_card(card, category):
    """Extract facility info from a div-based card layout."""
    facility = {
        "source": "adph",
        "adph_category": category,
        "facility_type": ADPH_TO_FACILITY_TYPE.get(category, "Other"),
        "facility_name": "",
        "address": "",
        "city": "",
        "state": "AL",
        "zip": "",
        "county": "",
        "phone": "",
        "administrator": "",
        "license_number": "",
    }

    # Look for common field patterns
    name_el = card.find(["h2", "h3", "h4", "strong", "b"])
    if name_el:
        facility["facility_name"] = name_el.get_text(strip=True)

    # Look for labeled fields
    for label_el in card.find_all(["label", "span", "dt"]):
        label_text = label_el.get_text(strip=True).lower()
        value_el = label_el.find_next_sibling(["span", "dd", "div"])
        if not value_el:
            continue
        value = value_el.get_text(strip=True)

        if "address" in label_text:
            facility["address"] = value
        elif "city" in label_text:
            facility["city"] = value
        elif "county" in label_text:
            facility["county"] = value
        elif "phone" in label_text or "telephone" in label_text:
            facility["phone"] = clean_phone_raw(value)
        elif "admin" in label_text or "director" in label_text:
            facility["administrator"] = value
        elif "license" in label_text:
            facility["license_number"] = value

    # Extract ZIP
    text = card.get_text()
    zip_match = re.search(r"\b(\d{5})(?:-\d{4})?\b", text)
    if zip_match:
        facility["zip"] = zip_match.group(1)

    return facility


def parse_definition_list(dl, category):
    """Extract facility info from a definition list."""
    facility = {
        "source": "adph",
        "adph_category": category,
        "facility_type": ADPH_TO_FACILITY_TYPE.get(category, "Other"),
        "facility_name": "",
        "address": "",
        "city": "",
        "state": "AL",
        "zip": "",
        "county": "",
        "phone": "",
        "administrator": "",
        "license_number": "",
    }

    dts = dl.find_all("dt")
    dds = dl.find_all("dd")

    for dt, dd in zip(dts, dds):
        label = dt.get_text(strip=True).lower()
        value = dd.get_text(strip=True)

        if "name" in label or "facility" in label:
            facility["facility_name"] = value
        elif "address" in label:
            facility["address"] = value
        elif "city" in label:
            facility["city"] = value
        elif "county" in label:
            facility["county"] = value
        elif "phone" in label:
            facility["phone"] = clean_phone_raw(value)
        elif "admin" in label:
            facility["administrator"] = value
        elif "license" in label:
            facility["license_number"] = value

    return facility


def clean_phone_raw(phone):
    """Basic phone cleaning for scraped data."""
    if not phone:
        return ""
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return phone.strip()


def generate_license_id(facility):
    """Generate a source ID for an ADPH facility."""
    if facility.get("license_number"):
        return f"adph-{facility['license_number']}"
    # Fallback: hash of name + address
    key = f"{facility.get('facility_name', '')}|{facility.get('address', '')}|{facility.get('city', '')}"
    import hashlib
    return f"adph-{hashlib.md5(key.encode()).hexdigest()[:12]}"


def write_to_db(facilities):
    """Write scraped facilities to the staging_adph table."""
    try:
        from tools.db import get_cursor
    except Exception:
        print("  Database not available, skipping DB write")
        return 0

    count = 0
    with get_cursor() as cur:
        for fac in facilities:
            source_id = generate_license_id(fac)
            try:
                cur.execute("""
                    INSERT INTO staging_adph (license_number, raw_data)
                    VALUES (%s, %s)
                    ON CONFLICT (license_number) DO UPDATE SET
                        raw_data = EXCLUDED.raw_data,
                        ingested_at = NOW()
                """, (source_id, json.dumps(fac)))
                count += 1
            except Exception as e:
                print(f"  DB error for {fac.get('facility_name', '?')}: {e}")
    return count


def scrape_all(json_only=False):
    """Scrape all ADPH facility categories."""
    print("Harvest Med Waste — ADPH Facility Scraper")
    print(f"Target: {BASE_URL}")
    print(f"Categories: {len(FACILITY_CATEGORIES)}")
    print()

    session = get_session()
    all_facilities = []

    for i, category in enumerate(FACILITY_CATEGORIES):
        print(f"[{i+1}/{len(FACILITY_CATEGORIES)}] Scraping: {category}...", flush=True)

        # Try the main directory page (the URL pattern may vary)
        # Common patterns for ADPH directory:
        url_slug = category.replace(" ", "")
        urls_to_try = [
            f"{BASE_URL}?category={category.replace(' ', '+')}",
            f"{BASE_URL}Default.aspx?category={category.replace(' ', '+')}",
            f"{BASE_URL}{url_slug}.aspx",
        ]

        page_facilities = []
        for url in urls_to_try:
            cache_name = f"{category.replace(' ', '_').lower()}.html"
            html = fetch_page(session, url, cache_name)
            if html:
                page_facilities = parse_facility_list(html, category)
                if page_facilities:
                    break
            time.sleep(DELAY)

        if page_facilities:
            all_facilities.extend(page_facilities)
            print(f"  Found {len(page_facilities)} facilities")
        else:
            print(f"  No facilities found (page may require JS or different URL)")

        time.sleep(DELAY)

    print(f"\nTotal facilities scraped: {len(all_facilities)}")

    # Save to JSON
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(all_facilities, f, indent=2)
    print(f"Saved to {OUTPUT_FILE}")

    # Write to database
    if not json_only and all_facilities:
        db_count = write_to_db(all_facilities)
        print(f"Wrote {db_count} records to staging_adph table")

    # Print summary by category
    print("\n--- Summary by Category ---")
    cat_counts = {}
    for fac in all_facilities:
        cat = fac.get("adph_category", "Unknown")
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
    for cat, count in sorted(cat_counts.items(), key=lambda x: -x[1]):
        print(f"  {cat}: {count}")

    return all_facilities


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape ADPH facility directory")
    parser.add_argument("--json-only", action="store_true", help="Skip database write")
    args = parser.parse_args()
    scrape_all(json_only=args.json_only)
