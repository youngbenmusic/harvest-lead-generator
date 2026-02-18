"""
normalize.py — Transform raw records from each data source into a common schema.

Reads from staging tables (staging_npi, staging_adph, staging_cms) and
produces normalized records ready for deduplication.

Reuses taxonomy mapping and phone cleaning from process_leads.py.

Usage:
    python tools/normalize.py
    python tools/normalize.py --source npi      # Normalize only NPI records
    python tools/normalize.py --source adph     # Normalize only ADPH records
    python tools/normalize.py --json            # Read from .tmp JSON files instead of DB
"""

import json
import os
import re
import sys
import argparse

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from tools.process_leads import TAXONOMY_MAP, classify_taxonomy, clean_phone

# Address abbreviation standardization
ADDRESS_ABBREVS = {
    "STREET": "ST", "AVENUE": "AVE", "BOULEVARD": "BLVD", "DRIVE": "DR",
    "LANE": "LN", "ROAD": "RD", "COURT": "CT", "CIRCLE": "CIR",
    "PLACE": "PL", "HIGHWAY": "HWY", "PARKWAY": "PKWY", "NORTH": "N",
    "SOUTH": "S", "EAST": "E", "WEST": "W", "NORTHEAST": "NE",
    "NORTHWEST": "NW", "SOUTHEAST": "SE", "SOUTHWEST": "SW",
    "SUITE": "STE", "APARTMENT": "APT", "BUILDING": "BLDG",
    "FLOOR": "FL", "UNIT": "UNIT", "ROOM": "RM",
}


def normalize_address(address):
    """Normalize an address string for matching.

    Standardizes abbreviations, removes suite/unit numbers,
    uppercases, and strips extra whitespace.
    """
    if not address:
        return ""
    addr = address.upper().strip()
    # Remove suite/unit/apt numbers for matching
    addr = re.sub(r"\b(STE|SUITE|APT|UNIT|RM|ROOM|BLDG|FL|FLOOR|#)\s*\.?\s*\w*", "", addr)
    # Standardize abbreviations
    for full, abbr in ADDRESS_ABBREVS.items():
        addr = re.sub(r"\b" + full + r"\b\.?", abbr, addr)
    # Remove periods after abbreviations
    addr = re.sub(r"\.(?=\s|$)", "", addr)
    # Collapse whitespace
    addr = re.sub(r"\s+", " ", addr).strip()
    return addr


def normalize_name(name):
    """Normalize a facility name for matching."""
    if not name:
        return ""
    n = name.upper().strip()
    # Remove common suffixes
    n = re.sub(r"\b(LLC|INC|CORP|PC|PA|MD|DDS|DMD|DO|DPM|OD|DVM|PLLC)\b\.?", "", n)
    # Remove punctuation
    n = re.sub(r"[,.'\"&]", "", n)
    # Collapse whitespace
    n = re.sub(r"\s+", " ", n).strip()
    return n


def normalize_npi_record(raw_data):
    """Transform a raw NPI API record into the common schema."""
    basic = raw_data.get("basic", {})
    addresses = raw_data.get("addresses", [])
    taxonomies = raw_data.get("taxonomies", [])

    # Get practice address
    address = {}
    for addr in addresses:
        if addr.get("address_purpose") == "LOCATION":
            address = addr
            break
    if not address and addresses:
        address = addresses[0]

    # Get primary taxonomy
    taxonomy_code = ""
    for tax in taxonomies:
        if tax.get("primary", False):
            taxonomy_code = tax.get("code", "")
            break
    if not taxonomy_code and taxonomies:
        taxonomy_code = taxonomies[0].get("code", "")

    # Determine entity type and name
    entity_type = raw_data.get("enumeration_type", "") or basic.get("enumeration_type", "")
    if entity_type == "NPI-2":
        name = basic.get("organization_name", "").strip()
    else:
        first = basic.get("first_name", "").strip()
        last = basic.get("last_name", "").strip()
        credential = basic.get("credential", "").strip()
        name = f"{first} {last}".strip()
        if credential:
            name = f"{name}, {credential}"

    npi_number = str(raw_data.get("number", ""))

    # Extract enumeration date (NPI registration date = proxy for facility age)
    enumeration_date = basic.get("enumeration_date", "")

    return {
        "source": "npi",
        "source_id": f"npi-{npi_number}",
        "facility_name": name,
        "facility_type": classify_taxonomy(taxonomy_code),
        "address_line1": address.get("address_1", "").strip(),
        "address_line2": address.get("address_2", "").strip(),
        "city": address.get("city", "").strip(),
        "state": address.get("state", "AL").strip(),
        "zip5": address.get("postal_code", "")[:5].strip(),
        "county": address.get("county_name", "").strip() if "county_name" in address else "",
        "phone": clean_phone(address.get("telephone_number", "")),
        "fax": clean_phone(address.get("fax_number", "")),
        "administrator": "",
        "npi_number": npi_number,
        "license_number": "",
        "taxonomy_code": taxonomy_code,
        "entity_type": entity_type,
        "facility_established_date": enumeration_date if enumeration_date else None,
        # Normalized versions for matching
        "_norm_name": normalize_name(name),
        "_norm_address": normalize_address(address.get("address_1", "")),
    }


def normalize_adph_record(raw_data):
    """Transform a raw ADPH scraped record into the common schema."""
    name = raw_data.get("facility_name", "")

    return {
        "source": "adph",
        "source_id": f"adph-{raw_data.get('license_number', '')}",
        "facility_name": name,
        "facility_type": raw_data.get("facility_type", "Other"),
        "address_line1": raw_data.get("address", ""),
        "address_line2": "",
        "city": raw_data.get("city", ""),
        "state": "AL",
        "zip5": raw_data.get("zip", "")[:5],
        "county": raw_data.get("county", ""),
        "phone": clean_phone(raw_data.get("phone", "")),
        "fax": "",
        "administrator": raw_data.get("administrator", ""),
        "npi_number": "",
        "license_number": raw_data.get("license_number", ""),
        "taxonomy_code": "",
        "entity_type": "NPI-2",  # ADPH facilities are always organizations
        "_norm_name": normalize_name(name),
        "_norm_address": normalize_address(raw_data.get("address", "")),
    }


def normalize_cms_record(raw_data):
    """Transform a raw CMS POS record into the common schema."""
    name = raw_data.get("facility_name", "")

    return {
        "source": "cms",
        "source_id": f"cms-{raw_data.get('provider_id', '')}",
        "facility_name": name,
        "facility_type": "Hospital",  # CMS POS is primarily hospitals
        "address_line1": raw_data.get("address", ""),
        "address_line2": "",
        "city": raw_data.get("city", ""),
        "state": "AL",
        "zip5": raw_data.get("zip", "")[:5],
        "county": raw_data.get("county", ""),
        "phone": clean_phone(raw_data.get("phone", "")),
        "fax": "",
        "administrator": "",
        "npi_number": "",
        "license_number": "",
        "taxonomy_code": "",
        "entity_type": "NPI-2",
        "bed_count": raw_data.get("bed_count"),
        "hospital_type": raw_data.get("hospital_type", ""),
        "ownership_type": raw_data.get("ownership_type", ""),
        "_norm_name": normalize_name(name),
        "_norm_address": normalize_address(raw_data.get("address", "")),
    }


def load_from_db(source=None):
    """Load raw records from staging tables."""
    from tools.db import fetch_all

    records = []

    if source is None or source == "npi":
        rows = fetch_all("SELECT npi_number, raw_data FROM staging_npi")
        for row in rows:
            raw = row["raw_data"] if isinstance(row["raw_data"], dict) else json.loads(row["raw_data"])
            records.append(normalize_npi_record(raw))
        print(f"  NPI: {len(rows)} records normalized")

    if source is None or source == "adph":
        rows = fetch_all("SELECT license_number, raw_data FROM staging_adph")
        for row in rows:
            raw = row["raw_data"] if isinstance(row["raw_data"], dict) else json.loads(row["raw_data"])
            records.append(normalize_adph_record(raw))
        print(f"  ADPH: {len(rows)} records normalized")

    if source is None or source == "cms":
        rows = fetch_all("SELECT provider_id, raw_data FROM staging_cms")
        for row in rows:
            raw = row["raw_data"] if isinstance(row["raw_data"], dict) else json.loads(row["raw_data"])
            records.append(normalize_cms_record(raw))
        print(f"  CMS: {len(rows)} records normalized")

    return records


def load_from_json():
    """Load raw records from .tmp JSON files (fallback when DB not available)."""
    records = []

    npi_file = os.path.join(PROJECT_ROOT, ".tmp", "npi_raw.json")
    if os.path.exists(npi_file):
        with open(npi_file) as f:
            raw = json.load(f)
        for r in raw:
            state = ""
            for addr in r.get("addresses", []):
                if addr.get("address_purpose") == "LOCATION":
                    state = addr.get("state", "")
                    break
            if state.upper() == "AL":
                records.append(normalize_npi_record(r))
        print(f"  NPI (JSON): {len(records)} Alabama records normalized")

    adph_file = os.path.join(PROJECT_ROOT, ".tmp", "adph_results.json")
    if os.path.exists(adph_file):
        with open(adph_file) as f:
            raw = json.load(f)
        adph_count = 0
        for r in raw:
            records.append(normalize_adph_record(r))
            adph_count += 1
        print(f"  ADPH (JSON): {adph_count} records normalized")

    cms_file = os.path.join(PROJECT_ROOT, ".tmp", "cms_pos_alabama.json")
    if os.path.exists(cms_file):
        with open(cms_file) as f:
            raw = json.load(f)
        cms_count = 0
        for r in raw:
            records.append(normalize_cms_record(r))
            cms_count += 1
        print(f"  CMS (JSON): {cms_count} records normalized")

    return records


def normalize_all(source=None, use_json=False):
    """Main normalization pipeline."""
    print("Harvest Med Waste — Data Normalization")
    print()

    if use_json:
        records = load_from_json()
    else:
        try:
            records = load_from_db(source)
        except Exception as e:
            print(f"  DB error: {e}")
            print("  Falling back to JSON files...")
            records = load_from_json()

    print(f"\nTotal normalized records: {len(records)}")

    # Summary by source
    source_counts = {}
    for r in records:
        s = r["source"]
        source_counts[s] = source_counts.get(s, 0) + 1
    for s, count in sorted(source_counts.items()):
        print(f"  {s}: {count}")

    # Save normalized records
    output_file = os.path.join(PROJECT_ROOT, ".tmp", "normalized_records.json")
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(records, f, indent=2)
    print(f"\nSaved normalized records to {output_file}")

    return records


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Normalize raw data sources")
    parser.add_argument("--source", choices=["npi", "adph", "cms"], help="Normalize only this source")
    parser.add_argument("--json", action="store_true", help="Read from .tmp JSON files instead of DB")
    args = parser.parse_args()
    normalize_all(source=args.source, use_json=args.json)
