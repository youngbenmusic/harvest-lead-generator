"""
process_leads.py — Process raw NPI data into structured leads.

Reads .tmp/npi_raw.json, maps taxonomy codes to facility categories,
deduplicates by address, cleans phone numbers, removes individuals
where an organization exists at the same address, and outputs
data/alabama_leads.json.

Usage:
    python tools/process_leads.py
"""

import json
import os
import re
import sys
import hashlib
from datetime import date

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE = os.path.join(PROJECT_ROOT, ".tmp", "npi_raw.json")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "alabama_leads.json")
HISTORY_FILE = os.path.join(OUTPUT_DIR, "lead_history.json")

# ---------------------------------------------------------------------------
# Taxonomy code to facility type mapping
# ---------------------------------------------------------------------------
# Keys are taxonomy code prefixes. The first matching prefix wins.
TAXONOMY_MAP = [
    # Dental
    ("1223", "Dental"),
    ("1224", "Dental"),
    ("1225", "Dental"),
    ("122", "Dental"),
    ("124Q", "Dental"),
    ("124", "Dental"),
    ("126", "Dental"),

    # Veterinary
    ("174", "Veterinary"),

    # Hospitals
    ("282N", "Hospital"),
    ("282", "Hospital"),
    ("283", "Hospital"),
    ("284", "Hospital"),
    ("286", "Hospital"),

    # Urgent Care
    ("261QU0200", "Urgent Care"),
    ("261QU", "Urgent Care"),

    # Surgery Centers
    ("2085R", "Surgery Center"),
    ("2086", "Surgery Center"),
    ("208600", "Surgery Center"),
    ("341", "Surgery Center"),  # Ambulatory Surgical Center

    # Labs
    ("291", "Lab"),
    ("292", "Lab"),
    ("293", "Lab"),

    # Nursing Homes / Long Term Care
    ("311", "Nursing Home"),
    ("313", "Nursing Home"),
    ("314", "Nursing Home"),
    ("315", "Nursing Home"),
    ("324", "Nursing Home"),

    # Clinics (catch-all for 261 after Urgent Care is matched)
    ("261QM", "Medical Practice"),
    ("261Q", "Medical Practice"),
    ("261", "Medical Practice"),

    # Medical Practice (physicians, specialists)
    ("207Q", "Medical Practice"),  # Family Medicine
    ("207R", "Medical Practice"),  # Internal Medicine
    ("207", "Medical Practice"),
    ("208", "Medical Practice"),
    ("209", "Medical Practice"),

    # Other provider types we still want to capture
    ("363", "Medical Practice"),  # Nurse Practitioner
    ("367", "Medical Practice"),  # Nurse Anesthetist
    ("163", "Medical Practice"),  # Registered Nurse
    ("225", "Medical Practice"),  # Physical Therapist
    ("227", "Medical Practice"),  # Occupational Therapist
    ("235", "Medical Practice"),  # Speech-Language Pathologist
    ("111", "Medical Practice"),  # Chiropractor
    ("152", "Medical Practice"),  # Optometry
    ("213", "Medical Practice"),  # Podiatry
    ("332", "Other"),  # Pharmacy
    ("333", "Other"),  # Dialysis
    ("273", "Other"),  # Home Health
]


def classify_taxonomy(taxonomy_code):
    """Map a taxonomy code to a human-readable facility type."""
    if not taxonomy_code:
        return "Other"
    for prefix, category in TAXONOMY_MAP:
        if taxonomy_code.startswith(prefix):
            return category
    return "Other"


def clean_phone(phone):
    """Clean phone number to (XXX) XXX-XXXX format."""
    if not phone:
        return ""
    digits = re.sub(r"\D", "", str(phone))
    # Remove leading 1 for US country code
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return str(phone)  # return original if can't parse


def extract_provider_info(record):
    """Extract structured fields from an NPPES API record."""
    basic = record.get("basic", {})
    addresses = record.get("addresses", [])
    taxonomies = record.get("taxonomies", [])

    # Get the practice/business address (location type = DOM for mailing, use first available)
    address = {}
    for addr in addresses:
        if addr.get("address_purpose") == "LOCATION":
            address = addr
            break
    if not address and addresses:
        address = addresses[0]

    # Get primary taxonomy code
    taxonomy_code = ""
    for tax in taxonomies:
        if tax.get("primary", False):
            taxonomy_code = tax.get("code", "")
            break
    if not taxonomy_code and taxonomies:
        taxonomy_code = taxonomies[0].get("code", "")

    # Determine name — enumeration_type is at root level, not inside basic
    entity_type = record.get("enumeration_type", "") or basic.get("enumeration_type", "")
    if entity_type == "NPI-2":
        # Organization
        name = basic.get("organization_name", "").strip()
    else:
        # Individual — use first + last name
        first = basic.get("first_name", "").strip()
        last = basic.get("last_name", "").strip()
        credential = basic.get("credential", "").strip()
        name = f"{first} {last}".strip()
        if credential:
            name = f"{name}, {credential}"

    return {
        "npi_number": str(record.get("number", "")),
        "name": name,
        "entity_type": entity_type,  # NPI-1 = individual, NPI-2 = organization
        "taxonomy_code": taxonomy_code,
        "facility_type": classify_taxonomy(taxonomy_code),
        "address": address.get("address_1", "").strip(),
        "address_2": address.get("address_2", "").strip(),
        "city": address.get("city", "").strip(),
        "state": address.get("state", "AL").strip(),
        "zip": address.get("postal_code", "")[:5].strip(),
        "county": address.get("county_name", "").strip() if "county_name" in address else "",
        "phone": clean_phone(address.get("telephone_number", "")),
        "fax": clean_phone(address.get("fax_number", "")),
    }


def make_address_key(record):
    """Create a normalized address key for deduplication."""
    parts = [
        record["address"].upper().strip(),
        record["city"].upper().strip(),
        record["zip"][:5].strip(),
    ]
    return "|".join(parts)


def generate_id(record):
    """Generate a stable, unique ID from NPI number."""
    return f"npi-{record['npi_number']}"


def process_leads():
    """Main processing pipeline."""
    # Load raw data
    print(f"Loading raw data from {INPUT_FILE}...")
    with open(INPUT_FILE, "r") as f:
        raw_records = json.load(f)
    print(f"  Loaded {len(raw_records)} raw records")

    # Extract and classify all records
    print("Extracting and classifying providers...")
    providers = []
    for record in raw_records:
        info = extract_provider_info(record)
        # Only keep Alabama records (should already be filtered, but just in case)
        if info["state"].upper() == "AL":
            providers.append(info)
    print(f"  {len(providers)} Alabama providers extracted")

    # Separate organizations and individuals
    orgs = [p for p in providers if p["entity_type"] == "NPI-2"]
    individuals = [p for p in providers if p["entity_type"] == "NPI-1"]
    print(f"  {len(orgs)} organizations, {len(individuals)} individuals")

    # Build a set of addresses where organizations exist
    org_addresses = set()
    for org in orgs:
        key = make_address_key(org)
        if key:
            org_addresses.add(key)

    # Filter out individuals at addresses where an organization already exists
    filtered_individuals = []
    removed_count = 0
    for ind in individuals:
        key = make_address_key(ind)
        if key in org_addresses:
            removed_count += 1
        else:
            filtered_individuals.append(ind)
    print(f"  Removed {removed_count} individuals at organization addresses")

    # Combine organizations + remaining individuals
    all_providers = orgs + filtered_individuals
    print(f"  {len(all_providers)} providers after individual filtering")

    # Deduplicate by address — keep the record with the most info
    print("Deduplicating by address...")
    address_groups = {}
    for p in all_providers:
        key = make_address_key(p)
        if not key or key == "||":
            continue
        if key not in address_groups:
            address_groups[key] = []
        address_groups[key].append(p)

    deduped = []
    for key, group in address_groups.items():
        if len(group) == 1:
            deduped.append(group[0])
        else:
            # Prefer organizations over individuals
            orgs_in_group = [p for p in group if p["entity_type"] == "NPI-2"]
            if orgs_in_group:
                # Pick the org with the longest name (usually the most complete)
                best = max(orgs_in_group, key=lambda p: len(p["name"]))
            else:
                best = max(group, key=lambda p: len(p["name"]))
            deduped.append(best)

    print(f"  {len(deduped)} unique leads after deduplication")

    # Load previous history for new_this_week detection
    previous_npis = set()
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r") as f:
            history = json.load(f)
            previous_npis = set(history.get("npi_numbers", []))
        print(f"  Loaded {len(previous_npis)} NPIs from history")

    # Build final lead records
    today = date.today().isoformat()
    leads = []
    for p in deduped:
        is_new = p["npi_number"] not in previous_npis
        lead = {
            "id": generate_id(p),
            "name": p["name"],
            "facility_type": p["facility_type"],
            "address": p["address"],
            "city": p["city"],
            "county": p["county"],
            "state": p["state"],
            "zip": p["zip"],
            "phone": p["phone"],
            "fax": p["fax"],
            "taxonomy_code": p["taxonomy_code"],
            "npi_number": p["npi_number"],
            "status": "New",
            "notes": "",
            "date_added": today,
            "new_this_week": is_new if previous_npis else False,
        }
        leads.append(lead)

    # Sort by facility type, then name
    leads.sort(key=lambda x: (x["facility_type"], x["name"]))

    # Print summary by category
    print("\n--- Lead Summary by Facility Type ---")
    type_counts = {}
    for lead in leads:
        ft = lead["facility_type"]
        type_counts[ft] = type_counts.get(ft, 0) + 1
    for ft, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"  {ft}: {count}")
    print(f"  TOTAL: {len(leads)}")

    # Save leads
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(leads, f, indent=2)
    print(f"\nSaved {len(leads)} leads to {OUTPUT_FILE}")

    # Update history
    all_npis = [lead["npi_number"] for lead in leads]
    history_data = {
        "last_updated": today,
        "npi_numbers": all_npis,
        "total_leads": len(leads),
    }
    with open(HISTORY_FILE, "w") as f:
        json.dump(history_data, f, indent=2)
    print(f"Updated history file: {HISTORY_FILE}")


if __name__ == "__main__":
    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: Input file not found: {INPUT_FILE}")
        print("Run tools/download_npi.py first to download NPI data.")
        sys.exit(1)
    process_leads()
