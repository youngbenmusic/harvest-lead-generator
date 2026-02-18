"""
download_cms_pos.py — Download CMS Provider of Services (POS) data for Alabama.

Extracts hospital bed counts, teaching status, and ownership type from the
CMS POS file. This data enriches our leads with facility size information.

The POS file is updated quarterly by CMS.

Usage:
    python tools/download_cms_pos.py
    python tools/download_cms_pos.py --json-only   # Skip DB, write JSON only
"""

import csv
import io
import json
import os
import sys
import zipfile
import argparse

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

try:
    import requests
except ImportError:
    print("ERROR: Install requests: pip install requests")
    sys.exit(1)

# CMS POS Other file — contains hospital bed counts and more
# This URL may change; check https://data.cms.gov for current link
POS_DATA_URL = "https://data.cms.gov/provider-data/sites/default/files/resources/c87e1ebb6e0fa658d3a1e1a0884744a6/pos_other_dec24.csv"

OUTPUT_FILE = os.path.join(PROJECT_ROOT, ".tmp", "cms_pos_alabama.json")


def download_pos_data():
    """Download CMS POS data and filter for Alabama."""
    print("Downloading CMS Provider of Services data...")
    print(f"URL: {POS_DATA_URL}")

    try:
        resp = requests.get(POS_DATA_URL, timeout=120, stream=True)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"Error downloading POS data: {e}")
        print("You can manually download from https://data.cms.gov/provider-characteristics/")
        return []

    content = resp.text
    print(f"  Downloaded {len(content) / (1024*1024):.1f} MB")

    reader = csv.DictReader(io.StringIO(content))
    alabama_records = []

    for row in reader:
        state = row.get("STATE_CD", "") or row.get("PRVDR_STATE_CD", "") or row.get("State Code", "")
        if state.upper() != "AL":
            continue

        record = {
            "source": "cms",
            "provider_id": row.get("PRVDR_NUM", "") or row.get("Provider Number", ""),
            "facility_name": row.get("FAC_NAME", "") or row.get("Facility Name", ""),
            "address": row.get("ST_ADR", "") or row.get("Street Address", ""),
            "city": row.get("CITY_NAME", "") or row.get("City", ""),
            "state": "AL",
            "zip": (row.get("ZIP_CD", "") or row.get("Zip Code", ""))[:5],
            "county": row.get("COUNTY_NAME", "") or row.get("County Name", ""),
            "phone": row.get("PHNE_NUM", "") or row.get("Phone Number", ""),
            "bed_count": parse_int(row.get("BED_CNT", "") or row.get("Number of Beds", "")),
            "hospital_type": row.get("GNRL_FAC_TYPE", "") or row.get("General Facility Type", ""),
            "ownership_type": row.get("OWNR_CD", "") or row.get("Ownership Type", ""),
            "teaching_status": row.get("MDCL_SCHL_AFLTN_CD", "") or row.get("Teaching Status", ""),
            "provider_type": row.get("PRVDR_CTGRY_CD", "") or row.get("Provider Category", ""),
            "certification_date": row.get("CRTFCTN_DT", "") or row.get("Certification Date", ""),
        }
        alabama_records.append(record)

    print(f"  Found {len(alabama_records)} Alabama providers in POS data")
    return alabama_records


def parse_int(val):
    """Safely parse an integer from string."""
    try:
        return int(str(val).strip())
    except (ValueError, TypeError):
        return None


def write_to_db(records):
    """Write POS records to the staging_cms table."""
    try:
        from tools.db import get_cursor
    except Exception:
        print("  Database not available, skipping DB write")
        return 0

    count = 0
    with get_cursor() as cur:
        for rec in records:
            provider_id = rec.get("provider_id", "")
            if not provider_id:
                continue
            try:
                cur.execute("""
                    INSERT INTO staging_cms (provider_id, raw_data)
                    VALUES (%s, %s)
                    ON CONFLICT (provider_id) DO UPDATE SET
                        raw_data = EXCLUDED.raw_data,
                        ingested_at = NOW()
                """, (provider_id, json.dumps(rec)))
                count += 1
            except Exception as e:
                print(f"  DB error for {rec.get('facility_name', '?')}: {e}")
    return count


def main(json_only=False):
    print("Harvest Med Waste — CMS Provider of Services Download")
    print()

    records = download_pos_data()
    if not records:
        print("No records found. Check the download URL.")
        return

    # Save JSON
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(records, f, indent=2)
    print(f"\nSaved {len(records)} records to {OUTPUT_FILE}")

    # Write to database
    if not json_only:
        db_count = write_to_db(records)
        print(f"Wrote {db_count} records to staging_cms table")

    # Summary
    bed_counts = [r["bed_count"] for r in records if r.get("bed_count")]
    print(f"\n--- CMS POS Summary ---")
    print(f"  Total Alabama providers: {len(records)}")
    print(f"  With bed counts: {len(bed_counts)}")
    if bed_counts:
        print(f"  Total beds: {sum(bed_counts)}")
        print(f"  Avg beds per facility: {sum(bed_counts) / len(bed_counts):.0f}")
        print(f"  Max beds: {max(bed_counts)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download CMS POS data for Alabama")
    parser.add_argument("--json-only", action="store_true", help="Skip database write")
    args = parser.parse_args()
    main(json_only=args.json_only)
