"""
export_dashboard.py — Export leads from PostgreSQL to JSON for the web dashboard.

Reads from the leads table and generates data/alabama_leads.json in the
format the existing vanilla JS dashboard expects.

Usage:
    python tools/export_dashboard.py
"""

import json
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from tools.db import fetch_all, close

OUTPUT_FILE = os.path.join(PROJECT_ROOT, "data", "alabama_leads.json")


def export():
    print("Exporting leads from database to dashboard JSON...")

    rows = fetch_all("""
        SELECT
            lead_uid, facility_name, facility_type,
            address_line1, address_line2, city, state, zip5, county,
            phone, fax, npi_number, taxonomy_code, entity_type,
            bed_count, estimated_waste_lbs_per_day, estimated_monthly_volume,
            waste_tier, distance_from_birmingham, service_zone,
            completeness_score, lead_score, priority_tier,
            status, notes, date_added, first_seen, last_updated
        FROM leads
        ORDER BY facility_type, facility_name
    """)

    leads = []
    for row in rows:
        lead = {
            "id": row["lead_uid"],
            "name": row["facility_name"],
            "facility_type": row["facility_type"],
            "address": row["address_line1"] or "",
            "address_2": row["address_line2"] or "",
            "city": row["city"] or "",
            "county": row["county"] or "",
            "state": row["state"] or "AL",
            "zip": row["zip5"] or "",
            "phone": row["phone"] or "",
            "fax": row["fax"] or "",
            "taxonomy_code": row["taxonomy_code"] or "",
            "npi_number": row["npi_number"] or "",
            "status": row["status"] or "New",
            "notes": row["notes"] or "",
            "date_added": row["date_added"].isoformat() if row["date_added"] else "",
            "new_this_week": False,
            # New enrichment fields for dashboard v2
            "lead_score": row["lead_score"] or 0,
            "priority_tier": row["priority_tier"] or "",
            "estimated_waste_lbs_per_day": float(row["estimated_waste_lbs_per_day"]) if row["estimated_waste_lbs_per_day"] else None,
            "waste_tier": row["waste_tier"] or "",
            "distance_from_birmingham": float(row["distance_from_birmingham"]) if row["distance_from_birmingham"] else None,
            "service_zone": row["service_zone"] or "",
            "bed_count": row["bed_count"],
        }
        leads.append(lead)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(leads, f, indent=2)

    print(f"Exported {len(leads)} leads to {OUTPUT_FILE}")
    close()


if __name__ == "__main__":
    export()
