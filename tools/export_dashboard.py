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

OUTPUT_FILE = os.path.join(PROJECT_ROOT, "data", "alabama_leads.json")


def export():
    from tools.db import fetch_all, close

    print("Exporting leads from database to dashboard JSON...")

    rows = fetch_all("""
        SELECT
            lead_uid, facility_name, facility_type,
            address_line1, address_line2, city, state, zip5, county,
            phone, fax, npi_number, taxonomy_code, entity_type,
            bed_count, estimated_waste_lbs_per_day, estimated_monthly_volume,
            waste_tier, distance_from_birmingham, service_zone,
            completeness_score, lead_score, priority_tier,
            status, notes, date_added, first_seen, last_updated,
            latitude, longitude, facility_established_date,
            contact_email, contact_name, contact_title
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
            "latitude": float(row["latitude"]) if row["latitude"] else None,
            "longitude": float(row["longitude"]) if row["longitude"] else None,
            "facility_established_date": row["facility_established_date"].isoformat() if row["facility_established_date"] else None,
            "contact_email": row["contact_email"] or "",
            "contact_name": row["contact_name"] or "",
            "contact_title": row["contact_title"] or "",
        }
        leads.append(lead)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(leads, f, indent=2)

    print(f"Exported {len(leads)} leads to {OUTPUT_FILE}")
    close()


def export_from_json():
    """Export scored leads from JSON pipeline to dashboard format.

    Reads .tmp/scored_leads.json (or .tmp/enriched_leads.json as fallback)
    and writes data/alabama_leads.json in the format the dashboard expects.
    """
    input_file = os.path.join(PROJECT_ROOT, ".tmp", "scored_leads.json")
    if not os.path.exists(input_file):
        input_file = os.path.join(PROJECT_ROOT, ".tmp", "enriched_leads.json")
    if not os.path.exists(input_file):
        print(f"ERROR: No scored/enriched JSON found in .tmp/")
        return

    print(f"Exporting from {input_file} to dashboard JSON...")
    with open(input_file) as f:
        leads = json.load(f)

    dashboard_leads = []
    for row in leads:
        lead = {
            "id": row.get("lead_uid") or row.get("id", ""),
            "name": row.get("facility_name") or row.get("name", ""),
            "facility_type": row.get("facility_type", ""),
            "address": row.get("address_line1") or row.get("address", ""),
            "address_2": row.get("address_line2") or row.get("address_2", ""),
            "city": row.get("city", ""),
            "county": row.get("county", ""),
            "state": row.get("state", "AL"),
            "zip": row.get("zip5") or row.get("zip", ""),
            "phone": row.get("phone", ""),
            "fax": row.get("fax", ""),
            "taxonomy_code": row.get("taxonomy_code", ""),
            "npi_number": row.get("npi_number", ""),
            "status": row.get("status", "New"),
            "notes": row.get("notes", ""),
            "date_added": row.get("date_added", ""),
            "new_this_week": row.get("new_this_week", False),
            # Enrichment fields
            "lead_score": row.get("lead_score", 0),
            "priority_tier": row.get("priority_tier", ""),
            "estimated_waste_lbs_per_day": row.get("estimated_waste_lbs_per_day"),
            "waste_tier": row.get("waste_tier", ""),
            "distance_from_birmingham": row.get("distance_from_birmingham"),
            "service_zone": row.get("service_zone", ""),
            "bed_count": row.get("bed_count"),
            "latitude": row.get("latitude"),
            "longitude": row.get("longitude"),
            "facility_established_date": row.get("facility_established_date"),
            "contact_email": row.get("contact_email", ""),
            "contact_name": row.get("contact_name", ""),
            "contact_title": row.get("contact_title", ""),
        }
        dashboard_leads.append(lead)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(dashboard_leads, f, indent=2)

    print(f"Exported {len(dashboard_leads)} leads to {OUTPUT_FILE}")


if __name__ == "__main__":
    export()
