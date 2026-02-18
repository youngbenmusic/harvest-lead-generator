"""
migrate_json_to_db.py — One-time migration of existing leads from JSON to PostgreSQL.

Reads data/alabama_leads.json and inserts all leads into the PostgreSQL
database, creating lead_sources records for NPI attribution.

Usage:
    python tools/migrate_json_to_db.py
"""

import json
import os
import sys
from datetime import date

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from tools.db import get_conn, upsert_lead, upsert_lead_source, run_migration, close

LEADS_FILE = os.path.join(PROJECT_ROOT, "data", "alabama_leads.json")
MIGRATION_FILE = os.path.join(PROJECT_ROOT, "migrations", "001_initial_schema.sql")


def migrate():
    # Apply schema migration first
    print("Applying database schema...")
    run_migration(MIGRATION_FILE)

    # Load existing leads
    print(f"Loading leads from {LEADS_FILE}...")
    with open(LEADS_FILE, "r") as f:
        leads = json.load(f)
    print(f"  Loaded {len(leads)} leads")

    # Insert into database
    print("Migrating leads to PostgreSQL...")
    inserted = 0
    errors = 0

    for i, lead in enumerate(leads):
        try:
            lead_data = {
                "lead_uid": lead["id"],  # e.g., "npi-1234567890"
                "facility_name": lead.get("name", ""),
                "facility_type": lead.get("facility_type", "Other"),
                "address_line1": lead.get("address", ""),
                "address_line2": lead.get("address_2", ""),
                "city": lead.get("city", ""),
                "state": lead.get("state", "AL"),
                "zip5": lead.get("zip", "")[:5] if lead.get("zip") else "",
                "county": lead.get("county", ""),
                "phone": lead.get("phone", ""),
                "fax": lead.get("fax", ""),
                "npi_number": lead.get("npi_number", ""),
                "taxonomy_code": lead.get("taxonomy_code", ""),
                "entity_type": "",  # Not stored in current JSON
                "status": lead.get("status", "New"),
                "notes": lead.get("notes", ""),
                "date_added": lead.get("date_added", date.today().isoformat()),
            }

            lead_id = upsert_lead(lead_data)

            # Create NPI source attribution
            if lead.get("npi_number"):
                upsert_lead_source(
                    lead_id=lead_id,
                    source="npi",
                    source_id=f"npi-{lead['npi_number']}",
                    confidence=1.0,
                )

            inserted += 1
            if (i + 1) % 1000 == 0:
                print(f"  Migrated {i + 1}/{len(leads)} leads...", flush=True)

        except Exception as e:
            errors += 1
            if errors <= 10:
                print(f"  Error on lead {lead.get('id', '?')}: {e}")

    print(f"\nMigration complete:")
    print(f"  Inserted/updated: {inserted}")
    print(f"  Errors: {errors}")
    print(f"  Total in source: {len(leads)}")

    close()


if __name__ == "__main__":
    if not os.path.exists(LEADS_FILE):
        print(f"ERROR: Leads file not found: {LEADS_FILE}")
        print("Run the data pipeline first.")
        sys.exit(1)
    migrate()
