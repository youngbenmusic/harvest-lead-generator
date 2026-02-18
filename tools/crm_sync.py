"""
crm_sync.py — CRM sync engine with adapter pattern.

Syncs qualified leads (score >= 50) to the configured CRM.
Checks for existing contacts before creating to avoid duplicates.

Usage:
    python tools/crm_sync.py                    # Sync using configured adapter
    python tools/crm_sync.py --adapter json     # Use JSON file adapter
    python tools/crm_sync.py --adapter hubspot  # Use HubSpot adapter
    python tools/crm_sync.py --min-score 80     # Only sync Hot leads
    python tools/crm_sync.py --dry-run          # Preview without syncing
"""

import json
import os
import sys
import argparse
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
except ImportError:
    pass

# Minimum score to sync to CRM
DEFAULT_MIN_SCORE = 50

# Available adapters
ADAPTERS = {
    "json": "tools.crm_adapters.json_file.JSONFileAdapter",
    "hubspot": "tools.crm_adapters.hubspot.HubSpotAdapter",
    "pipedrive": "tools.crm_adapters.pipedrive.PipedriveAdapter",
}


def get_adapter(adapter_name):
    """Instantiate a CRM adapter by name."""
    if adapter_name not in ADAPTERS:
        raise ValueError(f"Unknown adapter: {adapter_name}. Available: {list(ADAPTERS.keys())}")

    module_path, class_name = ADAPTERS[adapter_name].rsplit(".", 1)
    import importlib
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls()


def load_leads_from_db(min_score):
    """Load qualified leads from the database."""
    from tools.db import fetch_all
    rows = fetch_all("""
        SELECT id, lead_uid, facility_name, facility_type,
               address_line1, city, state, zip5, county,
               phone, fax, administrator, npi_number,
               lead_score, priority_tier, status, crm_id,
               estimated_waste_lbs_per_day, distance_from_birmingham
        FROM leads
        WHERE lead_score >= %s
        ORDER BY lead_score DESC
    """, (min_score,))
    return rows


def load_leads_from_json(min_score):
    """Load qualified leads from JSON file."""
    input_file = os.path.join(PROJECT_ROOT, ".tmp", "scored_leads.json")
    if not os.path.exists(input_file):
        input_file = os.path.join(PROJECT_ROOT, "data", "alabama_leads.json")

    with open(input_file) as f:
        leads = json.load(f)

    return [l for l in leads if (l.get("lead_score", 0) or 0) >= min_score]


def log_sync_action(lead_id, action, adapter_name, crm_id=None, payload=None, success=True, error=None):
    """Log a CRM sync action to the database."""
    try:
        from tools.db import get_cursor
        with get_cursor() as cur:
            cur.execute("""
                INSERT INTO crm_sync_log (lead_id, action, crm_adapter, crm_lead_id, payload, success, error_msg)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (lead_id, action, adapter_name, crm_id,
                  json.dumps(payload) if payload else None, success, error))
    except Exception:
        pass  # DB logging is best-effort


def sync_leads(adapter_name="json", min_score=DEFAULT_MIN_SCORE, dry_run=False):
    """Main CRM sync pipeline."""
    print("Harvest Med Waste — CRM Sync Engine")
    print(f"  Adapter: {adapter_name}")
    print(f"  Min score: {min_score}")
    print(f"  Dry run: {dry_run}")
    print()

    # Load adapter
    adapter = get_adapter(adapter_name)
    print(f"  Adapter initialized: {adapter}")

    # Load qualified leads
    try:
        leads = load_leads_from_db(min_score)
        source = "database"
    except Exception:
        leads = load_leads_from_json(min_score)
        source = "JSON"

    print(f"  Loaded {len(leads)} qualified leads from {source}")
    print()

    if not leads:
        print("No leads meet the minimum score threshold.")
        return {"created": 0, "updated": 0, "skipped": 0, "errors": 0}

    stats = {"created": 0, "updated": 0, "skipped": 0, "errors": 0}

    for i, lead in enumerate(leads):
        facility_name = lead.get("facility_name", lead.get("name", ""))
        phone = lead.get("phone", "")
        lead_id = lead.get("id")  # DB id or None

        try:
            # Check if already synced
            if lead.get("crm_id"):
                # Update existing
                if dry_run:
                    print(f"  [DRY RUN] Would update: {facility_name} (CRM: {lead['crm_id']})")
                    stats["updated"] += 1
                    continue

                success = adapter.update_lead(lead["crm_id"], lead)
                if success:
                    stats["updated"] += 1
                    log_sync_action(lead_id, "updated", adapter_name, lead["crm_id"])
                else:
                    stats["errors"] += 1
                continue

            # Search CRM for existing contact
            existing = adapter.search_contact(facility_name, phone)
            if existing:
                crm_id = existing.get("id", existing.get("lead_uid", ""))
                if dry_run:
                    print(f"  [DRY RUN] Would skip (exists): {facility_name}")
                    stats["skipped"] += 1
                    continue

                stats["skipped"] += 1
                log_sync_action(lead_id, "skipped_duplicate", adapter_name, str(crm_id))

                # Update crm_id in our DB
                try:
                    from tools.db import execute
                    if lead_id:
                        execute("UPDATE leads SET crm_id = %s, crm_synced_at = NOW() WHERE id = %s",
                                (str(crm_id), lead_id))
                except Exception:
                    pass
                continue

            # Create new lead in CRM
            if dry_run:
                print(f"  [DRY RUN] Would create: {facility_name} "
                      f"(Score: {lead.get('lead_score', 0)}, Tier: {lead.get('priority_tier', '?')})")
                stats["created"] += 1
                continue

            crm_id = adapter.create_lead(lead)
            stats["created"] += 1
            log_sync_action(lead_id, "created", adapter_name, crm_id)

            # Store CRM ID back in our DB
            try:
                from tools.db import execute
                if lead_id:
                    execute("UPDATE leads SET crm_id = %s, crm_synced_at = NOW() WHERE id = %s",
                            (crm_id, lead_id))
            except Exception:
                pass

        except Exception as e:
            stats["errors"] += 1
            error_msg = str(e)
            if stats["errors"] <= 10:
                print(f"  Error syncing {facility_name}: {error_msg}")
            log_sync_action(lead_id, "error", adapter_name, error=error_msg, success=False)

        if (i + 1) % 100 == 0:
            print(f"  Processed {i + 1}/{len(leads)}...", flush=True)

    # Print summary
    print(f"\n--- CRM Sync Summary ---")
    print(f"  Created: {stats['created']}")
    print(f"  Updated: {stats['updated']}")
    print(f"  Skipped (duplicates): {stats['skipped']}")
    print(f"  Errors: {stats['errors']}")

    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync leads to CRM")
    parser.add_argument("--adapter", default="json", choices=list(ADAPTERS.keys()),
                        help="CRM adapter to use")
    parser.add_argument("--min-score", type=int, default=DEFAULT_MIN_SCORE,
                        help="Minimum lead score to sync")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview sync without making changes")
    args = parser.parse_args()

    sync_leads(adapter_name=args.adapter, min_score=args.min_score, dry_run=args.dry_run)
