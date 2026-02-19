"""
enrich.py — Enrichment engine that runs all enabled plugins on leads.

Loads enrichment plugins and applies them to each lead, merging the
results back into the lead record. Produces an enrichment log for
auditing and a summary report on completion.

Usage:
    python tools/enrich.py                  # Enrich from DB
    python tools/enrich.py --json           # Enrich from .tmp JSON files
    python tools/enrich.py --plugins waste_volume,geo_distance  # Run specific plugins only
    python tools/enrich.py --dry-run        # Preview without modifying data
"""

import json
import os
import sys
import argparse
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from tools.enrichment_plugins.waste_volume import WasteVolumeEstimator
from tools.enrichment_plugins.geo_distance import GeoDistanceCalculator
from tools.enrichment_plugins.cms_bed_count import CMSBedCountEnricher
from tools.enrichment_plugins.data_completeness import DataCompletenessScorer
from tools.enrichment_plugins.hunter_email import HunterEmailEnricher

# Registry of available plugins
AVAILABLE_PLUGINS = {
    "waste_volume": WasteVolumeEstimator,
    "geo_distance": GeoDistanceCalculator,
    "cms_bed_count": CMSBedCountEnricher,
    "data_completeness": DataCompletenessScorer,
    "hunter_email": HunterEmailEnricher,
}

# Default plugin execution order (data_completeness should run last)
DEFAULT_ORDER = ["cms_bed_count", "waste_volume", "geo_distance", "hunter_email", "data_completeness"]

ENRICHMENT_LOG_FILE = os.path.join(PROJECT_ROOT, ".tmp", "enrichment_log.json")


def get_plugins(plugin_names=None):
    """Instantiate and return the requested plugins in order."""
    if plugin_names:
        names = [n.strip() for n in plugin_names if n.strip() in AVAILABLE_PLUGINS]
    else:
        names = DEFAULT_ORDER

    plugins = []
    for name in names:
        cls = AVAILABLE_PLUGINS.get(name)
        if cls:
            plugins.append(cls())
    return plugins


def enrich_lead(lead, plugins):
    """Run all plugins on a single lead and merge results."""
    for plugin in plugins:
        try:
            if plugin.can_enrich(lead):
                fields = plugin.enrich(lead)
                if fields:
                    lead.update(fields)
        except Exception as e:
            print(f"  Plugin {plugin.name} error on {lead.get('facility_name', '?')}: {e}")
    return lead


def enrich_all(leads, plugin_names=None, dry_run=False):
    """Run enrichment on a list of leads. Returns enriched leads + stats."""
    print("Harvest Med Waste — Enrichment Engine")
    if dry_run:
        print("  *** DRY RUN — no data will be modified ***")
    print()

    plugins = get_plugins(plugin_names)
    print(f"Active plugins: {[p.name for p in plugins]}")
    print(f"Leads to enrich: {len(leads)}")
    print()

    stats = {p.name: {"enriched": 0, "skipped": 0, "errors": 0} for p in plugins}
    enrichment_log = []
    start = time.time()

    for i, lead in enumerate(leads):
        for plugin in plugins:
            log_entry = {
                "lead_uid": lead.get("lead_uid", lead.get("id", f"idx-{i}")),
                "plugin": plugin.name,
                "status": None,
                "fields_added": [],
                "error": None,
            }

            try:
                if plugin.can_enrich(lead):
                    fields = plugin.enrich(lead)
                    if fields:
                        if not dry_run:
                            lead.update(fields)
                        # Track which fields were added (skip internal fields)
                        log_entry["fields_added"] = [
                            k for k in fields.keys() if not k.startswith("_")
                        ]
                        log_entry["status"] = "enriched"
                        stats[plugin.name]["enriched"] += 1
                    else:
                        log_entry["status"] = "skipped"
                        stats[plugin.name]["skipped"] += 1
                else:
                    log_entry["status"] = "skipped"
                    stats[plugin.name]["skipped"] += 1
            except Exception as e:
                log_entry["status"] = "error"
                log_entry["error"] = str(e)
                stats[plugin.name]["errors"] += 1
                if stats[plugin.name]["errors"] <= 5:
                    print(f"  {plugin.name} error: {e}")

            enrichment_log.append(log_entry)

        if (i + 1) % 2000 == 0:
            print(f"  Enriched {i + 1}/{len(leads)}...", flush=True)

    elapsed = time.time() - start

    # Flush geo cache if the geo_distance plugin was used
    for plugin in plugins:
        if hasattr(plugin, "flush_cache"):
            plugin.flush_cache()

    # Write enrichment log
    os.makedirs(os.path.dirname(ENRICHMENT_LOG_FILE), exist_ok=True)
    with open(ENRICHMENT_LOG_FILE, "w") as f:
        json.dump(enrichment_log, f, indent=2)

    # Print summary
    total_enriched = sum(s["enriched"] for s in stats.values())
    total_errors = sum(s["errors"] for s in stats.values())
    total_skipped = sum(s["skipped"] for s in stats.values())

    print(f"\nEnrichment complete ({elapsed:.1f}s):")
    print(f"  Enriched: {total_enriched:,} lead-plugin pairs")
    print(f"  Failed: {total_errors:,} (see {ENRICHMENT_LOG_FILE})")
    print(f"  Skipped: {total_skipped:,}")
    print()
    print("  By plugin:")
    for plugin in plugins:
        s = stats[plugin.name]
        parts = [f"{s['enriched']} enriched"]
        if s["skipped"]:
            parts.append(f"{s['skipped']} skipped")
        if s["errors"]:
            parts.append(f"{s['errors']} failed")
        print(f"    {plugin.name}: {', '.join(parts)}")

    return leads, stats


def enrich_from_json(plugin_names=None, dry_run=False):
    """Load leads from JSON, enrich, and save."""
    input_file = os.path.join(PROJECT_ROOT, ".tmp", "deduplicated_leads.json")
    if not os.path.exists(input_file):
        # Fall back to existing leads file
        input_file = os.path.join(PROJECT_ROOT, "data", "alabama_leads.json")

    if not os.path.exists(input_file):
        print(f"ERROR: No input file found.")
        sys.exit(1)

    print(f"Loading from {input_file}...")
    with open(input_file) as f:
        leads = json.load(f)

    # Normalize field names if coming from legacy JSON
    for lead in leads:
        if "name" in lead and "facility_name" not in lead:
            lead["facility_name"] = lead["name"]
        if "address" in lead and "address_line1" not in lead:
            lead["address_line1"] = lead.get("address", "")
        if "zip" in lead and "zip5" not in lead:
            lead["zip5"] = lead.get("zip", "")[:5]

    leads, stats = enrich_all(leads, plugin_names, dry_run=dry_run)

    if not dry_run:
        output_file = os.path.join(PROJECT_ROOT, ".tmp", "enriched_leads.json")
        with open(output_file, "w") as f:
            json.dump(leads, f, indent=2)
        print(f"\nSaved {len(leads)} enriched leads to {output_file}")
    else:
        print("\n  Dry run complete — no files written.")

    return leads


def enrich_from_db(plugin_names=None, dry_run=False):
    """Load leads from database, enrich, and update."""
    from tools.db import fetch_all, get_cursor

    rows = fetch_all("""
        SELECT id, lead_uid, facility_name, facility_type,
               address_line1, address_line2, city, state, zip5, county,
               phone, fax, administrator, npi_number, license_number,
               taxonomy_code, entity_type, bed_count,
               estimated_waste_lbs_per_day, distance_from_birmingham,
               latitude, longitude, facility_established_date,
               contract_expiry_date,
               contact_email, contact_name, contact_title, email_confidence
        FROM leads
    """)

    if not rows:
        print("No leads in database. Run the pipeline first.")
        return []

    leads, stats = enrich_all(rows, plugin_names, dry_run=dry_run)

    if not dry_run:
        # Update database with enrichment fields
        print("\nSaving enrichments to database...")
        with get_cursor() as cur:
            for lead in leads:
                cur.execute("""
                    UPDATE leads SET
                        bed_count = COALESCE(%s, bed_count),
                        estimated_waste_lbs_per_day = %s,
                        estimated_monthly_volume = %s,
                        waste_tier = %s,
                        distance_from_birmingham = %s,
                        service_zone = %s,
                        latitude = COALESCE(%s, latitude),
                        longitude = COALESCE(%s, longitude),
                        completeness_score = %s,
                        contact_email = COALESCE(%s, contact_email),
                        contact_name = COALESCE(%s, contact_name),
                        contact_title = COALESCE(%s, contact_title),
                        email_confidence = COALESCE(%s, email_confidence),
                        last_updated = NOW()
                    WHERE id = %s
                """, (
                    lead.get("bed_count"),
                    lead.get("estimated_waste_lbs_per_day"),
                    lead.get("estimated_monthly_volume"),
                    lead.get("waste_tier"),
                    lead.get("distance_from_birmingham"),
                    lead.get("service_zone"),
                    lead.get("latitude"),
                    lead.get("longitude"),
                    lead.get("completeness_score"),
                    lead.get("contact_email"),
                    lead.get("contact_name"),
                    lead.get("contact_title"),
                    lead.get("email_confidence"),
                    lead["id"],
                ))
        print("  Database updated")
    else:
        print("\n  Dry run complete — database not modified.")

    return leads


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich lead data")
    parser.add_argument("--json", action="store_true", help="Read from JSON files")
    parser.add_argument("--plugins", type=str, help="Comma-separated plugin names")
    parser.add_argument("--dry-run", action="store_true", help="Preview enrichment without modifying data")
    args = parser.parse_args()

    plugin_list = args.plugins.split(",") if args.plugins else None

    if args.json:
        enrich_from_json(plugin_list, dry_run=args.dry_run)
    else:
        try:
            enrich_from_db(plugin_list, dry_run=args.dry_run)
        except Exception as e:
            print(f"DB error: {e}")
            print("Falling back to JSON mode...")
            enrich_from_json(plugin_list, dry_run=args.dry_run)
