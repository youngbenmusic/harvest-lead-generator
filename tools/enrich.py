"""
enrich.py — Enrichment engine that runs all enabled plugins on leads.

Loads enrichment plugins and applies them to each lead, merging the
results back into the lead record.

Usage:
    python tools/enrich.py                  # Enrich from DB
    python tools/enrich.py --json           # Enrich from .tmp JSON files
    python tools/enrich.py --plugins waste_volume,geo_distance  # Run specific plugins only
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

# Registry of available plugins
AVAILABLE_PLUGINS = {
    "waste_volume": WasteVolumeEstimator,
    "geo_distance": GeoDistanceCalculator,
    "cms_bed_count": CMSBedCountEnricher,
    "data_completeness": DataCompletenessScorer,
}

# Default plugin execution order (data_completeness should run last)
DEFAULT_ORDER = ["cms_bed_count", "waste_volume", "geo_distance", "data_completeness"]


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


def enrich_all(leads, plugin_names=None):
    """Run enrichment on a list of leads. Returns enriched leads + stats."""
    print("Harvest Med Waste — Enrichment Engine")
    print()

    plugins = get_plugins(plugin_names)
    print(f"Active plugins: {[p.name for p in plugins]}")
    print(f"Leads to enrich: {len(leads)}")
    print()

    stats = {p.name: {"enriched": 0, "skipped": 0, "errors": 0} for p in plugins}
    start = time.time()

    for i, lead in enumerate(leads):
        for plugin in plugins:
            try:
                if plugin.can_enrich(lead):
                    fields = plugin.enrich(lead)
                    if fields:
                        lead.update(fields)
                        stats[plugin.name]["enriched"] += 1
                    else:
                        stats[plugin.name]["skipped"] += 1
                else:
                    stats[plugin.name]["skipped"] += 1
            except Exception as e:
                stats[plugin.name]["errors"] += 1
                if stats[plugin.name]["errors"] <= 5:
                    print(f"  {plugin.name} error: {e}")

        if (i + 1) % 2000 == 0:
            print(f"  Enriched {i + 1}/{len(leads)}...", flush=True)

    elapsed = time.time() - start

    # Print stats
    print(f"\n--- Enrichment Summary ({elapsed:.1f}s) ---")
    for plugin in plugins:
        s = stats[plugin.name]
        print(f"  {plugin.name}: {s['enriched']} enriched, {s['skipped']} skipped, {s['errors']} errors")

    return leads, stats


def enrich_from_json(plugin_names=None):
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
        if "address" in lead and "address_line1" not in lead:
            lead["address_line1"] = lead.get("address", "")
        if "zip" in lead and "zip5" not in lead:
            lead["zip5"] = lead.get("zip", "")[:5]

    leads, stats = enrich_all(leads, plugin_names)

    output_file = os.path.join(PROJECT_ROOT, ".tmp", "enriched_leads.json")
    with open(output_file, "w") as f:
        json.dump(leads, f, indent=2)
    print(f"\nSaved {len(leads)} enriched leads to {output_file}")

    return leads


def enrich_from_db(plugin_names=None):
    """Load leads from database, enrich, and update."""
    from tools.db import fetch_all, get_cursor

    rows = fetch_all("""
        SELECT id, lead_uid, facility_name, facility_type,
               address_line1, address_line2, city, state, zip5, county,
               phone, fax, administrator, npi_number, license_number,
               taxonomy_code, entity_type, bed_count,
               estimated_waste_lbs_per_day, distance_from_birmingham
        FROM leads
    """)

    if not rows:
        print("No leads in database. Run the pipeline first.")
        return []

    leads, stats = enrich_all(rows, plugin_names)

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
                    completeness_score = %s,
                    last_updated = NOW()
                WHERE id = %s
            """, (
                lead.get("bed_count"),
                lead.get("estimated_waste_lbs_per_day"),
                lead.get("estimated_monthly_volume"),
                lead.get("waste_tier"),
                lead.get("distance_from_birmingham"),
                lead.get("service_zone"),
                lead.get("completeness_score"),
                lead["id"],
            ))
    print("  Database updated")

    return leads


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich lead data")
    parser.add_argument("--json", action="store_true", help="Read from JSON files")
    parser.add_argument("--plugins", type=str, help="Comma-separated plugin names")
    args = parser.parse_args()

    plugin_list = args.plugins.split(",") if args.plugins else None

    if args.json:
        enrich_from_json(plugin_list)
    else:
        try:
            enrich_from_db(plugin_list)
        except Exception as e:
            print(f"DB error: {e}")
            print("Falling back to JSON mode...")
            enrich_from_json(plugin_list)
