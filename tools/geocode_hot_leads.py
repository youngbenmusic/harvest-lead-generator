"""
geocode_hot_leads.py — Run geocode enrichment on Hot leads only.

Loads alabama_leads.json, filters to Hot priority tier, runs the
geo_distance plugin, and saves the results back to the full dataset.
"""

import json
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from tools.enrichment_plugins.geo_distance import GeoDistanceCalculator

INPUT_FILE = os.path.join(PROJECT_ROOT, "data", "alabama_leads.json")


def normalize_lead(lead):
    """Normalize legacy field names for the geo_distance plugin."""
    if "name" in lead and "facility_name" not in lead:
        lead["facility_name"] = lead["name"]
    if "address" in lead and "address_line1" not in lead:
        lead["address_line1"] = lead.get("address", "")
    if "zip" in lead and "zip5" not in lead:
        lead["zip5"] = lead.get("zip", "")[:5]
    return lead


def main():
    print("Harvest Med Waste — Geocode Hot Leads")
    print()

    with open(INPUT_FILE) as f:
        all_leads = json.load(f)

    hot_leads = [l for l in all_leads if l.get("priority_tier") == "Hot"]
    print(f"Total leads: {len(all_leads)}")
    print(f"Hot leads to geocode: {len(hot_leads)}")
    print()

    plugin = GeoDistanceCalculator()
    enriched = 0
    skipped = 0
    errors = 0

    for i, lead in enumerate(hot_leads):
        normalize_lead(lead)
        try:
            if plugin.can_enrich(lead):
                fields = plugin.enrich(lead)
                if fields:
                    lead.update(fields)
                    enriched += 1
                else:
                    skipped += 1
            else:
                skipped += 1
        except Exception as e:
            errors += 1
            if errors <= 10:
                print(f"  Error on {lead.get('name', '?')}: {e}")

        if (i + 1) % 100 == 0:
            print(f"  Progress: {i + 1}/{len(hot_leads)} "
                  f"(enriched: {enriched}, skipped: {skipped}, errors: {errors})",
                  flush=True)

    # Flush the geocode cache
    plugin.flush_cache()

    # Save updated leads back
    with open(INPUT_FILE, "w") as f:
        json.dump(all_leads, f, indent=2)

    print()
    print(f"Done! Results:")
    print(f"  Enriched: {enriched}")
    print(f"  Skipped:  {skipped}")
    print(f"  Errors:   {errors}")
    print(f"  Saved to: {INPUT_FILE}")


if __name__ == "__main__":
    main()
