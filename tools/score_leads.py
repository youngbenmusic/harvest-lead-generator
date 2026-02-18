"""
score_leads.py — Weighted lead scoring model.

Scores leads on a 0-100 scale based on:
  - Waste volume potential (35%)
  - Facility type priority (25%)
  - Geographic proximity to Birmingham (20%)
  - Data completeness (10%)
  - Source confidence (10%)

Priority tiers:
  - Hot (80-100): Large facilities, close to Birmingham, high waste volume
  - Warm (50-79): Mid-size facilities or further distance
  - Cool (25-49): Small facilities, limited data
  - Cold (0-24): Minimal waste generators or very distant

Usage:
    python tools/score_leads.py
    python tools/score_leads.py --json    # Score from JSON files
"""

import json
import os
import sys
import argparse

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# Facility type priority scores (out of 25)
FACILITY_TYPE_SCORES = {
    "Hospital": 25,
    "Surgery Center": 22,
    "Nursing Home": 20,
    "Lab": 18,
    "Urgent Care": 16,
    "Dental": 14,
    "Veterinary": 12,
    "Medical Practice": 10,
    "Other": 5,
}

# Distance-based proximity scores (out of 20)
PROXIMITY_THRESHOLDS = [
    (30,  20),   # <30 miles from Birmingham
    (60,  15),   # <60 miles
    (100, 10),   # <100 miles
    (150, 5),    # <150 miles
    (9999, 2),   # >150 miles
]

# Source confidence scores (out of 10)
SOURCE_SCORES = {
    "npi_and_adph": 10,
    "adph_only": 7,
    "npi_only": 5,
    "cms_only": 4,
    "unknown": 3,
}

# Maximum daily waste for normalization (lbs/day)
MAX_WASTE_FOR_SCORING = 5000  # Large hospital ~5000 lbs/day

# Tier thresholds
TIER_THRESHOLDS = [
    (80, "Hot"),
    (50, "Warm"),
    (25, "Cool"),
    (0,  "Cold"),
]


def score_waste_volume(lead):
    """Score waste volume potential (0-35 scale)."""
    waste = lead.get("estimated_waste_lbs_per_day")
    if not waste or waste <= 0:
        return 5  # Baseline for unknown waste volume

    # Normalize to 0-35 scale with diminishing returns
    normalized = min(waste / MAX_WASTE_FOR_SCORING, 1.0)
    # Use square root for diminishing returns (big hospitals don't dominate too much)
    return round(normalized ** 0.5 * 35, 1)


def score_facility_type(lead):
    """Score facility type priority (0-25 scale)."""
    facility_type = lead.get("facility_type", "Other")
    return FACILITY_TYPE_SCORES.get(facility_type, 5)


def score_proximity(lead):
    """Score geographic proximity to Birmingham (0-20 scale)."""
    distance = lead.get("distance_from_birmingham")
    if distance is None:
        return 8  # Midpoint for unknown distance

    for threshold, score in PROXIMITY_THRESHOLDS:
        if distance <= threshold:
            return score
    return 2


def score_completeness(lead):
    """Score data completeness (0-10 scale)."""
    completeness = lead.get("completeness_score", 0)
    return round(completeness * 10, 1)


def score_source_confidence(lead):
    """Score source confidence (0-10 scale)."""
    sources = lead.get("sources", [])
    if not sources:
        # Infer from available fields
        has_npi = bool(lead.get("npi_number"))
        has_adph = bool(lead.get("license_number") or lead.get("administrator"))
        if has_npi and has_adph:
            return SOURCE_SCORES["npi_and_adph"]
        elif has_adph:
            return SOURCE_SCORES["adph_only"]
        elif has_npi:
            return SOURCE_SCORES["npi_only"]
        return SOURCE_SCORES["unknown"]

    source_types = set(s.get("source", "") for s in sources)
    if "npi" in source_types and "adph" in source_types:
        return SOURCE_SCORES["npi_and_adph"]
    elif "adph" in source_types:
        return SOURCE_SCORES["adph_only"]
    elif "npi" in source_types:
        return SOURCE_SCORES["npi_only"]
    elif "cms" in source_types:
        return SOURCE_SCORES["cms_only"]
    return SOURCE_SCORES["unknown"]


def get_tier(score):
    """Map a numeric score to a priority tier."""
    for threshold, tier in TIER_THRESHOLDS:
        if score >= threshold:
            return tier
    return "Cold"


def score_lead(lead):
    """Score a single lead and return the score breakdown."""
    waste_score = score_waste_volume(lead)
    type_score = score_facility_type(lead)
    proximity_score = score_proximity(lead)
    completeness_score = score_completeness(lead)
    source_score = score_source_confidence(lead)

    total = round(waste_score + type_score + proximity_score +
                  completeness_score + source_score)
    total = min(total, 100)

    tier = get_tier(total)

    breakdown = {
        "waste_volume": waste_score,
        "facility_type": type_score,
        "proximity": proximity_score,
        "completeness": completeness_score,
        "source_confidence": source_score,
    }

    return total, tier, breakdown


def score_all(leads):
    """Score all leads. Returns scored leads and summary stats."""
    print("Harvest Med Waste — Lead Scoring Engine")
    print(f"  Leads to score: {len(leads)}")
    print()

    tier_counts = {"Hot": 0, "Warm": 0, "Cool": 0, "Cold": 0}

    for lead in leads:
        total, tier, breakdown = score_lead(lead)
        lead["lead_score"] = total
        lead["priority_tier"] = tier
        lead["score_breakdown"] = breakdown
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

    # Sort by score descending
    leads.sort(key=lambda l: l.get("lead_score", 0), reverse=True)

    # Print summary
    print("--- Scoring Summary ---")
    for tier in ["Hot", "Warm", "Cool", "Cold"]:
        count = tier_counts[tier]
        pct = (count / len(leads) * 100) if leads else 0
        print(f"  {tier}: {count} ({pct:.1f}%)")
    print()

    # Top 10 leads
    print("Top 10 Leads:")
    for lead in leads[:10]:
        print(f"  [{lead['lead_score']}] {lead.get('priority_tier', '?')} — "
              f"{lead.get('facility_name', '?')} ({lead.get('city', '?')})")

    return leads, tier_counts


def score_from_json():
    """Load leads from JSON, score, and save."""
    input_file = os.path.join(PROJECT_ROOT, ".tmp", "enriched_leads.json")
    if not os.path.exists(input_file):
        input_file = os.path.join(PROJECT_ROOT, "data", "alabama_leads.json")

    if not os.path.exists(input_file):
        print("ERROR: No input file found.")
        sys.exit(1)

    print(f"Loading from {input_file}...")
    with open(input_file) as f:
        leads = json.load(f)

    leads, tier_counts = score_all(leads)

    output_file = os.path.join(PROJECT_ROOT, ".tmp", "scored_leads.json")
    with open(output_file, "w") as f:
        json.dump(leads, f, indent=2)
    print(f"\nSaved {len(leads)} scored leads to {output_file}")

    return leads


def score_from_db():
    """Load leads from database, score, and update."""
    from tools.db import fetch_all, get_cursor, record_score_history

    rows = fetch_all("""
        SELECT id, lead_uid, facility_name, facility_type, city, zip5,
               npi_number, license_number, administrator,
               bed_count, estimated_waste_lbs_per_day,
               distance_from_birmingham, completeness_score
        FROM leads
    """)

    if not rows:
        print("No leads in database.")
        return []

    # Get source data for confidence scoring
    source_data = fetch_all("""
        SELECT lead_id, source, source_id FROM lead_sources
    """)
    lead_sources = {}
    for s in source_data:
        if s["lead_id"] not in lead_sources:
            lead_sources[s["lead_id"]] = []
        lead_sources[s["lead_id"]].append(s)

    for row in rows:
        row["sources"] = lead_sources.get(row["id"], [])

    leads, tier_counts = score_all(rows)

    # Update database
    print("Saving scores to database...")
    with get_cursor() as cur:
        for lead in leads:
            cur.execute("""
                UPDATE leads SET
                    lead_score = %s,
                    priority_tier = %s,
                    last_updated = NOW()
                WHERE id = %s
            """, (lead["lead_score"], lead["priority_tier"], lead["id"]))

            # Record score history
            record_score_history(
                lead["id"],
                lead["lead_score"],
                lead["priority_tier"],
                lead.get("score_breakdown", {}),
            )
    print("  Database updated")

    return leads


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Score leads")
    parser.add_argument("--json", action="store_true", help="Read from JSON files")
    args = parser.parse_args()

    if args.json:
        score_from_json()
    else:
        try:
            score_from_db()
        except Exception as e:
            print(f"DB error: {e}")
            print("Falling back to JSON mode...")
            score_from_json()
