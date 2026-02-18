"""
score_leads.py — Weighted lead scoring model.

Scores leads on a 0-100 scale based on:
  - Waste volume potential (35%)
  - Distance from Birmingham (25%)
  - Facility type priority (20%)
  - Contract expiry proximity (10%)
  - Facility age / NPI enumeration date (10%)

Priority tiers:
  - Hot (75-100): Large facilities, close to Birmingham, high waste volume
  - Warm (50-74): Mid-size facilities or further distance
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
from datetime import datetime, date

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# Facility type priority scores (out of 20)
FACILITY_TYPE_SCORES = {
    "Hospital": 20,
    "Surgery Center": 18,
    "Nursing Home": 16,
    "Lab": 14,
    "Urgent Care": 12,
    "Dialysis": 11,
    "Dental": 10,
    "Veterinary": 8,
    "Podiatry": 7,
    "Medical Practice": 6,
    "Medical Spa": 5,
    "Pharmacy": 4,
    "Funeral Home": 3,
    "Tattoo": 2,
    "Other": 1,
}

# Distance-based proximity scores (out of 25)
PROXIMITY_THRESHOLDS = [
    (30,  25),   # <30 miles from Birmingham
    (60,  20),   # <60 miles
    (100, 15),   # <100 miles
    (150, 8),    # <150 miles
    (9999, 2),   # >150 miles
]

# Maximum daily waste for normalization (lbs/day)
MAX_WASTE_FOR_SCORING = 5000  # Large hospital ~5000 lbs/day

# Tier thresholds
TIER_THRESHOLDS = [
    (75, "Hot"),
    (50, "Warm"),
    (25, "Cool"),
    (0,  "Cold"),
]


def score_waste_volume(lead):
    """Score waste volume potential (0-35 scale)."""
    waste = lead.get("estimated_waste_lbs_per_day")
    if not waste or waste <= 0:
        return 5  # Baseline for unknown waste volume

    # Normalize to 0-35 scale with diminishing returns (square root)
    normalized = min(waste / MAX_WASTE_FOR_SCORING, 1.0)
    return round(normalized ** 0.5 * 35, 1)


def score_facility_type(lead):
    """Score facility type priority (0-20 scale)."""
    facility_type = lead.get("facility_type", "Other")
    return FACILITY_TYPE_SCORES.get(facility_type, 1)


def score_proximity(lead):
    """Score geographic proximity to Birmingham (0-25 scale)."""
    distance = lead.get("distance_from_birmingham")
    if distance is None:
        return 10  # Midpoint for unknown distance

    for threshold, score in PROXIMITY_THRESHOLDS:
        if distance <= threshold:
            return score
    return 2


def score_contract_expiry(lead):
    """Score contract expiry proximity (0-10 scale).

    Known expiring within 6mo = 10
    Known expiring within 1yr = 7
    Unknown = 5 (neutral)
    Locked >1yr = 2
    """
    expiry_str = lead.get("contract_expiry_date")
    if not expiry_str:
        return 5  # Neutral for unknown

    try:
        if isinstance(expiry_str, date):
            expiry = expiry_str
        else:
            expiry = datetime.strptime(str(expiry_str)[:10], "%Y-%m-%d").date()

        days_until = (expiry - date.today()).days

        if days_until <= 0:
            return 10  # Already expired — hot opportunity
        elif days_until <= 180:
            return 10  # Expiring within 6 months
        elif days_until <= 365:
            return 7   # Expiring within 1 year
        else:
            return 2   # Locked >1 year
    except (ValueError, TypeError):
        return 5  # Can't parse — treat as unknown


def score_facility_age(lead):
    """Score facility age from NPI enumeration date (0-10 scale).

    Newer facilities are higher priority — they may not have
    established waste disposal contracts yet.
    <2 years = 10, <5 years = 8, <10 years = 5, >10 years = 3
    """
    est_str = lead.get("facility_established_date")
    if not est_str:
        return 5  # Neutral for unknown

    try:
        if isinstance(est_str, date):
            est_date = est_str
        else:
            est_date = datetime.strptime(str(est_str)[:10], "%Y-%m-%d").date()

        years = (date.today() - est_date).days / 365.25

        if years < 2:
            return 10
        elif years < 5:
            return 8
        elif years < 10:
            return 5
        else:
            return 3
    except (ValueError, TypeError):
        return 5  # Can't parse — treat as unknown


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
    contract_score = score_contract_expiry(lead)
    age_score = score_facility_age(lead)

    total = round(waste_score + type_score + proximity_score +
                  contract_score + age_score)
    total = min(total, 100)

    tier = get_tier(total)

    breakdown = {
        "waste_volume": waste_score,
        "facility_type": type_score,
        "proximity": proximity_score,
        "contract_expiry": contract_score,
        "facility_age": age_score,
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
               distance_from_birmingham, completeness_score,
               facility_established_date, contract_expiry_date
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
