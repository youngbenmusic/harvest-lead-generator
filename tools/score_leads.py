"""
score_leads.py — Weighted lead scoring model.

Scores leads on a 0-100 scale based on:
  - Facility type priority (30%)
  - Waste volume potential (25%)
  - Distance from Birmingham (15%)
  - Data confidence (15%)
  - Opportunity window (15%)

Tier assignment uses percentile cutoffs to guarantee a usable distribution:
  - Hot:  top 12%
  - Warm: next 28%
  - Cool: next 35%
  - Cold: bottom 25%

Usage:
    python tools/score_leads.py
    python tools/score_leads.py --json    # Score from JSON files
"""

import json
import math
import os
import sys
import argparse
from datetime import datetime, date

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from tools.enrichment_plugins.geo_distance import AL_ZIP_CENTROIDS, haversine, BIRMINGHAM_LAT, BIRMINGHAM_LON

# Facility type priority scores (out of 30)
FACILITY_TYPE_SCORES = {
    "Hospital": 30,
    "Surgery Center": 27,
    "Dialysis": 23,
    "Nursing Home": 20,
    "Lab": 17,
    "Urgent Care": 15,
    "Dental": 12,
    "Veterinary": 10,
    "Podiatry": 8,
    "Medical Practice": 7,
    "Medical Spa": 6,
    "Pharmacy": 4,
    "Funeral Home": 3,
    "Tattoo": 2,
    "Other": 1,
}

# Distance-based proximity scores (out of 15)
PROXIMITY_THRESHOLDS = [
    (30,  15),   # <30 miles from Birmingham
    (60,  12),   # <60 miles
    (100, 9),    # <100 miles
    (150, 5),    # <150 miles
    (9999, 1),   # >150 miles
]

# Maximum daily waste for normalization (lbs/day)
MAX_WASTE_FOR_SCORING = 5000  # Large hospital ~5000 lbs/day


def _zip_to_distance(zip5):
    """Compute distance from Birmingham using ZIP centroid table.

    Returns distance in miles, or None if ZIP prefix not found.
    """
    if not zip5 or len(zip5) < 3:
        return None
    prefix = zip5[:3]
    coords = AL_ZIP_CENTROIDS.get(prefix)
    if coords is None:
        return None
    return haversine(BIRMINGHAM_LAT, BIRMINGHAM_LON, coords[0], coords[1])


def score_waste_volume(lead):
    """Score waste volume potential (0-25 scale, log-scaled)."""
    waste = lead.get("estimated_waste_lbs_per_day")
    if not waste or waste <= 0:
        return 3  # Baseline for unknown waste volume

    # Log scale: log(1+waste) / log(1+MAX) * 25
    score = math.log1p(waste) / math.log1p(MAX_WASTE_FOR_SCORING) * 25
    return round(min(score, 25), 1)


def score_facility_type(lead):
    """Score facility type priority (0-30 scale)."""
    facility_type = lead.get("facility_type", "Other")
    return FACILITY_TYPE_SCORES.get(facility_type, 1)


def score_proximity(lead):
    """Score geographic proximity to Birmingham (0-15 scale).

    Uses actual distance if available, otherwise computes from ZIP centroid.
    """
    distance = lead.get("distance_from_birmingham")

    # ZIP centroid fallback
    if distance is None:
        zip5 = lead.get("zip5") or lead.get("zip") or ""
        distance = _zip_to_distance(zip5)

    if distance is None:
        return 5  # True unknown — no ZIP match

    for threshold, score in PROXIMITY_THRESHOLDS:
        if distance <= threshold:
            return score
    return 1


def score_opportunity(lead):
    """Score opportunity window (0-15 scale).

    Merges contract expiry proximity and facility age into one component.
    Contract expiry: max 8 points. Facility age: max 7 points.
    """
    # --- Contract expiry (0-8) ---
    contract_score = 4  # Neutral default for unknown
    expiry_str = lead.get("contract_expiry_date")
    if expiry_str:
        try:
            if isinstance(expiry_str, date) and not isinstance(expiry_str, datetime):
                expiry = expiry_str
            else:
                expiry = datetime.strptime(str(expiry_str)[:10], "%Y-%m-%d").date()

            days_until = (expiry - date.today()).days
            if days_until <= 0:
                contract_score = 8   # Already expired
            elif days_until <= 180:
                contract_score = 8   # Expiring within 6 months
            elif days_until <= 365:
                contract_score = 5   # Expiring within 1 year
            else:
                contract_score = 1   # Locked >1 year
        except (ValueError, TypeError):
            contract_score = 4

    # --- Facility age (0-7) ---
    age_score = 3  # Neutral default for unknown
    est_str = lead.get("facility_established_date")
    if est_str:
        try:
            if isinstance(est_str, date) and not isinstance(est_str, datetime):
                est_date = est_str
            else:
                est_date = datetime.strptime(str(est_str)[:10], "%Y-%m-%d").date()

            years = (date.today() - est_date).days / 365.25
            if years < 1:
                age_score = 7
            elif years < 2:
                age_score = 6
            elif years < 3:
                age_score = 5
            elif years < 5:
                age_score = 4
            elif years < 8:
                age_score = 3
            elif years < 12:
                age_score = 2
            elif years < 20:
                age_score = 1
            else:
                age_score = 0
        except (ValueError, TypeError):
            age_score = 3

    return min(contract_score + age_score, 15)


def score_data_confidence(lead):
    """Score data confidence (0-15 scale).

    NPI-2 organizations: +7 over NPI-1 individuals.
    Completeness score: 0-5 points.
    Multi-source leads: 0-3 points.
    """
    score = 0

    # Entity type: NPI-2 (org) vs NPI-1 (individual)
    entity = lead.get("entity_type", "")
    if entity == "NPI-2":
        score += 7
    elif entity == "NPI-1":
        score += 0
    else:
        score += 3  # Unknown entity type

    # Completeness score (0.0-1.0 mapped to 0-5)
    completeness = lead.get("completeness_score")
    if completeness is not None:
        score += round(completeness * 5, 1)
    else:
        score += 2  # Neutral

    # Multi-source bonus (0-3)
    sources = lead.get("sources", [])
    num_sources = len(sources) if isinstance(sources, list) else 0
    if num_sources >= 3:
        score += 3
    elif num_sources == 2:
        score += 2
    elif num_sources == 1:
        score += 0
    else:
        score += 0

    return min(round(score, 1), 15)


def assign_tiers(leads):
    """Assign tiers based on percentile cutoffs.

    Hot:  top 12%
    Warm: next 28%  (top 12-40%)
    Cool: next 35%  (top 40-75%)
    Cold: bottom 25%
    """
    if not leads:
        return

    # Sort by score descending to determine percentile cutoffs
    scores = sorted([l.get("lead_score", 0) for l in leads], reverse=True)
    n = len(scores)

    hot_idx = max(0, int(n * 0.12) - 1)
    warm_idx = max(0, int(n * 0.40) - 1)
    cool_idx = max(0, int(n * 0.75) - 1)

    hot_cutoff = scores[hot_idx]
    warm_cutoff = scores[warm_idx]
    cool_cutoff = scores[cool_idx]

    for lead in leads:
        s = lead.get("lead_score", 0)
        if s >= hot_cutoff:
            lead["priority_tier"] = "Hot"
        elif s >= warm_cutoff:
            lead["priority_tier"] = "Warm"
        elif s >= cool_cutoff:
            lead["priority_tier"] = "Cool"
        else:
            lead["priority_tier"] = "Cold"


def score_lead(lead):
    """Score a single lead and return the score breakdown."""
    waste_score = score_waste_volume(lead)
    type_score = score_facility_type(lead)
    proximity_score = score_proximity(lead)
    opportunity_score = score_opportunity(lead)
    confidence_score = score_data_confidence(lead)

    total = round(waste_score + type_score + proximity_score +
                  opportunity_score + confidence_score)
    total = min(total, 100)

    breakdown = {
        "waste_volume": waste_score,
        "facility_type": type_score,
        "proximity": proximity_score,
        "opportunity": opportunity_score,
        "data_confidence": confidence_score,
    }

    return total, breakdown


def score_all(leads):
    """Score all leads. Returns scored leads and summary stats."""
    print("Harvest Med Waste — Lead Scoring Engine")
    print(f"  Leads to score: {len(leads)}")
    print()

    for lead in leads:
        total, breakdown = score_lead(lead)
        lead["lead_score"] = total
        lead["score_breakdown"] = breakdown

    # Assign tiers using percentile cutoffs
    assign_tiers(leads)

    # Sort by score descending
    leads.sort(key=lambda l: l.get("lead_score", 0), reverse=True)

    # Count tiers
    tier_counts = {"Hot": 0, "Warm": 0, "Cool": 0, "Cold": 0}
    for lead in leads:
        tier = lead.get("priority_tier", "Cold")
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

    # Print summary
    print("--- Scoring Summary ---")
    for tier in ["Hot", "Warm", "Cool", "Cold"]:
        count = tier_counts[tier]
        pct = (count / len(leads) * 100) if leads else 0
        print(f"  {tier}: {count} ({pct:.1f}%)")

    # Score range
    if leads:
        scores = [l["lead_score"] for l in leads]
        print(f"\n  Score range: {min(scores)} — {max(scores)}")
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
               npi_number, license_number, administrator, entity_type,
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
