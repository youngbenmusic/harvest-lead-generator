"""
waste_volume.py — Estimate regulated medical waste (RMW) volume by facility type.

Uses industry-standard estimates for RMW generation rates. Hospitals and
nursing homes are per-bed calculations; all others are flat weekly estimates.
"""

from tools.enrichment_plugins.base import EnrichmentPlugin

# RMW volume estimates
# For per-bed types: lbs RMW per bed per day
# For flat types: lbs RMW per week
WASTE_ESTIMATES = {
    "Hospital": {
        "per_bed": True,
        "daily_per_bed": 4.5,     # 30 lbs/bed/day total × 15% RMW
        "default_beds": 150,      # Average Alabama hospital if bed count unknown
    },
    "Nursing Home": {
        "per_bed": True,
        "daily_per_bed": 0.75,    # 5 lbs/bed/day total × 15% RMW
        "default_beds": 100,
    },
    "Surgery Center": {
        "per_bed": False,
        "weekly_lbs": 35,
    },
    "Dental": {
        "per_bed": False,
        "weekly_lbs": 2,          # Range 1-3
    },
    "Veterinary": {
        "per_bed": False,
        "weekly_lbs": 10,         # Range 5-15
    },
    "Urgent Care": {
        "per_bed": False,
        "weekly_lbs": 20,         # Range 10-30
    },
    "Lab": {
        "per_bed": False,
        "weekly_lbs": 20,
    },
    "Medical Practice": {
        "per_bed": False,
        "weekly_lbs": 5,
    },
    "Dialysis": {
        "per_bed": False,
        "weekly_lbs": 200,        # 20 lbs/station/week × default 10 stations
    },
    "Podiatry": {
        "per_bed": False,
        "weekly_lbs": 4,          # Sharps, pathological tissue, range 2-6
    },
    "Medical Spa": {
        "per_bed": False,
        "weekly_lbs": 3,          # Sharps (injectables), range 2-5
    },
    "Pharmacy": {
        "per_bed": False,
        "weekly_lbs": 3.5,        # Range 2-5
    },
    "Funeral Home": {
        "per_bed": False,
        "weekly_lbs": 7.5,        # Range 5-10
    },
    "Tattoo": {
        "per_bed": False,
        "weekly_lbs": 1.5,        # Range 1-2
    },
    "Other": {
        "per_bed": False,
        "weekly_lbs": 2,
    },
}

# Waste tier thresholds (lbs RMW per week)
TIER_THRESHOLDS = [
    (50,  "High"),       # >50 lbs/week RMW
    (10,  "Medium"),     # 10-50 lbs/week
    (2,   "Low"),        # 2-10 lbs/week
    (0,   "Minimal"),    # <2 lbs/week
]


class WasteVolumeEstimator(EnrichmentPlugin):
    name = "waste_volume"
    description = "Estimate daily and monthly regulated medical waste volume"

    def can_enrich(self, lead: dict) -> bool:
        return bool(lead.get("facility_type"))

    def enrich(self, lead: dict) -> dict:
        facility_type = lead.get("facility_type", "Other")
        config = WASTE_ESTIMATES.get(facility_type, WASTE_ESTIMATES["Other"])

        bed_count = lead.get("bed_count")

        if config["per_bed"]:
            # Per-bed calculation (Hospital, Nursing Home)
            beds = bed_count if bed_count and bed_count > 0 else config["default_beds"]
            daily_lbs = config["daily_per_bed"] * beds
        else:
            # Flat weekly estimate → convert to daily
            daily_lbs = config["weekly_lbs"] / 7

        monthly_volume = daily_lbs * 30
        weekly_lbs = daily_lbs * 7

        # Determine waste tier based on weekly volume
        waste_tier = "Minimal"
        for threshold, tier in TIER_THRESHOLDS:
            if weekly_lbs >= threshold:
                waste_tier = tier
                break

        return {
            "estimated_waste_lbs_per_day": round(daily_lbs, 2),
            "estimated_monthly_volume": round(monthly_volume, 2),
            "waste_tier": waste_tier,
        }
