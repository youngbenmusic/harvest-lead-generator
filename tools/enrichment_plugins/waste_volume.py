"""
waste_volume.py — Estimate medical waste volume based on facility type and size.

Uses industry-standard estimates for medical waste generation rates
per facility type, adjusted by bed count where available.
"""

from tools.enrichment_plugins.base import EnrichmentPlugin

# Waste volume estimates (lbs per day)
WASTE_ESTIMATES = {
    "Hospital":         {"base": 33, "per_bed": True,  "multiplier": 1.0},
    "Surgery Center":   {"base": 50, "per_bed": False, "multiplier": 1.0},
    "Nursing Home":     {"base": 5,  "per_bed": True,  "multiplier": 1.0},
    "Dental":           {"base": 8,  "per_bed": False, "multiplier": 1.0},
    "Urgent Care":      {"base": 15, "per_bed": False, "multiplier": 1.0},
    "Lab":              {"base": 25, "per_bed": False, "multiplier": 1.0},
    "Medical Practice": {"base": 5,  "per_bed": False, "multiplier": 1.0},
    "Veterinary":       {"base": 8,  "per_bed": False, "multiplier": 1.0},
    "Other":            {"base": 3,  "per_bed": False, "multiplier": 1.0},
}

# Waste tier thresholds (lbs/day)
TIER_THRESHOLDS = [
    (100, "High"),
    (30,  "Medium"),
    (10,  "Low"),
    (0,   "Minimal"),
]


class WasteVolumeEstimator(EnrichmentPlugin):
    name = "waste_volume"
    description = "Estimate daily and monthly medical waste volume"

    def can_enrich(self, lead: dict) -> bool:
        return bool(lead.get("facility_type"))

    def enrich(self, lead: dict) -> dict:
        facility_type = lead.get("facility_type", "Other")
        config = WASTE_ESTIMATES.get(facility_type, WASTE_ESTIMATES["Other"])

        bed_count = lead.get("bed_count")

        if config["per_bed"] and bed_count and bed_count > 0:
            daily_lbs = config["base"] * bed_count * config["multiplier"]
        else:
            daily_lbs = config["base"] * config["multiplier"]
            # If hospital without bed count, use average Alabama hospital size (~150 beds)
            if facility_type == "Hospital" and not bed_count:
                daily_lbs = config["base"] * 150

        monthly_volume = daily_lbs * 30

        # Determine waste tier
        waste_tier = "Minimal"
        for threshold, tier in TIER_THRESHOLDS:
            if daily_lbs >= threshold:
                waste_tier = tier
                break

        return {
            "estimated_waste_lbs_per_day": round(daily_lbs, 2),
            "estimated_monthly_volume": round(monthly_volume, 2),
            "waste_tier": waste_tier,
        }
