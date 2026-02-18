"""
data_completeness.py — Score how complete a lead's data is.

Checks which fields are populated and returns a completeness score
between 0.0 (no data) and 1.0 (fully populated).
"""

from tools.enrichment_plugins.base import EnrichmentPlugin

# Fields and their weights for completeness scoring
SCORED_FIELDS = {
    "facility_name": 1.0,
    "facility_type": 0.5,
    "address_line1": 1.0,
    "city": 0.8,
    "zip5": 0.5,
    "county": 0.3,
    "phone": 1.0,
    "fax": 0.2,
    "administrator": 0.8,
    "npi_number": 0.5,
    "license_number": 0.3,
    "taxonomy_code": 0.3,
    "bed_count": 0.4,
}


class DataCompletenessScorer(EnrichmentPlugin):
    name = "data_completeness"
    description = "Score lead data completeness (0-1)"

    def can_enrich(self, lead: dict) -> bool:
        return True

    def enrich(self, lead: dict) -> dict:
        total_weight = sum(SCORED_FIELDS.values())
        filled_weight = 0.0

        for field, weight in SCORED_FIELDS.items():
            value = lead.get(field)
            if value and str(value).strip():
                filled_weight += weight

        score = round(filled_weight / total_weight, 2) if total_weight > 0 else 0

        return {
            "completeness_score": score,
        }
