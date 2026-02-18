"""
cms_bed_count.py — Enrich leads with CMS Provider of Services bed count data.

Matches leads against CMS POS data by facility name and address to add
bed counts, hospital type, and ownership information.
"""

import json
import os
from tools.enrichment_plugins.base import EnrichmentPlugin
from tools.normalize import normalize_name, normalize_address

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CMS_FILE = os.path.join(PROJECT_ROOT, ".tmp", "cms_pos_alabama.json")

# Facility types that never have beds — set explicitly to 0
NO_BED_TYPES = {
    "Dental", "Veterinary", "Urgent Care", "Pharmacy",
    "Tattoo", "Funeral Home", "Medical Practice", "Lab",
}


class CMSBedCountEnricher(EnrichmentPlugin):
    name = "cms_bed_count"
    description = "Add bed counts and hospital classification from CMS POS data"

    def __init__(self):
        self._cms_data = None
        self._name_index = {}
        self._address_index = {}
        self._loaded = False

    def _load(self):
        """Load CMS POS data into memory and build lookup indexes."""
        if self._loaded:
            return

        self._loaded = True

        # Try database first
        try:
            from tools.db import fetch_all
            rows = fetch_all("SELECT provider_id, raw_data FROM staging_cms")
            if rows:
                self._cms_data = []
                for row in rows:
                    data = row["raw_data"] if isinstance(row["raw_data"], dict) else json.loads(row["raw_data"])
                    self._cms_data.append(data)
        except Exception:
            pass

        # Fall back to JSON file
        if not self._cms_data and os.path.exists(CMS_FILE):
            with open(CMS_FILE) as f:
                self._cms_data = json.load(f)

        if not self._cms_data:
            self._cms_data = []
            return

        # Build name and address indexes for fast lookups
        for rec in self._cms_data:
            name = normalize_name(rec.get("facility_name", ""))
            if name:
                self._name_index[name] = rec

            # Build address index: normalized "address|city"
            addr = normalize_address(rec.get("address", ""))
            city = (rec.get("city", "") or "").upper().strip()
            if addr and city:
                key = f"{addr}|{city}"
                self._address_index[key] = rec

    def can_enrich(self, lead: dict) -> bool:
        self._load()
        facility_type = lead.get("facility_type", "")

        # Non-bed facility types: set bed_count=0, no CMS lookup needed
        if facility_type in NO_BED_TYPES:
            return not lead.get("bed_count") and lead.get("bed_count") != 0

        # Only query CMS for Hospital, Nursing Home, Surgery Center
        if facility_type not in ("Hospital", "Nursing Home", "Surgery Center"):
            return False

        return bool(self._cms_data)

    def enrich(self, lead: dict) -> dict:
        self._load()
        facility_type = lead.get("facility_type", "")

        # Explicitly set 0 beds for non-bed facility types
        if facility_type in NO_BED_TYPES:
            return {"bed_count": 0}

        if not self._cms_data:
            return {}

        # Already has bed count from dedup merge
        if lead.get("bed_count"):
            return {}

        cms_rec = None
        match_confidence = 0.0

        # Try exact name match first
        lead_name = normalize_name(lead.get("facility_name", ""))
        if lead_name and lead_name in self._name_index:
            cms_rec = self._name_index[lead_name]
            match_confidence = 1.0

        # Try partial name matching
        if not cms_rec and lead_name:
            for name, rec in self._name_index.items():
                if name and (lead_name in name or name in lead_name):
                    cms_rec = rec
                    match_confidence = 0.8
                    break

        # Fallback: address-based matching
        if not cms_rec:
            lead_addr = normalize_address(lead.get("address_line1", ""))
            lead_city = (lead.get("city", "") or "").upper().strip()
            if lead_addr and lead_city:
                key = f"{lead_addr}|{lead_city}"
                if key in self._address_index:
                    cms_rec = self._address_index[key]
                    match_confidence = 0.7

        if not cms_rec:
            return {}

        result = {"_cms_match_confidence": match_confidence}
        if cms_rec.get("bed_count"):
            result["bed_count"] = cms_rec["bed_count"]
        if cms_rec.get("hospital_type"):
            result["hospital_type"] = cms_rec["hospital_type"]
        if cms_rec.get("ownership_type"):
            result["ownership_type"] = cms_rec["ownership_type"]

        return result
