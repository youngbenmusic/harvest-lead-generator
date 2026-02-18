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


class CMSBedCountEnricher(EnrichmentPlugin):
    name = "cms_bed_count"
    description = "Add bed counts and hospital classification from CMS POS data"

    def __init__(self):
        self._cms_data = None
        self._name_index = {}
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

        # Build name index for fast lookups
        for rec in self._cms_data:
            name = normalize_name(rec.get("facility_name", ""))
            if name:
                self._name_index[name] = rec

    def can_enrich(self, lead: dict) -> bool:
        self._load()
        if not self._cms_data:
            return False
        # Only try for hospitals and larger facilities
        facility_type = lead.get("facility_type", "")
        return facility_type in ("Hospital", "Nursing Home", "Surgery Center")

    def enrich(self, lead: dict) -> dict:
        self._load()
        if not self._cms_data:
            return {}

        # Already has bed count from dedup merge
        if lead.get("bed_count"):
            return {}

        # Try matching by normalized name
        lead_name = normalize_name(lead.get("facility_name", ""))
        cms_rec = self._name_index.get(lead_name)

        if not cms_rec:
            # Try partial name matching
            for name, rec in self._name_index.items():
                if lead_name and name and (lead_name in name or name in lead_name):
                    cms_rec = rec
                    break

        if not cms_rec:
            return {}

        result = {}
        if cms_rec.get("bed_count"):
            result["bed_count"] = cms_rec["bed_count"]
        if cms_rec.get("hospital_type"):
            result["hospital_type"] = cms_rec["hospital_type"]
        if cms_rec.get("ownership_type"):
            result["ownership_type"] = cms_rec["ownership_type"]

        return result
