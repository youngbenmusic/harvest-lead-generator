"""
json_file.py — JSON file CRM adapter.

Exports qualified leads to a JSON file for the web dashboard.
This is the default adapter used when no CRM is configured.
"""

import json
import os
from tools.crm_adapters.base import CRMAdapter

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUTPUT_FILE = os.path.join(PROJECT_ROOT, "data", "alabama_leads.json")


class JSONFileAdapter(CRMAdapter):
    name = "json_file"

    def __init__(self, output_path=None):
        self.output_path = output_path or OUTPUT_FILE
        self._leads = None

    def _load(self):
        if self._leads is None:
            if os.path.exists(self.output_path):
                with open(self.output_path) as f:
                    self._leads = json.load(f)
            else:
                self._leads = []
        return self._leads

    def _save(self):
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        with open(self.output_path, "w") as f:
            json.dump(self._leads, f, indent=2)

    def search_contact(self, facility_name: str, phone: str) -> dict | None:
        leads = self._load()
        name_lower = facility_name.lower() if facility_name else ""
        for lead in leads:
            if phone and lead.get("phone") == phone:
                return lead
            if name_lower and lead.get("name", "").lower() == name_lower:
                return lead
        return None

    def create_lead(self, lead: dict) -> str:
        self._load()
        lead_id = lead.get("id", lead.get("lead_uid", ""))
        self._leads.append(lead)
        self._save()
        return lead_id

    def update_lead(self, crm_id: str, data: dict) -> bool:
        self._load()
        for lead in self._leads:
            if lead.get("id") == crm_id:
                lead.update(data)
                self._save()
                return True
        return False

    def get_all_leads(self) -> list[dict]:
        return self._load()
