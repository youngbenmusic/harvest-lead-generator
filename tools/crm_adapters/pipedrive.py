"""
pipedrive.py — Pipedrive CRM adapter.

Uses Pipedrive REST API v1 to sync leads.
Requires PIPEDRIVE_API_KEY and PIPEDRIVE_DOMAIN in .env.
"""

import os

try:
    import requests
except ImportError:
    requests = None

from tools.crm_adapters.base import CRMAdapter


class PipedriveAdapter(CRMAdapter):
    name = "pipedrive"

    def __init__(self):
        self.api_key = os.environ.get("PIPEDRIVE_API_KEY", "")
        self.domain = os.environ.get("PIPEDRIVE_DOMAIN", "")
        if not self.api_key or not self.domain:
            raise ValueError("PIPEDRIVE_API_KEY and PIPEDRIVE_DOMAIN must be set")
        self.base_url = f"https://{self.domain}.pipedrive.com/api/v1"

    def _request(self, method, path, data=None, params=None):
        if not requests:
            raise ImportError("requests package required for Pipedrive adapter")
        url = f"{self.base_url}{path}"
        if params is None:
            params = {}
        params["api_token"] = self.api_key
        resp = requests.request(method, url, params=params, json=data, timeout=30)
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    def search_contact(self, facility_name: str, phone: str) -> dict | None:
        term = phone or facility_name
        if not term:
            return None

        result = self._request("GET", "/persons/search", params={"term": term, "limit": 1})
        items = result.get("data", {}).get("items", [])
        if items:
            return items[0].get("item", {})
        return None

    def create_lead(self, lead: dict) -> str:
        # Create organization first
        org_data = {
            "name": lead.get("facility_name", lead.get("name", "")),
            "address": ", ".join(filter(None, [
                lead.get("address_line1", lead.get("address", "")),
                lead.get("city", ""),
                lead.get("state", "AL"),
                lead.get("zip5", lead.get("zip", "")),
            ])),
        }
        org_result = self._request("POST", "/organizations", data=org_data)
        org_id = org_result.get("data", {}).get("id")

        # Create person (contact) linked to org
        person_data = {
            "name": lead.get("administrator", lead.get("facility_name", lead.get("name", ""))),
            "phone": [{"value": lead.get("phone", ""), "primary": True}],
            "org_id": org_id,
        }
        person_result = self._request("POST", "/persons", data=person_data)
        person_id = str(person_result.get("data", {}).get("id", ""))

        # Create deal
        deal_data = {
            "title": f"Medical Waste — {lead.get('facility_name', lead.get('name', ''))}",
            "org_id": org_id,
            "person_id": person_id,
            "stage_id": 1,  # Default first stage
        }
        deal_result = self._request("POST", "/deals", data=deal_data)
        return str(deal_result.get("data", {}).get("id", person_id))

    def update_lead(self, crm_id: str, data: dict) -> bool:
        update_data = {}
        if "phone" in data:
            update_data["phone"] = [{"value": data["phone"], "primary": True}]
        if "facility_name" in data or "name" in data:
            update_data["name"] = data.get("facility_name", data.get("name", ""))

        if update_data:
            self._request("PUT", f"/persons/{crm_id}", data=update_data)
        return True

    def get_all_leads(self) -> list[dict]:
        results = []
        start = 0

        while True:
            data = self._request("GET", "/persons", params={"start": start, "limit": 100})
            items = data.get("data", [])
            if not items:
                break
            results.extend(items)

            pagination = data.get("additional_data", {}).get("pagination", {})
            if not pagination.get("more_items_in_collection"):
                break
            start += 100

        return results
