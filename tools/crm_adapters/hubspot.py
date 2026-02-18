"""
hubspot.py — HubSpot CRM adapter.

Uses HubSpot CRM API v3 to sync leads.
Requires HUBSPOT_API_KEY in .env.
"""

import os
import json

try:
    import requests
except ImportError:
    requests = None

from tools.crm_adapters.base import CRMAdapter

HUBSPOT_API_BASE = "https://api.hubapi.com"


class HubSpotAdapter(CRMAdapter):
    name = "hubspot"

    def __init__(self):
        self.api_key = os.environ.get("HUBSPOT_API_KEY", "")
        if not self.api_key:
            raise ValueError("HUBSPOT_API_KEY not set in environment")
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _request(self, method, path, data=None):
        if not requests:
            raise ImportError("requests package required for HubSpot adapter")
        url = f"{HUBSPOT_API_BASE}{path}"
        resp = requests.request(method, url, headers=self.headers, json=data, timeout=30)
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    def search_contact(self, facility_name: str, phone: str) -> dict | None:
        filters = []
        if phone:
            filters.append({
                "propertyName": "phone",
                "operator": "EQ",
                "value": phone,
            })
        if not filters and facility_name:
            filters.append({
                "propertyName": "company",
                "operator": "CONTAINS_TOKEN",
                "value": facility_name,
            })

        if not filters:
            return None

        data = {
            "filterGroups": [{"filters": filters}],
            "properties": ["company", "phone", "email", "hs_lead_status"],
            "limit": 1,
        }
        result = self._request("POST", "/crm/v3/objects/contacts/search", data)
        results = result.get("results", [])
        if results:
            return results[0]
        return None

    def create_lead(self, lead: dict) -> str:
        properties = {
            "company": lead.get("facility_name", lead.get("name", "")),
            "phone": lead.get("phone", ""),
            "address": lead.get("address_line1", lead.get("address", "")),
            "city": lead.get("city", ""),
            "state": lead.get("state", "AL"),
            "zip": lead.get("zip5", lead.get("zip", "")),
            "hs_lead_status": "NEW_LEAD",
        }
        # Add custom properties if they exist in HubSpot
        if lead.get("facility_type"):
            properties["industry"] = lead["facility_type"]
        if lead.get("npi_number"):
            properties["npi_number"] = lead["npi_number"]

        data = {"properties": properties}
        result = self._request("POST", "/crm/v3/objects/contacts", data)
        return result.get("id", "")

    def update_lead(self, crm_id: str, data: dict) -> bool:
        properties = {}
        field_map = {
            "phone": "phone",
            "facility_name": "company",
            "city": "city",
            "state": "state",
        }
        for our_field, hs_field in field_map.items():
            if our_field in data:
                properties[hs_field] = data[our_field]

        if not properties:
            return True

        self._request("PATCH", f"/crm/v3/objects/contacts/{crm_id}", {"properties": properties})
        return True

    def get_all_leads(self) -> list[dict]:
        results = []
        after = None

        while True:
            params = "?limit=100&properties=company,phone,email,hs_lead_status"
            if after:
                params += f"&after={after}"
            data = self._request("GET", f"/crm/v3/objects/contacts{params}")
            results.extend(data.get("results", []))

            paging = data.get("paging", {})
            next_page = paging.get("next", {})
            after = next_page.get("after")
            if not after:
                break

        return results
