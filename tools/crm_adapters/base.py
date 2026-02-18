"""
base.py — CRM adapter interface.

All CRM adapters must inherit from CRMAdapter and implement the
search, create, update, and list methods.
"""

from abc import ABC, abstractmethod


class CRMAdapter(ABC):
    """Base class for CRM adapters."""

    name: str = "base"

    @abstractmethod
    def search_contact(self, facility_name: str, phone: str) -> dict | None:
        """Search CRM for an existing contact. Returns contact dict or None."""
        pass

    @abstractmethod
    def create_lead(self, lead: dict) -> str:
        """Create a new lead in the CRM. Returns the CRM lead ID."""
        pass

    @abstractmethod
    def update_lead(self, crm_id: str, data: dict) -> bool:
        """Update an existing CRM lead. Returns True on success."""
        pass

    @abstractmethod
    def get_all_leads(self) -> list[dict]:
        """Get all leads from the CRM for dedup checking."""
        pass

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.name}>"
