"""
base.py — Enrichment plugin interface.

All enrichment plugins must inherit from EnrichmentPlugin and implement
the can_enrich() and enrich() methods.
"""

from abc import ABC, abstractmethod


class EnrichmentPlugin(ABC):
    """Base class for enrichment plugins."""

    name: str = "base"
    description: str = "Base enrichment plugin"

    @abstractmethod
    def can_enrich(self, lead: dict) -> bool:
        """Return True if this plugin can add data to the given lead."""
        pass

    @abstractmethod
    def enrich(self, lead: dict) -> dict:
        """Return a dict of enrichment fields to merge into the lead.

        Should NOT modify the lead in place — return only the new/updated fields.
        """
        pass

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.name}>"
