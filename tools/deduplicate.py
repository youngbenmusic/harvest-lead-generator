"""
deduplicate.py — Composite matching algorithm to merge records across sources.

Takes normalized records and produces a deduplicated master lead list,
merging data from NPI, ADPH, and CMS sources.

Match hierarchy (highest confidence first):
1. NPI number match (confidence 1.0)
2. License number match (confidence 0.95)
3. Exact address + name match (confidence 0.9)
4. Fuzzy address + fuzzy name (confidence 0.75)
5. Same address, different name — flagged for review (confidence 0.5)

Usage:
    python tools/deduplicate.py
    python tools/deduplicate.py --json   # Read from .tmp/normalized_records.json
"""

import json
import os
import sys
import argparse
from collections import defaultdict

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from tools.normalize import normalize_address, normalize_name

try:
    from rapidfuzz import fuzz
    HAS_RAPIDFUZZ = True
except ImportError:
    HAS_RAPIDFUZZ = False
    print("Warning: rapidfuzz not installed. Fuzzy matching disabled.")
    print("Install with: pip install rapidfuzz")


def make_address_key(record):
    """Create a normalized address key for exact matching."""
    parts = [
        normalize_address(record.get("address_line1", "")),
        record.get("city", "").upper().strip(),
        record.get("zip5", "")[:5].strip(),
    ]
    key = "|".join(parts)
    return key if key != "||" else None


def merge_records(group):
    """Merge a group of matched records into a single master record.

    Priority: ADPH administrator > NPI org name > NPI individual.
    Union all available fields.
    """
    if len(group) == 1:
        merged = dict(group[0]["record"])
        merged["sources"] = [{"source": group[0]["record"]["source"],
                               "source_id": group[0]["record"]["source_id"],
                               "confidence": group[0].get("confidence", 1.0)}]
        return merged

    # Sort: prefer NPI-2 (org) records, then by source priority (adph > npi > cms)
    source_priority = {"adph": 0, "npi": 1, "cms": 2}
    sorted_group = sorted(group, key=lambda g: (
        0 if g["record"].get("entity_type") == "NPI-2" else 1,
        source_priority.get(g["record"]["source"], 9),
    ))

    # Start with best record as base
    merged = dict(sorted_group[0]["record"])
    sources = []

    for item in sorted_group:
        rec = item["record"]
        confidence = item.get("confidence", 1.0)
        sources.append({
            "source": rec["source"],
            "source_id": rec["source_id"],
            "confidence": confidence,
        })

        # Fill in missing fields from other sources
        for field in ["phone", "fax", "county", "administrator", "npi_number",
                       "license_number", "taxonomy_code", "bed_count",
                       "hospital_type", "ownership_type"]:
            if not merged.get(field) and rec.get(field):
                merged[field] = rec[field]

        # Prefer ADPH administrator name
        if rec["source"] == "adph" and rec.get("administrator"):
            merged["administrator"] = rec["administrator"]

        # Prefer ADPH facility name for organizations
        if rec["source"] == "adph" and rec.get("facility_name"):
            merged["facility_name"] = rec["facility_name"]

        # Get bed count from CMS
        if rec["source"] == "cms" and rec.get("bed_count"):
            merged["bed_count"] = rec["bed_count"]

    merged["sources"] = sources
    return merged


def deduplicate(records):
    """Main deduplication pipeline.

    Returns a list of merged lead records and a list of review flags.
    """
    print("Harvest Med Waste — Deduplication Engine")
    print(f"  Input records: {len(records)}")
    print()

    # Build indexes for matching
    npi_index = defaultdict(list)       # NPI number → records
    license_index = defaultdict(list)   # License number → records
    address_index = defaultdict(list)   # Normalized address key → records

    for i, rec in enumerate(records):
        entry = {"idx": i, "record": rec, "confidence": 1.0}

        if rec.get("npi_number"):
            npi_index[rec["npi_number"]].append(entry)

        if rec.get("license_number"):
            license_index[rec["license_number"]].append(entry)

        addr_key = make_address_key(rec)
        if addr_key:
            address_index[addr_key].append(entry)

    # Track which records have been assigned to a group
    assigned = set()
    groups = []  # list of groups, each group is a list of {"record": ..., "confidence": ...}
    review_flags = []

    # Pass 1: NPI number matching (confidence 1.0)
    for npi, entries in npi_index.items():
        if not npi:
            continue
        group_indices = set()
        group = []
        for e in entries:
            if e["idx"] not in assigned:
                e["confidence"] = 1.0
                group.append(e)
                group_indices.add(e["idx"])
                assigned.add(e["idx"])
        if group:
            groups.append(group)

    print(f"  Pass 1 (NPI match): {len(groups)} groups")

    # Pass 2: License number matching (confidence 0.95)
    license_groups = 0
    for lic, entries in license_index.items():
        if not lic:
            continue
        unassigned = [e for e in entries if e["idx"] not in assigned]
        if not unassigned:
            # Check if any can be merged into existing groups
            for e in entries:
                if e["idx"] not in assigned:
                    continue
                # Find the group this record belongs to
                for g in groups:
                    if any(ge["idx"] == e["idx"] for ge in g):
                        # Add unassigned records from same license to this group
                        for ue in entries:
                            if ue["idx"] not in assigned:
                                ue["confidence"] = 0.95
                                g.append(ue)
                                assigned.add(ue["idx"])
                        break
            continue

        # Create a new group for unassigned records with same license
        for e in unassigned:
            e["confidence"] = 0.95
            assigned.add(e["idx"])
        groups.append(unassigned)
        license_groups += 1

    print(f"  Pass 2 (License match): +{license_groups} groups")

    # Pass 3: Exact address + name matching (confidence 0.9)
    exact_match_groups = 0
    for addr_key, entries in address_index.items():
        unassigned = [e for e in entries if e["idx"] not in assigned]
        if len(unassigned) < 2:
            if unassigned:
                unassigned[0]["confidence"] = 0.9
                groups.append(unassigned)
                assigned.add(unassigned[0]["idx"])
            continue

        # Sub-group by normalized name
        name_groups = defaultdict(list)
        for e in unassigned:
            norm_name = e["record"].get("_norm_name", normalize_name(e["record"].get("facility_name", "")))
            name_groups[norm_name].append(e)

        for norm_name, name_entries in name_groups.items():
            for e in name_entries:
                e["confidence"] = 0.9
                assigned.add(e["idx"])
            groups.append(name_entries)
            exact_match_groups += 1

    print(f"  Pass 3 (Exact addr+name): +{exact_match_groups} groups")

    # Pass 4: Fuzzy matching for remaining unassigned records (confidence 0.75)
    fuzzy_groups = 0
    remaining = [{"idx": i, "record": r, "confidence": 0.75}
                 for i, r in enumerate(records) if i not in assigned]

    if remaining and HAS_RAPIDFUZZ:
        # Group remaining by ZIP code for efficiency
        zip_groups = defaultdict(list)
        for e in remaining:
            z = e["record"].get("zip5", "")[:5]
            zip_groups[z].append(e)

        for zip_code, zip_entries in zip_groups.items():
            matched_in_zip = set()
            for i, e1 in enumerate(zip_entries):
                if i in matched_in_zip:
                    continue
                group = [e1]
                name1 = e1["record"].get("_norm_name", "")
                addr1 = e1["record"].get("_norm_address", "")

                for j, e2 in enumerate(zip_entries[i+1:], i+1):
                    if j in matched_in_zip:
                        continue
                    name2 = e2["record"].get("_norm_name", "")
                    addr2 = e2["record"].get("_norm_address", "")

                    name_sim = fuzz.ratio(name1, name2) / 100.0
                    addr_sim = fuzz.ratio(addr1, addr2) / 100.0

                    if name_sim > 0.85 and addr_sim > 0.8:
                        e2["confidence"] = 0.75
                        group.append(e2)
                        matched_in_zip.add(j)

                if len(group) > 1:
                    fuzzy_groups += 1

                for e in group:
                    assigned.add(e["idx"])
                groups.append(group)

        print(f"  Pass 4 (Fuzzy match): +{fuzzy_groups} groups")
    elif remaining:
        # No fuzzy matching — just add remaining as singletons
        for e in remaining:
            assigned.add(e["idx"])
            groups.append([e])
        print(f"  Pass 4 (No fuzzy): {len(remaining)} singletons added")

    # Pass 5: Add any truly remaining records as singletons
    for i, rec in enumerate(records):
        if i not in assigned:
            groups.append([{"idx": i, "record": rec, "confidence": 0.5}])
            review_flags.append({
                "record": rec,
                "reason": "Unmatched record",
                "confidence": 0.5,
            })

    # Merge each group
    print(f"\n  Total groups: {len(groups)}")
    print("  Merging records...")

    merged_leads = []
    for group in groups:
        merged = merge_records(group)
        # Remove internal matching fields
        merged.pop("_norm_name", None)
        merged.pop("_norm_address", None)
        merged_leads.append(merged)

    # Org-over-individual dedup: remove individuals at org addresses
    org_addresses = set()
    for lead in merged_leads:
        if lead.get("entity_type") == "NPI-2":
            key = make_address_key(lead)
            if key:
                org_addresses.add(key)

    before_count = len(merged_leads)
    merged_leads = [
        lead for lead in merged_leads
        if lead.get("entity_type") == "NPI-2"
        or make_address_key(lead) not in org_addresses
    ]
    removed = before_count - len(merged_leads)
    if removed:
        print(f"  Removed {removed} individuals at organization addresses")

    print(f"\n  Final deduplicated leads: {len(merged_leads)}")

    # Summary
    print("\n--- Dedup Summary ---")
    type_counts = {}
    for lead in merged_leads:
        ft = lead.get("facility_type", "Other")
        type_counts[ft] = type_counts.get(ft, 0) + 1
    for ft, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"  {ft}: {count}")

    multi_source = sum(1 for l in merged_leads if len(l.get("sources", [])) > 1)
    print(f"\n  Multi-source leads: {multi_source}")
    print(f"  Flagged for review: {len(review_flags)}")

    return merged_leads, review_flags


def deduplicate_from_file():
    """Load normalized records from JSON and deduplicate."""
    input_file = os.path.join(PROJECT_ROOT, ".tmp", "normalized_records.json")
    if not os.path.exists(input_file):
        print(f"ERROR: {input_file} not found. Run tools/normalize.py first.")
        sys.exit(1)

    with open(input_file) as f:
        records = json.load(f)

    merged, review = deduplicate(records)

    # Save results
    output_file = os.path.join(PROJECT_ROOT, ".tmp", "deduplicated_leads.json")
    with open(output_file, "w") as f:
        json.dump(merged, f, indent=2)
    print(f"\nSaved {len(merged)} leads to {output_file}")

    if review:
        review_file = os.path.join(PROJECT_ROOT, ".tmp", "review_flags.json")
        with open(review_file, "w") as f:
            json.dump(review, f, indent=2, default=str)
        print(f"Saved {len(review)} review flags to {review_file}")

    return merged, review


def deduplicate_and_save_to_db(records):
    """Deduplicate records and save to the leads table."""
    from tools.db import upsert_lead, upsert_lead_source

    merged, review = deduplicate(records)

    print("\nSaving to database...")
    saved = 0
    for lead in merged:
        # Generate lead_uid from primary source
        lead_uid = lead.get("source_id", "")
        if lead.get("npi_number"):
            lead_uid = f"npi-{lead['npi_number']}"
        elif lead.get("license_number"):
            lead_uid = f"adph-{lead['license_number']}"

        lead_data = {
            "lead_uid": lead_uid,
            "facility_name": lead.get("facility_name", ""),
            "facility_type": lead.get("facility_type", "Other"),
            "address_line1": lead.get("address_line1", ""),
            "address_line2": lead.get("address_line2", ""),
            "city": lead.get("city", ""),
            "state": lead.get("state", "AL"),
            "zip5": lead.get("zip5", ""),
            "county": lead.get("county", ""),
            "phone": lead.get("phone", ""),
            "fax": lead.get("fax", ""),
            "administrator": lead.get("administrator", ""),
            "npi_number": lead.get("npi_number", ""),
            "license_number": lead.get("license_number", ""),
            "taxonomy_code": lead.get("taxonomy_code", ""),
            "entity_type": lead.get("entity_type", ""),
            "bed_count": lead.get("bed_count"),
            "status": "New",
        }

        lead_id = upsert_lead(lead_data)

        # Save source attributions
        for src in lead.get("sources", []):
            upsert_lead_source(
                lead_id=lead_id,
                source=src["source"],
                source_id=src["source_id"],
                confidence=src.get("confidence", 1.0),
            )

        saved += 1
        if saved % 1000 == 0:
            print(f"  Saved {saved}/{len(merged)}...", flush=True)

    print(f"  Saved {saved} leads to database")
    return merged, review


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deduplicate normalized records")
    parser.add_argument("--json", action="store_true", help="Read from .tmp JSON files")
    args = parser.parse_args()
    deduplicate_from_file()
