# Workflow: NPI Data Pipeline

## Objective
Download the CMS National Provider Identifier (NPI) database, filter for Alabama healthcare providers, and process them into structured leads for the dashboard.

## Architecture Overview

The pipeline has evolved from a simple download-and-process script into a multi-stage automated system:

```
Orchestrator (tools/orchestrator.py)
  ├── Ingest: NPI API + ADPH Scraper + CMS POS
  ├── Normalize: Common schema transform
  ├── Deduplicate: Composite matching across sources
  ├── Enrich: Waste volume, geo distance, bed counts, completeness
  ├── Score: Weighted 0-100 scoring with priority tiers
  ├── Export: Dashboard JSON generation
  └── CRM Sync: Push qualified leads to CRM
```

## Data Sources

| Source | Tool | API Key | Frequency |
|--------|------|---------|-----------|
| NPPES NPI Registry | `tools/download_npi.py` | None (free) | Weekly |
| ADPH Facility Directory | `tools/scrape_adph.py` | None (public) | Monthly |
| CMS Provider of Services | `tools/download_cms_pos.py` | None (public) | Quarterly |

## Running the Pipeline

### Full pipeline (with database)
```bash
docker-compose up -d                    # Start PostgreSQL
python tools/orchestrator.py            # Run all stages
```

### Full pipeline (JSON mode, no database)
```bash
python tools/orchestrator.py --json
```

### Individual stages
```bash
python tools/download_npi.py            # Download NPI data
python tools/scrape_adph.py             # Scrape ADPH facilities
python tools/download_cms_pos.py        # Download CMS bed counts
python tools/normalize.py --json        # Normalize all sources
python tools/deduplicate.py --json      # Deduplicate
python tools/enrich.py --json           # Run enrichment plugins
python tools/score_leads.py --json      # Score leads
python tools/export_dashboard.py        # Export to dashboard JSON
```

### Skip data download (use cached data)
```bash
python tools/orchestrator.py --skip-ingest --json
```

## Key Fields Extracted

From NPI: provider name, address, phone, fax, taxonomy code, entity type
From ADPH: administrator name, license number, facility category
From CMS: bed count, hospital type, ownership type

## Taxonomy Codes (Provider Types)
These taxonomy codes map to our target customer types:
- `122300000X` — Dentist (General)
- `174400000X` — Veterinarian
- `207Q00000X` — Family Medicine
- `208600000X` — Surgery
- `261QU0200X` — Urgent Care Clinic
- `282N00000X` — General Acute Care Hospital
- `311Z00000X` — Custodial Care Facility (Nursing Homes)
- `291U00000X` — Clinical Medical Laboratory
- Full mapping in `tools/process_leads.py` (TAXONOMY_MAP)

## Edge Cases
- NPPES API has no strict rate limits but uses 0.5s delays between requests
- ADPH portal may require JS rendering — falls back to cached data
- Some NPI records have outdated addresses
- CMS POS URL changes quarterly — update `tools/download_cms_pos.py` if download fails

## Success Criteria
- 10,000+ Alabama healthcare facility leads in the database
- Each lead has: name, type, address, city, phone, score, tier
- No duplicate facilities (composite dedup across sources)
- Dashboard displays scores and tier filtering
