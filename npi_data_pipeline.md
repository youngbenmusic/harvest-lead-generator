# Workflow: NPI Data Pipeline

## Objective
Download the CMS National Provider Identifier (NPI) database, filter for Alabama healthcare providers, and process them into structured leads for the dashboard.

## Background
The NPI database is maintained by CMS (Centers for Medicare & Medicaid Services) and is freely available for public download. Every healthcare provider in the U.S. is required to have an NPI number. The database includes: provider name, business name, address, phone, fax, specialty/taxonomy, and whether they're an individual or organization.

## Data Source
- **Full NPI Data Dissemination (monthly):** https://download.cms.gov/nppes/NPI_Files.html
- The full file is very large (~8GB zipped, ~35GB unzipped). It contains 7M+ records nationally.
- There is also a weekly incremental update file for new/changed records.
- File format: CSV with headers

## Key Fields We Need
From the NPI CSV, extract these columns:
- `NPI` — unique provider ID
- `Provider Organization Name (Legal Business Name)` — for organizations
- `Provider Last Name (Legal Name)` + `Provider First Name` — for individuals
- `Provider First Line Business Mailing Address`
- `Provider Business Mailing Address City Name`
- `Provider Business Mailing Address State Name` — filter for "AL"
- `Provider Business Mailing Address Postal Code`
- `Provider Business Mailing Address Telephone Number`
- `Provider Business Mailing Address Fax Number`
- `Healthcare Provider Taxonomy Code_1` — this tells us what type of provider
- `Entity Type Code` — 1 = individual, 2 = organization

## Taxonomy Codes (Provider Types)
These taxonomy codes map to our target customer types:
- `122300000X` — Dentist (General)
- `1223G0001X` — Dentist (General Practice)
- `124Q00000X` — Dental Hygienist
- `174400000X` — Veterinarian (suggest searching separately or by keyword)
- `207Q00000X` — Family Medicine
- `208600000X` — Surgery
- `261QU0200X` — Urgent Care Clinic
- `282N00000X` — General Acute Care Hospital
- `283Q00000X` — Psychiatric Hospital
- `311Z00000X` — Custodial Care Facility (Nursing Homes)
- `261QM1200X` — Medical Specialty Clinic
- `291U00000X` — Clinical Medical Laboratory
- `207R00000X` — Internal Medicine
- `2085R0001X` — Dermatology (surgery centers)

Note: This is not exhaustive. The full taxonomy list is at https://taxonomy.nucc.org/. We should cast a wide net and then categorize.

## Step-by-Step Process

### Step 1: Download NPI Data
- **Tool:** `tools/download_npi.py`
- Downloads the latest monthly NPI data dissemination file from CMS
- Saves to `.tmp/npi_raw/`
- NOTE: The file is large. Consider using the NPPES API for filtered queries instead if bandwidth/storage is an issue: https://npiregistry.cms.hhs.gov/api/

### Alternative: Use the NPPES API (Recommended for v1)
Instead of downloading the full 8GB file, use the NPPES API to query directly:
- Endpoint: `https://npiregistry.cms.hhs.gov/api/?version=2.1`
- Parameters: `&state=AL&limit=200&skip=0`
- Paginate through results (max 200 per request, but can loop through all)
- This is FREE, no API key required, and returns JSON
- Much faster and lighter than downloading the full file
- **This is the recommended approach for v1**

### Step 2: Filter for Alabama
- **Tool:** `tools/filter_alabama.py`
- If using full file: filter CSV where state = "AL"
- If using API: already filtered by the query parameter
- Save filtered results to `data/alabama_providers_raw.json`

### Step 3: Categorize and Clean
- **Tool:** `tools/process_leads.py`
- Map taxonomy codes to human-readable categories (Dental, Hospital, Vet, Lab, etc.)
- Deduplicate by address (multiple providers at same practice)
- Clean phone numbers to consistent format
- Add fields: `status` (default: "New"), `notes` (default: empty), `date_added`, `last_updated`
- Remove individual practitioners where an organization record exists at the same address (we want the practice, not each doctor separately)
- Save to `data/alabama_leads.json`

### Step 4: Export to Web App
- **Tool:** `tools/export_to_app.py`
- Converts `alabama_leads.json` into the format the web app expects
- Copies to `app/data/leads.json`

## Edge Cases
- Some providers may have outdated addresses or phone numbers — NPI data isn't always current
- Some records may be individuals without a practice name — flag these differently
- Taxonomy codes don't cover tattoo parlors or funeral homes — these will need to be sourced separately (future enhancement, possibly from Alabama business filings)
- API rate limits: NPPES API doesn't have strict rate limits but be respectful — add 0.5s delays between requests

## Success Criteria
- 1,000+ Alabama healthcare facility leads loaded into the dashboard
- Each lead has: name, type, address, city, county, phone, and status
- No duplicate facilities
- Data is clean and professional enough that David could start calling leads immediately

## Future Enhancements
- Weekly diff script that checks for new NPI registrations (`workflows/weekly_refresh.md`)
- Cross-reference with Alabama Secretary of State for new business filings
- Add estimated facility size based on number of providers at same address
- Google Maps integration for route planning
