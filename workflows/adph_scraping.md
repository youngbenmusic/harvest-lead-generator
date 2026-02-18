# ADPH Facility Scraping Workflow

## Objective
Scrape the Alabama Department of Public Health (ADPH) Facilities Directory to capture licensed healthcare facilities with administrator names, license numbers, and facility details not available through NPI data.

## Data Source
- **URL:** https://dph1.adph.state.al.us/FacilitiesDirectory/
- **Update Frequency:** Checked monthly (ADPH updates licensing data periodically)
- **Cost:** Free (public government data)

## Facility Categories (19 total)
1. Hospitals
2. Critical Access Hospitals
3. Nursing Homes
4. Assisted Living Facilities
5. Ambulatory Surgical Centers
6. End Stage Renal Disease Facilities
7. Home Health Agencies
8. Hospices
9. Intermediate Care Facilities
10. Clinical Laboratories
11. Rehabilitation Centers
12. Rural Health Clinics
13. Portable X-Ray Suppliers
14. Psychiatric Residential Treatment Facilities
15. Comprehensive Outpatient Rehabilitation Facilities
16. Community Mental Health Centers
17. Organ Procurement Organizations
18. Religious Nonmedical Health Care Institutions
19. Outpatient Physical Therapy

## Fields Extracted
- Facility name
- Street address
- City, state, ZIP
- County
- Phone number
- Administrator / director name
- License number
- Facility type / category

## Process
1. **Run scraper:** `python tools/scrape_adph.py`
2. **Raw HTML cached:** `.tmp/adph_raw/` (for debugging)
3. **Results saved:** `.tmp/adph_results.json`
4. **DB staging:** Records written to `staging_adph` table
5. **Next step:** Run `tools/normalize.py` to transform into common schema

## Rate Limiting
- 1.5 second delay between requests
- Respectful User-Agent header
- Cache raw HTML to avoid re-fetching during debugging

## Edge Cases
- ADPH portal may require JavaScript rendering → fall back to manual data entry or Playwright
- Some categories may return no results (e.g., niche facility types)
- License numbers may not always be present
- Duplicate facilities may appear across categories

## Verification
- Spot-check 10 facilities against the live ADPH portal
- Compare hospital count against known Alabama hospital count (~100)
- Verify administrator names are populated for major facilities
