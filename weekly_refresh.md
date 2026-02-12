# Workflow: Weekly Lead Refresh

## Objective
Check for new healthcare provider registrations in Alabama each week and add them to the lead database as fresh leads, flagged as "New This Week."

## Frequency
Run every Monday morning.

## Process

### Step 1: Query NPPES API for Recent Additions
- Use the NPPES API with the `enumeration_date` parameter
- Query for providers in Alabama with enumeration dates in the past 7 days
- Endpoint: `https://npiregistry.cms.hhs.gov/api/?version=2.1&state=AL&limit=200`
- Filter results by checking enumeration date field

### Step 2: Compare Against Existing Leads
- Load current `data/alabama_leads.json`
- Compare new results by NPI number
- Any NPI not already in the database = new lead

### Step 3: Process New Leads
- Run new leads through the same cleaning/categorization as `tools/process_leads.py`
- Set `status` to "New"
- Set `date_added` to current date
- Add `new_this_week: true` flag

### Step 4: Generate Weekly Summary
- Output a brief summary: "X new leads found this week"
- Breakdown by facility type
- Save summary to `data/weekly_reports/YYYY-MM-DD.md`

### Step 5: Update Lead Database
- Append new leads to `data/alabama_leads.json`
- Re-export to web app via `tools/export_to_app.py`

## Automation (Future)
- This can be set up as a cron job on Mac: `crontab -e`
- Or scheduled via GitHub Actions if the project is hosted on GitHub
- For now, run manually each Monday by typing: `python tools/weekly_refresh.py`

## Edge Cases
- If no new leads found, still generate the summary (shows the system is working)
- If API is down, log the error and retry next day
- Reset `new_this_week` flag on all existing leads before adding new ones (so only current week's leads are highlighted)
