# Workflow: Weekly Lead Refresh

## Objective
Automatically check for new healthcare provider registrations in Alabama each week, process them through the full pipeline, and update the dashboard.

## Frequency
Every Monday at 6:00 AM CT.

## Running Manually

### Full refresh (recommended)
```bash
python tools/orchestrator.py --json
```

### Quick refresh (skip ADPH/CMS, just NPI)
```bash
python tools/orchestrator.py --json --stages ingest,normalize,deduplicate,enrich,score,export
```

### Skip download entirely (re-score existing data)
```bash
python tools/orchestrator.py --json --skip-ingest
```

## Automated Scheduling

### Option 1: Cron (Mac/Linux)
```bash
crontab -e
# Add this line (runs Monday 6am CT):
0 6 * * 1 cd /path/to/harvest-lead-generator && python tools/orchestrator.py --json >> .tmp/pipeline.log 2>&1
```

### Option 2: GitHub Actions
Create `.github/workflows/weekly_refresh.yml`:
```yaml
name: Weekly Lead Refresh
on:
  schedule:
    - cron: '0 12 * * 1'  # Monday noon UTC = 6am CT
  workflow_dispatch:       # Allow manual trigger

jobs:
  refresh:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - run: pip install -r requirements.txt
      - run: python tools/orchestrator.py --json --skip-ingest
      - run: |
          git config user.name "GitHub Actions"
          git config user.email "actions@github.com"
          git add data/alabama_leads.json
          git diff --cached --quiet || git commit -m "Weekly lead refresh"
          git push
```

### Option 3: Cloud Deployment (Docker)
```bash
# Build container
docker build -t harvest-pipeline .

# Run pipeline in container
docker run --env-file .env harvest-pipeline

# Deploy to AWS Lambda / Google Cloud Run with CloudWatch / Cloud Scheduler trigger
```

## Pipeline Stages (weekly run)

1. **Ingest** — Query NPPES API for all Alabama providers (incremental where possible)
2. **Normalize** — Transform raw records into common schema
3. **Deduplicate** — Merge records across NPI, ADPH, CMS sources
4. **Enrich** — Calculate waste volumes, distances, completeness
5. **Score** — Assign 0-100 score and Hot/Warm/Cool/Cold tier
6. **Export** — Generate `data/alabama_leads.json` for dashboard
7. **CRM Sync** — Push qualified leads (score >= 50) to CRM (if configured)

## What Happens Each Week
- New providers get `new_this_week: true` flag
- Existing providers get re-scored (bed counts or data may have changed)
- Dashboard JSON is regenerated with latest scores
- Pipeline run is logged to `pipeline_runs` table (DB mode)

## Edge Cases
- If no new leads found, pipeline still completes (re-scores existing leads)
- If NPPES API is down, pipeline logs the error and uses cached data
- If ADPH scraping fails, NPI data is still processed (non-fatal)
- `new_this_week` flag is reset on existing leads each run

## Error Alerting (Future)
- AWS SNS or email notification on pipeline failure
- Slack webhook for weekly summary report
