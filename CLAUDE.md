# Harvest Med Waste — Lead Generator

## About This Project
This is a lead generation and sales pipeline dashboard for **Harvest Med Waste Disposal LLC**, a medical waste disposal company founded by **David Dyer**, based in Birmingham, Alabama. The company uses ozone sterilization technology (via Clean Waste Systems) to process non-hazardous medical waste from healthcare facilities. The company is actively looking for a new facility location in an industrial zone after a previous site in Blount County was not approved.

## Target Customers
Every facility that generates medical waste is a potential client:
- Hospitals and hospital systems
- Dental offices
- Veterinary clinics
- Urgent care centers
- Dermatology and surgery centers
- Medical and diagnostic labs
- Tattoo parlors (sharps waste)
- Funeral homes
- Nursing homes and assisted living facilities
- Physician private practices (family medicine, orthopedic, etc.)

## Geographic Focus
- Primary: Alabama (all counties)
- Secondary: Surrounding states (Mississippi, Tennessee, Georgia, Florida panhandle) — future expansion

## Business Context
- Medical waste management is a $2B+ U.S. industry, projected to grow to $3B+ by 2030
- U.S. healthcare providers generate 3M+ tons of medical waste per year
- Harvest Med Waste uses "humidizone" ozone sterilization — shreds and sterilizes waste, which can then go to regular landfills
- Key differentiator: environmentally friendly alternative to incineration
- David Dyer's background: Founded DE Medical, co-owned Southern Lab Partners (COVID testing labs employing 130+ people), sold to LabCorp

## Architecture (WAT Framework — adapted from Nate Herk)

This project uses a simplified WAT (Workflows, Agents, Tools) structure:

### Layer 1: Workflows (Instructions)
- Markdown files in `workflows/` that define step-by-step processes
- Each workflow specifies: objective, required inputs, which tools to use, expected outputs, and edge case handling

### Layer 2: Agent (Claude Code — that's you)
- Read the relevant workflow
- Run tools in the correct sequence
- Handle failures gracefully
- Ask clarifying questions when something is ambiguous
- Don't try to do everything directly — use the tools

### Layer 3: Tools (Python Scripts)
- Deterministic scripts in `tools/` that handle execution
- API calls, data processing, file operations
- Credentials stored in `.env` only

### Self-Improvement Loop
When something breaks:
1. Identify what broke
2. Fix the tool or script
3. Verify the fix works
4. Update the workflow with the new approach
5. Move forward with a stronger system

## File Structure

```
harvest-lead-generator/
├── CLAUDE.md                  # This file — project context and instructions
├── .env                       # API keys and credentials (gitignored)
├── workflows/                 # Step-by-step process documentation
│   ├── npi_data_pipeline.md   # How to download and process NPI data
│   ├── lead_enrichment.md     # How to enrich leads with additional data
│   └── weekly_refresh.md      # How to check for new leads weekly
├── tools/                     # Python scripts for execution
│   ├── download_npi.py        # Downloads NPI data from CMS
│   ├── filter_alabama.py      # Filters NPI data for Alabama providers
│   ├── process_leads.py       # Cleans and structures lead data
│   ├── enrich_leads.py        # Adds supplemental info to leads (future)
│   └── export_to_app.py       # Exports processed leads to the web app
├── data/                      # Processed data files
│   ├── alabama_leads.json     # Current lead database
│   └── lead_history.json      # Historical tracking for weekly diffs
├── .tmp/                      # Temporary files (raw downloads, intermediates)
├── app/                       # Web application files
│   ├── index.html             # Main dashboard
│   ├── style.css              # Styling
│   └── app.js                 # Application logic
└── README.md                  # Setup instructions for David / team
```

## Core Principle
Local files are for processing. The web app is the deliverable. Everything in `.tmp/` is disposable.

## Tech Preferences
- Python for data processing scripts
- HTML/CSS/JavaScript for the web dashboard (keep it simple — no heavy frameworks needed for v1)
- Local storage for persisting lead status and notes in the browser
- Keep dependencies minimal — this should be easy to run and maintain

## Style
- Clean, professional UI — David is a serious business owner, not a tech bro
- Mobile-friendly — he'll want to check this from his phone
- Fast — no unnecessary loading or complexity
