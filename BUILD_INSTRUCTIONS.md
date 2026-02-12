# Build Instructions — Harvest Lead Generator

## How to Use These Files

You have four files to set up your project:

1. **CLAUDE.md** — Drop this in your project root (`~/Desktop/harvest-lead-generator/`). This is Claude Code's memory file that gives it full context about the project.

2. **npi_data_pipeline.md** — Drop this in `workflows/`. This tells Claude Code exactly how to download and process the NPI data.

3. **weekly_refresh.md** — Drop this in `workflows/`. This tells Claude Code how to check for new leads each week.

4. **These build instructions** — Follow the steps below to build the entire project.

---

## Setup

Open Terminal and run:

```bash
mkdir -p ~/Desktop/harvest-lead-generator/workflows
mkdir -p ~/Desktop/harvest-lead-generator/tools
mkdir -p ~/Desktop/harvest-lead-generator/data
mkdir -p ~/Desktop/harvest-lead-generator/.tmp
mkdir -p ~/Desktop/harvest-lead-generator/app
```

Then copy your files into place:
- CLAUDE.md → `~/Desktop/harvest-lead-generator/CLAUDE.md`
- npi_data_pipeline.md → `~/Desktop/harvest-lead-generator/workflows/npi_data_pipeline.md`
- weekly_refresh.md → `~/Desktop/harvest-lead-generator/workflows/weekly_refresh.md`

---

## Build Sequence

Navigate to your project and launch Claude Code:

```bash
cd ~/Desktop/harvest-lead-generator
claude
```

### Prompt 1: Build the Data Pipeline

Tell Claude Code:

"Read the workflow in workflows/npi_data_pipeline.md. Build all the Python tools it references. Start with the NPPES API approach (not the full file download). Create tools/download_npi.py that queries the NPPES API for all Alabama healthcare providers, paginates through all results, and saves the raw JSON to .tmp/npi_raw.json. Then create tools/process_leads.py that takes the raw data, maps taxonomy codes to human-readable categories (Dental, Hospital, Veterinary, Lab, Urgent Care, Surgery Center, Nursing Home, Medical Practice, Other), deduplicates by address, cleans phone numbers, and outputs a structured alabama_leads.json to the data/ folder. Each lead should have: id, name, facility_type, address, city, county, state, zip, phone, fax, taxonomy_code, npi_number, status (default New), notes (default empty), date_added, and new_this_week flag."

### Prompt 2: Run the Pipeline

"Run the data pipeline. Execute tools/download_npi.py first, then tools/process_leads.py. Show me how many leads we got and a breakdown by facility type."

Note: This will make real API calls to the NPPES API (it's free, no key needed). It may take a few minutes to paginate through all Alabama providers.

### Prompt 3: Build the Web Dashboard

"Now build the web dashboard in the app/ folder. Read the CLAUDE.md for design preferences. Create a professional, mobile-friendly, single-page web application with:

- A stats bar at the top showing: Total Leads, New This Week, Contacted, Interested, Conversion Rate
- A search bar to search by business name or city
- Filter dropdowns for: facility type, county, and status
- A sortable table of all leads showing: name, type, city, county, phone, status
- When you click a lead row, it expands to show full details: complete address, fax, NPI number, taxonomy, and a notes text area
- The status dropdown on each lead should have options: New, Contacted, Interested, Proposal Sent, Closed Won, Not Interested
- Notes and status changes should persist using localStorage
- New This Week leads should be highlighted with a subtle badge or background color
- Color scheme: professional and clean. Use navy/dark blue as primary, with white background and subtle gray accents. A green accent for positive stats (Closed Won) and red for Not Interested.
- Make it responsive so it works on mobile

Load the lead data from data/alabama_leads.json."

### Prompt 4: Test It

"Open the dashboard in my browser and let me test it. Tell me how to open the app locally."

Claude Code will likely tell you to run a simple local server:
```bash
cd app
python3 -m http.server 8000
```
Then open http://localhost:8000 in your browser.

### Prompt 5: Build the Weekly Refresh

"Read workflows/weekly_refresh.md and build tools/weekly_refresh.py. It should query the NPPES API for Alabama providers added in the last 7 days, compare against our existing data/alabama_leads.json, add any new leads with a new_this_week flag set to true, reset the flag on all existing leads, generate a summary report, and save the updated leads file."

### Prompt 6: Polish and Export

"Review the entire project. Make sure everything is clean, well-documented, and ready to demo. Add a README.md with setup instructions that a non-technical person (David) could follow to run the dashboard on their own computer. Also add an export to CSV button on the dashboard so David can download the leads into a spreadsheet if he wants."

---

## What You'll Have When Done

- A working web dashboard with 1,000+ real Alabama healthcare facility leads
- Each lead has contact info, facility type, and a sales pipeline tracker
- A weekly refresh script that finds new leads automatically
- CSV export so David can work in spreadsheets if he prefers
- A clean, documented project that demonstrates your Claude Code skills

---

## Tips

- If Claude Code asks permission to run a command, say yes (unless it's something that costs money)
- If something breaks, tell Claude Code: "That didn't work. Here's the error: [paste error]. Fix it."
- If the API pagination takes too long, tell Claude Code to limit to the first 5,000 results for now
- Save your work frequently — Claude Code sessions can timeout
- If you want to restart a session, just `cd` into the project folder and type `claude` again — it'll read the CLAUDE.md and pick up context

---

## Demo Script for David

When you show this to David, walk him through it like this:

1. "I built a lead generation tool for Harvest Med Waste using AI"
2. Show the stats bar — "We have X healthcare facilities across Alabama already in the system"
3. Filter by Dental — "Here's every dental office in Alabama. You can see their name, city, phone number"
4. Click a lead — "When you reach out, you update the status and add notes here"
5. Filter by county — "If you're planning routes, you can filter by county to see clusters"
6. Show the CSV export — "You can also download this as a spreadsheet anytime"
7. "Every Monday it automatically checks for new healthcare facilities opening in Alabama and adds them"

That's a 2-minute demo that shows real, immediate business value.
