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
- Podiatry offices
- Dialysis centers
- Medical spas (aesthetic clinics)

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

## Firecrawl Integration

### Overview

This section provides step-by-step instructions for integrating Firecrawl into the Harvest Med Waste lead enrichment pipeline. The goal is to crawl healthcare facility websites to extract contact emails, staff names, phone numbers, and decision-maker info — replacing or supplementing Hunter.io at a fraction of the cost.

### The Strategy: Three-Tier Email Enrichment

Instead of relying solely on Hunter.io ($49+/month), you'll build a layered approach:

| Tier | Tool | Cost | Use Case |
|------|------|------|----------|
| **1 (Primary)** | Firecrawl | Free (500 pages) or $16/mo (3,000 pages) | Crawl facility websites for contact/about/staff pages |
| **2 (Fallback)** | Google Custom Search API | Free (100/day = ~3,000/mo) | Search the open web for published emails |
| **3 (Last Resort)** | Email Permutation + SMTP Verify | Free | Guess email patterns and verify via mail server |
| **Optional** | Hunter.io Free Tier | Free (25 lookups/mo) | High-priority leads where you need guaranteed accuracy |

### Step 1: Get Your Firecrawl API Key

1. Go to [firecrawl.dev](https://www.firecrawl.dev)
2. Sign up (no credit card required for free tier)
3. Copy your API key (starts with `fc-`)
4. Add it to your project's `.env` file:

```
FIRECRAWL_API_KEY=fc-YOUR_API_KEY_HERE
```

### Step 2: Install the Python SDK

In your `harvest-lead-generator` project directory:

```bash
pip install firecrawl-py python-dotenv
```

Add `firecrawl-py` to your `requirements.txt` as well.

### Step 3: Build the Firecrawl Enrichment Module

Build a new enrichment module called `tools/firecrawl_enrich.py` that does the following:

1. **Load leads from the existing data file** (alabama_leads.json or whatever the current data source is). For each lead that has a website URL but is missing an email address, queue it for Firecrawl enrichment.

2. **For each facility website, use Firecrawl's scrape endpoint** to crawl the contact page, about page, and staff/team page. The strategy is:
   - First, use Firecrawl's `/map` endpoint to discover all URLs on the facility's domain
   - Filter for URLs containing keywords like: contact, about, staff, team, providers, physicians, our-team, meet-our, directory, leadership
   - Scrape those specific pages using the `/scrape` endpoint with `formats=["markdown"]`
   - Parse the markdown content to extract: email addresses (regex), phone numbers (regex), staff names with titles/roles

3. **Email extraction regex should catch:**
   - Standard email patterns: `name@domain.com`
   - Obfuscated emails: `name [at] domain [dot] com`, `name(at)domain.com`
   - mailto: links embedded in the markdown

4. **Prioritize finding decision-makers** by looking for titles like:
   - Office Manager, Practice Manager, Facility Manager
   - Compliance Officer, Safety Officer, Environmental Health
   - Administrator, Director of Operations, COO
   - Owner, Medical Director
   - These are the people who make waste disposal purchasing decisions

5. **Rate limiting and credit management:**
   - Add a 2-second delay between Firecrawl API calls
   - Track credits used (1 credit per page scraped)
   - Stop and warn if approaching 450 credits (to stay within the 500 free tier)
   - Add a `--max-credits` flag so I can set a budget per run
   - Log every API call with URL, credits used, and whether data was found

6. **Output:**
   - Update each lead record with: `contact_email`, `contact_name`, `contact_title`, `contact_phone`, `enrichment_source` (set to "firecrawl"), `enrichment_date`
   - Save enriched data back to the data file
   - Print a summary: "Enriched X leads, found Y emails, Z decision-makers, used N credits"

7. **Error handling:**
   - Skip leads with no website URL
   - Handle 404s, timeouts, and rate limits gracefully
   - Log failures with the facility name and reason
   - Save partial progress every 50 leads so a crash doesn't lose everything

Use the `firecrawl` Python SDK (already installed via `pip install firecrawl-py`). Load the API key from the .env file using python-dotenv. The key is stored as `FIRECRAWL_API_KEY`.

### Step 4: Build the Google Search Fallback

After Firecrawl enrichment runs, many leads will still be missing emails (small clinics without websites, or sites that don't list emails publicly). Build the fallback:

Build a second enrichment module called `tools/google_search_enrich.py` that runs AFTER the Firecrawl enrichment to fill in gaps:

1. **Only process leads that are still missing email addresses** after Firecrawl enrichment.

2. **Use the Google Custom Search JSON API** (free: 100 queries/day). Set up:
   - Go to [programmablesearchengine.google.com](https://programmablesearchengine.google.com) and create a Custom Search Engine that searches the entire web
   - Get your Search Engine ID (cx)
   - Go to [console.cloud.google.com](https://console.cloud.google.com) and enable the "Custom Search API"
   - Get your API key
   - Add both to .env: `GOOGLE_CSE_API_KEY` and `GOOGLE_CSE_CX`

3. **For each lead missing an email, construct search queries:**
   - `"{facility_name}" "{city}" email`
   - `"{facility_name}" "@" contact`
   - Parse the search result snippets and page descriptions for email addresses using the same regex from the Firecrawl module

4. **Rate limiting:**
   - Respect the 100 queries/day free limit
   - Add a `--daily-limit` flag (default 95 to leave buffer)
   - Track queries used today and stop when limit is reached
   - Can be run daily via cron to chip away at the backlog

5. **Update leads with `enrichment_source` set to "google_search" to track where data came from.**

### Step 5: Build the Email Guesser + SMTP Verifier

For leads still missing emails after both Firecrawl and Google Search:

Build `tools/email_guess_verify.py` as the final fallback:

1. **Only process leads that are still missing email addresses** after Firecrawl and Google Search enrichment.

2. **For each lead with a website domain, generate email permutations** using any contact name found during earlier enrichment (or the facility name if no contact was found). Common patterns:
   - `first@domain.com`
   - `first.last@domain.com`
   - `firstlast@domain.com`
   - `flast@domain.com`
   - `first.l@domain.com`
   - `info@domain.com`, `contact@domain.com`, `office@domain.com` (generic fallbacks)

3. **Verify each candidate email using SMTP:**
   - Look up the domain's MX records using `dnspython`
   - Connect to the mail server via `smtplib`
   - Use RCPT TO to check if the mailbox exists
   - Mark results as: `verified` (server confirms exists), `unverified` (server accepts all / can't determine), or `invalid` (server rejects)

4. **Important caveats to handle:**
   - Google Workspace and Microsoft 365 often accept all addresses — mark these as "unverified" not "verified"
   - Add a 3-second delay between SMTP checks to avoid blacklisting
   - Use a `--max-checks` flag to control how many leads to process per run
   - Never actually send any emails — only check if the address exists

5. **Update leads with `enrichment_source` set to "smtp_verify" and include a `email_confidence` field: "high" for verified, "medium" for unverified, "low" for generic guesses like info@.**

Install required packages: `pip install dnspython`

### Step 6: Build the Orchestrator

Tie all three tiers together:

Build `tools/enrich_orchestrator.py` that runs the full enrichment pipeline in order:

1. Run Firecrawl enrichment (Tier 1)
2. Run Google Search enrichment on remaining gaps (Tier 2)
3. Run Email Guess + SMTP Verify on remaining gaps (Tier 3)
4. Print a final summary showing:
   - Total leads processed
   - Emails found by each tier
   - Decision-makers identified
   - Leads still missing emails
   - Credits/queries used per service

Make this runnable as: `python tools/enrich_orchestrator.py`

Add flags:
- `--tier` to run only a specific tier (1, 2, or 3)
- `--limit` to cap how many leads to process
- `--dry-run` to preview what would be processed without making API calls

### Should You Keep Hunter.io?

**Short answer: Yes, keep the free tier as a spot-check tool, but don't pay for it.**

| Scenario | Recommendation |
|----------|---------------|
| You have < 25 high-priority hot leads per month | Use Hunter free tier for those 25, Firecrawl for the rest |
| You're doing bulk enrichment of 1,000+ leads | Firecrawl + Google Search + SMTP — Hunter is too expensive at scale |
| A sales rep needs to verify a specific contact before calling | Hunter's single-lookup is perfect for this |
| You're running weekly automated enrichment | Firecrawl pipeline only — no human-in-the-loop needed |

**If you're currently paying for Hunter, cancel the paid plan.** The free tier (25 searches + 50 verifications per month) is enough for spot-checking, and your Firecrawl pipeline handles the heavy lifting.

If you already have Hunter integration code, keep it in your codebase but make it optional — a Tier 0 that David's team can use manually for individual lookups through the dashboard, while the automated pipeline uses the free tools.

### Cost Comparison

| Approach | Monthly Cost | Leads Enriched |
|----------|-------------|----------------|
| Hunter.io Starter | $49/mo | 500 lookups |
| Hunter.io Growth | $149/mo | 5,000 lookups |
| **Your Firecrawl Pipeline** | **$0-16/mo** | **3,000-14,000+ leads** |

Breakdown of your pipeline:
- Firecrawl free tier: 500 pages/mo = ~200-300 leads (facilities often have 2-3 pages to crawl)
- Firecrawl Hobby ($16/mo): 3,000 pages = ~1,000-1,500 leads
- Google Custom Search: 3,000 free queries/mo = ~1,500-2,000 additional leads
- SMTP verification: Unlimited and free (just slow)

For your 14,000 leads, the initial enrichment run would cost about $16 (one month of Hobby plan) plus free Google Search queries spread over a few weeks. After that, weekly refreshes on new leads would stay comfortably within the free tiers.

### .env File Template

```env
# Firecrawl
FIRECRAWL_API_KEY=fc-YOUR_KEY_HERE

# Google Custom Search
GOOGLE_CSE_API_KEY=YOUR_GOOGLE_API_KEY
GOOGLE_CSE_CX=YOUR_SEARCH_ENGINE_ID

# Hunter.io (optional — free tier for spot checks)
HUNTER_API_KEY=YOUR_HUNTER_KEY

# SMTP Verification
SMTP_FROM_EMAIL=verify@yourdomain.com
```

### File Structure After Integration

```
harvest-lead-generator/
├── tools/
│   ├── firecrawl_enrich.py          # Tier 1: Website crawling
│   ├── google_search_enrich.py      # Tier 2: Google search fallback
│   ├── email_guess_verify.py        # Tier 3: SMTP verification
│   ├── enrich_orchestrator.py       # Runs all tiers in sequence
│   ├── hunter_lookup.py             # Optional: Manual single lookups
│   ├── download_npi.py              # Existing
│   ├── process_leads.py             # Existing
│   └── enrich.py                    # Existing geocoding/scoring
├── data/
│   └── alabama_leads.json           # Your lead database
├── .env                             # API keys (gitignored)
├── .env.example                     # Template without real keys
├── requirements.txt                 # Updated with new dependencies
└── CLAUDE.md                        # Updated project context
```

### Running the Pipeline

```bash
# Full enrichment (all three tiers)
python tools/enrich_orchestrator.py

# Just Firecrawl (saves credits on other services)
python tools/enrich_orchestrator.py --tier 1

# Preview without making API calls
python tools/enrich_orchestrator.py --dry-run

# Limit to 100 leads (for testing)
python tools/enrich_orchestrator.py --limit 100

# Just Google Search (run daily to chip away at backlog)
python tools/enrich_orchestrator.py --tier 2 --limit 95
```

### Email Enrichment Pipeline Summary

Three-tier system for finding facility contact emails:
- Tier 1: Firecrawl (website crawling) — primary, most accurate
- Tier 2: Google Custom Search API — fallback for facilities without useful websites
- Tier 3: Email permutation + SMTP verification — last resort guess-and-check
- Optional: Hunter.io free tier for manual spot-checks

API keys stored in .env file. Free tier limits:
- Firecrawl: 500 credits/month (1 credit per page)
- Google CSE: 100 queries/day
- Hunter: 25 searches/month
- SMTP: Unlimited but slow (3-second delays)

Decision-maker titles to prioritize: Office Manager, Practice Manager, Compliance Officer, Administrator, Director of Operations, Owner, Medical Director.

### Next Steps

1. **Get your API keys** — Firecrawl (2 min), Google Custom Search (5 min)
2. **Add them to .env** in your project
3. **Build the enrichment modules** in order (Steps 3-6)
4. **Test with `--limit 10 --dry-run`** first to see what would happen
5. **Run Tier 1 on your full dataset** and check results
6. **Set up a cron job** for weekly enrichment once everything works
