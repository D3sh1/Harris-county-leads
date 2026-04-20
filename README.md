# Lucas County — Motivated Seller Lead Scraper
Automated bot that scrapes public county records daily and scores distressed property owners as motivated seller leads.
## What It Does
Pulls records from public sources, cross-references them, and scores every owner 0–100 based on financial distress signals.
| Source | Data |
|---|---|
| Lucas County Clerk of Courts | Lis pendens, liens, probate, foreclosure, judgments |
| Lucas County Auditor | Property address, mailing address, parcel data |
| iCare Portal | Code violations / blight notices |
## Scoring (max 100 pts)
| Signal | Points |
|---|---|
| Tax delinquency | +30 |
| Code violation | +25 |
| Probate filing | +20 |
| Multiple liens | +15 |
| Divorce / bankruptcy | +10 |
## Folder Structure
- /src/scraper.py — Main Python bot
- /src/requirements.txt — Python dependencies
- /data/output.json — All leads
- /dashboard/index.html — Live dashboard
- /.github/workflows/scraper.yml — Auto-runs daily
## Setup
pip install -r src/requirements.txt
playwright install chromium
python src/scraper.py
## Live Dashboard
Enable GitHub Pages: Settings → Pages → Branch: main → Folder: /dashboard
## Automation
The bot runs automatically every day at 6:00 AM UTC via GitHub Actions.

