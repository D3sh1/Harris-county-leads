#!/usr/bin/env python3
"""
Harris County (Houston, TX) — Motivated Seller Lead Scraper v5.1
================================================================
Public sources (no login required):
  1. Harris County Tax Sale Listing   — delinquent properties going to auction
  2. Harris County Clerk Foreclosures — lis pendens / foreclosure notices by month

Distress scoring (max 100 pts):
  Tax delinquency / sale  +30
  Foreclosure notice      +30
  Multiple signals        +15
  High delinquency amount +10
"""

import json, csv, re, sys, argparse, logging
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

ROOT = Path(__file__).parent.parent
DATA = ROOT / "data"
DASH = ROOT / "dashboard"
DATA.mkdir(exist_ok=True)
DASH.mkdir(exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.hctax.net/",
})

def get(url, **kw):
    try:
        r = SESSION.get(url, timeout=30, **kw)
        r.raise_for_status()
        log.info("GET %s → %d (%d bytes)", url, r.status_code, len(r.content))
        return r
    except Exception as e:
        log.warning("GET %s → %s", url, e)
        return None

def post(url, data, **kw):
    try:
        r = SESSION.post(url, data=data, timeout=30, **kw)
        r.raise_for_status()
        log.info("POST %s → %d (%d bytes)", url, r.status_code, len(r.content))
        return r
    except Exception as e:
        log.warning("POST %s → %s", url, e)
        return None

def clean(s):
    return " ".join(str(s).split()) if s else ""

# ── 1. Harris County Tax Sale Listing ─────────────────────────────────────

TAX_SALE_URL = "https://www.hctax.net/Property/listings/taxsalelisting"

def scrape_tax_sale():
    """Scrape the live Harris County delinquent tax sale property list."""
    log.info("=== Harris County Tax Sale Listing ===")
    log.info("URL: %s", TAX_SALE_URL)

    r = get(TAX_SALE_URL)
    if not r:
        log.warning("Tax Sale: no response")
        return []

    soup = BeautifulSoup(r.text, "lxml")
    log.info("Tax Sale page title: %s", soup.title.string if soup.title else "n/a")

    tables = soup.find_all("table")
    log.info("Tax Sale: %d table(s) found", len(tables))

    records = []
    current = None

    for table in tables:
        for row in table.find_all("tr"):
            cells = [clean(c.get_text()) for c in row.find_all(["th", "td"])]
            text  = " ".join(cells)

            # New property starts when we see Account# / Cause#
            acct  = re.search(r'Account#[:\s]+(\S+)', text)
            cause = re.search(r'Cause#[:\s]+(\S+)',   text)

            if acct or cause:
                if current and (current.get("address") or current.get("case_number")):
                    records.append(current)
                current = {
                    "source":         "Harris Co. Tax Sale",
                    "doc_type":       "TAX_DELINQUENT",
                    "case_number":    cause.group(1) if cause else "",
                    "account_number": acct.group(1)  if acct  else "",
                    "owner":          "",
                    "address":        "",
                    "zip":            "",
                    "filing_date":    "",
                    "adjudged_value": "",
                    "minimum_bid":    "",
                    "tax_years":      "",
                    "sale_status":    "",
                    "score":          0,
                    "signals":        ["tax_delinquency"],
                }
                continue

            if current is None:
                continue

            # Parse key-value rows (label in col-0, value in col-1)
            if len(cells) >= 2:
                key = cells[0].lower().rstrip(": ")
                val = cells[1]
                if   "address"  in key:                      current["address"]        = val
                elif "zip"      in key:                      current["zip"]            = val
                elif "owner"    in key or "name" in key:     current["owner"]          = val
                elif "adjudged" in key:                      current["adjudged_value"] = val
                elif "minimum"  in key or "bid"  in key:    current["minimum_bid"]    = val
                elif "judgment" in key and "date" in key:    current["filing_date"]    = val
                elif "tax year" in key or "years" in key:    current["tax_years"]      = val
                elif "status"   in key:                      current["sale_status"]    = val
            elif len(cells) == 1 and current.get("address") == "":
                line = cells[0]
                if re.search(r'\d', line) and len(line) > 5:
                    current["address"] = line

    # save last record
    if current and (current.get("address") or current.get("case_number")):
        records.append(current)

    log.info("Tax Sale → %d record(s)", len(records))
    return records

# ── 2. Harris County Clerk — Foreclosure Notices ───────────────────────────

FRCL_URL = "https://www.cclerk.hctx.net/Applications/WebSearch/FRCL_R.aspx"

def scrape_clerk_foreclosures():
    """Scrape recent foreclosure postings from Harris County Clerk."""
    log.info("=== Harris County Clerk Foreclosures ===")
    log.info("URL: %s", FRCL_URL)

    r = get(FRCL_URL)
    if not r:
        log.warning("Foreclosure Clerk: no response")
        return []

    soup = BeautifulSoup(r.text, "lxml")
    log.info("Clerk page title: %s", soup.title.string if soup.title else "n/a")

    vs  = soup.find("input", {"id": "__VIEWSTATE"})
    ev  = soup.find("input", {"id": "__EVENTVALIDATION"})
    vsg = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})

    now   = datetime.now()
    year  = str(now.year)
    month = f"{now.month:02d}"

    payload = {
        "__VIEWSTATE":          vs["value"]  if vs  else "",
        "__EVENTVALIDATION":    ev["value"]  if ev  else "",
        "__VIEWSTATEGENERATOR": vsg["value"] if vsg else "",
        "ddlYear":              year,
        "ddlMonth":             month,
        "btnSearch":            "Search",
    }

    log.info("Posting foreclosure search for %s/%s...", month, year)
    r2 = post(FRCL_URL, payload)
    if not r2:
        return []

    soup2 = BeautifulSoup(r2.text, "lxml")
    records = []
    tables = soup2.find_all("table")
    log.info("Clerk foreclosures: %d table(s) found", len(tables))

    for table in tables:
        rows = table.find_all("tr")
        if not rows:
            continue
        headers = [clean(c.get_text()) for c in rows[0].find_all(["th", "td"])]
        log.info("Foreclosure table headers: %s", headers)

        for row in rows[1:]:
            cells = [clean(td.get_text()) for td in row.find_all("td")]
            if len(cells) < 2:
                continue

            rec = {
                "source":         "Harris Co. Clerk Foreclosures",
                "doc_type":       "FORECLOSURE",
                "case_number":    cells[0] if cells else "",
                "owner":          cells[1] if len(cells) > 1 else "",
                "address":        cells[2] if len(cells) > 2 else "",
                "filing_date":    cells[3] if len(cells) > 3 else "",
                "adjudged_value": "",
                "minimum_bid":    "",
                "tax_years":      "",
                "sale_status":    "",
                "zip":            "",
                "score":          0,
                "signals":        ["foreclosure"],
            }

            for i, h in enumerate(headers):
                h_low = h.lower()
                v = cells[i] if i < len(cells) else ""
                if "address"  in h_low:                     rec["address"]     = v
                elif "grantor" in h_low or "owner" in h_low: rec["owner"]      = v
                elif "file"    in h_low and "date" in h_low: rec["filing_date"] = v
                elif "doc"     in h_low or "number" in h_low: rec["case_number"] = v

            if rec["case_number"] or rec["owner"] or rec["address"]:
                records.append(rec)

    log.info("Clerk Foreclosures → %d record(s)", len(records))
    return records

# ── Scoring ────────────────────────────────────────────────────────────────

WEIGHTS = {
    "tax_delinquency": 30,
    "foreclosure":     30,
    "high_value":      10,
    "multiple_liens":  15,
}

def score_lead(rec):
    pts = sum(WEIGHTS.get(s, 0) for s in rec.get("signals", []))
    rec["score"] = min(pts, 100)
    return rec

# ── Output writers ─────────────────────────────────────────────────────────

def write_json(records):
    path = DATA / "output.json"
    payload = {
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "source": "Harris County Tax Sale / Clerk Foreclosures",
        "total": len(records),
        "records": records,
    }
    path.write_text(json.dumps(payload, indent=2, default=str))
    log.info("Wrote %s (%d records)", path, len(records))

def write_ghl_csv(records):
    path = DATA / "ghl_export.csv"
    fields = ["owner", "address", "zip", "source", "doc_type",
              "filing_date", "case_number", "adjudged_value",
              "minimum_bid", "tax_years", "sale_status", "score", "signals"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in records:
            row = dict(r)
            row["signals"] = ", ".join(r.get("signals", []))
            w.writerow(row)
    log.info("Wrote %s", path)

def write_dashboard(records):
    path = DASH / "index.html"
    now  = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    rows = ""
    for r in sorted(records, key=lambda x: x.get("score", 0), reverse=True):
        sig   = ", ".join(r.get("signals", []))
        score = r.get("score", 0)
        color = "#c0392b" if score >= 50 else "#e67e22" if score >= 25 else "#27ae60"
        bid   = r.get("minimum_bid", "") or r.get("adjudged_value", "")
        rows += (
            f"<tr>"
            f"<td style='color:{color};font-weight:bold'>{score}</td>"
            f"<td>{r.get('owner','')}</td>"
            f"<td>{r.get('address','')} {r.get('zip','')}</td>"
            f"<td>{r.get('source','')}</td>"
            f"<td>{r.get('doc_type','')}</td>"
            f"<td>{r.get('filing_date','')}</td>"
            f"<td>{r.get('case_number','')}</td>"
            f"<td>{bid}</td>"
            f"<td>{sig}</td>"
            f"</tr>\n"
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Harris County — Motivated Seller Leads</title>
<style>
  body{{font-family:Arial,sans-serif;margin:20px;background:#f5f5f5}}
  h1{{color:#2c3e50}}
  .meta{{color:#666;margin-bottom:16px}}
  table{{border-collapse:collapse;width:100%;background:#fff;box-shadow:0 1px 4px rgba(0,0,0,.15)}}
  th{{background:#2c3e50;color:#fff;padding:10px 12px;text-align:left;cursor:pointer;white-space:nowrap}}
  td{{padding:9px 12px;border-bottom:1px solid #eee;font-size:.9em}}
  tr:hover td{{background:#f0f8ff}}
</style>
</head>
<body>
<h1>🏘 Harris County (Houston, TX) — Motivated Seller Leads</h1>
<p class="meta">Last updated: {now} &nbsp;|&nbsp; Total leads: {len(records)}</p>
<table id="t">
<thead>
<tr>
  <th onclick="sortTable(0)">Score ▼</th>
  <th onclick="sortTable(1)">Owner</th>
  <th onclick="sortTable(2)">Address</th>
  <th onclick="sortTable(3)">Source</th>
  <th onclick="sortTable(4)">Type</th>
  <th onclick="sortTable(5)">Date</th>
  <th onclick="sortTable(6)">Case #</th>
  <th onclick="sortTable(7)">Bid / Value</th>
  <th onclick="sortTable(8)">Signals</th>
</tr>
</thead>
<tbody>
{rows if rows else '<tr><td colspan="9" style="text-align:center;padding:30px;color:#999">No leads found — check Actions logs</td></tr>'}
</tbody>
</table>
<script>
function sortTable(col){{
  const t=document.getElementById('t'),rows=[...t.tBodies[0].rows];
  const asc=t.dataset.sort==col;t.dataset.sort=asc?'':col;
  rows.sort((a,b)=>{{
    const x=a.cells[col].innerText,y=b.cells[col].innerText;
    return asc?(isNaN(x)?x>y?1:-1:+x-+y):(isNaN(y)?y>x?1:-1:+y-+x);
  }});
  rows.forEach(r=>t.tBodies[0].append(r));
}}
</script>
</body>
</html>"""
    path.write_text(html, encoding="utf-8")
    log.info("Wrote %s", path)

# ── Main ───────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days",  type=int, default=7)
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("=== Harris County Scraper v5.1 starting ===")
    log.info("Days back: %d | Limit: %d", args.days, args.limit)

    all_records = []
    all_records += scrape_tax_sale()
    all_records += scrape_clerk_foreclosures()

    for r in all_records:
        score_lead(r)

    all_records.sort(key=lambda x: x.get("score", 0), reverse=True)
    all_records = all_records[:args.limit]

    log.info("=== Total leads: %d ===", len(all_records))

    write_json(all_records)
    write_ghl_csv(all_records)
    write_dashboard(all_records)

    log.info("=== Done ===")

if __name__ == "__main__":
    main()
