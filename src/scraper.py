#!/usr/bin/env python3
"""
Lucas County Motivated Seller Lead Scraper — v3
================================================
Public sources only (no login required):
  1. Lucas County Sheriff Sales   — foreclosure listings
  2. Lucas County Domestic Docket — divorce / domestic filings
  3. iCare portal                 — code violations / blight notices

Distress scoring (max 100 pts):
  Foreclosure / Sheriff Sale  +30
  Code violation              +25
  Probate / domestic filing   +20
  Multiple records            +15
  Divorce / bankruptcy        +10
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

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# ── helpers ────────────────────────────────────────────────────────────────

def get(url, **kw):
    try:
        r = requests.get(url, headers=HEADERS, timeout=30, **kw)
        r.raise_for_status()
        return r
    except Exception as e:
        log.warning("GET %s → %s", url, e)
        return None

def post(url, data, **kw):
    try:
        r = requests.post(url, headers=HEADERS, data=data, timeout=30, **kw)
        r.raise_for_status()
        return r
    except Exception as e:
        log.warning("POST %s → %s", url, e)
        return None

def clean(s):
    return " ".join(str(s).split()) if s else ""

# ── 1. Sheriff Sales scraper ───────────────────────────────────────────────

SHERIFF_URL = "http://lcapps.co.lucas.oh.us/foreclosure/"

def scrape_sheriff(days_back=7):
    log.info("Sheriff Sales → %s", SHERIFF_URL)
    r = get(SHERIFF_URL)
    if not r:
        log.warning("Sheriff Sales: no response")
        return []

    soup = BeautifulSoup(r.text, "lxml")
    log.info("Sheriff Sales page title: %s", soup.title.string if soup.title else "n/a")

    # Log first 2000 chars to understand structure
    log.debug("Sheriff HTML snippet: %s", r.text[:2000])

    records = []
    cutoff = datetime.now() - timedelta(days=days_back)

    # Try common table patterns
    tables = soup.find_all("table")
    log.info("Sheriff: found %d table(s)", len(tables))

    for table in tables:
        rows = table.find_all("tr")
        for row in rows[1:]:  # skip header
            cells = [clean(td.get_text()) for td in row.find_all("td")]
            if len(cells) < 3:
                continue
            rec = {
                "source": "Sheriff Sale",
                "raw": cells,
                "owner": cells[1] if len(cells) > 1 else "",
                "address": cells[2] if len(cells) > 2 else "",
                "case_number": cells[0] if cells else "",
                "filing_date": cells[3] if len(cells) > 3 else "",
                "doc_type": "FORECLOSURE",
                "score": 0,
                "signals": ["foreclosure"],
            }
            if rec["owner"] or rec["address"]:
                records.append(rec)

    # Also try list/div patterns if no table rows found
    if not records:
        items = soup.find_all(["li", "div"], class_=re.compile(r"(row|item|record|case)", re.I))
        log.info("Sheriff: found %d list/div items", len(items))
        for item in items:
            text = clean(item.get_text())
            if text:
                records.append({
                    "source": "Sheriff Sale",
                    "raw": [text],
                    "owner": text[:60],
                    "address": "",
                    "case_number": "",
                    "filing_date": "",
                    "doc_type": "FORECLOSURE",
                    "score": 0,
                    "signals": ["foreclosure"],
                })

    log.info("Sheriff Sales → %d record(s)", len(records))
    return records

# ── 2. Domestic Relations Docket ───────────────────────────────────────────

DOMESTIC_URL = "https://lucapps.co.lucas.oh.us/onlinedockets/Default.aspx"

def scrape_domestic(days_back=7):
    log.info("Domestic Docket → %s", DOMESTIC_URL)
    r = get(DOMESTIC_URL)
    if not r:
        log.warning("Domestic: no response")
        return []

    soup = BeautifulSoup(r.text, "lxml")
    log.info("Domestic page title: %s", soup.title.string if soup.title else "n/a")
    log.debug("Domestic HTML snippet: %s", r.text[:2000])

    records = []
    tables = soup.find_all("table")
    log.info("Domestic: found %d table(s)", len(tables))

    for table in tables:
        rows = table.find_all("tr")
        for row in rows[1:]:
            cells = [clean(td.get_text()) for td in row.find_all("td")]
            if len(cells) < 2:
                continue
            rec = {
                "source": "Domestic Docket",
                "raw": cells,
                "owner": cells[0] if cells else "",
                "address": "",
                "case_number": cells[1] if len(cells) > 1 else "",
                "filing_date": cells[2] if len(cells) > 2 else "",
                "doc_type": "DOMESTIC",
                "score": 0,
                "signals": ["divorce"],
            }
            if rec["owner"]:
                records.append(rec)

    log.info("Domestic Docket → %d record(s)", len(records))
    return records

# ── 3. iCare code violations ───────────────────────────────────────────────

ICARE_URL = "https://icare.toledo.oh.gov/icomplain/Home.aspx"

def scrape_icare():
    log.info("iCare → %s", ICARE_URL)
    r = get(ICARE_URL)
    if not r:
        log.warning("iCare: no response")
        return []

    soup = BeautifulSoup(r.text, "lxml")
    log.info("iCare page title: %s", soup.title.string if soup.title else "n/a")

    vs  = soup.find("input", {"id": "__VIEWSTATE"})
    ev  = soup.find("input", {"id": "__EVENTVALIDATION"})
    vsv = vs["value"]  if vs  else ""
    evv = ev["value"]  if ev  else ""

    log.info("iCare: ViewState present=%s, EventValidation present=%s", bool(vsv), bool(evv))

    payload = {
        "__VIEWSTATE":       vsv,
        "__EVENTVALIDATION": evv,
        "ctl00$ContentPlaceHolder1$btnSearch": "Search",
        "ctl00$ContentPlaceHolder1$ddlStatus": "Open",
    }
    r2 = post(ICARE_URL, payload)
    if not r2:
        return []

    soup2 = BeautifulSoup(r2.text, "lxml")
    records = []
    tables = soup2.find_all("table")
    log.info("iCare results: found %d table(s)", len(tables))

    for table in tables:
        rows = table.find_all("tr")
        for row in rows[1:]:
            cells = [clean(td.get_text()) for td in row.find_all("td")]
            if len(cells) < 2:
                continue
            rec = {
                "source": "iCare",
                "raw": cells,
                "owner": cells[1] if len(cells) > 1 else "",
                "address": cells[2] if len(cells) > 2 else cells[0],
                "case_number": cells[0] if cells else "",
                "filing_date": cells[3] if len(cells) > 3 else "",
                "doc_type": "CODE_VIOLATION",
                "score": 0,
                "signals": ["code_violation"],
            }
            if rec["address"]:
                records.append(rec)

    log.info("iCare → %d record(s)", len(records))
    return records

# ── Scoring ────────────────────────────────────────────────────────────────

WEIGHTS = {
    "foreclosure":    30,
    "code_violation": 25,
    "probate":        20,
    "multiple_liens": 15,
    "divorce":        10,
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
        "source": "Lucas County Sheriff / Domestic / iCare",
        "total": len(records),
        "records": records,
    }
    path.write_text(json.dumps(payload, indent=2, default=str))
    log.info("Wrote %s (%d records)", path, len(records))


def write_ghl_csv(records):
    path = DATA / "ghl_export.csv"
    fields = ["owner", "address", "source", "doc_type", "filing_date",
              "case_number", "score", "signals"]
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
        rows += (
            f"<tr>"
            f"<td style='color:{color};font-weight:bold'>{score}</td>"
            f"<td>{r.get('owner','')}</td>"
            f"<td>{r.get('address','')}</td>"
            f"<td>{r.get('source','')}</td>"
            f"<td>{r.get('doc_type','')}</td>"
            f"<td>{r.get('filing_date','')}</td>"
            f"<td>{r.get('case_number','')}</td>"
            f"<td>{sig}</td>"
            f"</tr>\n"
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lucas County — Motivated Seller Leads</title>
<style>
  body{{font-family:Arial,sans-serif;margin:20px;background:#f5f5f5}}
  h1{{color:#2c3e50}}
  .meta{{color:#666;margin-bottom:16px}}
  table{{border-collapse:collapse;width:100%;background:#fff;box-shadow:0 1px 4px rgba(0,0,0,.15)}}
  th{{background:#2c3e50;color:#fff;padding:10px 12px;text-align:left;cursor:pointer}}
  td{{padding:9px 12px;border-bottom:1px solid #eee;font-size:.9em}}
  tr:hover td{{background:#f0f8ff}}
  .badge{{display:inline-block;padding:2px 8px;border-radius:12px;color:#fff;font-size:.8em}}
</style>
</head>
<body>
<h1>🏘 Lucas County — Motivated Seller Leads</h1>
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
  <th onclick="sortTable(7)">Signals</th>
</tr>
</thead>
<tbody>
{rows if rows else '<tr><td colspan="8" style="text-align:center;padding:30px;color:#999">No leads found — check Actions logs</td></tr>'}
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

    log.info("=== Lucas County Scraper v3 starting ===")
    log.info("Days back: %d | Limit: %d", args.days, args.limit)

    all_records = []
    all_records += scrape_sheriff(days_back=args.days)
    all_records += scrape_domestic(days_back=args.days)
    all_records += scrape_icare()

    # Remove raw field (too verbose) and score
    for r in all_records:
        r.pop("raw", None)
        score_lead(r)

    # Sort by score
    all_records.sort(key=lambda x: x.get("score", 0), reverse=True)

    # Apply limit
    all_records = all_records[:args.limit]

    log.info("=== Total leads: %d ===", len(all_records))

    write_json(all_records)
    write_ghl_csv(all_records)
    write_dashboard(all_records)

    log.info("=== Done ===")

if __name__ == "__main__":
    main()
