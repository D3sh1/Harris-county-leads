#!/usr/bin/env python3
"""
Lucas County Motivated Seller Lead Scraper  —  v2 (merged)
===========================================================
Combines Playwright browser automation with iCare code-violation
enrichment, ParcelLookup via auditor bulk DBF, and an auto-generated
HTML dashboard + GoHighLevel CSV export.

Output files:
  /data/output.json      — JSON array of all leads
  /data/ghl_export.csv   — GoHighLevel CRM import
  /dashboard/index.html  — sortable HTML dashboard
"""

import argparse
import asyncio
import csv
import json
import logging
import os
import re
import sys
import tempfile
import time
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright, Page
    from playwright.async_api import TimeoutError as PlaywrightTimeout
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False


ROOT          = Path(__file__).resolve().parent.parent
DATA_DIR      = ROOT / "data"
DASHBOARD_DIR = ROOT / "dashboard"
OUTPUT_JSON   = DATA_DIR / "output.json"
GHL_CSV       = DATA_DIR / "ghl_export.csv"
DASHBOARD_HTML= DASHBOARD_DIR / "index.html"

CLERK_URL     = "https://www.co.lucas.oh.us/83/Clerk-of-Courts"
AUDITOR_BASE  = "https://www.lucascountyohioauditor.gov"
ICARE_URL     = "http://icare.co.lucas.oh.us/LucasCare/search/commonsearch.aspx"

RETRY_COUNT   = 3
RETRY_WAIT    = 3
REQ_DELAY     = 1.2
REQ_TIMEOUT   = 30

DOC_CATEGORIES = {
    "LP":       ("foreclosure",   "Lis Pendens"),
    "NOFC":     ("foreclosure",   "Notice of Foreclosure"),
    "TAXDEED":  ("tax",           "Tax Deed"),
    "JUD":      ("judgment",      "Judgment"),
    "CCJ":      ("judgment",      "Certified Judgment"),
    "DRJUD":    ("divorce",       "Domestic Judgment"),
    "LNCORPTX": ("tax",           "Corp Tax Lien"),
    "LNIRS":    ("tax",           "IRS Lien"),
    "LNFED":    ("tax",           "Federal Lien"),
    "LN":       ("lien",          "Lien"),
    "LNMECH":   ("lien",          "Mechanic Lien"),
    "LNHOA":    ("lien",          "HOA Lien"),
    "MEDLN":    ("lien",          "Medicaid Lien"),
    "PRO":      ("probate",       "Probate Document"),
    "NOC":      ("commencement",  "Notice of Commencement"),
    "RELLP":    ("release",       "Release of Lis Pendens"),
}

GUIDE_WEIGHTS = {
    "tax_delinquent":       30,
    "code_violation":       25,
    "probate_filing":       20,
    "multiple_liens":       15,
    "divorce_or_bankruptcy":10,
}
BONUS_WEIGHTS = {"high_amount": 5, "new_this_week": 3, "corp_owner": 3}

TAX_DOC_CODES     = {"TAXDEED", "LNCORPTX", "LNIRS", "LNFED"}
PROBATE_DOC_CODES = {"PRO"}
DIVORCE_DOC_CODES = {"DRJUD", "LP", "NOFC"}
LIEN_DOC_CODES    = {"LN", "LNMECH", "LNHOA", "MEDLN", "JUD", "CCJ"}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("lucas")


@dataclass
class DistressSignals:
    tax_delinquent:        bool = False
    code_violation:        bool = False
    probate_filing:        bool = False
    multiple_liens:        bool = False
    divorce_or_bankruptcy: bool = False
    high_amount:   bool = False
    new_this_week: bool = False
    corp_owner:    bool = False


@dataclass
class Lead:
    document_number:   str = ""
    doc_type_code:     str = ""
    doc_type_label:    str = ""
    doc_category:      str = ""
    file_date:         str = ""
    grantor:           str = ""
    grantee:           str = ""
    legal_description: str = ""
    amount:            str = ""
    clerk_url:         str = ""
    property_address:  str = ""
    property_city:     str = ""
    property_state:    str = "OH"
    property_zip:      str = ""
    mail_address:      str = ""
    mail_city:         str = ""
    mail_state:        str = ""
    mail_zip:          str = ""
    code_violation_ids: List[str] = field(default_factory=list)
    signals:     DistressSignals = field(default_factory=DistressSignals)
    flags:       List[str]       = field(default_factory=list)
    seller_score: int            = 0
    sources:    List[str] = field(default_factory=list)
    scraped_at: str       = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")


_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}
_last_req: float = 0.0


def _throttle():
    global _last_req
    elapsed = time.time() - _last_req
    if elapsed < REQ_DELAY:
        time.sleep(REQ_DELAY - elapsed)
    _last_req = time.time()


def _get(url, **kwargs):
    for attempt in range(1, RETRY_COUNT + 1):
        _throttle()
        try:
            r = requests.get(url, headers=_HEADERS, timeout=REQ_TIMEOUT, **kwargs)
            r.raise_for_status()
            return r
        except Exception as exc:
            log.warning("GET %d/%d %s: %s", attempt, RETRY_COUNT, url, exc)
            if attempt < RETRY_COUNT:
                time.sleep(RETRY_WAIT)
    return None


def _post(url, data, **kwargs):
    for attempt in range(1, RETRY_COUNT + 1):
        _throttle()
        try:
            r = requests.post(url, headers=_HEADERS, data=data, timeout=REQ_TIMEOUT * 2, **kwargs)
            r.raise_for_status()
            return r
        except Exception as exc:
            log.warning("POST %d/%d %s: %s", attempt, RETRY_COUNT, url, exc)
            if attempt < RETRY_COUNT:
                time.sleep(RETRY_WAIT)
    return None


def _soup(resp):
    try:
        return BeautifulSoup(resp.text, "lxml")
    except Exception:
        return BeautifulSoup(resp.text, "html.parser")


def _text(tag):
    return tag.get_text(separator=" ", strip=True) if tag else ""


class ParcelLookup:
    _COL = {
        "owner":    ("OWNER", "OWN1", "OWNNAME"),
        "site_adr": ("SITE_ADDR", "SITEADDR", "SITEADDRESS"),
        "site_cty": ("SITE_CITY", "SITECITY"),
        "site_zip": ("SITE_ZIP",  "SITEZIP"),
        "mail_adr": ("ADDR_1", "MAILADR1", "MAILADDR1"),
        "mail_cty": ("CITY",   "MAILCITY"),
        "mail_st":  ("STATE",  "MAILSTATE"),
        "mail_zip": ("ZIP",    "MAILZIP"),
    }

    def __init__(self):
        self._index = {}
        self.loaded = False

    def load(self):
        if not HAS_DBF:
            log.warning("[Parcel] dbfread unavailable.")
            return
        try:
            dbf_path = self._download()
            if dbf_path:
                self._index_dbf(dbf_path)
                log.info("[Parcel] Indexed %d records.", len(self._index))
                self.loaded = True
        except Exception as exc:
            log.error("[Parcel] %s", exc)

    def lookup(self, owner):
        if not self.loaded or not owner:
            return {}
        for key in self._name_variants(owner):
            hit = self._index.get(key)
            if hit:
                return hit
        return {}

    @staticmethod
    def _name_variants(name):
        n = name.upper().strip()
        v = [n]
        if "," in n:
            p = [x.strip() for x in n.split(",", 1)]
            v += [f"{p[1]} {p[0]}", f"{p[0]} {p[1]}"]
        else:
            p = n.split()
            if len(p) >= 2:
                v += [f"{p[-1]}, {' '.join(p[:-1])}", f"{p[-1]} {' '.join(p[:-1])}"]
        return v

    def _download(self):
        for path in ["/downloads/parcel_data.zip", "/gis/downloads/parcel.zip", "/GIS/Downloads/Parcel.zip"]:
            r = _get(AUDITOR_BASE + path)
            if r and len(r.content) > 10_000:
                return self._save(r.content)
        for page in ("/gis", ""):
            r = _get(AUDITOR_BASE + page)
            if not r:
                continue
            for a in _soup(r).find_all("a", href=True):
                href = a["href"]
                if any(k in href.lower() for k in ("parcel", "property", "bulk")) and href.lower().endswith(".zip"):
                    url = href if href.startswith("http") else AUDITOR_BASE + href
                    dl = _get(url)
                    if dl and len(dl.content) > 10_000:
                        return self._save(dl.content)
        return None

    @staticmethod
    def _save(content):
        tmp = Path(tempfile.mkdtemp()) / "parcel.zip"
        tmp.write_bytes(content)
        out = tmp.parent / "extracted"
        out.mkdir(exist_ok=True)
        try:
            with zipfile.ZipFile(tmp) as zf:
                for name in zf.namelist():
                    if name.lower().endswith(".dbf"):
                        zf.extract(name, out)
                        return out / name
        except zipfile.BadZipFile:
            pass
        return None

    def _fc(self, row, key):
        for c in self._COL[key]:
            if c in row:
                return str(row[c] or "").strip()
        return ""

    def _index_dbf(self, dbf_path):
        for rec in DBF(str(dbf_path), encoding="latin-1", lowernames=False):
            try:
                row = {k.upper(): v for k, v in dict(rec).items()}
                owner = self._fc(row, "owner")
                if not owner:
                    continue
                entry = {
                    "property_address": self._fc(row, "site_adr"),
                    "property_city":    self._fc(row, "site_cty") or "Toledo",
                    "property_state":   "OH",
                    "property_zip":     self._fc(row, "site_zip"),
                    "mail_address":     self._fc(row, "mail_adr"),
                    "mail_city":        self._fc(row, "mail_cty"),
                    "mail_state":       self._fc(row, "mail_st") or "OH",
                    "mail_zip":         self._fc(row, "mail_zip"),
                }
                for v in self._name_variants(owner):
                    self._index.setdefault(v, entry)
            except Exception:
                continue


class ICareScraper:
    def get_violations(self, address):
        if not address:
            return []
        try:
            tokens = self._get_tokens()
            if not tokens:
                return []
            return self._search(address, tokens)
        except Exception as exc:
            log.debug("[iCare] %s", exc)
            return []

    def _get_tokens(self):
        resp = _get(ICARE_URL, params={"mode": "address"})
        if not resp:
            return {}
        bs = _soup(resp)
        out = {}
        for sel, key in [
            ("input#__VIEWSTATE",         "__VIEWSTATE"),
            ("input#__EVENTVALIDATION",   "__EVENTVALIDATION"),
            ("input#__VIEWSTATEGENERATOR","__VIEWSTATEGENERATOR"),
        ]:
            tag = bs.select_one(sel)
            if tag:
                out[key] = tag.get("value", "")
        return out

    def _search(self, address, tokens):
        data = {**tokens,
                "ctl00$ContentPlaceHolder1$txtAddress": address,
                "ctl00$ContentPlaceHolder1$btnSearch":  "Search"}
        resp = _post(ICARE_URL, data=data, params={"mode": "address"})
        if not resp:
            return []
        ids = []
        for row in _soup(resp).select("table#GridView1 tbody tr, table.rgMasterTable tbody tr"):
            cells = row.find_all("td")
            if cells:
                cid = _text(cells[0])
                if cid and cid.lower() not in ("case id", "case number", ""):
                    ids.append(cid)
        return ids


class ClerkScraper:
    def __init__(self, headless=True):
        self.headless = headless

    async def scrape(self, days=7, limit=500):
        if not HAS_PLAYWRIGHT:
            log.error("[Clerk] Playwright not installed.")
            return []
        date_from = (datetime.now() - timedelta(days=days)).strftime("%m/%d/%Y")
        date_to   = datetime.now().strftime("%m/%d/%Y")
        leads = []
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=self.headless,
                                                args=["--no-sandbox", "--disable-dev-shm-usage"])
            ctx = await browser.new_context(user_agent=_HEADERS["User-Agent"])
            page = await ctx.new_page()
            try:
                for code, (cat, label) in DOC_CATEGORIES.items():
                    if len(leads) >= limit:
                        break
                    log.info("[Clerk] %s (%s)", code, label)
                    try:
                        recs = await self._query(page, code, cat, label, date_from, date_to)
                        leads.extend(recs)
                    except Exception as exc:
                        log.warning("[Clerk] %s: %s", code, exc)
            finally:
                await browser.close()
        return leads

    async def _query(self, page, code, cat, label, df, dt):
        for url in [
            f"https://www.co.lucas.oh.us/DocumentSearch?docType={code}&fromDate={df}&toDate={dt}",
            f"https://clerk.co.lucas.oh.us/search?type={code}&start={df}&end={dt}",
        ]:
            try:
                await page.goto(url, wait_until="networkidle", timeout=20_000)
                leads = await self._parse(page, code, cat, label)
                if leads:
                    return leads
            except Exception:
                pass
        try:
            await page.goto(CLERK_URL, wait_until="networkidle", timeout=30_000)
            await self._fill_form(page, code, df, dt)
            return await self._parse(page, code, cat, label)
        except Exception:
            return []

    @staticmethod
    async def _fill_form(page, code, df, dt):
        for sel in ["#docType", "input[name='docType']", "select[name='docType']", "#DocumentType"]:
            el = await page.query_selector(sel)
            if el:
                tag = (await el.get_attribute("tagName") or "").upper()
                if tag == "SELECT":
                    await el.select_option(value=code)
                else:
                    await el.fill(code)
                break
        for sel in ["#fromDate", "input[name='fromDate']", "#StartDate"]:
            el = await page.query_selector(sel)
            if el:
                await el.fill(df)
                break
        for sel in ["#toDate", "input[name='toDate']", "#EndDate"]:
            el = await page.query_selector(sel)
            if el:
                await el.fill(dt)
                break
        for sel in ["button[type='submit']", "input[type='submit']", "#search", "#btnSearch"]:
            el = await page.query_selector(sel)
            if el:
                await el.click()
                await page.wait_for_load_state("networkidle", timeout=15_000)
                break

    async def _parse(self, page, code, cat, label):
        leads = []
        content = await page.content()
        bs = BeautifulSoup(content, "lxml")
        for table in bs.find_all("table"):
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if not headers or not any(kw in " ".join(headers) for kw in ("doc", "gran", "file", "date")):
                continue
            for row in table.find_all("tr")[1:]:
                lead = self._row(row, headers, code, cat, label, page.url)
                if lead:
                    leads.append(lead)
        return leads

    def _row(self, row, headers, code, cat, label, base_url):
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            return None
        vals = [c.get_text(strip=True) for c in cells]

        def _col(*keys):
            for k in keys:
                if k in headers:
                    idx = headers.index(k)
                    return vals[idx] if idx < len(vals) else ""
            return ""

        link = row.find("a", href=True)
        href = urljoin(base_url, link["href"]) if link else base_url
        doc_num = _col("doc #", "doc number", "document", "instrument")
        grantor = _col("grantor", "owner", "seller", "from")
        if not doc_num and not grantor:
            return None
        return Lead(
            document_number=doc_num or vals[0], doc_type_code=code,
            doc_type_label=label, doc_category=cat,
            file_date=_col("date", "filed", "file date", "recorded"),
            grantor=grantor, grantee=_col("grantee", "buyer", "to"),
            amount=_col("amount", "consideration", "debt"),
            legal_description=_col("legal", "description", "parcel"),
            clerk_url=href, sources=["Lucas County Clerk"],
        )


def _parse_amount(s):
    try:
        return float(re.sub(r"[^\d.]", "", s))
    except (ValueError, TypeError):
        return 0.0


def _is_recent(filed, days):
    cutoff = datetime.now() - timedelta(days=days)
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(filed.strip(), fmt) >= cutoff
        except ValueError:
            continue
    return False


def score_lead(lead, lookback_days=7):
    sig = lead.signals
    code = lead.doc_type_code
    owner_upper = lead.grantor.upper()
    amt = _parse_amount(lead.amount)

    if code in TAX_DOC_CODES:
        sig.tax_delinquent = True
        lead.flags.append(lead.doc_type_label)
    if code in PROBATE_DOC_CODES:
        sig.probate_filing = True
        lead.flags.append("Probate / estate")
    if code in DIVORCE_DOC_CODES:
        sig.divorce_or_bankruptcy = True
        lead.flags.append("Foreclosure / lis pendens" if code in ("LP", "NOFC") else "Domestic judgment")
    if code in LIEN_DOC_CODES:
        lead.flags.append(lead.doc_type_label)
    if lead.code_violation_ids:
        sig.code_violation = True
        lead.flags.append(f"Code violation ({len(lead.code_violation_ids)})")
    if amt > 100_000:
        sig.high_amount = True
        lead.flags.append(f"High amount ${amt:,.0f}")
    if _is_recent(lead.file_date, lookback_days):
        sig.new_this_week = True
        lead.flags.append("New this week")
    if any(kw in owner_upper for kw in ("LLC", "INC", "CORP", "LTD", " LP", "L.P.", "TRUST", "ESTATE")):
        sig.corp_owner = True
        lead.flags.append("LLC / corp owner")

    s = 0
    s += GUIDE_WEIGHTS["tax_delinquent"]        if sig.tax_delinquent        else 0
    s += GUIDE_WEIGHTS["code_violation"]        if sig.code_violation        else 0
    s += GUIDE_WEIGHTS["probate_filing"]        if sig.probate_filing        else 0
    s += GUIDE_WEIGHTS["multiple_liens"]        if sig.multiple_liens        else 0
    s += GUIDE_WEIGHTS["divorce_or_bankruptcy"] if sig.divorce_or_bankruptcy else 0
    s += BONUS_WEIGHTS["high_amount"]   if sig.high_amount   else 0
    s += BONUS_WEIGHTS["new_this_week"] if sig.new_this_week else 0
    s += BONUS_WEIGHTS["corp_owner"]    if sig.corp_owner    else 0
    lead.seller_score = min(s, 100)


def deduplicate_and_flag_liens(leads):
    lien_count = {}
    for lead in leads:
        if lead.doc_type_code in LIEN_DOC_CODES:
            key = lead.grantor.upper().strip()
            if key:
                lien_count[key] = lien_count.get(key, 0) + 1
    for lead in leads:
        if lien_count.get(lead.grantor.upper().strip(), 0) >= 2:
            lead.signals.multiple_liens = True
    seen = {}
    for lead in leads:
        key = lead.document_number.strip().lower() or f"{lead.grantor}|{lead.file_date}".lower()
        if key not in seen or lead.seller_score > seen[key].seller_score:
            seen[key] = lead
    return list(seen.values())


def _to_dict(lead):
    d = asdict(lead)
    d["signals"] = asdict(lead.signals)
    return d


def write_json(leads):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = [_to_dict(l) for l in leads]
    OUTPUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("[Output] JSON → %s (%d records)", OUTPUT_JSON, len(payload))


def write_ghl_csv(leads):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    cols = ["First Name", "Last Name", "Mailing Address", "Mailing City", "Mailing State",
            "Mailing Zip", "Property Address", "Property City", "Property State", "Property Zip",
            "Lead Type", "Document Type", "Date Filed", "Document Number", "Amount",
            "Seller Score", "Flags", "Source", "URL"]

    def _split(name):
        parts = name.strip().split()
        return (parts[0], " ".join(parts[1:])) if len(parts) >= 2 else (name, "")

    with open(GHL_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for lead in leads:
            first, last = _split(lead.grantor)
            w.writerow({
                "First Name": first, "Last Name": last,
                "Mailing Address": lead.mail_address, "Mailing City": lead.mail_city,
                "Mailing State": lead.mail_state, "Mailing Zip": lead.mail_zip,
                "Property Address": lead.property_address, "Property City": lead.property_city,
                "Property State": lead.property_state, "Property Zip": lead.property_zip,
                "Lead Type": lead.doc_category, "Document Type": lead.doc_type_label,
                "Date Filed": lead.file_date, "Document Number": lead.document_number,
                "Amount": lead.amount, "Seller Score": lead.seller_score,
                "Flags": " | ".join(lead.flags),
                "Source": "Lucas County Clerk", "URL": lead.clerk_url,
            })
    log.info("[Output] GHL CSV → %s", GHL_CSV)


def write_dashboard(leads):
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    records = sorted([_to_dict(l) for l in leads], key=lambda r: r["seller_score"], reverse=True)
    json_data = json.dumps(records)
    total = len(records)
    high  = sum(1 for r in records if r["seller_score"] >= 70)
    med   = sum(1 for r in records if 40 <= r["seller_score"] < 70)
    low   = sum(1 for r in records if r["seller_score"] < 40)
    gen   = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"/>
<title>Lucas County Leads</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:system-ui,sans-serif;background:#0f172a;color:#e2e8f0}}
header{{padding:2rem;background:#1e3a5f;border-bottom:1px solid #1e40af}}
h1{{color:#60a5fa}}
.stats{{display:flex;gap:1rem;padding:1rem 2rem;background:#1e293b}}
.stat{{flex:1;background:#0f172a;padding:1rem;border-radius:8px}}
.stat-val{{font-size:2rem;font-weight:700}}
table{{width:100%;border-collapse:collapse;font-size:.85rem}}
th{{background:#1e293b;padding:.6rem;text-align:left;cursor:pointer}}
td{{padding:.5rem;border-bottom:1px solid #1e293b}}
.b{{display:inline-block;padding:.2rem .5rem;border-radius:10px;font-weight:700}}
.h{{background:#7f1d1d;color:#fca5a5}}
.m{{background:#78350f;color:#fde68a}}
.l{{background:#14532d;color:#86efac}}
</style></head><body>
<header><h1>Lucas County Leads</h1><p>{gen} | {total} leads</p></header>
<div class="stats">
<div class="stat">Total<div class="stat-val">{total}</div></div>
<div class="stat">High≥70<div class="stat-val" style="color:#ef4444">{high}</div></div>
<div class="stat">Med 40-69<div class="stat-val" style="color:#f59e0b">{med}</div></div>
<div class="stat">Low<div class="stat-val" style="color:#22c55e">{low}</div></div>
</div>
<table><thead><tr><th>Score</th><th>Address</th><th>Owner</th><th>Doc Type</th><th>Filed</th></tr></thead>
<tbody id="t"></tbody></table>
<script>
const D={json_data};
document.getElementById('t').innerHTML=D.map(r=>{{
  const s=r.seller_score, c=s>=70?'h':s>=40?'m':'l';
  return `<tr><td><span class="b ${{c}}">${{s}}</span></td>
  <td>${{r.property_address||'—'}}</td><td>${{r.grantor||'—'}}</td>
  <td>${{r.doc_type_label||'—'}}</td><td>${{r.file_date||'—'}}</td></tr>`;
}}).join('');
</script></body></html>"""
    DASHBOARD_HTML.write_text(html, encoding="utf-8")
    log.info("[Output] Dashboard → %s", DASHBOARD_HTML)


async def run(days=7, limit=500, headless=True):
    log.info("=== Lucas County Lead Scraper (days=%d) ===", days)
    parcel = ParcelLookup()
    parcel.load()
    clerk = ClerkScraper(headless=headless)
    leads = await clerk.scrape(days=days, limit=limit)
    log.info("Got %d leads from clerk", len(leads))

    for lead in leads:
        hit = parcel.lookup(lead.grantor)
        if hit:
            for k, v in hit.items():
                if not getattr(lead, k, None):
                    setattr(lead, k, v)

    icare = ICareScraper()
    for i, lead in enumerate(leads, 1):
        addr = lead.property_address or lead.mail_address
        if addr:
            vids = icare.get_violations(addr)
            if vids:
                lead.code_violation_ids.extend(vids)

    for lead in leads:
        score_lead(lead, lookback_days=days)
    leads = deduplicate_and_flag_liens(leads)
    for lead in leads:
        score_lead(lead, lookback_days=days)
    leads.sort(key=lambda l: l.seller_score, reverse=True)

    write_json(leads)
    write_ghl_csv(leads)
    write_dashboard(leads)
    return leads


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days",  type=int, default=7)
    p.add_argument("--limit", type=int, default=500)
    args = p.parse_args()
    leads = asyncio.run(run(days=args.days, limit=args.limit))
    print(f"\nDone. {len(leads)} leads. Top score: {leads[0].seller_score if leads else 0}")


if __name__ == "__main__":
    main()
