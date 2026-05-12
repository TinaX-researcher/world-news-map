#!/usr/bin/env python3
"""Refresh policy rates and 10Y bond yields for the world-news-map page.

Scrapes Wikipedia for central-bank policy rates and worldgovernmentbonds.com
for 10-year sovereign bond yields, then writes a sibling data.js file that
the HTML loads at startup and merges into the inline curated defaults.

Usage:
    pip install -r requirements.txt
    python3 update_data.py
"""

import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)
HEADERS = {"User-Agent": UA}
# Yahoo blocks "browsery" UAs that look like scrapers; the short generic one works.
YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0"}

# Maps country labels from upstream sources to the ISO2 codes the HTML uses.
NAME_TO_ISO2 = {
    "United States": "US", "U.S.": "US", "USA": "US",
    "China": "CN", "People's Republic of China": "CN",
    "Japan": "JP",
    "Germany": "DE",
    "United Kingdom": "GB", "UK": "GB",
    "France": "FR",
    "Italy": "IT",
    "Spain": "ES",
    "Netherlands": "NL",
    "Switzerland": "CH",
    "Sweden": "SE",
    "Norway": "NO",
    "Denmark": "DK",
    "Finland": "FI",
    "Ireland": "IE",
    "Poland": "PL",
    "Canada": "CA",
    "Mexico": "MX",
    "Brazil": "BR",
    "Argentina": "AR",
    "Chile": "CL",
    "Colombia": "CO",
    "Peru": "PE",
    "Russia": "RU", "Russian Federation": "RU",
    "Turkey": "TR", "Türkiye": "TR", "Turkiye": "TR",
    "South Africa": "ZA",
    "India": "IN",
    "Indonesia": "ID",
    "Thailand": "TH",
    "Vietnam": "VN", "Viet Nam": "VN",
    "Malaysia": "MY",
    "Philippines": "PH",
    "Singapore": "SG",
    "South Korea": "KR", "Korea, South": "KR", "Republic of Korea": "KR",
    "Taiwan": "TW",
    "Hong Kong": "HK",
    "Australia": "AU",
    "New Zealand": "NZ",
    "United Arab Emirates": "AE", "UAE": "AE",
    "Saudi Arabia": "SA",
    "Israel": "IL",
    "Egypt": "EG",
    "Nigeria": "NG",
    "Kenya": "KE",
    "Pakistan": "PK",
    "Bangladesh": "BD",
    "Austria": "AT",
    "Belgium": "BE",
    "Portugal": "PT",
    "Greece": "GR",
    "Czech Republic": "CZ", "Czechia": "CZ",
    "Hungary": "HU",
    "Romania": "RO",
    "Ukraine": "UA",
}


def parse_pct(s: str):
    """Pull the first number out of a string like '4.25%' or '4,25 %'."""
    if not s:
        return None
    cleaned = s.replace(",", ".")
    m = re.search(r"-?\d+\.?\d*", cleaned)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def clean_country_label(s: str) -> str:
    """Strip leading flag glyphs, bracketed markers, and extra whitespace."""
    s = re.sub(r"\[[^\]]*\]", "", s)
    s = re.sub(r"^[^A-Za-z]+", "", s)
    return s.strip()


EUROZONE = {"DE", "FR", "IT", "ES", "NL", "BE", "AT", "PT", "IE", "GR", "FI"}
EUROZONE_LABELS = {"Eurozone", "European Central Bank", "Euro Area", "European Union"}

# ISO2 -> Yahoo Finance ticker for the benchmark index.
# Omitted countries (e.g. Vietnam, Nigeria, Kenya, Ukraine) lack a clean Yahoo ticker.
YAHOO_TICKERS = {
    "US": "^GSPC", "CN": "000300.SS", "JP": "^N225", "DE": "^GDAXI", "GB": "^FTSE",
    "FR": "^FCHI", "IT": "FTSEMIB.MI", "ES": "^IBEX", "NL": "^AEX", "CH": "^SSMI",
    "SE": "^OMX", "NO": "OBXP.OL", "DK": "^OMXC25", "FI": "^OMXH25", "IE": "^ISEQ",
    "PL": "WIG20.WA", "CA": "^GSPTSE", "MX": "^MXX", "BR": "^BVSP", "AR": "^MERV",
    "CL": "^IPSA", "RU": "IMOEX.ME", "TR": "XU100.IS", "IN": "^NSEI", "ID": "^JKSE",
    "TH": "^SET.BK", "MY": "^KLSE", "PH": "PSEI.PS", "SG": "^STI", "KR": "^KS11",
    "TW": "^TWII", "HK": "^HSI", "AU": "^AXJO", "NZ": "^NZ50", "SA": "^TASI.SR",
    "IL": "TA35.TA", "EG": "^CASE30", "PK": "^KSE", "AT": "^ATX", "BE": "^BFX",
    "PT": "PSI20.LS", "GR": "GD.AT", "CZ": "^PX", "HU": "^BUX",
}


def fetch_ytd(symbol: str, max_retries: int = 3):
    """Return YTD percentage change for a Yahoo ticker, or None on failure."""
    now_utc = datetime.now(timezone.utc)
    year_start = datetime(now_utc.year, 1, 1, tzinfo=timezone.utc)
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        f"?period1={int(year_start.timestamp())}"
        f"&period2={int(now_utc.timestamp())}&interval=1d"
    )
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=YAHOO_HEADERS, timeout=15)
            if r.status_code == 429:
                time.sleep(2 ** attempt + 1)
                continue
            if r.status_code != 200:
                return None
            result = r.json().get("chart", {}).get("result")
            if not result:
                return None
            closes = [c for c in result[0]["indicators"]["quote"][0].get("close", []) if c is not None]
            if len(closes) < 2:
                return None
            return round((closes[-1] / closes[0] - 1) * 100, 2)
        except Exception:
            return None
    return None


def load_existing_data(path: Path) -> dict:
    """Pull the data dict out of a previous data.js so we can preserve fields
    that this run failed to scrape."""
    if not path.exists():
        return {}
    try:
        text = path.read_text(encoding="utf-8")
        m = re.search(r"window\.MACRO_OVERRIDE\s*=\s*(\{.*\});", text, re.DOTALL)
        if not m:
            return {}
        return json.loads(m.group(1)).get("data", {}) or {}
    except Exception:
        return {}


def scrape_ytd_returns():
    """Fetch YTD returns for all known Yahoo tickers, throttled to avoid 429s."""
    out = {}
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(fetch_ytd, sym): iso for iso, sym in YAHOO_TICKERS.items()}
        for fut in as_completed(futures):
            iso = futures[fut]
            v = fut.result()
            if v is not None:
                out[iso] = v
    return out


def scrape_policy_rates():
    url = "https://en.wikipedia.org/wiki/List_of_countries_by_central_bank_interest_rates"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    rates = {}
    eurozone_rate = None
    for table in soup.select("table.wikitable"):
        for row in table.select("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            label = clean_country_label(cells[0].get_text(" ", strip=True))

            def first_rate_in_row():
                for col in cells[1:4]:
                    v = parse_pct(col.get_text(" ", strip=True))
                    if v is not None and -5 < v < 100:
                        return v
                return None

            if label in EUROZONE_LABELS and eurozone_rate is None:
                eurozone_rate = first_rate_in_row()
                continue

            iso = NAME_TO_ISO2.get(label)
            if not iso or iso in rates:
                continue
            v = first_rate_in_row()
            if v is not None:
                rates[iso] = v

    if eurozone_rate is not None:
        for iso in EUROZONE:
            rates.setdefault(iso, eurozone_rate)

    return rates


def scrape_yields():
    url = "https://countryeconomy.com/bonds"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    yields = {}
    for row in soup.select("table tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        label = clean_country_label(cells[0].get_text(" ", strip=True))
        iso = NAME_TO_ISO2.get(label)
        if not iso or iso in yields:
            continue
        # Layout on countryeconomy.com: [country, date, yield, ...]
        y = parse_pct(cells[2].get_text(" ", strip=True))
        if y is not None and -5 < y < 100:
            yields[iso] = y
    return yields


def main():
    here = Path(__file__).parent

    print("Fetching policy rates from Wikipedia...")
    try:
        rates = scrape_policy_rates()
        print(f"  {len(rates)} countries matched")
    except Exception as e:
        print(f"  failed: {e}")
        rates = {}

    print("Fetching 10Y bond yields from countryeconomy.com...")
    try:
        yields = scrape_yields()
        print(f"  {len(yields)} countries matched")
    except Exception as e:
        print(f"  failed: {e}")
        yields = {}

    print("Fetching YTD index returns from Yahoo Finance...")
    try:
        ytd = scrape_ytd_returns()
        print(f"  {len(ytd)} indices")
    except Exception as e:
        print(f"  failed: {e}")
        ytd = {}

    if not rates and not yields and not ytd:
        print("Nothing scraped; leaving data.js untouched.")
        sys.exit(1)

    # Preserve fields from the previous run that weren't scraped this time.
    # Lets the workflow tolerate a single source failing (e.g. Yahoo rate-limiting)
    # without wiping all YTD values.
    existing = load_existing_data(here / "data.js")

    merged = {}
    all_isos = set(existing) | set(rates) | set(yields) | set(ytd)
    for iso in all_isos:
        entry = dict(existing.get(iso, {}))
        if iso in rates:
            entry["rate"] = round(rates[iso], 2)
        if iso in yields:
            entry["yield10y"] = round(yields[iso], 2)
        if iso in ytd:
            entry["ytd"] = ytd[iso]
        if entry:
            merged[iso] = entry

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    payload = {"updated": today, "data": merged}
    js = (
        f"// Auto-generated by update_data.py on {today}\n"
        f"window.MACRO_OVERRIDE = {json.dumps(payload, indent=2)};\n"
    )
    out_path = here / "data.js"
    out_path.write_text(js, encoding="utf-8")
    print(f"Wrote {out_path} ({len(merged)} countries, dated {today})")


if __name__ == "__main__":
    main()
