#!/usr/bin/env python3
"""
AQI.in Widget Scraper — Core Engine (v2 - HTTPX Optimized).

🎯 KEY DISCOVERY: Widget URLs bypass Cloudflare completely!
→ httpx only — no Playwright needed
→ 540 URLs × 0.1-0.2s avg = ~15-30 seconds total
→ 0 Cloudflare challenge, full data (AQI + 6 pollutants + weather)

Usage:
    from scraper_core import scrape_urls_sync, scrape_urls_async, scrape_in_batches
"""

import base64
import json
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import List, Optional

import httpx
from bs4 import BeautifulSoup

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/121.0.6167.160 Mobile/15E148 Safari/604.1"
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Referer": "https://www.aqi.in/vi/dashboard/",
    }

logger = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────
MAX_CONCURRENT = 2
REQUEST_TIMEOUT = 15.0
REQUEST_DELAY_MIN = 0.3
REQUEST_DELAY_MAX = 1.0
MAX_RETRIES = 3
WORKERS = 2

# ─── Dataclasses ────────────────────────────────────────────────────────

@dataclass
class PollutantReading:
    parameter: str   # pm25, pm10, co, so2, no2, o3
    value: float
    unit: str        # µg/m³, ppb

@dataclass
class LocationData:
    station_name: str
    aqi: int
    aqi_status: str = ""
    timestamp_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_local: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    temperature: Optional[float] = None
    humidity: Optional[float] = None
    wind_speed: Optional[float] = None
    pollutants: List[PollutantReading] = field(default_factory=list)
    raw_payload: str = ""
    success: bool = True
    error: Optional[str] = None


# ─── Widget URL Generator ─────────────────────────────────────────────

def generate_widget_url(location_path: str) -> str:
    """
    Generate AQI.in widget URL for a Vietnam location.
    location_path: 'ha-noi/hanoi' or full dashboard path
    """
    path = location_path
    if 'dashboard/' in path:
        path = path.split('dashboard/')[-1]
        
    config = {
        "o_w": 1, "o_w_t_u": "c", "o_t": "l", "o_a_s": "us", "w_t_i": 2,
        "ls": [{"s": path}]
    }
    encoded = base64.b64encode(json.dumps(config, separators=(',', ':')).encode()).decode()
    return f"https://www.aqi.in/widget?p={encoded}"


# ─── HTML Parser ────────────────────────────────────────────────────────

def parse_widget_html(html: str, station_id: str, url: str = "") -> LocationData:
    """Parse AQI.in widget HTML → extract AQI + 6 pollutants + weather."""
    soup = BeautifulSoup(html, 'html.parser')
    now = datetime.now(timezone.utc)

    station_name = "Unknown"
    aqi = 0
    temperature = None
    humidity = None
    pollutants: List[PollutantReading] = []

    try:
        # 1. Station name only
        loc_elem = soup.select_one('header p.line-clamp-2')
        if loc_elem:
            station_name = loc_elem.get_text(strip=True)

        # 2. AQI value
        aqi_elem = soup.find('span', class_=lambda c: c and 'text-[3.5em]' in c)
        if aqi_elem:
            text = aqi_elem.get_text(strip=True)
            if text.isdigit():
                aqi = int(text)

        # 3. Pollutants - Precise div-based approach
        for div in soup.select('div.flex.flex-col.items-center.justify-center'):
            name_span = div.select_one('span.opacity-60')
            val_span = div.select_one('span.truncate')
            if not (name_span and val_span):
                continue
                
            name = name_span.get_text(strip=True).upper()
            val_text = val_span.get_text(strip=True)
            param = None
            
            if "CO" == name: param = "co"
            elif "NO2" in name or "NO₂" in name: param = "no2"
            elif "O3" in name or "O₃" in name: param = "o3"
            elif "SO2" in name or "SO₂" in name: param = "so2"
            elif "PM10" in name or "PM₁₀" in name: param = "pm10"
            elif "PM2.5" in name or "PM₂.₅" in name: param = "pm25"
            
            if param:
                try:
                    # Extracts number from "228ppb" or "9µg/m³"
                    # We look for the first part that is numeric-ish
                    import re
                    match = re.search(r"([0-9.,]+)", val_text)
                    if match:
                        val = float(match.group(1).replace(',', ''))
                        unit = val_text.replace(match.group(1), '').strip()
                        pollutants.append(PollutantReading(parameter=param, value=val, unit=unit))
                except (ValueError, IndexError):
                    continue

        # 4. Weather 
        # Pattern: span with 'font-bold' containing °C or %
        for w_span in soup.find_all('span', class_=lambda c: c and 'font-bold' in c):
            text = w_span.get_text(strip=True)
            try:
                if '°C' in text:
                    temperature = float(text.replace('°C', '').strip())
                elif '%' in text:
                    humidity = float(text.replace('%', '').strip())
            except ValueError:
                pass

    except Exception as e:
        logger.warning(f"Parse error for {station_id}: {e}")

    success = aqi > 0
    return LocationData(
        station_name=station_name,
        aqi=aqi,
        timestamp_utc=now,
        temperature=temperature,
        humidity=humidity,
        pollutants=pollutants,
        raw_payload=html,
        success=success,
        error=None if success else "No AQI data found"
    )

# ─── Fetcher Config ────────────────────────────────────────────────────────

def fetch_one(location_id: str, client: httpx.Client) -> tuple:
    """Fetch AQI data for one location, with exponential backoff retry on 403/429."""
    # 1. Add randomized polite delay
    time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
    
    url = generate_widget_url(location_id)
    last_error = None

    for attempt in range(MAX_RETRIES):
        try:
            r = client.get(url, headers=get_headers(), timeout=REQUEST_TIMEOUT)

            if r.status_code == 200:
                return location_id, parse_widget_html(r.text, location_id, url=url)

            elif r.status_code in (403, 429):
                # 2. Substantial cool-down for rate limits
                # First attempt: short backoff, Second+: long wait (30-60s)
                cool_down = 30 + random.uniform(0, 30) if attempt > 0 else (5 + random.uniform(0, 5))
                logger.warning(
                    f"[{location_id}] HTTP {r.status_code} on attempt {attempt + 1}/{MAX_RETRIES}, "
                    f"cooling down {cool_down:.1f}s"
                )
                time.sleep(cool_down)
                last_error = f"Rate limited: {r.status_code}"
                continue  # retry

            else:
                raise Exception(f"HTTP {r.status_code}")

        except Exception as e:
            last_error = str(e)
            if attempt < MAX_RETRIES - 1:
                time.sleep((2 ** attempt) + random.uniform(0, 0.5))

    # All retries exhausted
    return location_id, LocationData(
        station_name="Unknown",
        aqi=0,
        success=False,
        error=f"Failed after {MAX_RETRIES} retries: {last_error}",
        raw_payload=str(last_error),
    )

# ─── Public APIs ────────────────────────────────────────────────────────

def scrape_urls_sync(url_paths: List[str], workers: int = WORKERS, progress_callback=None) -> List[tuple]:
    """Scrape URLs synchronously. Each thread gets its own httpx.Client to avoid
    HTTP/2 connection sharing (which triggers 403 from AQI.in WAF)."""
    results = []
    total = len(url_paths)

    def fetch_with_own_client(path: str) -> tuple:
        with httpx.Client(http2=True, timeout=REQUEST_TIMEOUT) as client:
            return fetch_one(path, client)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_with_own_client, path): path for path in url_paths}
        for future in as_completed(futures):
            station_id, res = future.result()
            results.append((station_id, res))
            if progress_callback:
                progress_callback(len(results), total, station_id, res)
    return results

def scrape_in_batches(url_paths: List[str], batch_size: int = 20, batch_delay: float = 2.0, workers: int = WORKERS, progress_callback=None) -> List[tuple]:
    all_results = []
    for i in range(0, len(url_paths), batch_size):
        batch = url_paths[i:i + batch_size]
        results = scrape_urls_sync(batch, workers=workers, progress_callback=progress_callback)
        all_results.extend(results)
        if i + batch_size < len(url_paths):
            time.sleep(batch_delay + random.uniform(0, 1))
    return all_results

if __name__ == "__main__":
    test_paths = ["ha-noi/hanoi", "ho-chi-minh/ho-chi-minh-city"]
    print(f"Testing scraper for {test_paths}...")
    results = scrape_urls_sync(test_paths)
    for sid, r in results:
        print(f"\n[{'✅' if r.success else '❌'}] {r.station_name} (ID: {sid}, AQI: {r.aqi})")
        print(f"Pollutants: {[(p.parameter, p.value) for p in r.pollutants]}")
        print(f"Weather: {r.temperature}°C, {r.humidity}%")
