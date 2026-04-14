#!/usr/bin/env python3
"""
AQI.in API Scraper — Core Engine (v3 - JSON API Optimized).

🎯 KEY DISCOVERY: The official JSON API provides richer data and is more stable than widgets.
→ httpx + JSON parsing
→ Faster, lower bandwidth
→ Full data (AQI + pollutants + detailed weather + lat/long)

Usage:
    from scraper_core import scrape_urls_sync, scrape_urls_async, scrape_in_batches
"""

import logging
import random
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

import httpx

# ─── Config ────────────────────────────────────────────────────────────
API_ENDPOINT = "https://apiserver.aqi.in/aqi/v3/getLocationDetailsBySlug"
# Token provided by user (valid until 2026-04-20)
DEFAULT_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySUQiOjEsImlhdCI6MTc3NjEyODg1NSwiZXhwIjoxNzc2NzMzNjU1fQ.SvCWKEgmBagGRy8sGMYuAYgNU_ZCKzp_BHqh7Hh6X0E"
AQIIN_TOKEN = os.environ.get("AQIIN_TOKEN", DEFAULT_TOKEN)

# ─── Config ────────────────────────────────────────────────────────────
API_ENDPOINT = "https://apiserver.aqi.in/aqi/v3/getLocationDetailsBySlug"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
]

# Token provided by user (Should be set as AQIIN_TOKEN environment variable/secret)
AQIIN_TOKEN = os.environ.get("AQIIN_TOKEN", "")

# Token cache to avoid hitting homepage every request
_TOKEN_CACHE = None

def get_session_token() -> str:
    """Automated token extraction from aqi.in homepage."""
    global _TOKEN_CACHE
    if _TOKEN_CACHE:
        return _TOKEN_CACHE
        
    try:
        logger.info("Attempting to fetch fresh session token from aqi.in homepage...")
        # Use more realistic browser-like headers for the homepage fetch
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "max-age=0",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1"
        }
        
        with httpx.Client(timeout=15.0, follow_redirects=True) as client:
            r = client.get("https://www.aqi.in/", headers=headers)
            r.raise_for_status()
            
            # Extract token2 using regex (flexible for escaped or non-escaped quotes)
            import re
            match = re.search(r'token2\\?":\\?"(eyJhbGci[^\\"]+)', r.text)
            if match:
                _TOKEN_CACHE = match.group(1)
                logger.info("Successfully acquired fresh token from homepage.")
                return _TOKEN_CACHE
            else:
                logger.error(f"Token pattern not found in HTML. Check if site structure changed.")
                raise Exception("Token pattern not found in HTML")
                
    except Exception as e:
        logger.warning(f"Auto-refresh failed: {e}. Falling back to default token for this session.")
        # Cache the fallback token so we don't spam the homepage for every station
        _TOKEN_CACHE = AQIIN_TOKEN
        return _TOKEN_CACHE

def get_headers():
    token = get_session_token()
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Authorization": f"Bearer {token}",
        "Connection": "keep-alive",
        "Host": "apiserver.aqi.in",
        "Origin": "https://www.aqi.in",
        "Referer": "https://www.aqi.in/",
        "sec-ch-ua": '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
    }

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
REQUEST_TIMEOUT = 15.0
REQUEST_DELAY_MIN = 0.3
REQUEST_DELAY_MAX = 0.8
WORKERS = 2

# ─── Dataclasses ────────────────────────────────────────────────────────

@dataclass
class PollutantReading:
    parameter: str   # pm25, pm10, co, so2, no2, o3
    value: float
    unit: str

@dataclass
class LocationData:
    station_name: str
    aqi: int
    timestamp_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    temperature: Optional[float] = None
    humidity: Optional[float] = None
    pollutants: List[PollutantReading] = field(default_factory=list)
    raw_payload: str = ""
    success: bool = True
    error: Optional[str] = None


# ─── API Parser ────────────────────────────────────────────────────────

def parse_api_json(data: dict, slug: str) -> LocationData:
    """Parse aqi.in API JSON response."""
    try:
        if not data or data.get("status") != "success" or not data.get("data"):
            return LocationData(station_name="Unknown", aqi=0, success=False, error="Invalid API response")

        entry = data["data"][0]
        iaqi = entry.get("iaqi", {})
        weather = entry.get("weather", {})
        
        # Pollutants
        pollutants = []
        # Mapping API keys to our internal parameter names
        mapping = {
            "pm25": "pm25", "pm10": "pm10", "co": "co", 
            "so2": "so2", "no2": "no2", "o3": "o3"
        }
        
        for api_key, internal_name in mapping.items():
            if api_key in iaqi:
                # API usually returns CO in ppm and others in µg/m³ or ppb
                unit = 'ppm' if internal_name == 'co' else ('µg/m³' if 'pm' in internal_name else 'ppb')
                pollutants.append(PollutantReading(
                    parameter=internal_name, 
                    value=float(iaqi[api_key]), 
                    unit=unit
                ))

        return LocationData(
            station_name=entry.get("station", "Unknown"),
            aqi=iaqi.get("aqi", 0),
            latitude=entry.get("latitude"),
            longitude=entry.get("longitude"),
            temperature=weather.get("temp_c"),
            humidity=weather.get("humidity"),
            pollutants=pollutants,
            raw_payload=str(data),
            success=True
        )

    except Exception as e:
        logger.error(f"Error parsing JSON for {slug}: {e}")
        return LocationData(station_name="Unknown", aqi=0, success=False, error=f"Parse error: {e}")


# ─── Fetcher ───────────────────────────────────────────────────────────

def fetch_one(location_id: str, client: httpx.Client) -> tuple:
    """Fetch AQI data for one location using JSON API."""
    # Polite delay
    time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
    
    params = {
        "slug": location_id,
        "type": "3",
        "source": "web"
    }
    
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            r = client.get(API_ENDPOINT, params=params, headers=get_headers(), timeout=REQUEST_TIMEOUT)

            if r.status_code == 200:
                data = r.json()
                return location_id, parse_api_json(data, location_id)

            elif r.status_code in (403, 429):
                cool_down = 30 + random.uniform(0, 30) if attempt > 0 else (5 + random.uniform(0, 5))
                logger.warning(f"[{location_id}] HTTP {r.status_code} on attempt {attempt + 1}. Cooling down {cool_down:.1f}s")
                time.sleep(cool_down)
                last_error = f"Rate limited: {r.status_code}"
                continue

            elif r.status_code == 401:
                logger.error("401 Unauthorized: The Bearer Token has likely expired.")
                return location_id, LocationData(station_name="Unknown", aqi=0, success=False, error="Token expired")

            else:
                last_error = f"HTTP {r.status_code}"
                time.sleep(2)

        except Exception as e:
            last_error = str(e)
            time.sleep(2)

    return location_id, LocationData(
        station_name="Unknown", aqi=0, success=False, 
        error=f"Failed after {MAX_RETRIES} retries: {last_error}"
    )


# ─── Public APIs ────────────────────────────────────────────────────────

def scrape_urls_sync(url_paths: List[str], workers: int = WORKERS, progress_callback=None) -> List[tuple]:
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
    test_paths = ["vietnam/hanoi/chuong-my", "vietnam/ha-noi/hanoi/hanoi-us-embassy"]
    print(f"Testing JSON API scraper for {test_paths}...")
    results = scrape_urls_sync(test_paths)
    for sid, r in results:
        print(f"\n[{'✅' if r.success else '❌'}] {r.station_name} (AQI: {r.aqi})")
        print(f"Pollutants: {[(p.parameter, p.value) for p in r.pollutants]}")
        print(f"Weather: {r.temperature}°C, {r.humidity}%")
        print(f"Coords: {r.latitude}, {r.longitude}")
