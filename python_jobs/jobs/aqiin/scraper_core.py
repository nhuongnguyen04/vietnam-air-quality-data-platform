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
PROXY_URL = os.environ.get("PROXY_URL", "")
API_ENDPOINT = "https://apiserver.aqi.in/aqi/v3/getLocationDetailsBySlug"
# Token provided by user (valid until 2026-04-20)
DEFAULT_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySUQiOjEsImlhdCI6MTc3NjcxMTU3OSwiZXhwIjoxNzc3MzE2Mzc5fQ.P0RqVT7tdVvEjxAJUCMDxuzXX3SbsvXc7c_Ovpzj67g"
AQIIN_TOKEN = os.environ.get("AQIIN_TOKEN") or DEFAULT_TOKEN

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/120.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
]

# ─── State ─────────────────────────────────────────────────────────────
# Token cache or other runtime state if needed
_TOKEN_CACHE = None
import threading
_TOKEN_LOCK = threading.Lock()

def get_session_token() -> str:
    """Return token from environment variable."""
    if not AQIIN_TOKEN:
        logger.warning("AQIIN_TOKEN is NOT set. Current requests will likely fail with 401.")
    return AQIIN_TOKEN

def get_headers():
    token = get_session_token()
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "AQIIN-TOKEN": token,
        "authorization": f"bearer {token}",
        "Origin": "https://www.aqi.in",
        "Referer": "https://www.aqi.in/",
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
        if not data or not isinstance(data, dict):
             return LocationData(station_name="Unknown", aqi=0, success=False, error=f"Invalid API response level: {type(data)}")

        if data.get("status") == "failed":
            return LocationData(
                station_name="Unknown", aqi=0, success=False, 
                error=data.get("message", "API returned failed status")
            )
            
        if not data.get("data") or len(data["data"]) == 0:
            return LocationData(station_name="Unknown", aqi=0, success=False, error="No data records in response")

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
    
    # Manually construct URL to prevent encoding of / to %2F in the slug
    # We try type=3 first (City level), then fallback to type=4 (Station level) if 404
    for current_type in (3, 4):
        full_url = f"{API_ENDPOINT}?slug={location_id}&type={current_type}&source=web"
        
        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                r = client.get(full_url, headers=get_headers(), timeout=REQUEST_TIMEOUT)

                if r.status_code == 200:
                    data = r.json()
                    # If success is false in the JSON even with HTTP 200, it might be a 'No data found' message
                    if isinstance(data, dict) and data.get("status") == "failed" and current_type == 3:
                        logger.debug(f"[{location_id}] Type 3 failed (failed status), will try Type 4")
                        break # Break retry loop to try next type

                    return location_id, parse_api_json(data, location_id)

                elif r.status_code == 404 and current_type == 3:
                    # Potential station level slug, try type 4 next
                    break

                elif r.status_code in (403, 429):
                    cool_down = 30 + random.uniform(0, 30) if attempt > 0 else (5 + random.uniform(0, 5))
                    logger.warning(f"[{location_id}] HTTP {r.status_code} on attempt {attempt + 1}. Cooling down {cool_down:.1f}s")
                    time.sleep(cool_down)
                    last_error = f"Rate limited: {r.status_code}"
                    continue

                if r.status_code == 401:
                    logger.error("401 Unauthorized: The Bearer Token has likely expired or is invalid.")
                elif r.status_code == 403:
                    logger.error("403 Forbidden: IP blocked or Cloudflare challenge triggered.")
                elif r.status_code == 429:
                    logger.error("429 Too Many Requests: Rate limit exceeded.")
                else:
                    logger.error(f"HTTP Error {r.status_code} for path {location_id}. Response: {r.text[:200]}")
                return location_id, LocationData(station_name="Unknown", aqi=0, success=False, error=str(r.status_code))

            except Exception as e:
                last_error = str(e)
                time.sleep(2)
        
        # If we reached here because of a 404 or a 'failed' status in Type 3, the outer loop continues to Type 4
        # If Type 4 also fails or we have a hard error, the inside return or finally the loop exit handles it.

    return location_id, LocationData(
        station_name="Unknown", aqi=0, success=False, 
        error=f"Failed after {MAX_RETRIES} retries: {last_error}"
    )


# ─── Public APIs ────────────────────────────────────────────────────────

def scrape_urls_sync(url_paths: List[str], workers: int = WORKERS, progress_callback=None) -> List[tuple]:
    """Scrape URLs synchronously with Proxy support and stabilized settings."""
    results = []
    total = len(url_paths)

    def fetch_with_own_client(path: str) -> tuple:
        proxy = PROXY_URL if PROXY_URL else None
        # Disable http2 for better compatibility with free proxies (avoid 400 Bad Request)
        with httpx.Client(http2=False, timeout=REQUEST_TIMEOUT, proxy=proxy) as client:
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
