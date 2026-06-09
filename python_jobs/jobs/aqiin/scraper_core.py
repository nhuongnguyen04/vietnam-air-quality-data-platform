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

import base64
import json
import logging
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx

# ─── Config ────────────────────────────────────────────────────────────
PROXY_URL = os.environ.get("PROXY_URL", "")
API_ENDPOINT = "https://apiserver.aqi.in/aqi/v3/getLocationDetailsBySlug"
# Token provided by user (valid until 2026-04-20)
DEFAULT_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySUQiOjEsImlhdCI6MTc4MDkwMjcyMCwiZXhwIjoxNzgxNTA3NTIwfQ.fweb-jpVdGkgWuw0Wns6ibuFW6kfaY5C3cSZD-b0WRw"
AQIIN_TOKEN = os.environ.get("AQIIN_TOKEN") or DEFAULT_TOKEN

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/120.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
]

# ─── State ─────────────────────────────────────────────────────────────
_TOKEN_CACHE = None
_ENV_TOKEN_CHECKED = False
_DYNAMIC_FETCH_FAILED = False
_TOKEN_LOCK = threading.Lock()

def is_token_expired(token: str, margin_seconds: int = 3600) -> bool:
    """Decode JWT and check if it is expired or close to expiration."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return True
        payload_b64 = parts[1]
        payload_b64 += "=" * ((4 - len(payload_b64) % 4) % 4)
        import base64
        import json
        payload = json.loads(base64.urlsafe_b64decode(payload_b64).decode("utf-8"))
        exp = payload.get("exp")
        if not exp:
            return False  # No expiration claim, assume valid
        
        now_ts = datetime.now(timezone.utc).timestamp()
        if now_ts + margin_seconds > exp:
            logger.info(f"Token is expired or expiring soon (exp: {datetime.fromtimestamp(exp, tz=timezone.utc)})")
            return True
        return False
    except Exception as e:
        logger.warning(f"Error checking token expiration: {e}")
        return True

def fetch_token_with_curl_cffi(use_proxy: bool = False) -> str | None:
    """Fetch a fresh AQI.in user token using curl_cffi, optionally with proxies."""
    try:
        from curl_cffi import requests as curl_requests
    except ImportError:
        logger.warning("curl_cffi package not found. Cannot fetch token via curl_cffi.")
        return None

    import random
    import re

    url = "https://www.aqi.in/vi/dashboard/vietnam/ha-noi/hanoi/hanoi-us-embassy"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    if not use_proxy:
        logger.info("Fetching fresh token via direct curl_cffi...")
        try:
            r = curl_requests.get(url, headers=headers, impersonate="chrome120", timeout=15)
            if r.status_code == 200 and "Just a moment" not in r.text:
                pattern = r'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9\.[A-Za-z0-9-_=]+\.[A-Za-z0-9-_=]+'
                matches = re.findall(pattern, r.text)
                if matches:
                    logger.info(f"Successfully fetched token directly: {matches[0][:30]}...")
                    return matches[0]
            logger.warning(f"Direct curl_cffi fetch failed. Status: {r.status_code}, Stuck on Cloudflare: {'Just a moment' in r.text}")
        except Exception as e:
            logger.warning(f"Direct curl_cffi fetch failed with exception: {e}")
        return None

    # Using proxy
    logger.info("Fetching free proxy list from proxyscrape for token retrieval...")
    proxy_url = "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=5000&country=all&ssl=yes&anonymity=all"
    proxies = []
    try:
        # Use simple httpx request to get proxies
        with httpx.Client(timeout=10) as client:
            resp = client.get(proxy_url)
            if resp.status_code == 200:
                proxies = [line.strip() for line in resp.text.split("\n") if line.strip()]
                logger.info(f"Fetched {len(proxies)} proxies from proxyscrape.")
    except Exception as e:
        logger.warning(f"Failed to fetch proxies from proxyscrape: {e}")

    if not proxies:
        return None

    random.shuffle(proxies)
    max_attempts = min(30, len(proxies))
    for i in range(max_attempts):
        proxy_ip = proxies[i]
        logger.info(f"[{i+1}/{max_attempts}] Trying proxy: {proxy_ip}...")
        proxy_dict = {
            "http": f"http://{proxy_ip}",
            "https": f"http://{proxy_ip}"
        }
        try:
            r = curl_requests.get(url, headers=headers, impersonate="chrome120", proxies=proxy_dict, timeout=10)
            if r.status_code == 200:
                html = r.text
                if "Just a moment" in html:
                    logger.debug("   ❌ Stuck on Cloudflare Challenge page.")
                else:
                    pattern = r'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9\.[A-Za-z0-9-_=]+\.[A-Za-z0-9-_=]+'
                    matches = re.findall(pattern, html)
                    if matches:
                        import base64
                        import json
                        for tok in set(matches):
                            parts = tok.split(".")
                            if len(parts) == 3:
                                try:
                                    p_b64 = parts[1] + "=" * ((4 - len(parts[1]) % 4) % 4)
                                    p_data = json.loads(base64.urlsafe_b64decode(p_b64).decode("utf-8"))
                                    if p_data.get("userID") == 1:
                                        logger.info(f"   ✅ SUCCESS! Found User Token: {tok[:30]}... (userID: 1)")
                                        return tok
                                except:
                                    continue
                        logger.info(f"   ✅ SUCCESS! Found Token: {matches[0][:30]}...")
                        return matches[0]
                    else:
                        logger.debug("   ❌ No token found in HTML.")
            else:
                logger.debug(f"   ❌ HTTP Error {r.status_code}")
        except Exception as e:
            logger.debug(f"   ❌ Proxy failed: {e}")

    logger.warning("❌ All proxy attempts failed.")
    return None

def get_session_token() -> str:
    """Return a valid token. Checks cache, env, and falls back to dynamic fetching (curl_cffi)."""
    global _TOKEN_CACHE, _ENV_TOKEN_CHECKED, _DYNAMIC_FETCH_FAILED
    with _TOKEN_LOCK:
        # 1. Check if we have a valid cached token
        if _TOKEN_CACHE and not is_token_expired(_TOKEN_CACHE):
            return _TOKEN_CACHE

        # 2. Check token from environment variable (only once at startup)
        if not _ENV_TOKEN_CHECKED:
            _ENV_TOKEN_CHECKED = True
            env_token = os.environ.get("AQIIN_TOKEN")
            if env_token and env_token != DEFAULT_TOKEN:
                if not is_token_expired(env_token):
                    logger.info("Using AQIIN_TOKEN from environment.")
                    _TOKEN_CACHE = env_token
                    return _TOKEN_CACHE

        # 3. Dynamic fetch (only if it has not failed in this run)
        if not _DYNAMIC_FETCH_FAILED:
            logger.info("Current token is missing or expired. Fetching fresh token...")
            try:
                # 3a. Direct curl_cffi (Works on residential IPs, very fast)
                fresh = fetch_token_with_curl_cffi(use_proxy=False)
                if fresh:
                    _TOKEN_CACHE = fresh
                    return _TOKEN_CACHE
                
                # 3b. Proxy-based curl_cffi (Works on GHA/datacenter IPs)
                fresh = fetch_token_with_curl_cffi(use_proxy=True)
                if fresh:
                    _TOKEN_CACHE = fresh
                    return _TOKEN_CACHE
            except Exception as e:
                logger.error(f"Failed to fetch token via curl_cffi: {e}")
            
            # Since all dynamic fetches failed, disable for this run to avoid repeated hangs
            logger.warning("All dynamic token fetch attempts failed. Disabling dynamic fetch for this run.")
            _DYNAMIC_FETCH_FAILED = True

        # 4. Fallback to hardcoded AQIIN_TOKEN (e.g. DEFAULT_TOKEN)
        logger.warning("Using fallback AQIIN_TOKEN.")
        _TOKEN_CACHE = AQIIN_TOKEN
        return _TOKEN_CACHE



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
    latitude: float | None = None
    longitude: float | None = None
    temperature: float | None = None
    humidity: float | None = None
    pollutants: list[PollutantReading] = field(default_factory=list)
    raw_payload: str = ""
    success: bool = True
    error: str | None = None


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
                # API returns CO in ppb just like other gases (so2, no2, o3)
                unit = 'µg/m³' if 'pm' in internal_name else 'ppb'
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
    """Fetch AQI data for one location using the JSON API directly."""
    # Polite delay
    time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

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
                        logger.debug(f"[{location_id}] Type 3 API failed (failed status), will try Type 4")
                        break # Break retry loop to try next type

                    return location_id, parse_api_json(data, location_id)

                elif r.status_code == 404 and current_type == 3:
                    # Potential station level slug, try type 4 next
                    break

                elif r.status_code in (403, 429):
                    cool_down = 30 + random.uniform(0, 30) if attempt > 0 else (5 + random.uniform(0, 5))
                    logger.warning(f"[{location_id}] HTTP {r.status_code} on API attempt {attempt + 1}. Cooling down {cool_down:.1f}s")
                    time.sleep(cool_down)
                    last_error = f"Rate limited: {r.status_code}"
                    continue

                if r.status_code == 401:
                    logger.error("401 Unauthorized: The Bearer Token has likely expired or is invalid. Clearing token cache...")
                    with _TOKEN_LOCK:
                        global _TOKEN_CACHE
                        _TOKEN_CACHE = None
                    last_error = "Token invalid or expired (401)"
                    continue
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

    return location_id, LocationData(
        station_name="Unknown", aqi=0, success=False,
        error=f"Failed API retries: {last_error}"
    )


# ─── Public APIs ────────────────────────────────────────────────────────

def scrape_urls_sync(url_paths: list[str], workers: int = WORKERS, progress_callback=None) -> list[tuple]:
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

def scrape_in_batches(url_paths: list[str], batch_size: int = 20, batch_delay: float = 2.0, workers: int = WORKERS, progress_callback=None) -> list[tuple]:
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
    for _sid, r in results:
        print(f"\n[{'✅' if r.success else '❌'}] {r.station_name} (AQI: {r.aqi})")
        print(f"Pollutants: {[(p.parameter, p.value) for p in r.pollutants]}")
        print(f"Weather: {r.temperature}°C, {r.humidity}%")
        print(f"Coords: {r.latitude}, {r.longitude}")
