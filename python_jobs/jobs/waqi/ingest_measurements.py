#!/usr/bin/env python3
"""
WAQI API Ingestion Job.

🎯 CLEAN API IMPLEMENTATION:
- Directly queries the official World Air Quality Index (WAQI) JSON API.
- Uses coordinates from unified_stations_metadata.csv (the source of truth).
- Concurrently queries coordinates and maps results directly to the ClickHouse schema.
- Does not need or use a web scraper (no scraper_core.py).
"""

import argparse
import csv
import logging
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common import JobLogger, get_data_writer

# ─── Config ────────────────────────────────────────────────────────────
WORKERS = 8
REQUEST_TIMEOUT = 10.0
WAQI_API_URL = "https://api.waqi.info/feed"
VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

def get_metadata_path() -> Path:
    """Locate unified_stations_metadata.csv locally or inside Airflow container."""
    local_path = Path(__file__).resolve().parent.parent.parent.parent / "dbt" / "dbt_tranform" / "seeds" / "unified_stations_metadata.csv"
    if local_path.exists():
        return local_path

    container_path = Path("/opt/dbt/dbt_tranform/seeds/unified_stations_metadata.csv")
    if container_path.exists():
        return container_path

    raise FileNotFoundError("Could not locate unified_stations_metadata.csv")

def load_stations(limit: int = None) -> list[dict]:
    """Load stations with their coordinates directly from the seed metadata."""
    csv_path = get_metadata_path()
    stations = []
    with open(csv_path, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            stations.append({
                "station_name": row["station_name"],
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"])
            })
    return stations[:limit] if limit else stations

def fetch_waqi_station(station: dict, token: str, client: httpx.Client) -> dict | None:
    """Fetch real-time air quality data for a station's coordinates from WAQI API."""
    lat = station["latitude"]
    lon = station["longitude"]
    url = f"{WAQI_API_URL}/geo:{lat};{lon}/?token={token}"

    # Tiny sleep to avoid aggressive bursts
    time.sleep(random.uniform(0.02, 0.08))

    try:
        r = client.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            res_json = r.json()
            if res_json.get("status") == "ok":
                return {
                    "station_name": station["station_name"],
                    "station_lat": lat,
                    "station_lon": lon,
                    "data": res_json.get("data", {}),
                    "success": True,
                    "error": None
                }
            else:
                return {
                    "station_name": station["station_name"],
                    "station_lat": lat,
                    "station_lon": lon,
                    "success": False,
                    "error": res_json.get("data", "WAQI API error status")
                }
        else:
            return {
                "station_name": station["station_name"],
                "station_lat": lat,
                "station_lon": lon,
                "success": False,
                "error": f"HTTP Error {r.status_code}"
            }
    except Exception as e:
        return {
            "station_name": station["station_name"],
            "station_lat": lat,
            "station_lon": lon,
            "success": False,
            "error": str(e)
        }

def parse_waqi_timestamp(time_data: dict | None) -> datetime:
    """Return WAQI observation time as a timezone-aware UTC datetime.

    WAQI's `time.s` is local station time. Prefer `time.iso` or `time.tz`
    so a value like 2026-06-08T15:00:00+07:00 becomes 08:00 UTC.
    """
    if not isinstance(time_data, dict):
        return datetime.now(timezone.utc)

    iso_value = time_data.get("iso")
    if iso_value:
        try:
            parsed = datetime.fromisoformat(str(iso_value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=VIETNAM_TZ)
            return parsed.astimezone(timezone.utc)
        except Exception:
            pass

    time_str = time_data.get("s")
    tz_value = time_data.get("tz")
    if time_str:
        try:
            if tz_value:
                parsed = datetime.fromisoformat(f"{time_str}{tz_value}")
            else:
                parsed = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=VIETNAM_TZ)
            return parsed.astimezone(timezone.utc)
        except Exception:
            pass

    return datetime.now(timezone.utc)

def process_results(results: list[dict], batch_id: str) -> tuple[list[dict], list[dict]]:
    """Parse WAQI JSON responses into measurements and station dimensions."""
    measurement_records = []
    station_records_dict = {}
    seen_station_ids = set()

    for res in results:
        if not res or not res.get("success"):
            continue

        data = res["data"]
        idx = data.get("idx")
        if idx is None:
            continue

        city = data.get("city", {})
        geo = city.get("geo", [])
        city_url = city.get("url", "")
        waqi_station_name = city.get("name", res["station_name"])

        # Populate station dimension record (deduped by station_id)
        station_id = int(idx)
        if station_id not in station_records_dict:
            station_records_dict[station_id] = {
                "station_id": station_id,
                "station_name": waqi_station_name,
                "latitude": float(geo[0]) if len(geo) > 0 else float(res["station_lat"]),
                "longitude": float(geo[1]) if len(geo) > 1 else float(res["station_lon"]),
                "city_url": str(city_url),
                "updated_at": datetime.now(timezone.utc)
            }

        # Deduplicate measurements by physical station_id (eliminate 18.7x coordinate redundancy)
        if station_id in seen_station_ids:
            continue
        seen_station_ids.add(station_id)

        aqi = data.get("aqi", 0)
        iaqi = data.get("iaqi", {})
        timestamp = parse_waqi_timestamp(data.get("time"))

        def get_unit(param: str) -> str:
            if param in ('pm25', 'pm10'):
                return 'µg/m³'
            if param == 'co':
                return 'ppm'
            if param in ('no2', 'so2', 'o3'):
                return 'ppb'
            return 'µg/m³'

        # 1. Map Pollutants
        mapping_params = {
            "pm25": "pm25", "pm10": "pm10", "co": "co",
            "so2": "so2", "no2": "no2", "o3": "o3"
        }
        for api_key, db_param in mapping_params.items():
            if api_key in iaqi:
                val = iaqi[api_key].get("v")
                if val is not None:
                    measurement_records.append({
                        "source": "waqi",
                        "ingest_time": datetime.now(timezone.utc),
                        "ingest_batch_id": batch_id,
                        "station_name": waqi_station_name,
                        "timestamp_utc": timestamp,
                        "parameter": db_param,
                        "value": float(val),
                        "aqi_reported": int(aqi) if aqi is not None else 0,
                        "unit": get_unit(db_param),
                        "quality_flag": "valid",
                        "raw_payload": str(data)
                    })

        # 2. Map Weather
        if "t" in iaqi and iaqi["t"].get("v") is not None:
            measurement_records.append({
                "source": "waqi",
                "ingest_time": datetime.now(timezone.utc),
                "ingest_batch_id": batch_id,
                "station_name": waqi_station_name,
                "timestamp_utc": timestamp,
                "parameter": "temp",
                "value": float(iaqi["t"].get("v")),
                "aqi_reported": int(aqi) if aqi is not None else 0,
                "unit": "°C",
                "quality_flag": "valid",
                "raw_payload": str(data)
            })

        if "h" in iaqi and iaqi["h"].get("v") is not None:
            measurement_records.append({
                "source": "waqi",
                "ingest_time": datetime.now(timezone.utc),
                "ingest_batch_id": batch_id,
                "station_name": waqi_station_name,
                "timestamp_utc": timestamp,
                "parameter": "hum",
                "value": float(iaqi["h"].get("v")),
                "aqi_reported": int(aqi) if aqi is not None else 0,
                "unit": "%",
                "quality_flag": "valid",
                "raw_payload": str(data)
            })

    return measurement_records, list(station_records_dict.values())

async def run(limit: int, min_success_ratio: float):
    token = os.environ.get("WAQI_TOKEN")
    if not token:
        raise RuntimeError("WAQI_TOKEN environment variable is required")

    batch_id = f"waqi_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    stations = load_stations(limit=limit)
    logger = logging.getLogger("ingest_waqi")

    logger.info(f"Loaded {len(stations)} stations from metadata. Scraping via WAQI API...")

    results = []
    with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {
                executor.submit(fetch_waqi_station, station, token, client): station
                for station in stations
            }

            for idx, future in enumerate(as_completed(futures)):
                res = future.result()
                results.append(res)
                status = "✅" if res["success"] else "❌"
                logger.info(f"[{idx+1}/{len(stations)}] {status} {res['station_name']}")

    successful = sum(1 for r in results if r["success"])
    failed = len(results) - successful
    success_ratio = successful / len(results) if results else 0

    # Process results into dynamic measurements and station dimensions
    measurement_records, station_records = process_results(results, batch_id)
    
    writer = get_data_writer()
    
    n_s = 0
    if station_records:
        writer.write_batch("dim_waqi_stations", station_records, source="waqi")
        n_s = len(station_records)
        
    n_m = 0
    if measurement_records:
        writer.write_batch("raw_waqi_measurements", measurement_records, source="waqi")
        n_m = len(measurement_records)

    if os.environ.get('GITHUB_ACTIONS') == 'true':
        status_msg = "✅ Success" if failed == 0 else f"⚠️ {failed} failed"
        print(f"::notice::WAQI API Ingestion: {status_msg} ({successful}/{len(results)} stations), {n_s} stations updated, {n_m} measurements added")

    if success_ratio < min_success_ratio:
        raise RuntimeError(
            "WAQI coverage below threshold: "
            f"{successful}/{len(results)} ({success_ratio:.2%}) < {min_success_ratio:.2%}. "
            "Failing run."
        )

    return {
        "successful": successful,
        "failed": failed,
        "total": len(results),
        "success_ratio": round(success_ratio, 4),
        "stations": n_s,
        "records": n_m,
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="full")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--min-success-ratio",
        type=float,
        default=float(os.environ.get("WAQI_MIN_SUCCESS_RATIO", "0.80")),
    )
    args = parser.parse_args()

    with JobLogger("ingest_waqi", source="waqi") as logger:
        import asyncio
        start = datetime.now(timezone.utc)
        try:
            stats = asyncio.run(run(args.limit, args.min_success_ratio))
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            logger.info(f"Completed in {elapsed:.1f}s — {stats}")
        except Exception as e:
            logger.error(f"Failed: {e}", exc_info=True)
            sys.exit(1)

if __name__ == "__main__":
    main()
