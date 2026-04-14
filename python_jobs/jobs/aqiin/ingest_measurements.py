#!/usr/bin/env python3
"""
AQI.in Ingestion Job - Optimized HTTPX Version.

Sử dụng HTTPX Widget Scraper để crawl dữ liệu Việt Nam.
Tốc độ: ~540 trạm trong < 2 phút.
"""

import argparse
import logging
import sys
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common import get_data_writer, JobLogger, log_job_stats
from jobs.aqiin.scraper_core import (
    scrape_in_batches,
    LocationData,
    PollutantReading
)

# ─── Config ────────────────────────────────────────────────────────────
BATCH_SIZE = 20
BATCH_DELAY = 1.0
WORKERS = 1
LOCATIONS_FILE = Path(__file__).parent / "vietnam_locations.txt"

# ─── ClickHouse Writer ───────────────────────────────────────────────────────

def write_to_clickhouse(results: List[LocationData], batch_id: str):
    """Write results to ClickHouse raw_aqiin_* tables.

    D-AQI-01: schema updated to include unit + quality_flag (Phase 6).
    """
    writer = get_data_writer()
    measurement_records = []
    for sid, loc in results:
        if not loc.success:
            continue

        # Helper to get unit from parameter
        def get_unit(param: str) -> str:
            if param in ('pm25', 'pm2.5', 'pm2_5', 'pm10', 'pm10_concentration'):
                return 'µg/m³'
            if param in ('co', 'carbon_monoxide', 'co_concentration'):
                return 'ppm'
            if param in ('no2', 'nitrogen_dioxide', 'o3', 'ozone', 'so2', 'sulfur_dioxide', 'nh3', 'no'):
                return 'ppb'
            if param == 'temp':
                return '°C'
            if param == 'hum':
                return '%'
            return 'µg/m³'

        # Pollutant measurements
        for p in loc.pollutants:
            measurement_records.append({
                "source": "aqiin",
                "ingest_time": datetime.now(timezone.utc),
                "ingest_batch_id": batch_id,
                "station_name": loc.station_name,
                "timestamp_utc": loc.timestamp_utc,
                "parameter": p.parameter,
                "value": p.value,
                "aqi_reported": loc.aqi,
                "unit": get_unit(p.parameter),   # D-AQI-01
                "quality_flag": "valid",          # D-AQI-01 (community sensors)
                "raw_payload": loc.raw_payload,
            })

        # Weather measurements
        if loc.temperature is not None:
            measurement_records.append({
                "source": "aqiin",
                "ingest_time": datetime.now(timezone.utc),
                "ingest_batch_id": batch_id,
                "station_name": loc.station_name,
                "timestamp_utc": loc.timestamp_utc,
                "parameter": "temp",
                "value": loc.temperature,
                "aqi_reported": loc.aqi,
                "unit": "°C",                     # D-AQI-01
                "quality_flag": "valid",         # D-AQI-01
                "raw_payload": loc.raw_payload,
            })
        if loc.humidity is not None:
            measurement_records.append({
                "source": "aqiin",
                "ingest_time": datetime.now(timezone.utc),
                "ingest_batch_id": batch_id,
                "station_name": loc.station_name,
                "timestamp_utc": loc.timestamp_utc,
                "parameter": "hum",
                "value": loc.humidity,
                "aqi_reported": loc.aqi,
                "unit": "%",                      # D-AQI-01
                "quality_flag": "valid",          # D-AQI-01
                "raw_payload": loc.raw_payload,
            })

    if measurement_records:
        writer.write_batch("raw_aqiin_measurements", measurement_records, source="aqiin")

    return len(measurement_records)

def create_progress_callback(logger: logging.Logger):
    def callback(finished: int, total: int, sid: str, result: LocationData):
        if finished % 10 == 0 or finished == total:
            status = "✅" if result.success else "❌"
            logger.info(f"[{finished}/{total}] {status} {sid}: AQI={result.aqi}")
    return callback

def load_locations(limit: int = None) -> List[str]:
    if not LOCATIONS_FILE.exists():
        raise FileNotFoundError(f"Locations file not found: {LOCATIONS_FILE}")
    with open(LOCATIONS_FILE) as f:
        paths = [line.strip() for line in f if line.strip()]
    return paths[:limit] if limit else paths

async def run(mode: str, limit: int):
    batch_id = f"aqiin_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    paths = load_locations(limit=limit)
    logger = logging.getLogger("ingest_aqiin")
    
    results = scrape_in_batches(
        paths, 
        batch_size=BATCH_SIZE, 
        batch_delay=BATCH_DELAY,
        progress_callback=create_progress_callback(logger)
    )
    
    n_m = write_to_clickhouse(results, batch_id)
    
    # GHA Notice for API monitoring
    successful = sum(1 for sid, r in results if r.success)
    failed = len(results) - successful
    token_expired = any(r.error and "Token expired" in str(r.error) for sid, r in results)
    
    if os.environ.get('GITHUB_ACTIONS') == 'true':
        status_msg = "✅ Success" if failed == 0 else f"⚠️ {failed} failed"
        if token_expired:
            status_msg = "❌ TOKEN EXPIRED"
            
        print(f"::notice::AQI.in API Ingestion: {status_msg} ({successful}/{len(results)} stations), {n_m} records added")
    
    return {"successful": successful, "total": len(results), "records": n_m}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="full")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    with JobLogger("ingest_aqiin", source="aqiin") as logger:
        import asyncio
        start = datetime.now(timezone.utc)
        try:
            stats = asyncio.run(run(args.mode, args.limit))
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
            logger.info(f"Completed in {elapsed:.1f}s — {stats}")
        except Exception as e:
            logger.error(f"Failed: {e}", exc_info=True)
            sys.exit(1)

if __name__ == "__main__":
    main()