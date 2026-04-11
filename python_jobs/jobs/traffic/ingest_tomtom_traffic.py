#!/usr/bin/env python3
"""
TomTom Traffic Flow Ingestion Job (Optimized).

Fetches real-time traffic speeds and congestion data for Vietnam stations.
Optimized to group stations by unique coordinates to minimize API calls.

Author: Air Quality Data Platform
"""

import os
import sys
import logging
import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common.api_client import APIClient
from common import get_data_writer
from common.ingestion_control import update_control

# Config
def get_project_root() -> Path:
    """Find project root by searching for 'dbt' or 'python_jobs' directory in parents."""
    curr = Path(__file__).resolve()
    # Handle the case where we are in a container with a deep mount
    # or running locally. Search up to 6 levels up.
    for _ in range(6):
        if (curr / "dbt").exists() or (curr / "python_jobs").exists():
            return curr
        if curr.parent == curr:
            break
        curr = curr.parent
    
    # Fallback to the known structure if discovery fails
    # /opt/python/jobs/jobs/traffic/script.py (container) -> 5 levels up to /opt
    # python_jobs/jobs/traffic/script.py (host) -> 4 levels up to project root
    return Path(__file__).resolve().parent.parent.parent.parent

BASE_DIR = get_project_root()
STATION_METADATA_FILE = BASE_DIR / "dbt/dbt_tranform/seeds/unified_stations_metadata.csv"

def load_station_groups() -> Dict[Tuple[float, float], List[str]]:
    """Load stations and group them by (lat, lon)."""
    groups = {}
    if not STATION_METADATA_FILE.exists():
        # Fallback to original if unified is missing (should not happen in prod)
        STATION_METADATA_FILE_OLD = BASE_DIR / "dbt/dbt_tranform/seeds/vn_station_coordinates.csv"
        # Log path for debugging if it fails
        if not STATION_METADATA_FILE_OLD.exists():
            raise FileNotFoundError(
                f"No station metadata found at: \n"
                f"  - Primary: {STATION_METADATA_FILE}\n"
                f"  - Fallback: {STATION_METADATA_FILE_OLD}\n"
                f"  (BASE_DIR was resolved to: {BASE_DIR})"
            )
        target_file = STATION_METADATA_FILE_OLD
    else:
        target_file = STATION_METADATA_FILE
    
    with open(target_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            lat = round(float(row["latitude"]), 6)
            lon = round(float(row["longitude"]), 6)
            name = row["station_name"]
            key = (lat, lon)
            if key not in groups:
                groups[key] = []
            groups[key].append(name)
    return groups


def fetch_traffic_data(client: APIClient, lat: float, lon: float) -> Dict[str, Any]:
    """Fetch traffic flow data for a specific point."""
    # TomTom Flow Segment Data API
    # zoom level 22 is best for snapping to specific segments
    endpoint = "/traffic/services/4/flowSegmentData/absolute/22/json"
    
    resp = client.get(
        endpoint,
        params={
            "point": f"{lat},{lon}",
            "key": client.token
        }
    )
    
    return resp.get("flowSegmentData", {})


def run_traffic_ingestion(writer, client, groups: Dict[Tuple[float, float], List[str]]) -> int:
    logger = logging.getLogger(__name__)
    all_traffic = []
    
    total_stations = sum(len(names) for names in groups.values())
    unique_points = len(groups)
    logger.info(f"Processing {total_stations} stations at {unique_points} unique points")

    batch_id = f"traffic_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    for (lat, lon), station_names in groups.items():
        try:
            # 1. Fetch once per point - Using Zoom 22 for best precision
            # Some stations may be off-road, resulting in 400 Bad Request
            try:
                flow = fetch_traffic_data(client, lat, lon)
            except Exception as e:
                # Extract response text if available to check for TomTom-specific reason
                error_body = ""
                if hasattr(e, 'response') and e.response is not None:
                    error_body = e.response.text
                
                # Catch TomTom-specific "Point too far" error (HTTP 400)
                if "400" in str(e) and ("too far" in error_body.lower() or "INVALID_REQUEST" in error_body):
                    logger.info(f"Skipping point ({lat}, {lon}) - No nearby TomTom traffic segment found.")
                    continue
                raise # Re-raise other errors
            
            if not flow or flow.get("currentSpeed") is None:
                logger.debug(f"No traffic data for point ({lat}, {lon})")
                continue

            # 2. Distribute to all stations at this point
            for name in station_names:
                record = {
                    "source": "tomtom",
                    "ingest_time": datetime.now(timezone.utc),
                    "ingest_batch_id": batch_id,
                    "station_name": name,
                    "latitude": lat,
                    "longitude": lon,
                    "timestamp_utc": datetime.now(timezone.utc),
                    "current_speed": flow.get("currentSpeed"),
                    "free_flow_speed": flow.get("freeFlowSpeed"),
                    "current_travel_time": flow.get("currentTravelTime"),
                    "free_flow_travel_time": flow.get("freeFlowTravelTime"),
                    "confidence": flow.get("confidence"),
                    "road_closure": flow.get("roadClosure", False),
                    "raw_payload": str(flow)
                }
                all_traffic.append(record)
                
        except Exception as e:
            logger.warning(f"Traffic fetch failed for ({lat}, {lon}): {e}")

    if all_traffic:
        writer.write_batch("raw_tomtom_traffic", all_traffic, source="tomtom")
        logger.info(f"Wrote {len(all_traffic)} traffic records (unique points: {unique_points})")
    
    return len(all_traffic)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--limit-points", type=int, help="Limit the number of unique points to process")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logger = logging.getLogger(__name__)

    api_token = os.environ.get("TOMTOM_API_KEY")
    if not api_token:
        logger.error("TOMTOM_API_KEY not set")
        sys.exit(1)

    client = APIClient(
        base_url="https://api.tomtom.com",
        token=api_token,
        timeout=20,
    )
    writer = get_data_writer()

    try:
        groups = load_station_groups()
        if args.limit_points:
            # Slice dictionaries is messy in python < 3.7 but we assume >= 3.7
            limit = args.limit_points
            groups = dict(list(groups.items())[:limit])
            logger.info(f"Limiting processing to first {limit} unique points")
            
        count = run_traffic_ingestion(writer, client, groups)
        update_control(source="tomtom_traffic", records_ingested=count, success=True)
        logger.info(f"Traffic ingestion done: {count} records mapping to {len(groups)} API calls")
    except Exception as e:
        logger.error(f"Traffic ingestion failed: {e}")
        update_control(source="tomtom_traffic", records_ingested=0, success=False, error_message=str(e))
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
