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
from common.clickhouse_writer import create_clickhouse_writer
from common.ingestion_control import update_control

# Config
BASE_DIR = Path(__file__).parent.parent.parent.parent
STATION_METADATA_FILE = BASE_DIR / "dbt/dbt_tranform/seeds/unified_stations_metadata.csv"

def load_station_groups() -> Dict[Tuple[float, float], List[str]]:
    """Load stations and group them by (lat, lon)."""
    groups = {}
    if not STATION_METADATA_FILE.exists():
        # Fallback to original if unified is missing (should not happen in prod)
        STATION_METADATA_FILE_OLD = BASE_DIR / "dbt/dbt_tranform/seeds/vn_station_coordinates.csv"
        if not STATION_METADATA_FILE_OLD.exists():
            raise FileNotFoundError(f"No station metadata found at {STATION_METADATA_FILE}")
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
    # zoom level 10 is good for regional traffic
    endpoint = "/traffic/services/4/flowSegmentData/absolute/10/json"
    
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
            # 1. Fetch once per point
            flow = fetch_traffic_data(client, lat, lon)
            
            if not flow or flow.get("currentSpeed") is None:
                logger.warning(f"No traffic data for point ({lat}, {lon})")
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
    writer = create_clickhouse_writer()

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
