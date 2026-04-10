#!/usr/bin/env python3
"""
TomTom Traffic Flow Ingestion Job.

Fetches real-time traffic speeds and congestion data for 255 Vietnam stations
using the TomTom Traffic Flow API.

Author: Air Quality Data Platform
"""

import os
import sys
import logging
import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common.api_client import APIClient
from common.clickhouse_writer import create_clickhouse_writer
from common.ingestion_control import update_control

# Config
STATION_COORDS_FILE = Path(__file__).parent.parent.parent.parent / "dbt/dbt_tranform/seeds/vn_station_coordinates.csv"

def load_station_coordinates() -> List[Dict[str, Any]]:
    stations = []
    if not STATION_COORDS_FILE.exists():
        raise FileNotFoundError(f"Station coordinates file not found: {STATION_COORDS_FILE}")
    
    with open(STATION_COORDS_FILE, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            stations.append({
                "name": row["station_name"],
                "lat": float(row["latitude"]),
                "lon": float(row["longitude"])
            })
    return stations


def fetch_traffic_data(client: APIClient, station_name: str, lat: float, lon: float) -> Dict[str, Any]:
    """Fetch traffic flow data for a specific point."""
    # TomTom Flow Segment Data API
    # zoom level 10 is good for general traffic conditions
    endpoint = "/traffic/services/4/flowSegmentData/absolute/10/json"
    
    resp = client.get(
        endpoint,
        params={
            "point": f"{lat},{lon}",
            "key": client.token
        }
    )
    
    flow = resp.get("flowSegmentData", {})
    
    return {
        "source": "tomtom",
        "ingest_time": datetime.now(timezone.utc),
        "ingest_batch_id": f"traffic_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}",
        "station_name": station_name,
        "latitude": lat,
        "longitude": lon,
        "timestamp_utc": datetime.now(timezone.utc),
        "current_speed": flow.get("currentSpeed"),
        "free_flow_speed": flow.get("freeFlowSpeed"),
        "current_travel_time": flow.get("currentTravelTime"),
        "free_flow_travel_time": flow.get("freeFlowTravelTime"),
        "confidence": flow.get("confidence"),
        "road_closure": flow.get("roadClosure", False),
        "raw_payload": str(resp)
    }


def run_traffic_ingestion(writer, client, stations: List[Dict[str, Any]]) -> int:
    logger = logging.getLogger(__name__)
    all_traffic = []

    for st in stations:
        try:
            record = fetch_traffic_data(client, st["name"], st["lat"], st["lon"])
            if record["current_speed"] is not None:
                all_traffic.append(record)
        except Exception as e:
            logger.warning(f"[{st['name']}] traffic fetch failed: {e}")

    if all_traffic:
        writer.write_batch("raw_tomtom_traffic", all_traffic, source="tomtom")
        logger.info(f"Wrote {len(all_traffic)} traffic records")
    
    return len(all_traffic)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--limit", type=int, help="Limit the number of stations to process")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logger = logging.getLogger(__name__)

    api_token = os.environ.get("TOMTOM_API_KEY")
    if not api_token:
        logger.error("TOMTOM_API_KEY not set")
        sys.exit(1)

    # TomTom API base URL
    client = APIClient(
        base_url="https://api.tomtom.com",
        token=api_token,
        timeout=20,
    )
    writer = create_clickhouse_writer()

    try:
        stations = load_station_coordinates()
        if args.limit:
            stations = stations[:args.limit]
            logger.info(f"Limiting processing to first {args.limit} stations")
            
        count = run_traffic_ingestion(writer, client, stations)
        update_control(source="tomtom_traffic", records_ingested=count, success=True)
        logger.info(f"Traffic ingestion done: {count} records")
    except Exception as e:
        logger.error(f"Traffic ingestion failed: {e}")
        update_control(source="tomtom_traffic", records_ingested=0, success=False, error_message=str(e))
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
