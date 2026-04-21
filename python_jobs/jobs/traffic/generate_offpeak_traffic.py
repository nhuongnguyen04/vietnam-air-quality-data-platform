#!/usr/bin/env python3
"""
Off-peak Traffic Model Generator.

Runs between 21:00 and 06:00.
Generates traffic records based on Last-Known (20h) state, 
applying a night-time decay factor and blending with OSM Proxy.

Author: Air Quality Data Platform
"""

import os
import sys
import logging
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Add project root to path
_script_dir = os.path.dirname(os.path.abspath(__file__))
# jobs/traffic -> jobs -> python_jobs -> project_root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(_script_dir)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "python_jobs"))

from common import get_data_writer

logger = logging.getLogger(__name__)

DECAY_FACTORS = {
    21: 0.85,
    22: 0.70,
    23: 0.50,
    0: 0.30,
    1: 0.20,
    2: 0.20,
    3: 0.20,
    4: 0.30,
    5: 0.60,
    6: 0.80
}

def get_osm_proxy(highway_type, dist_km):
    """Same logic as in main ingestion script."""
    weights = {'motorway': 0.9, 'trunk': 0.8, 'primary': 0.7, 'secondary': 0.5, 'tertiary': 0.3, 'residential': 0.1}
    base = weights.get(highway_type, 0.2)
    dist_factor = np.exp(-1.0 * dist_km) 
    return base * dist_factor

def fetch_last_known_from_clickhouse():
    """Fetch the most recent traffic state for all wards from ClickHouse."""
    host = os.environ.get("CLICKHOUSE_HOST", "localhost")
    port = os.environ.get("CLICKHOUSE_PORT", "8123")
    user = os.environ.get("CLICKHOUSE_USER", "admin")
    password = os.environ.get("CLICKHOUSE_PASSWORD", "admin123456")
    database = os.environ.get("CLICKHOUSE_DB", "air_quality")

    url = f"http://{user}:{password}@{host}:{port}/?database={database}"
    
    # Query to get the latest traffic for each ward
    query = """
    SELECT ward_code, current_speed, free_flow_speed
    FROM air_quality.raw_tomtom_traffic
    WHERE timestamp_utc > now() - INTERVAL 24 HOUR
    ORDER BY timestamp_utc DESC
    LIMIT 1 BY ward_code
    FORMAT JSONEachRow
    """
    
    try:
        headers = {"Content-Type": "text/plain"}
        response = requests.post(url, data=query.encode("utf-8"), headers=headers, timeout=60)
        if response.status_code == 200:
            lines = response.text.strip().split('\n')
            data = {}
            for line in lines:
                if not line: continue
                import json
                row = json.loads(line)
                data[row['ward_code']] = {
                    'speed': row['current_speed'],
                    'ff_speed': row['free_flow_speed']
                }
            return data
    except Exception as e:
        logger.warning(f"Could not fetch last known traffic: {e}")
    return {}

import requests # Ensure requests is imported

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours-ago", type=int, default=1, help="Look back N hours for last known data")
    parser.add_argument("--force", action="store_true", help="Force execution even during peak hours")
    args = parser.parse_args()

    # Peak hour check: 07:00 - 20:00 (Vietnam Time)
    current_hour = datetime.now(timezone.utc).astimezone().hour
    if not args.force and 7 <= current_hour <= 20:
        print(f"Current hour {current_hour} is within Peak hours. Off-peak generator skipped.")
        return

    load_dotenv(os.path.join(PROJECT_ROOT, '.env'))
    
    path = os.path.join(PROJECT_ROOT, "dbt/dbt_tranform/seeds/vietnam_wards_with_osm.csv")
    all_wards = pd.read_csv(path)
    
    batch_id = f"offpeak_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    # Get decay factor for current hour
    decay = DECAY_FACTORS.get(current_hour, 0.5)
    logger.info(f"Applying off-peak decay factor {decay} for hour {current_hour}")
    
    # In practice, we query ClickHouse for the 20h baseline
    # baseline_speeds = fetch_last_known_from_clickhouse()
    
    results = []
    for _, ward in all_wards.iterrows():
        # Baseline Congestion (Simulated from 20h or OSM)
        # Ratio = 1 - (Speed / FF)
        # During off-peak, congestion drops.
        
        osm_proxy = get_osm_proxy(ward.get('nearest_highway_type', 'unknown'), ward.get('distance_to_road_km', 0.5))
        
        # Simple Model: Congestion = OSM_Proxy * Decay
        # (Assuming the last known 20h peak was roughly OSM_Proxy level)
        final_ratio = osm_proxy * decay
        
        # Use a nominal Free-flow speed (from metadata or constant)
        # In a real run, we'd use the actual FF from the 20h fetch.
        ff_speed = 60.0 # Default
        current_speed = ff_speed * (1.0 - final_ratio)
        
        results.append({
            "source": "tomtom",
            "traffic_source": "offpeak_model",
            "ingest_time": datetime.now(timezone.utc),
            "ingest_batch_id": batch_id,
            "ward_code": str(ward['code']),
            "ward_name": ward.get('ward', ''),
            "province_name": ward.get('province', ''),
            "latitude": ward['lat'],
            "longitude": ward['lon'],
            "nearest_highway_type": ward.get('nearest_highway_type', 'unknown'),
            "distance_to_road_km": ward.get('distance_to_road_km', 0),
            "timestamp_utc": datetime.now(timezone.utc),
            "current_speed": float(current_speed),
            "free_flow_speed": float(ff_speed),
            "current_travel_time": 0,
            "free_flow_travel_time": 0,
            "confidence": 0.4,
            "road_closure": False,
            "raw_payload": f"Off-peak model (Hour {current_hour}, Decay {decay})"
        })

    writer = get_data_writer()
    if results:
        writer.write_batch("raw_tomtom_traffic", results, source="tomtom")
        logger.info(f"Generated {len(results)} off-peak records for hour {current_hour}.")

if __name__ == "__main__":
    main()
