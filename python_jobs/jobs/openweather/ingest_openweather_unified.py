#!/usr/bin/env python3
"""
Unified OpenWeather Ingestion Job - High Speed / Multi-Token.

This script fetches BOTH weather and air pollution data for 653 points in Vietnam.
It uses a TokenManager to rotate through multiple API keys, maximizing throughput.

Author: Air Quality Data Platform
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common.api_client import APIClient
from common.token_manager import TokenManager
from common import get_data_writer
from common.ingestion_control import update_control
from models.openweather_models import (
    load_ingestion_points,
    get_weather_clusters,
    transform_city_response,
)

# Reuse transform from ingest_weather.py logic
def transform_weather_response(resp: Dict[str, Any], cluster_id: str, province: str, lat: float, lon: float) -> Dict[str, Any]:
    """Transform OpenWeather /weather response to meteorology schema."""
    main = resp.get("main", {})
    wind = resp.get("wind", {})
    clouds = resp.get("clouds", {})
    
    return {
        "source": "openweather",
        "ingest_time": datetime.now(timezone.utc),
        "province_name": province,
        "weather_cluster": cluster_id,
        "cluster_lat": lat,
        "cluster_lon": lon,
        "timestamp_utc": datetime.fromtimestamp(resp.get("dt"), tz=timezone.utc) if resp.get("dt") else datetime.now(timezone.utc),
        "temp": main.get("temp"),
        "feels_like": main.get("feels_like"),
        "temp_min": main.get("temp_min"),
        "temp_max": main.get("temp_max"),
        "pressure": main.get("pressure"),
        "humidity": main.get("humidity"),
        "visibility": resp.get("visibility"),
        "wind_speed": wind.get("speed"),
        "wind_deg": wind.get("deg"),
        "clouds_all": clouds.get("all"),
        "raw_payload": str(resp)
    }

def fetch_weather_for_cluster(cid: str, data: Dict[str, Any], client: APIClient) -> Tuple[str, Optional[Dict]]:
    """Fetch weather for a cluster representative."""
    lat, lon = data["lat"], data["lon"]
    province = data["province"]
    try:
        resp = client.get("/weather", params={"lat": lat, "lon": lon, "units": "metric"})
        record = transform_weather_response(resp, cid, province, lat, lon)
        return cid, record
    except Exception as e:
        logging.getLogger(__name__).warning(f"Cluster weather failed for {cid}: {e}")
        return cid, None

def fetch_pollution_for_point(pid: str, data: Dict[str, Any], client: APIClient, cluster_weather_lookup: Dict[str, Dict]) -> Tuple[Optional[List], Optional[Dict]]:
    """Fetch pollution for a point and associate with cluster weather."""
    lat, lon = data["lat"], data["lon"]
    province = data["province"]
    ward = data.get("ward", "")
    code = data.get("code", pid)
    cluster_id = data.get("cluster_id")
    
    pollution_records = []

    # 1. Fetch Pollution
    try:
        resp_p = client.get("/air_pollution", params={"lat": lat, "lon": lon})
        pollution_records = transform_city_response(resp_p, province, ward, code, lat, lon)
    except Exception as e:
        logging.getLogger(__name__).warning(f"[{code}] Pollution failed: {e}")

    # 2. Get Weather from Cluster Cache
    weather_record = None
    if cluster_id and cluster_id in cluster_weather_lookup:
        # Clone the cluster weather
        weather_record = cluster_weather_lookup[cluster_id].copy()

    return pollution_records, weather_record

def main():
    parser = argparse.ArgumentParser(description="High-Resolution OpenWeather Ingestion")
    parser.add_argument("--workers", type=int, default=None, help="Force number of workers")
    parser.add_argument("--limit", type=int, default=None, help="Limit points")
    parser.add_argument("--grid", type=float, default=0.2, help="Weather cluster grid size (degrees)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logger = logging.getLogger(__name__)

    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), '.env'))

    tokens = []
    token_str = os.environ.get("OPENWEATHER_API_TOKENS") or os.environ.get("OPENWEATHER_API_TOKEN")
    if token_str:
        tokens.extend([t.strip() for t in token_str.split(",") if t.strip()])
    for i in range(1, 21):
        val = os.environ.get(f"OPENWEATHER_API_TOKEN_{i}")
        if val: tokens.append(val.strip())

    if not tokens:
        logger.error("No OpenWeather tokens found.")
        sys.exit(1)
    
    tokens = list(set(tokens))
    token_manager = TokenManager(tokens)
    client = APIClient(
        base_url="https://api.openweathermap.org/data/2.5",
        token_manager=token_manager,
        auth_header_name="appid",
        auth_header_format="{}"
    )

    writer = get_data_writer()
    all_points = load_ingestion_points()
    if args.limit:
        all_points = dict(list(all_points.items())[:args.limit])
    
    # 1. Clustering
    clusters = get_weather_clusters(all_points, grid_size=args.grid)
    
    # Calculate workers
    num_workers = args.workers or (len(tokens) * 5)
    logger.info(f"Starting  ingestion for {len(all_points)} wards using {len(clusters)} weather clusters.")

    # 2. Phase 1: Weather Clusters
    cluster_weather_lookup = {}
    with ThreadPoolExecutor(max_workers=min(num_workers, len(clusters) or 1)) as executor:
        futures = {executor.submit(fetch_weather_for_cluster, cid, d, client): cid for cid, d in clusters.items()}
        for future in as_completed(futures):
            cid, record = future.result()
            if record:
                cluster_weather_lookup[cid] = record
    
    logger.info(f"Weather clusters fetched: {len(cluster_weather_lookup)}")

    # 3. Phase 2: Pollution for all points
    all_weather_records = []
    all_pollution_records = []
    chunk_size = 100
    processed = 0
    batch_id = f"ow__{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(fetch_pollution_for_point, pid, d, client, cluster_weather_lookup): pid for pid, d in all_points.items()}
        
        for future in as_completed(futures):
            p_list, w_rec = future.result()
            if p_list:
                for p in p_list:
                    p["ingest_batch_id"] = batch_id
                all_pollution_records.extend(p_list)
            if w_rec:
                # Note: Meteorology  table uses cluster_lat/cluster_lon but doesn't strictly need duplicating for every ward
                # however, if we want to query by ward_code join weather, we might want to store ward_code in meteorology  too?
                # User said: "cluster_lat, cluster_lon thay cho lat lon hiện tại".
                # To minimize data volume, we keep 1 record per cluster in meteorology_.
                # Actually, the user's DDL doesn't have ward_code in meteorology_.
                pass
            
            processed += 1
            if processed % chunk_size == 0:
                if all_pollution_records:
                    writer.write_batch("raw_openweather_measurements", all_pollution_records, source="openweather")
                    all_pollution_records = []
                logger.info(f"Progress: {processed}/{len(all_points)} wards processed.")

    # Write weather clusters (unique per cluster)
    if cluster_weather_lookup:
        weather_list = list(cluster_weather_lookup.values())
        for w in weather_list:
            w["ingest_batch_id"] = batch_id
        writer.write_batch("raw_openweather_meteorology", weather_list, source="openweather")

    # Final write
    if all_pollution_records:
        writer.write_batch("raw_openweather_measurements", all_pollution_records, source="openweather")

    logger.info(f"Ingestion complete. Total wards: {processed}, Clusters: {len(cluster_weather_lookup)}")
    update_control(source="openweather_unified", records_ingested=processed, success=True)

if __name__ == "__main__":
    main()
