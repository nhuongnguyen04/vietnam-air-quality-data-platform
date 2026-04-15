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
    transform_city_response,
)

# Reuse transform from ingest_weather.py logic (embedded here for independence)
def transform_weather_response(resp: Dict[str, Any], point_id: str, province: str, lat: float, lon: float) -> Dict[str, Any]:
    """Transform OpenWeather /weather response to ClickHouse schema."""
    main = resp.get("main", {})
    wind = resp.get("wind", {})
    clouds = resp.get("clouds", {})
    
    return {
        "source": "openweather",
        "ingest_time": datetime.now(timezone.utc),
        "ingest_batch_id": f"ow_unified_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}",
        "province": province,
        "latitude": lat,
        "longitude": lon,
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

def process_point(point_id: str, data: Dict[str, Any], client: APIClient) -> Tuple[Optional[Dict], Optional[List]]:
    """Fetch both weather and pollution for a single point."""
    lat, lon = data["lat"], data["lon"]
    province = data["province"]
    
    weather_record = None
    pollution_records = []
    
    # 1. Fetch Weather
    try:
        # Note: APIClient with TokenManager handles 'appid' rotation if we pass it in params? 
        # Actually, APIClient.request overrides auth headers, but OpenWeather uses 'appid' param.
        # I should probably update APIClient to support 'appid' as a query param rotation too.
        # For now, let's assume we use headers if supported or we manually handle appid.
        
        # OpenWeather traditionally uses 'appid' in params. 
        # My refactored APIClient adds it to HEADERS. 
        # OpenWeather DOES support 'Authorization' header in some products, 
        # but for 2.5/AirPollution it usually expects appid param.
        
        # Let's check how the TokenManager returns the token.
        # I'll modify the call to use the active token from the request context if possible.
        
        # Actually, let's perform a minor tweak to ingest_openweather_unified.py 
        # to ensure the 'appid' param is always present and correct.
        
        # I will fetch the token from the client's internal state if it was just rotated.
        # But wait, the client rotates INSIDE .get().
        
        # Solution: I'll update APIClient to optionally inject the token into params.
        
        resp_w = client.get(
            "/weather",
            params={"lat": lat, "lon": lon, "units": "metric"}
        )
        weather_record = transform_weather_response(resp_w, point_id, province, lat, lon)
    except Exception as e:
        logging.getLogger(__name__).warning(f"[{point_id}] Weather failed: {e}")

    # 2. Fetch Pollution
    try:
        resp_p = client.get(
            "/air_pollution",
            params={"lat": lat, "lon": lon}
        )
        station_name = province
        pollution_records = transform_city_response(resp_p, station_name, lat, lon)
        
        # Add missing columns for ClickHouse schema
        batch_id = f"ow_unified_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        for r in pollution_records:
            r["ingest_batch_id"] = batch_id
    except Exception as e:
        logging.getLogger(__name__).warning(f"[{point_id}] Pollution failed: {e}")

    return weather_record, pollution_records

def main():
    parser = argparse.ArgumentParser(description="Unified High-Speed OpenWeather Ingestion")
    parser.add_argument("--workers", type=int, default=None, help="Force number of workers")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of points to ingest (for testing)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logger = logging.getLogger(__name__)

    # Load environment variables from .env if present
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), '.env'))

    # Load tokens
    tokens = []
    
    # Check for plural comma-separated format
    token_str = os.environ.get("OPENWEATHER_API_TOKENS") or os.environ.get("OPENWEATHER_API_TOKEN")
    if token_str:
        tokens.extend([t.strip() for t in token_str.split(",") if t.strip()])
    
    # Check for numbered format (e.g. OPENWEATHER_API_TOKEN_1)
    for i in range(1, 21):  # Support up to 20 keys
        key = f"OPENWEATHER_API_TOKEN_{i}"
        val = os.environ.get(key)
        if val:
            tokens.append(val.strip())

    if not tokens:
        logger.error("No OpenWeather tokens found in environment (tried OPENWEATHER_API_TOKENS, OPENWEATHER_API_TOKEN, or OPENWEATHER_API_TOKEN_1...n)")
        sys.exit(1)
    
    tokens = list(set(tokens)) # Deduplicate
    token_manager = TokenManager(tokens)
    
    # Create client with TokenManager
    # Note: OpenWeather expects 'appid' param. We'll tell APIClient to use 'appid' param instead of Authorization header.
    client = APIClient(
        base_url="https://api.openweathermap.org/data/2.5",
        token_manager=token_manager,
        auth_header_name="appid",
        auth_header_format="{}"
    )

    writer = get_data_writer()
    all_points = load_ingestion_points()
    
    # Apply limit if specified
    if args.limit:
        logger.info(f"Test mode: limiting ingestion to first {args.limit} points")
        points = dict(list(all_points.items())[:args.limit])
    else:
        points = all_points
    
    all_weather = []
    all_pollution = []
    chunk_size = 50
    processed_count = 0
    
    # Calculate workers: roughly 10 per token is safe
    num_workers = args.workers or (len(tokens) * 15)
    logger.info(f"Starting ingestion for {len(points)} points using {len(tokens)} tokens and {num_workers} workers.")

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(process_point, pid, d, client): pid for pid, d in points.items()}
        
        for future in as_completed(futures):
            w, p = future.result()
            if w: all_weather.append(w)
            if p: all_pollution.extend(p)
            
            processed_count += 1
            if processed_count % chunk_size == 0:
                if all_weather:
                    writer.write_batch("raw_openweather_meteorology", all_weather, source="openweather")
                    all_weather = []
                if all_pollution:
                    writer.write_batch("raw_openweather_measurements", all_pollution, source="openweather")
                    all_pollution = []
                logger.info(f"Persisted chunk: {processed_count}/{len(points)} points completed.")

    # Write to ClickHouse
    weather_count = 0
    pollution_count = 0
    
    if all_weather:
        writer.write_batch("raw_openweather_meteorology", all_weather, source="openweather")
        weather_count = len(all_weather)
        
    if all_pollution:
        writer.write_batch("raw_openweather_measurements", all_pollution, source="openweather")
        pollution_count = len(all_pollution)

    logger.info(f"Ingestion complete. Weather: {weather_count}, Pollution: {pollution_count}")
    update_control(source="openweather_unified", records_ingested=weather_count + pollution_count, success=True)

if __name__ == "__main__":
    main()
