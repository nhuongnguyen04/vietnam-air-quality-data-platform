#!/usr/bin/env python3
"""
Traffic Pattern Engine - Python Calculation Job.

This job performs multi-type weighted interpolation to convert 3rd-hourly
TomTom traffic samples into 1-hourly traffic records based on location-specific
traffic profiles (Urban, Industrial, Highway, Rural) for Vietnam.

Usage:
    python jobs/traffic/calculate_hourly_traffic.py

Author: Air Quality Data Platform
"""

import os
import sys
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

import clickhouse_connect

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

def get_client():
    """Create ClickHouse client from environment variables."""
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        username=os.environ.get("CLICKHOUSE_USER", "admin"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "admin123456"),
        database=os.environ.get("CLICKHOUSE_DB", "air_quality"),
    )

def query_df(query, client):
    """Execute query and return DataFrame."""
    result = client.query(query)
    return pd.DataFrame(result.named_results())

def load_metadata(client):
    """Load station categories and traffic profiles from ClickHouse/Seeds."""
    logger.info("Loading metadata and profiles...")
    
    # These tables are populated via dbt seed
    stations_df = query_df("SELECT station_name, location_type FROM air_quality.vn_station_coordinates", client)
    profiles_df = query_df("SELECT location_type, hour, weight FROM air_quality.vn_traffic_profile", client)
    
    # Store as dictionaries for fast lookup
    station_map = dict(zip(stations_df['station_name'], stations_df['location_type']))
    
    # Profile map: {location_type: {hour: weight}}
    profile_map = {}
    for _, row in profiles_df.iterrows():
        ltype = row['location_type']
        if ltype not in profile_map:
            profile_map[ltype] = {}
        profile_map[ltype][int(row['hour'])] = float(row['weight'])
        
    return station_map, profile_map

def run_calculation():
    """Main calculation loop."""
    client = get_client()
    
    # 1. Load configuration
    station_map, profile_map = load_metadata(client)
    
    # 2. Read last 36 hours of raw samples
    # We look back enough to have at least 2 samples for a full day's interpolation
    logger.info("Reading raw traffic samples...")
    raw_query = """
    SELECT station_name, latitude, longitude, timestamp_utc, current_speed, free_flow_speed
    FROM air_quality.raw_tomtom_traffic
    WHERE timestamp_utc >= now() - interval 36 hour
    ORDER BY station_name, timestamp_utc
    """
    raw_df = query_df(raw_query, client)
    
    if raw_df.empty:
        logger.warning("No raw traffic samples found in the last 36 hours. Skipping calculation.")
        return

    # Calculate congestion ratio
    raw_df['congestion_ratio'] = 1.0 - (raw_df['current_speed'] / raw_df['free_flow_speed'])
    raw_df['congestion_ratio'] = raw_df['congestion_ratio'].clip(0, 1)

    calculated_records = []
    
    # 3. Process each station
    for station, group in raw_df.groupby('station_name'):
        ltype = station_map.get(station, 'urban')
        weights = profile_map.get(ltype, profile_map.get('urban'))
        
        station_lat = group['latitude'].iloc[0]
        station_lon = group['longitude'].iloc[0]
        
        # Sort by time
        group = group.sort_values('timestamp_utc')
        
        # We perform interpolation between consecutive samples
        for i in range(len(group) - 1):
            s1 = group.iloc[i]
            s2 = group.iloc[i+1]
            
            t1 = s1['timestamp_utc']
            t2 = s2['timestamp_utc']
            
            # Calculate hours between samples
            delta_hours = int((t2 - t1).total_seconds() / 3600)
            
            # For each hour in the window
            for h_offset in range(delta_hours):
                current_time = t1 + timedelta(hours=h_offset)
                hour_val = current_time.hour
                
                # Check if this is the exact sample point
                if h_offset == 0:
                    congestion = s1['congestion_ratio']
                    flag = 'real-time'
                else:
                    # Multi-profile Weighted Interpolation Logic:
                    # We scale the sample value based on the relative weights in the profile
                    w_t1 = weights.get(t1.hour, 0.5)
                    w_curr = weights.get(hour_val, 0.5)
                    
                    # Estimate based on the first sample and the profile evolution
                    # This captures the "Rush Hour" spikes even between 3-hour samples
                    congestion = s1['congestion_ratio'] * (w_curr / w_t1 if w_t1 > 0 else 1.0)
                    congestion = max(0.0, min(1.0, float(congestion)))
                    flag = 'interpolated'
                
                calculated_records.append({
                    'station_name': station,
                    'latitude': station_lat,
                    'longitude': station_lon,
                    'hour_utc': current_time,
                    'congestion_ratio': float(congestion),
                    'data_quality_flag': flag,
                    'updated_at': datetime.now()
                })
        
        # Add the very last sample of the group
        last_s = group.iloc[-1]
        calculated_records.append({
            'station_name': station,
            'latitude': station_lat,
            'longitude': station_lon,
            'hour_utc': last_s['timestamp_utc'],
            'congestion_ratio': float(np.clip(last_s['congestion_ratio'], 0, 1)),
            'data_quality_flag': 'real-time',
            'updated_at': datetime.now()
        })

    # 4. Write results back to ClickHouse
    if calculated_records:
        logger.info(f"Writing {len(calculated_records)} hourly records to ClickHouse...")
        # Convert to list of tuples for clickhouse-connect insert
        data_to_insert = [
            (
                r['station_name'], r['latitude'], r['longitude'], 
                r['hour_utc'], r['congestion_ratio'], r['data_quality_flag'], 
                r['updated_at']
            ) 
            for r in calculated_records
        ]
        
        client.insert(
            'air_quality.raw_tomtom_traffic_hourly',
            data_to_insert,
            column_names=['station_name', 'latitude', 'longitude', 'hour_utc', 'congestion_ratio', 'data_quality_flag', 'updated_at']
        )
        logger.info("Traffic calculation complete.")
    else:
        logger.info("No records to calculate.")

if __name__ == "__main__":
    run_calculation()
