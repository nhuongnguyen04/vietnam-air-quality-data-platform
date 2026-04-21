"""
Script to create Granular Streamlit Dashboard Service and Lineage in OpenMetadata.
This will create multiple Dashboards (one for each Streamlit Page) to correctly map data sources.
"""

import os
import requests
import base64

OM_URL = os.environ.get(
    "OPENMETADATA_URL",
    "http://openmetadata:8585/api",
).rstrip('/')
if not OM_URL.endswith('/api'):
    OM_URL += '/api'
OM_USER = os.environ.get("OM_ADMIN_USER", "admin@open-metadata.org")
OM_PASS = os.environ.get("OM_ADMIN_PASSWORD", "admin")

def om_login() -> str:
    """Login to OM and return JWT access token."""
    b64_pass = base64.b64encode(OM_PASS.encode("utf-8")).decode("utf-8")
    r = requests.post(
        f"{OM_URL}/v1/users/login",
        json={"email": OM_USER, "password": b64_pass},
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["accessToken"]

def get_entity_id(entity_type: str, fqn: str, token: str) -> str:
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{OM_URL}/v1/{entity_type}/name/{fqn.replace(' ', '%20')}"
    resp = requests.get(url, headers=headers, timeout=15)
    if resp.ok:
        return resp.json()["id"]
    return None

PAGES_CONFIG = [
    {
        "name": "streamlit_overview",
        "displayName": "Overview",
        "description": "Main AQI overview for Vietnam with current status and rollups.",
        "url": "http://localhost:8501/Overview",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_air_quality_national_summary",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_air_quality_overview_daily",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_air_quality_overview_hourly"
        ]
    },
    {
        "name": "streamlit_pollutants",
        "displayName": "Pollutants",
        "description": "Pollutant-level trend and composition analysis.",
        "url": "http://localhost:8501/Pollutants",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_pollutant_source_fingerprint",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_aqi_temporal_patterns",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.fct_air_quality_summary_hourly"
        ]
    },
    {
        "name": "streamlit_source_comparison",
        "displayName": "Source Comparison",
        "description": "Compare AQI from different sources (AQICN, OpenWeather, Sensors.Community).",
        "url": "http://localhost:8501/Source_Comparison",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.fct_aqi_weather_traffic_unified",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_aqi_current_status"
        ]
    },
    {
        "name": "streamlit_historical_trend",
        "displayName": "Historical Trend",
        "description": "Historical AQI trends from actual measurements — no forecast data required.",
        "url": "http://localhost:8501/Historical_Trend",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_air_quality_overview_daily",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_air_quality_overview_monthly",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.fct_air_quality_summary_daily"
        ]
    },
    {
        "name": "streamlit_alerts",
        "displayName": "Alerts",
        "description": "Current active alerts when AQI breaches critical thresholds.",
        "url": "http://localhost:8501/Alerts",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_aqi_current_status",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_platform_data_health"
        ]
    },
    {
        "name": "streamlit_traffic_impact",
        "displayName": "Traffic Impact",
        "description": "Traffic and air quality interaction trends.",
        "url": "http://localhost:8501/Traffic_Impact",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_traffic_hourly_trend",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_traffic_pollution_correlation_daily"
        ]
    },
    {
        "name": "streamlit_health_risk",
        "displayName": "Health Risk",
        "description": "Population exposure and risk ranking across regions.",
        "url": "http://localhost:8501/Health_Risk",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_aqi_health_impact_summary",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_regional_health_risk_ranking"
        ]
    },
    {
        "name": "streamlit_status",
        "displayName": "Status",
        "description": "Pipeline and data quality status for platform operations.",
        "url": "http://localhost:8501/Status",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_platform_data_health",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.fct_air_quality_summary_hourly"
        ]
    },
    {
        "name": "streamlit_weather_impact",
        "displayName": "Weather Impact",
        "description": "Weather and pollution relationship analysis.",
        "url": "http://localhost:8501/Weather_Impact",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_weather_hourly_trend",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_weather_pollution_correlation_daily"
        ]
    }
]

def create_streamlit_resources():
    token = om_login()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # 1. Create Dashboard Service
    print("1. Creating Streamlit Dashboard Service...")
    service_payload = {
        "name": "Streamlit_App",
        "displayName": "Vietnam Air Quality Streamlit",
        "serviceType": "CustomDashboard",
        "description": "Streamlit interactive dashboard for Vietnam Air Quality",
        "connection": {
            "config": {
                "type": "CustomDashboard"
            }
        }
    }
    resp = requests.put(f"{OM_URL}/v1/services/dashboardServices", json=service_payload, headers=headers)
    if not resp.ok:
        print(f"Failed to create service: {resp.text}")
        return
    print("   ✅ Service Streamlit_App created (or updated).")

    # 2. Iterate and Create individual Dashboards for each Streamlit Page
    for page in PAGES_CONFIG:
        print(f"\n2. Processing Dashboard: {page['displayName']}...")
        dashboard_payload = {
            "name": page["name"],
            "displayName": page["displayName"],
            "description": page["description"],
            "service": "Streamlit_App",
            "sourceUrl": page["url"]
        }
        resp = requests.put(f"{OM_URL}/v1/dashboards", json=dashboard_payload, headers=headers)
        if not resp.ok:
            print(f"   ❌ Failed to create dashboard {page['name']}: {resp.text}")
            continue
        
        dashboard_data = resp.json()
        dashboard_id = dashboard_data["id"]
        print(f"   ✅ Created Dashboard Entity ID: {dashboard_id}")

        # 3. Add Lineage from Tables to this specific Dashboard
        for table_fqn in page["tables"]:
            table_id = get_entity_id("tables", table_fqn, token)
            if not table_id:
                table_short = table_fqn.split(".")[-1]
                print(f"   ⚠ Table not found in OpenMetadata: {table_short}")
                continue
            
            lineage_payload = {
                "edge": {
                    "fromEntity": {"id": table_id, "type": "table"},
                    "toEntity": {"id": dashboard_id, "type": "dashboard"}
                }
            }
            res = requests.put(f"{OM_URL}/v1/lineage", json=lineage_payload, headers=headers)
            if res.ok:
                table_short = table_fqn.split(".")[-1]
                print(f"   ✅ Lineage added: {table_short} -> {page['displayName']}")
            else:
                print(f"   ❌ Failed lineage for {table_fqn}: {res.text}")

    # Remove the old main_dashboard if it exists to avoid confusion
    print("\nCleaning up old main_dashboard if present...")
    old_db_id = get_entity_id("dashboards", "Streamlit_App.main_dashboard", token)
    if old_db_id:
        requests.delete(f"{OM_URL}/v1/dashboards/{old_db_id}?hardDelete=true", headers=headers)
        print("   ✅ Deleted old monolithic dashboard.")

    print("\nDone! Streamlit dashboards and specific granular lineage are now available in OpenMetadata.")

if __name__ == "__main__":
    create_streamlit_resources()
