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
        "displayName": "Page 1: Overview",
        "description": "Main overview map, KPI trends and geographic distribution.",
        "url": "http://localhost:8501/Overview",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.mart_air_quality__daily_summary",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.mart_analytics__trends",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.mart_analytics__geographic"
        ]
    },
    {
        "name": "streamlit_pollutants",
        "displayName": "Page 2: Pollutants Analysis",
        "description": "Detailed breakdown of various pollutants and concentration KPIs.",
        "url": "http://localhost:8501/Pollutants",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.mart_kpis__pollutant_concentrations",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.mart_air_quality__daily_summary"
        ]
    },
    {
        "name": "streamlit_source_comparison",
        "displayName": "Page 3: Source Comparison",
        "description": "Compare AQI from different sources (AQICN, OpenWeather, Sensors.Community).",
        "url": "http://localhost:8501/Source_Comparison",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.mart_air_quality__daily_summary"
        ]
    },
    {
        "name": "streamlit_forecast",
        "displayName": "Page 4: Forecast Accuracy",
        "description": "Evaluate forecast performance against actual air quality metrics.",
        "url": "http://localhost:8501/Forecast",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.mart_analytics__forecast_accuracy"
        ]
    },
    {
        "name": "streamlit_alerts",
        "displayName": "Page 5: Alerts System",
        "description": "Current active alerts when AQI breaches critical thresholds.",
        "url": "http://localhost:8501/Alerts",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.mart_air_quality__alerts"
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
