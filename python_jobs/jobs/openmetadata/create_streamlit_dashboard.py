"""
Script to create Granular Streamlit Dashboard Service and Lineage in OpenMetadata.
This will create multiple Dashboards (one for each Streamlit Page) to correctly map data sources.
"""

import os
import requests
import base64
from urllib.parse import quote

OM_URL = os.environ.get(
    "OPENMETADATA_URL",
    "http://openmetadata:8585/api",
).rstrip('/')
if not OM_URL.endswith('/api'):
    OM_URL += '/api'
OM_USER = os.environ.get("OM_ADMIN_USER")
OM_PASS = os.environ.get("OM_ADMIN_PASSWORD")
if not OM_USER or not OM_PASS:
    raise RuntimeError("OM_ADMIN_USER and OM_ADMIN_PASSWORD must be set")

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
    url = f"{OM_URL}/v1/{entity_type}/name/{quote(fqn, safe='')}"
    resp = requests.get(url, headers=headers, timeout=15)
    if resp.ok:
        return resp.json()["id"]
    return None

PAGES_CONFIG = [
    {
        "name": "streamlit_overview",
        "displayName": "Tổng quan",
        "description": "Tổng quan chất lượng không khí Việt Nam với KPI, bản đồ và trạng thái hiện tại.",
        "url": "http://localhost:8501",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_air_quality_national_summary",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_air_quality_overview_daily",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_air_quality_overview_hourly"
        ]
    },
    {
        "name": "streamlit_pollutants",
        "displayName": "Chất ô nhiễm",
        "description": "Phân tích xu hướng và thành phần các chất ô nhiễm chính.",
        "url": "http://localhost:8501",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_pollutant_source_fingerprint",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_aqi_temporal_patterns",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.fct_air_quality_summary_hourly"
        ]
    },
    {
        "name": "streamlit_source_comparison",
        "displayName": "So sánh nguồn",
        "description": "So sánh nguồn dữ liệu AQI và độ tươi dữ liệu theo nguồn.",
        "url": "http://localhost:8501",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.fct_aqi_weather_traffic_unified",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_aqi_current_status"
        ]
    },
    {
        "name": "streamlit_historical_trend",
        "displayName": "Xu hướng lịch sử",
        "description": "Xu hướng AQI lịch sử từ dữ liệu đo thực tế theo ngày và theo tháng.",
        "url": "http://localhost:8501",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_air_quality_overview_daily",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_air_quality_overview_monthly",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.fct_air_quality_summary_daily"
        ]
    },
    {
        "name": "streamlit_alerts",
        "displayName": "Cảnh báo",
        "description": "Cảnh báo vi phạm ngưỡng và tình trạng sức khỏe nền tảng.",
        "url": "http://localhost:8501",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_aqi_current_status",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_platform_data_health"
        ]
    },
    {
        "name": "streamlit_traffic_impact",
        "displayName": "Ảnh hưởng Giao thông",
        "description": "Tương quan giữa giao thông và chất lượng không khí theo giờ.",
        "url": "http://localhost:8501",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_traffic_hourly_trend",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_traffic_pollution_correlation_daily"
        ]
    },
    {
        "name": "streamlit_health_risk",
        "displayName": "Rủi ro Sức khỏe",
        "description": "Xếp hạng phơi nhiễm và rủi ro sức khỏe theo khu vực.",
        "url": "http://localhost:8501",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_aqi_health_impact_summary",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_regional_health_risk_ranking"
        ]
    },
    {
        "name": "streamlit_status",
        "displayName": "Trạng thái Hệ thống",
        "description": "Tình trạng pipeline, độ tươi dữ liệu và sức khỏe hệ thống.",
        "url": "http://localhost:8501",
        "tables": [
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.dm_platform_data_health",
            "Vietnam Air Quality ClickHouse.air_quality.air_quality.fct_air_quality_summary_hourly"
        ]
    },
    {
        "name": "streamlit_weather_impact",
        "displayName": "Ảnh hưởng Thời tiết",
        "description": "Phân tích mối liên hệ giữa thời tiết và ô nhiễm không khí.",
        "url": "http://localhost:8501",
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
    resp = requests.put(
        f"{OM_URL}/v1/services/dashboardServices",
        json=service_payload,
        headers=headers,
        timeout=30,
    )
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
        resp = requests.put(
            f"{OM_URL}/v1/dashboards",
            json=dashboard_payload,
            headers=headers,
            timeout=30,
        )
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
            res = requests.put(
                f"{OM_URL}/v1/lineage",
                json=lineage_payload,
                headers=headers,
                timeout=30,
            )
            if res.ok:
                table_short = table_fqn.split(".")[-1]
                print(f"   ✅ Lineage added: {table_short} -> {page['displayName']}")
            else:
                print(f"   ❌ Failed lineage for {table_fqn}: {res.text}")

    # Remove the old main_dashboard if it exists to avoid confusion
    print("\nCleaning up old main_dashboard if present...")
    old_db_id = get_entity_id("dashboards", "Streamlit_App.main_dashboard", token)
    if old_db_id:
        requests.delete(
            f"{OM_URL}/v1/dashboards/{old_db_id}?hardDelete=true",
            headers=headers,
            timeout=30,
        )
        print("   ✅ Deleted old monolithic dashboard.")

    print("\nDone! Streamlit dashboards and specific granular lineage are now available in OpenMetadata.")

if __name__ == "__main__":
    create_streamlit_resources()
