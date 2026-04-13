"""
Register Streamlit and Grafana as Dashboard services in OpenMetadata.
Uses OM REST API 1.12+.
"""

import os
import requests
import base64

OM_URL = os.environ.get("OPENMETADATA_URL", "http://openmetadata:8585/api").rstrip("/")
if not OM_URL.endswith("/api"):
    OM_URL += "/api"
OM_USER = os.environ.get("OM_ADMIN_USER", "admin@open-metadata.org")
OM_PASS = os.environ.get("OM_ADMIN_PASSWORD", "admin")

def om_login() -> str:
    b64_pass = base64.b64encode(OM_PASS.encode("utf-8")).decode("utf-8")
    r = requests.post(
        f"{OM_URL}/v1/users/login",
        json={"email": OM_USER, "password": b64_pass},
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["accessToken"]

def register_dashboard_service(name: str, service_type: str, dashboard_url: str, token: str):
    print(f"Registering Dashboard Service: {name} ({service_type})...")
    payload = {
        "name": name,
        "displayName": name.replace("_", " ").title(),
        "serviceType": service_type,
        "connection": {
            "config": {
                "type": service_type,
                "hostPort": dashboard_url
            }
        }
    }
    r = requests.post(
        f"{OM_URL}/v1/services/dashboardServices",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload,
        timeout=30
    )
    if r.status_code == 201:
        print(f"  ✅ Service created: {name}")
    elif r.status_code == 409:
        print(f"  ℹ Service already exists: {name}")
    else:
        print(f"  ❌ Error {r.status_code}: {r.text}")

def main():
    try:
        token = om_login()
        # Streamlit is custom, often mapped as 'CustomDashboard' or handled as generic
        # However, OM 1.12 supports many connectors. We'll use 'CustomDashboard' for Streamlit
        # and 'Grafana' for Grafana.
        
        register_dashboard_service(
            "Vietnam_Air_Quality_Streamlit", 
            "CustomDashboard", # Fallback for non-native connectors
            "http://streamlit-dashboard:8501", 
            token
        )
        
        register_dashboard_service(
            "Vietnam_Infrastructure_Grafana", 
            "Grafana", 
            "http://grafana:3000", 
            token
        )
        
    except Exception as e:
        print(f"Failed to register services: {e}")

if __name__ == "__main__":
    main()
