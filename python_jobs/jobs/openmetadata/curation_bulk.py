"""
Bulk catalog curation using OM REST API.
No openmetadata-ingestion library needed — uses simple requests calls.

Usage (from project root):
    OPENMETADATA_URL=http://localhost:8585/api \
    OM_ADMIN_USER=admin@open-metadata.org \
    OM_ADMIN_PASSWORD=admin \
    python python_jobs/jobs/openmetadata/curation_bulk.py

Environment variables:
    OPENMETADATA_URL: OpenMetadata API base URL (default: http://openmetadata:8585/api)
    OM_ADMIN_USER:     OM admin username   (default: admin@open-metadata.org)
    OM_ADMIN_PASSWORD: OM admin password   (default: admin)
"""

import os
import sys
import requests
import base64

OM_URL = os.environ.get(
    "OPENMETADATA_URL",
    "http://openmetadata:8585/api",
).rstrip("/")
if not OM_URL.endswith("/api"):
    OM_URL += "/api"
OM_USER = os.environ.get("OM_ADMIN_USER", "admin@open-metadata.org")
OM_PASS = os.environ.get("OM_ADMIN_PASSWORD", "admin")

# ─── Authentication ──────────────────────────────────────────────────────────────

_token_cache: list[str] = []


def om_login() -> str:
    """
    Login to OM 1.12+ and return JWT access token.
    OM 1.12+ requires password as base64-encoded string and uses 'email' field.
    """
    b64_pass = base64.b64encode(OM_PASS.encode("utf-8")).decode("utf-8")
    r = requests.post(
        f"{OM_URL}/v1/users/login",
        json={"email": OM_USER, "password": b64_pass},
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    r.raise_for_status()
    token = r.json()["accessToken"]
    _token_cache.clear()
    _token_cache.append(token)
    return token


def get_token() -> str:
    """Return cached token or login fresh."""
    if _token_cache:
        return _token_cache[0]
    return om_login()


# ─── PATCH helpers ──────────────────────────────────────────────────────────────

def _patch_table_field(fqn_url: str, path: str, value, token: str) -> bool:
    """
    Send a single-field JSON-PATCH to a table.
    Returns True on 200/204, False on 404/4xx (error is logged, not raised).
    """
    r = requests.patch(
        f"{OM_URL}/v1/tables/name/{fqn_url}",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json-patch+json",
        },
        json=[{"op": "replace", "path": path, "value": value}],
        timeout=30,
    )
    if r.status_code == 404:
        return False
    if r.status_code >= 400:
        err = r.text[:150].replace("\n", " ")
        print(f"        ⚠ {path} → {r.status_code}: {err}")
        return False
    return True


def _get_user_id(username: str, token: str) -> str | None:
    """
    Look up OM user ID by username.
    OM 1.12+ uses '/v1/users/name/{username}' endpoint.
    Returns None if user not found.
    """
    r = requests.get(
        f"{OM_URL}/v1/users/name/{username}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    if r.ok:
        return r.json()["id"]
    return None


def _get_admin_user_id(token: str) -> str | None:
    """Get the admin user ID — try 'admin' first, then iterate users."""
    admin_id = _get_user_id("admin", token)
    if admin_id:
        return admin_id
    # Fall back: search via list
    r = requests.get(
        f"{OM_URL}/v1/users?limit=50",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    if r.ok:
        for user in r.json().get("data", []):
            if user.get("name") == "admin" or user.get("email", "").startswith("admin"):
                return user.get("id")
    return None


# ─── Main curation function ────────────────────────────────────────────────────

def patch_table(
    fqn: str,
    description: str = None,
    owner_name: str = None,
    tags: list[str] = None,
    tier: str = None,
) -> None:
    """
    Apply curation fields to a table via JSON-PATCH requests.

    OM 1.12 REST API compatibility for ClickHouse tables:
    - /description ✅  works reliably
    - /owner       ⚠️  works if user exists, requires user ID lookup
    - /tags        ❌  replace on empty array → 404; add works on existing-tag tables only
    - /tier        ❌  always → 500 in OM 1.12 for ClickHouse tables
    - /tags/{n}    ⚠️  index-based add — unpredictable

    WORKAROUND for tags/tier: apply via OM UI (Settings → Tags), or
    re-ingest tables from om-ingestion with om_reader user that sets initial tags.
    This script focuses on: descriptions + owners (where supported).
    """
    token = get_token()
    fqn_url = fqn.replace(" ", "%20")
    ok = True

    if description:
        if _patch_table_field(fqn_url, "/description", description, token):
            print("        ✅ description")
        else:
            ok = False

    # Owner: supported via GET /v1/users/name/admin → patch with user ID
    if owner_name:
        user_id = _get_admin_user_id(token)
        if user_id:
            if _patch_table_field(fqn_url, "/owner", {"id": user_id, "type": "user"}, token):
                print("        ✅ owner")
            else:
                ok = False
        else:
            print("        ⚠ owner: admin user not found in OM")

    # Tags/tier: OM 1.12 limitation — log warning, suggest UI
    if tags or tier:
        print(
            "        ⚠ tags/tier: not supported via REST PATCH in OM 1.12"
            " → apply via OM UI (Settings → Tags → Edit table)"
        )

    if ok:
        print(f"  ✅ {fqn}")


# ─── Curation definitions ──────────────────────────────────────────────────────

MART_CURATION = [
    {
        "fqn": "Vietnam Air Quality ClickHouse.air_quality.air_quality.fct_hourly_aqi",
        "description": (
            "Hourly AQI measurements per station and pollutant. Primary fact table for "
            "air quality analytics. Values normalized to US EPA AQI scale (0-500). "
            "Populated by dbt intermediate/int_unified__measurements. "
            "Serves Streamlit dashboard (Overview, Pollutants pages)."
        ),
        "owner": "admin",
        "tags": ["AirQuality", "Vietnam", "NoPII"],
        "tier": "Tier1",
    },
    {
        "fqn": "Vietnam Air Quality ClickHouse.air_quality.air_quality.fct_daily_aqi_summary",
        "description": (
            "Daily AQI summary per station. Aggregates hourly measurements into daily "
            "min/max/avg/median AQI. Uses US EPA category breakpoints."
        ),
        "owner": "admin",
        "tags": ["AirQuality", "Vietnam", "NoPII"],
        "tier": "Tier1",
    },
    {
        "fqn": "Vietnam Air Quality ClickHouse.air_quality.air_quality.fact_aqi_alerts",
        "description": (
            "Alert events when AQI exceeds WHO/US EPA exposure thresholds. "
            "Generated by dbt rules in intermediate/int_aqi_calculations. "
            "Serves as source for Streamlit alert notifications and Grafana alerts."
        ),
        "owner": "admin",
        "tags": ["AirQuality", "Alerts", "Vietnam", "NoPII"],
        "tier": "Tier1",
    },
    {
        "fqn": "Vietnam Air Quality ClickHouse.air_quality.air_quality.dim_locations",
        "description": (
            "Dimension table for monitoring station locations across 63 provinces "
            "of Vietnam. Includes GPS coordinates (lat 8-24, lon 102-110), elevation, "
            "station type, and source attribution. Sourced from AQI.in (~540 stations) "
            "and OpenWeather (62 provinces). D-AQI-02 (Phase 6): AQICN + Sensors.Community removed."
        ),
        "owner": "admin",
        "tags": ["AirQuality", "Vietnam", "NoPII"],
        "tier": "Tier2",
    },
    {
        "fqn": "Vietnam Air Quality ClickHouse.air_quality.air_quality.dim_time",
        "description": (
            "Time dimension table. Pre-generated hourly buckets (2020-2030) for "
            "efficient time-series joins in analytical queries."
        ),
        "owner": "admin",
        "tags": ["AirQuality"],
        "tier": "Tier2",
    },
    {
        "fqn": "Vietnam Air Quality ClickHouse.air_quality.air_quality.dim_pollutants",
        "description": (
            "Pollutant dimension. Defines PM2.5, PM10, O₃, NO₂, SO₂, CO with "
            "AQI breakpoints per pollutant per averaging period (1-hour, 8-hour, 24-hour)."
        ),
        "owner": "admin",
        "tags": ["AirQuality"],
        "tier": "Tier2",
    },
    {
        "fqn": "Vietnam Air Quality ClickHouse.air_quality.air_quality.mart_air_quality__dashboard",
        "description": (
            "Pre-aggregated dashboard-ready dataset. Combines hourly AQI, dominant "
            "pollutant, and trend indicators. Primary source for "
            "Streamlit dashboard."
        ),
        "owner": "admin",
        "tags": ["AirQuality", "Dashboard", "Vietnam", "NoPII"],
        "tier": "Tier1",
    },
    {
        "fqn": "Vietnam Air Quality ClickHouse.air_quality.air_quality.mart_air_quality__hourly",
        "description": (
            "Hourly aggregated metrics per station per hour. Includes AQI, "
            "pollutant breakdown, and data quality indicators."
        ),
        "owner": "admin",
        "tags": ["AirQuality", "Vietnam", "NoPII"],
        "tier": "Tier2",
    },
    {
        "fqn": "Vietnam Air Quality ClickHouse.air_quality.air_quality.mart_air_quality__daily_summary",
        "description": (
            "Daily summary metrics per station. Includes min/max/avg AQI, "
            "dominant pollutant, and sensor quality tier."
        ),
        "owner": "admin",
        "tags": ["AirQuality", "Vietnam", "NoPII"],
        "tier": "Tier2",
    },
    {
        "fqn": "Vietnam Air Quality ClickHouse.air_quality.air_quality.mart_air_quality__alerts",
        "description": (
            "Alert events table for Streamlit dashboard. Derives from "
            "fact_aqi_alerts with deduplication logic."
        ),
        "owner": "admin",
        "tags": ["AirQuality", "Alerts", "Vietnam", "NoPII"],
        "tier": "Tier1",
    },
    {
        "fqn": "Vietnam Air Quality ClickHouse.air_quality.air_quality.mart_air_quality__stations",
        "description": (
            "Station metadata denormalized for dashboard display. "
            "Combines AQI.in stations (~540 locations) and OpenWeather city metadata. "
            "D-AQI-02 (Phase 6): AQICN + Sensors.Community removed."
        ),
        "owner": "admin",
        "tags": ["AirQuality", "Vietnam", "NoPII"],
        "tier": "Tier2",
    },
]

RAW_CURATION = [
    {
        "fqn": "Vietnam Air Quality ClickHouse.air_quality.air_quality.raw_aqiin_measurements",
        "description": (
            "Raw AQI.in (~540 Vietnam monitoring stations) web-scraped measurements. "
            "Source: aqi.in widget API (httpx-based scraper). "
            "Parameters: pm25, pm10, co, no2, o3, so2, temp, hum. "
            "D-AQI-02 (Phase 6): Primary air quality source, replacing AQICN + Sensors.Community."
        ),
        "owner": "admin",
        "tags": ["Raw", "AQIin", "NoPII"],
        "tier": "Tier3",
    },
    {
        "fqn": "Vietnam Air Quality ClickHouse.air_quality.air_quality.raw_aqiin_stations",
        "description": (
            "Raw AQI.in station metadata (station name, province, URL slug). "
            "ReplacingMergeTree deduplicated on station_id. "
            "D-AQI-02 (Phase 6): Primary station registry."
        ),
        "owner": "admin",
        "tags": ["Raw", "AQIin", "NoPII"],
        "tier": "Tier3",
    },
    {
        "fqn": "Vietnam Air Quality ClickHouse.air_quality.air_quality.raw_openweather_measurements",
        "description": (
            "Raw OpenWeather Air Pollution API measurements for 62 Vietnam provinces. "
            "Parameters: pm25, pm10, o3, no2, so2, co, nh3, no. "
            "D-AQI-02 (Phase 6): Secondary air quality source alongside AQI.in."
        ),
        "owner": "admin",
        "tags": ["Raw", "OpenWeather", "NoPII"],
        "tier": "Tier3",
    },
    {
        "fqn": "Vietnam Air Quality ClickHouse.air_quality.air_quality.ingestion_control",
        "description": (
            "Ingestion run metadata tracking. Logs source, last_run, records_ingested, "
            "lag_seconds, error_message per source. Consumed by Grafana freshness dashboards."
        ),
        "owner": "admin",
        "tags": ["Infrastructure", "NoPII"],
        "tier": "Tier3",
    },
]


def get_entity_id(entity_type: str, fqn: str) -> str | None:
    """Get entity ID by FQN using OM REST API."""
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    # OM 1.12 handles FQN with spaces encoded
    url = f"{OM_URL}/v1/{entity_type}/name/{fqn.replace(' ', '%20')}"
    resp = requests.get(url, headers=headers, timeout=15)
    if resp.ok:
        return resp.json()["id"]
    return None


def get_dbt_model_fqns() -> list[str]:
    """Parse dbt manifest for all model FQNs. Returns list of table FQNs."""
    import json
    import os

    manifest_paths = [
        "/opt/dbt/dbt_tranform/target/manifest.json",
        "/opt/dbt-artifacts/manifest.json",
        os.path.join(
            os.path.dirname(__file__),
            "../../../../dbt/dbt_tranform/target/manifest.json",
        ),
        "/home/nhuong/vietnam-air-quality-data-platform/dbt/dbt_tranform/target/manifest.json",
    ]
    tables = []
    for manifest_path in manifest_paths:
        if os.path.exists(manifest_path):
            try:
                with open(manifest_path, "r") as f:
                    data = json.load(f)
                for node in data.get("nodes", {}).values():
                    if node.get("resource_type") == "model":
                        tables.append(
                            f"Vietnam Air Quality ClickHouse.air_quality.air_quality."
                            f"{node['name']}"
                        )
                break  # Stop at first valid manifest
            except Exception:
                continue
    return tables


def get_raw_table_fqns() -> list[str]:
    """Dynamically fetch raw_* tables and ingestion_control from OM."""
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = (
        f"{OM_URL}/v1/tables"
        f"?databaseSchema=Vietnam%20Air%20Quality%20ClickHouse.air_quality.air_quality"
        f"&limit=1000"
    )
    resp = requests.get(url, headers=headers, timeout=15)
    tables = []
    if resp.ok:
        for t in resp.json().get("data", []):
            name = t.get("name", "")
            if name.startswith("raw_") or name == "ingestion_control":
                tables.append(t.get("fullyQualifiedName", ""))
    return tables


def apply_pipeline_lineage() -> None:
    """
    Create pipeline→table lineage edges in OM.
    dag_ingest_hourly → raw tables
    dag_transform → dbt model tables (fct_*, mart_*, int_*)
    """
    print("Applying Dynamic Pipeline-to-Table Lineage...")
    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    raw_fqns = get_raw_table_fqns()
    dbt_fqns = get_dbt_model_fqns()

    dynamic_mappings = [
        {
            "pipeline_fqn": "Vietnam Air Quality Airflow.dag_ingest_hourly",
            "tables": raw_fqns,
        },
        {
            "pipeline_fqn": "Vietnam Air Quality Airflow.dag_transform",
            "tables": dbt_fqns,
        },
    ]

    edges_created = 0
    for mapping in dynamic_mappings:
        tables = mapping["tables"]
        if not tables:
            print(f"  ⚠ No tables found for pipeline: {mapping['pipeline_fqn']}")
            continue

        p_id = get_entity_id("pipelines", mapping["pipeline_fqn"])
        if not p_id:
            print(f"  ⚠ Pipeline not found: {mapping['pipeline_fqn']}")
            continue

        for t_fqn in tables:
            if not t_fqn:
                continue
            t_id = get_entity_id("tables", t_fqn)
            if not t_id:
                continue

            payload = {
                "edge": {
                    "fromEntity": {"id": p_id, "type": "pipeline"},
                    "toEntity": {"id": t_id, "type": "table"},
                }
            }
            resp = requests.put(
                f"{OM_URL}/v1/lineage", headers=headers, json=payload, timeout=15
            )
            if resp.ok:
                short_t = t_fqn.split(".")[-1]
                pipeline_name = mapping["pipeline_fqn"].split(".")[-1]
                print(f"  ✅ {pipeline_name} → {short_t}")
                edges_created += 1

    print(f"Pipeline Lineage: {edges_created} edges created.\n")


def apply_all_curation() -> int:
    """
    Apply curation to all mart and raw tables.
    Returns number of tables processed.
    """
    print("Starting catalog curation...")
    all_tables = MART_CURATION + RAW_CURATION
    for i, table in enumerate(all_tables, 1):
        short_name = table["fqn"].split(".")[-1]
        print(f"[{i}/{len(all_tables)}] {short_name}")
        try:
            patch_table(
                fqn=table["fqn"],
                description=table.get("description"),
                owner_name=table.get("owner"),
                tags=table.get("tags"),
                tier=table.get("tier"),
            )
        except Exception as e:
            print(f"  ⚠ Failed: {e}")
    print(f"\nCuration complete. Processed {len(all_tables)} tables.")
    return len(all_tables)


if __name__ == "__main__":
    count = apply_all_curation()
    apply_pipeline_lineage()
    print(f"\n✅ Done. {count} tables curated.")
