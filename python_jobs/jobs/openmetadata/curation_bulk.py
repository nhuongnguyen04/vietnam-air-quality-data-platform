"""
Bulk catalog curation — bổ sung thông tin KHÔNG tự động thu thập được bởi OM agents.

Phạm vi của script này:
  - Chỉ áp dụng cho các bảng KHÔNG phải dbt model (raw tables, reference tables, seeds).
  - Các dbt models (stg_*, int_*, dm_*, fct_*) được agent dbt pipeline tự xử lý
    từ manifest.json — descriptions, column docs, tags (meta), lineage.
  - Script này KHÔNG duplicate thông tin đã có trong dbt YAML docs.

Loại thông tin bổ sung:
  1. Description cho raw tables (raw_aqiin_*, raw_openweather_*, raw_tomtom_*)
  2. Description cho reference/seed tables (pollutants, seed_dim_time, source_calibration, ...)
  3. Pipeline→RawTable lineage (dag_ingest_hourly → raw_*) — dbt chỉ track dbt→dbt lineage

Usage (từ project root):
    OPENMETADATA_URL=http://localhost:8585/api \\
    OM_ADMIN_USER=admin@open-metadata.org \\
    OM_ADMIN_PASSWORD=admin \\
    python python_jobs/jobs/openmetadata/curation_bulk.py
"""

import os
import requests
import base64

OM_URL = os.environ.get("OPENMETADATA_URL", "http://openmetadata:8585/api").rstrip("/")
if not OM_URL.endswith("/api"):
    OM_URL += "/api"
OM_USER = os.environ.get("OM_ADMIN_USER", "admin@open-metadata.org")
OM_PASS = os.environ.get("OM_ADMIN_PASSWORD", "admin")

SERVICE = "Vietnam Air Quality ClickHouse"
DB = "air_quality"
SCHEMA = "air_quality"
PREFIX = f"{SERVICE}.{DB}.{SCHEMA}"

# ─── Auth ─────────────────────────────────────────────────────────────────────

_token_cache: list[str] = []


def om_login() -> str:
    b64_pass = base64.b64encode(OM_PASS.encode()).decode()
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
    return _token_cache[0] if _token_cache else om_login()


# ─── PATCH helpers ─────────────────────────────────────────────────────────────

def _patch_table(fqn: str, path: str, value) -> bool:
    """Thử 'add' rồi 'replace' để tương thích OM 1.12."""
    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json-patch+json",
    }
    url = f"{OM_URL}/v1/tables/name/{fqn.replace(' ', '%20')}"
    for op in ("add", "replace"):
        r = requests.patch(url, headers=headers,
                           json=[{"op": op, "path": path, "value": value}],
                           timeout=30)
        if r.status_code == 404:
            return False
        if r.status_code < 400:
            return True
        if r.status_code == 500 and "Non-existing" in r.text and op == "add":
            continue  # thử replace
        print(f"    ⚠ {path} [{op}] → {r.status_code}: {r.text[:120]}")
        return False
    return False


def get_entity_id(entity_type: str, fqn: str) -> str | None:
    token = get_token()
    r = requests.get(
        f"{OM_URL}/v1/{entity_type}/name/{fqn.replace(' ', '%20')}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    return r.json()["id"] if r.ok else None


# ─── Curation: chỉ non-dbt tables ─────────────────────────────────────────────
#
# Agent dbt pipeline tự xử lý (KHÔNG cần curation thủ công):
#   stg_aqiin__measurements, stg_openweather__measurements, stg_openweather__meteorology,
#   stg_tomtom__flow, stg_core__stations, stg_core__pollutants, stg_core__population,
#   int_core__measurements_unified, int_aqi__calculations, int_traffic__pattern_enrichment,
#   fct_air_quality_summary_hourly, fct_air_quality_summary_daily, fct_air_quality_summary_monthly,
#   fct_air_quality_summary_hourly_province, fct_air_quality_summary_daily_province,
#   fct_air_quality_summary_monthly_province, fct_other_measurements_hourly,
#   dm_aqi_weather_traffic_unified, dm_aqi_current_status, dm_platform_data_health,
#   dm_aqi_compliance_standards, dm_aqi_health_impact_summary, dm_aqi_temporal_patterns,
#   dm_pollutant_source_fingerprint, dm_regional_health_risk_ranking,
#   fct_traffic_pollution_correlation_daily
#
# Non-dbt tables cần curation thủ công (14 bảng):

NON_DBT_CURATION = [
    # ── Raw tables ── (nguồn: ingest scripts, GitHub Actions, dag_sync_gdrive)
    {
        "fqn": f"{PREFIX}.raw_aqiin_measurements",
        "description": (
            "Raw measurements từ AQI.in — ~540 trạm quan trắc không khí Việt Nam. "
            "Nguồn: widget API aqi.in (httpx scraper, chạy mỗi giờ qua dag_ingest_hourly). "
            "Columns: station_name, timestamp_utc, parameter, value, unit, raw_payload (ZSTD compressed JSON). "
            "D-AQI-02 (Phase 6): Nguồn AQI chính, thay thế AQICN. "
            "ReplacingMergeTree — dedup theo (station_name, timestamp_utc, parameter)."
        ),
    },
    {
        "fqn": f"{PREFIX}.raw_openweather_measurements",
        "description": (
            "Raw air pollution measurements từ OpenWeather Air Pollution API — 62 tỉnh Việt Nam. "
            "Nguồn: OpenWeather API (chạy mỗi giờ qua dag_ingest_hourly). "
            "Parameters: pm25, pm10, o3, no2, so2, co, nh3, no (µg/m³). "
            "ReplacingMergeTree — dedup theo (station_id, timestamp_utc, parameter)."
        ),
    },
    {
        "fqn": f"{PREFIX}.raw_openweather_meteorology",
        "description": (
            "Raw weather/meteorology data từ OpenWeather Weather API — 62 tỉnh Việt Nam. "
            "Nguồn: OpenWeather API (chạy mỗi giờ qua dag_ingest_hourly). "
            "Columns: province, timestamp_utc, temp, feels_like, pressure, humidity, "
            "wind_speed, wind_deg, wind_gust, clouds_all, weather_description. "
            "Được join trong dm_aqi_weather_traffic_unified để phân tích tương quan khí tượng-ô nhiễm."
        ),
    },
    {
        "fqn": f"{PREFIX}.raw_tomtom_traffic",
        "description": (
            "Raw traffic flow measurements từ TomTom Traffic Flow API — 255 điểm đo trên các thành phố lớn. "
            "Nguồn: TomTom API (chạy mỗi giờ qua GitHub Actions → Google Drive → dag_sync_gdrive). "
            "Columns: station_name, timestamp_utc, current_speed, free_flow_speed, "
            "current_travel_time, free_flow_travel_time, confidence, latitude, longitude. "
            "raw_payload: JSON gốc từ TomTom API (ZSTD compressed). "
            "D-AQI-02 (Phase 6): Nguồn traffic chính cho correlation analysis."
        ),
    },
    {
        "fqn": f"{PREFIX}.raw_tomtom_traffic_hourly",
        "description": (
            "TomTom traffic data đã được tính congestion_ratio theo giờ. "
            "Nguồn: script calculate_traffic_patterns.py (chạy sau dag_sync_gdrive). "
            "congestion_ratio = 1 - (current_speed / free_flow_speed), clamp [0, 1]. "
            "ReplacingMergeTree(updated_at) — dedup theo (station_name, hour_utc). "
            "Đây là nguồn chính cho stg_tomtom__flow → int_traffic__pattern_enrichment → dm_aqi_weather_traffic_unified."
        ),
    },
    {
        "fqn": f"{PREFIX}.ingestion_control",
        "description": (
            "Bảng kiểm soát và giám sát ingestion — một bản ghi cho mỗi data source. "
            "Được dag_ingest_hourly cập nhật sau mỗi lần ingest. "
            "Columns: source, last_run, last_success, records_ingested, lag_seconds, error_message, updated_at. "
            "ReplacingMergeTree(updated_at) — luôn giữ bản ghi mới nhất của mỗi source. "
            "Monitored bởi Grafana (freshness dashboard) và Prometheus alerting."
        ),
    },

    # ── Reference/Seed tables ── (không có trong dbt, được load thủ công hoặc từ seed)
    {
        "fqn": f"{PREFIX}.unified_stations_metadata",
        "description": (
            "Bảng metadata trạm quan trắc hợp nhất — 255 điểm đo. "
            "Nguồn: AQI.in station discovery + TomTom Search API geocoding. "
            "Columns: station_name, latitude, longitude, province (Int32 province code), source. "
            "Được load thủ công khi thêm station mới. "
            "Là nguồn cho stg_core__stations view trong dbt."
        ),
    },
    {
        "fqn": f"{PREFIX}.vn_station_coordinates",
        "description": (
            "Tọa độ GPS và phân loại địa điểm cho 255 trạm quan trắc Việt Nam. "
            "location_type: urban/suburban/rural/industrial — phân loại dùng cho traffic-pollution analysis. "
            "Columns: station_name, latitude, longitude, province, location_type. "
            "Được dùng trong fct_traffic_pollution_correlation_daily để nhóm theo location_type."
        ),
    },
    {
        "fqn": f"{PREFIX}.vn_traffic_profile",
        "description": (
            "Hồ sơ giao thông theo giờ cho từng loại địa điểm Việt Nam. "
            "weight: trọng số phân phối traffic trong 24 giờ/ngày. "
            "Columns: location_type, hour_of_day, weight, day_type (weekday/weekend). "
            "Được dùng để ước tính traffic khi không có dữ liệu TomTom thực."
        ),
    },
    {
        "fqn": f"{PREFIX}.population_density_2026",
        "description": (
            "Dân số và mật độ dân số theo tỉnh Việt Nam năm 2026. "
            "Nguồn: Tổng cục Thống kê Việt Nam (GSO 2026). "
            "Columns: province_name, area_km2, avg_population_thousands, density_per_km2. "
            "Được dùng để tính Population Exposure Score trong dm_aqi_weather_traffic_unified. "
            "Là nguồn cho stg_core__population view trong dbt."
        ),
    },
    {
        "fqn": f"{PREFIX}.pollutants",
        "description": (
            "Bảng tham chiếu thông số ô nhiễm — US EPA AQI breakpoints. "
            "Nguồn: dbt seed file seeds/pollutants.csv (được load vào ClickHouse khi deploy). "
            "Columns: pollutant_key, display_name, unit, epa_aqi_bp_lo, epa_aqi_bp_hi, "
            "conc_bp_lo, conc_bp_hi, health_effects. "
            "Là nguồn cho stg_core__pollutants view trong dbt. "
            "LƯU Ý: bảng này được tạo từ dbt seed nhưng không bị dbt pipeline OM track vì là seed, không phải model."
        ),
    },
    {
        "fqn": f"{PREFIX}.seed_dim_time",
        "description": (
            "Bảng thời gian pre-generated — phạm vi giờ từ 2020 đến 2030. "
            "Nguồn: dbt seed file (được load một lần khi setup). "
            "Columns: datetime_hour, date, day_of_week, hour_of_day, is_weekend, "
            "month, year, vietnam_tz_offset, vietnam_timezone_hour. "
            "Dùng để join time dimension trong analytical queries. "
            "LƯU Ý: là dbt seed — không được track bởi dbt pipeline OM theo mặc định."
        ),
    },
    {
        "fqn": f"{PREFIX}.source_calibration",
        "description": (
            "Hệ số hiệu chỉnh (calibration factors) cho từng data source và pollutant parameter. "
            "Columns: source, parameter, calibration_factor, calibration_method, valid_from, valid_to. "
            "calibration_factor được nhân với giá trị đo để chuẩn hóa giữa các nguồn. "
            "Được dbt models dùng để áp dụng calibration trong quá trình staging."
        ),
    },
    {
        "fqn": f"{PREFIX}.aqicn_station_lat_long",
        "description": (
            "Metadata trạm AQICN — 105 trạm quan trắc Việt Nam. "
            "D-AQI-02 (Phase 6): AQICN không còn là nguồn data chính. "
            "Bảng này được giữ lại làm tham chiếu lịch sử và backup. "
            "Columns: station_id, station_name, latitude, longitude, city, province, "
            "sensor_quality_tier, is_active. "
            "Không còn được sử dụng trong các dbt models hiện tại."
        ),
    },
]


# ─── Lineage: pipeline → raw tables (dbt chỉ track dbt→dbt, không track này) ──

PIPELINE_LINEAGE = [
    {
        "pipeline_fqn": "Vietnam Air Quality Airflow.dag_ingest_hourly",
        # Tất cả raw_* và ingestion_control được tạo bởi pipeline này
        "table_patterns": ["raw_aqiin_measurements", "raw_openweather_measurements",
                           "raw_openweather_meteorology", "ingestion_control"],
    },
    {
        "pipeline_fqn": "Vietnam Air Quality Airflow.dag_transform",
        # dbt pipeline OM tự track dbt→dbt lineage, nhưng dag_transform là Airflow trigger
        # Ta đánh dấu dag_transform → các bảng output cuối (dm_*, fct_*)
        # để người dùng biết DAG nào tạo ra data
        "table_patterns": ["raw_tomtom_traffic", "raw_tomtom_traffic_hourly"],
    },
]


# ─── Main ──────────────────────────────────────────────────────────────────────

def curate_non_dbt_tables() -> None:
    """Apply descriptions cho 14 non-dbt tables."""
    print(f"Curating {len(NON_DBT_CURATION)} non-dbt tables...")
    ok = 0
    skip = 0
    for table in NON_DBT_CURATION:
        name = table["fqn"].split(".")[-1]
        desc = table.get("description", "")
        if not desc:
            continue
        success = _patch_table(table["fqn"], "/description", desc)
        if success:
            print(f"  ✅ {name}")
            ok += 1
        else:
            print(f"  ⚠  {name} — table không tồn tại trong OM (chưa được index)")
            skip += 1
    print(f"\n  → {ok} updated | {skip} skipped (not yet indexed)\n")


def apply_pipeline_lineage() -> None:
    """
    Tạo lineage edges: Airflow pipeline → raw tables.
    dbt pipeline OM tự xử lý dbt→dbt lineage — ta chỉ bổ sung phần còn thiếu:
    dag_ingest_hourly → raw_* tables (không có trong dbt manifest).
    """
    print("Applying pipeline → raw table lineage...")
    token = get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Lấy tất cả raw tables đang có trong OM
    r = requests.get(
        f"{OM_URL}/v1/tables?databaseSchema={PREFIX.replace(' ', '%20')}&limit=500",
        headers={"Authorization": f"Bearer {token}"},
        timeout=15,
    )
    om_tables = {}
    if r.ok:
        for t in r.json().get("data", []):
            om_tables[t["name"]] = t["fullyQualifiedName"]

    edges_created = 0
    for mapping in PIPELINE_LINEAGE:
        p_id = get_entity_id("pipelines", mapping["pipeline_fqn"])
        if not p_id:
            print(f"  ⚠ Pipeline not in OM: {mapping['pipeline_fqn']}")
            continue

        for table_name in mapping["table_patterns"]:
            if table_name not in om_tables:
                print(f"  ⚠ Table not indexed yet: {table_name}")
                continue
            t_id = get_entity_id("tables", om_tables[table_name])
            if not t_id:
                continue
            resp = requests.put(
                f"{OM_URL}/v1/lineage",
                headers=headers,
                json={"edge": {
                    "fromEntity": {"id": p_id, "type": "pipeline"},
                    "toEntity": {"id": t_id, "type": "table"},
                }},
                timeout=15,
            )
            if resp.ok:
                pipeline_short = mapping["pipeline_fqn"].split(".")[-1]
                print(f"  ✅ {pipeline_short} → {table_name}")
                edges_created += 1

    print(f"\n  → {edges_created} lineage edges created\n")


if __name__ == "__main__":
    curate_non_dbt_tables()
    apply_pipeline_lineage()
    print("✅ Curation complete.")
    print()
    print("NOTE: dbt models (stg_*, int_*, dm_*, fct_*) được dbt pipeline OM xử lý tự động")
    print("  → Trigger dbt pipeline trong OM UI để sync descriptions từ dbt YAML.")
