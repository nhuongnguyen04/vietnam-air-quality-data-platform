#!/usr/bin/env python3
"""
Vietnam Air Quality — Superset Dashboard Setup Script

Tạo datasets, charts, và dashboards qua Superset REST API.
Chạy sau khi Superset khởi động và mart tables đã được populate.

Usage:
    python setup_dashboards.py [--dry-run] [--skip-existing]

Dependencies:
    requests (có sẵn trong Superset container)
    Chứng thực: ADMIN_USER + ADMIN_PASSWORD từ env

Author: Air Quality Data Platform
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("setup_dashboards")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class SupersetConfig:
    """Cấu hình kết nối Superset REST API."""

    base_url: str = os.environ.get("SUPERSET_BASE_URL", "http://localhost:8088")
    username: str = os.environ.get("SUPERSET_ADMIN_USER", "admin")
    password: str = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")
    db_host: str = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
    db_port: str = os.environ.get("CLICKHOUSE_PORT", "8123")
    db_user: str = os.environ.get("CLICKHOUSE_USER", "admin")
    db_password: str = os.environ.get("CLICKHOUSE_PASSWORD", "admin123456")
    db_name: str = os.environ.get("CLICKHOUSE_DB", "air_quality")
    verify_ssl: bool = os.environ.get("SUPERSET_SKIP_SSL_VERIFY", "false").lower() == "true"

    def clickhouse_uri(self) -> str:
        return (
            f"clickhouse+connect://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


# ---------------------------------------------------------------------------
# Superset API Client
# ---------------------------------------------------------------------------


class SupersetAPIClient:
    """
    Superset REST API client với CSRF + session cookie authentication.

    Workflow:
    1. POST /api/v1/security/login  → nhận access token
    2. Dùng Bearer token cho tất cả request tiếp theo
    """

    def __init__(self, config: SupersetConfig) -> None:
        self.config = config
        self.session = requests.Session()
        self.session.verify = not config.verify_ssl
        self.base = config.base_url
        self._access_token: str | None = None
        self._csrf_token: str | None = None

    # ── Auth ───────────────────────────────────────────────────────────────

    def login(self) -> None:
        """Xác thực và lưu access token."""
        logger.info("Authenticating to Superset API as %s...", self.config.username)
        resp = self.session.post(
            f"{self.base}/api/v1/security/login",
            json={
                "username": self.config.username,
                "password": self.config.password,
                "provider": "db",
                "refresh": True,
            },
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        logger.info("Authenticated successfully.")

    def _headers(self) -> dict[str, str]:
        """Headers cho tất cả authenticated requests."""
        headers: dict[str, str] = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }
        if self._csrf_token:
            headers["X-CSRF-Token"] = self._csrf_token
        return headers

    def _get(self, path: str, **kwargs: Any) -> requests.Response:
        resp = self.session.get(
            f"{self.base}{path}",
            headers=self._headers(),
            timeout=kwargs.pop("timeout", 30),
            **kwargs,
        )
        resp.raise_for_status()
        return resp

    def _post(self, path: str, **kwargs: Any) -> requests.Response:
        resp = self.session.post(
            f"{self.base}{path}",
            headers=self._headers(),
            timeout=kwargs.pop("timeout", 30),
            **kwargs,
        )
        resp.raise_for_status()
        return resp

    def _put(self, path: str, **kwargs: Any) -> requests.Response:
        resp = self.session.put(
            f"{self.base}{path}",
            headers=self._headers(),
            timeout=kwargs.pop("timeout", 30),
            **kwargs,
        )
        resp.raise_for_status()
        return resp

    def _delete(self, path: str, **kwargs: Any) -> requests.Response:
        resp = self.session.delete(
            f"{self.base}{path}",
            headers=self._headers(),
            timeout=kwargs.pop("timeout", 30),
            **kwargs,
        )
        return resp

    # ── Utility ────────────────────────────────────────────────────────────

    def wait_for_healthy(self, retries: int = 20, wait_seconds: int = 15) -> bool:
        """Chờ Superset healthy trước khi thực hiện API calls."""
        for attempt in range(1, retries + 1):
            try:
                resp = self.session.get(
                    f"{self.base}/api/v1/status", timeout=10
                )
                if resp.status_code == 200:
                    logger.info("Superset API is healthy.")
                    return True
            except requests.RequestException:
                pass
            logger.info("Waiting for Superset... (%d/%d)", attempt, retries)
            time.sleep(wait_seconds)
        logger.error("Superset did not become healthy in time.")
        return False

    # ── Databases ───────────────────────────────────────────────────────────

    def get_database_by_name(self, name: str) -> dict[str, Any] | None:
        """Tìm database đã tồn tại theo tên."""
        try:
            resp = self._get("/api/v1/database/", params={"filters": [{"col": "database_name", "opr": "eq", "value": name}]})
            result = resp.json().get("result", [])
            return result[0] if result else None
        except requests.RequestException:
            return None

    def create_database(self, name: str, sqlalchemy_uri: str) -> int:
        """Tạo mới database connection. Trả về database id."""
        existing = self.get_database_by_name(name)
        if existing:
            logger.info("Database '%s' already exists (id=%d). Skipping.", name, existing["id"])
            return existing["id"]

        payload = {
            "database_name": name,
            "sqlalchemy_uri": sqlalchemy_uri,
            "configuration_method": "sqlalchemy_uri",
            "extra": json.dumps({
                "metadata_params": {},
                "engine_params": {
                    "connect_args": {"connect_timeout": 30}
                },
                "metadata_cache_timeout": {},
            }),
            "impersonate_user": False,
        }
        resp = self._post("/api/v1/database/", json=payload)
        db_id = resp.json()["id"]
        logger.info("Created database '%s' (id=%d).", name, db_id)
        return db_id

    # ── Datasets ───────────────────────────────────────────────────────────

    def get_dataset_by_name(self, table_name: str) -> dict[str, Any] | None:
        """Tìm dataset đã tồn tại theo table name."""
        try:
            resp = self._get(
                "/api/v1/dataset/",
                params={"filters": [{"col": "table_name", "opr": "eq", "value": table_name}]},
            )
            result = resp.json().get("result", [])
            return result[0] if result else None
        except requests.RequestException:
            return None

    def create_dataset(
        self,
        table_name: str,
        schema_name: str,
        database_id: int,
        sql: str | None = None,
    ) -> int:
        """
        Tạo dataset (virtual dataset = saved SQL query).

        Nếu sql=None → dataset trỏ trực tiếp vào table (schema.table_name).
        Nếu sql được cung cấp → virtual dataset với custom SQL.
        Superset 4.x: virtual dataset có dataset_type='virtual'.
        """
        existing = self.get_dataset_by_name(table_name)
        if existing:
            logger.info("Dataset '%s' already exists (id=%d). Skipping.", table_name, existing["id"])
            return existing["id"]

        if sql:
            # Virtual dataset (custom SQL query)
            payload: dict[str, Any] = {
                "database_id": database_id,
                "datasource_name": table_name,
                "description": "",
                "sql": sql,
                "dataset_type": "virtual",
            }
        else:
            # Physical table dataset
            payload = {
                "database_id": database_id,
                "schema": schema_name or "default",
                "table_name": table_name,
                "dataset_type": "table",
            }

        resp = self._post("/api/v1/dataset/", json=payload)
        ds_id = resp.json()["id"]
        logger.info("Created dataset '%s' (id=%d, type=%s).", table_name, ds_id, payload.get("dataset_type", "table"))
        return ds_id

    # ── Charts / Slices ─────────────────────────────────────────────────────

    def create_chart(
        self,
        slice_name: str,
        dataset_id: int,
        viz_type: str,
        sql: str,
        datasource_id: int,
        datasource_type: str = "table",
        config: dict[str, Any] | None = None,
    ) -> int:
        """
        Tạo chart slice. Trả về slice id.

        SQL query được truyền qua `params` JSON — Superset sẽ dùng nó
        khi user mở chart trong SQL Lab hoặc dashboard.
        """
        # Build params including SQL for virtual datasets
        params = dict(config or {})
        if sql:
            params["sql"] = sql
            params["is_virtual"] = True

        payload: dict[str, Any] = {
            "datasource_id": datasource_id,
            "datasource_type": datasource_type,
            "viz_type": viz_type,
            "slice_name": slice_name,
            "params": json.dumps(params),
        }
        resp = self._post("/api/v1/chart/", json=payload)
        slice_id = resp.json()["id"]
        logger.info("Created chart '%s' (id=%d, viz_type=%s).", slice_name, slice_id, viz_type)
        return slice_id

    # ── Dashboards ─────────────────────────────────────────────────────────

    def get_dashboard_by_slug(self, slug: str) -> dict[str, Any] | None:
        """Tìm dashboard đã tồn tại theo slug."""
        try:
            resp = self._get("/api/v1/dashboard/", params={"filters": [{"col": "slug", "opr": "eq", "value": slug}]})
            result = resp.json().get("result", [])
            return result[0] if result else None
        except requests.RequestException:
            return None

    def create_dashboard(self, title: str, slug: str, description: str = "") -> int:
        """Tạo dashboard. Trả về dashboard id."""
        existing = self.get_dashboard_by_slug(slug)
        if existing:
            logger.info("Dashboard '%s' already exists (id=%d). Skipping.", title, existing["id"])
            return existing["id"]

        resp = self._post(
            "/api/v1/dashboard/",
            json={
                "dashboard_title": title,
                "slug": slug,
                "description": description,
                "published": True,
                "css": "",
                "json_metadata": json.dumps({"default_filters": "{}"}),
            },
        )
        dash_id = resp.json()["id"]
        logger.info("Created dashboard '%s' (id=%d).", title, dash_id)
        return dash_id

    def set_dashboard_position(
        self,
        dashboard_id: int,
        positions: list[dict[str, Any]],
    ) -> None:
        """Cập nhật vị trí các chart trên dashboard."""
        resp = self._get(f"/api/v1/dashboard/{dashboard_id}")
        current = resp.json()
        metadata = json.loads(current.get("json_metadata", "{}"))
        metadata["positions"] = positions
        self._put(
            f"/api/v1/dashboard/{dashboard_id}",
            json={"json_metadata": json.dumps(metadata)},
        )
        logger.info("Updated dashboard %d positions (%d charts).", dashboard_id, len(positions))


# ---------------------------------------------------------------------------
# Dataset Definitions
# ---------------------------------------------------------------------------

VIETNAM_FILTER = "WHERE latitude BETWEEN 8 AND 24 AND longitude BETWEEN 102 AND 110"
VIETNAM_GEO_FILTER = (
    "latitude BETWEEN 8 AND 24 AND longitude BETWEEN 102 AND 110"
)


def get_dataset_definitions() -> list[dict[str, Any]]:
    """Định nghĩa tất cả datasets cần thiết cho dashboards."""
    return [
        {
            "name": "fct_hourly_aqi",
            "table_name": "fct_hourly_aqi",
            "schema": "air_quality",
            "sql": None,  # Direct table
        },
        {
            "name": "dim_locations",
            "table_name": "dim_locations",
            "schema": "air_quality",
            "sql": None,
        },
        {
            "name": "fct_daily_aqi_summary",
            "table_name": "fct_daily_aqi_summary",
            "schema": "air_quality",
            "sql": None,
        },
        {
            "name": "mart_air_quality__dashboard",
            "table_name": "mart_air_quality__dashboard",
            "schema": "air_quality",
            "sql": None,
        },
        {
            "name": "raw_aqicn_forecast",
            "table_name": "raw_aqicn_forecast",
            "schema": "air_quality",
            "sql": None,
        },
        # ── Virtual datasets (custom SQL with joins) ──────────────────────
        {
            "name": "ds_aqi_by_city",
            "table_name": "ds_aqi_by_city",
            "schema": "",
            "sql": f"""
SELECT
    l.city,
    f.station_name,
    f.normalized_aqi,
    f.dominant_pollutant,
    f.timestamp_utc
FROM fct_hourly_aqi f
JOIN dim_locations l ON f.station_id = l.station_id
WHERE f.timestamp_utc >= NOW() - INTERVAL 1 HOUR
  AND l.{VIETNAM_GEO_FILTER}
ORDER BY f.normalized_aqi DESC
""".strip(),
        },
        {
            "name": "ds_aqi_24h_trend",
            "table_name": "ds_aqi_24h_trend",
            "schema": "",
            "sql": f"""
SELECT
    f.datetime_hour,
    f.normalized_aqi,
    l.city
FROM fct_hourly_aqi f
JOIN dim_locations l ON f.station_id = l.station_id
WHERE f.datetime_hour >= NOW() - INTERVAL 24 HOUR
  AND l.{VIETNAM_GEO_FILTER}
ORDER BY f.datetime_hour
""".strip(),
        },
        {
            "name": "ds_pm25_trend",
            "table_name": "ds_pm25_trend",
            "schema": "",
            "sql": f"""
SELECT
    f.datetime_hour,
    f.avg_value AS pm25_value,
    l.station_name
FROM fct_hourly_aqi f
JOIN dim_locations l ON f.station_id = l.station_id
WHERE f.datetime_hour >= NOW() - INTERVAL 24 HOUR
  AND f.pollutant = 'pm25'
  AND l.{VIETNAM_GEO_FILTER}
ORDER BY f.datetime_hour
""".strip(),
        },
        {
            "name": "ds_pollutant_levels_by_city",
            "table_name": "ds_pollutant_levels_by_city",
            "schema": "",
            "sql": f"""
SELECT
    l.city,
    f.pollutant,
    avg(f.avg_value) AS avg_level
FROM fct_hourly_aqi f
JOIN dim_locations l ON f.station_id = l.station_id
WHERE f.timestamp_utc >= NOW() - INTERVAL 24 HOUR
  AND l.{VIETNAM_GEO_FILTER}
GROUP BY l.city, f.pollutant
ORDER BY l.city
""".strip(),
        },
        {
            "name": "ds_source_comparison",
            "table_name": "ds_source_comparison",
            "schema": "",
            "sql": f"""
SELECT
    l.city,
    f.normalized_aqi,
    multiIf(
        l.station_id LIKE 'AQICN%', 'AQICN',
        l.station_id LIKE 'OPENWEATHER%', 'OpenWeather',
        l.station_id LIKE 'SENSORSCM%', 'Sensors.Community',
        'Other'
    ) AS source_name
FROM fct_hourly_aqi f
JOIN dim_locations l ON f.station_id = l.station_id
WHERE f.timestamp_utc >= NOW() - INTERVAL 24 HOUR
  AND l.{VIETNAM_GEO_FILTER}
GROUP BY l.city, source_name
HAVING source_name != 'Other'
ORDER BY l.city
""".strip(),
        },
        {
            "name": "ds_forecast_vs_actual",
            "table_name": "ds_forecast_vs_actual",
            "schema": "",
            "sql": f"""
SELECT
    f.forecast_time,
    f.aqi AS forecast_aqi,
    a.normalized_aqi AS actual_aqi,
    f.station_name,
    l.city
FROM raw_aqicn_forecast f
LEFT JOIN dim_locations l ON f.station_name = l.station_name
LEFT JOIN fct_hourly_aqi a
      ON l.station_id = a.station_id
      AND toStartOfHour(f.forecast_time) = a.datetime_hour
WHERE f.forecast_time >= NOW() - INTERVAL 24 HOUR
ORDER BY f.forecast_time
""".strip(),
        },
        {
            "name": "ds_stations_map",
            "table_name": "ds_stations_map",
            "schema": "",
            "sql": f"""
SELECT
    l.station_name,
    l.latitude,
    l.longitude,
    l.city,
    l.source,
    l.is_active,
    f.normalized_aqi,
    f.timestamp_utc
FROM dim_locations l
LEFT JOIN fct_hourly_aqi f ON l.station_id = f.station_id
  AND f.timestamp_utc >= NOW() - INTERVAL 1 HOUR
WHERE l.{VIETNAM_GEO_FILTER}
ORDER BY l.city, l.station_name
""".strip(),
        },
        {
            "name": "ds_dominant_pollutant",
            "table_name": "ds_dominant_pollutant",
            "schema": "",
            "sql": f"""
SELECT
    dominant_pollutant,
    count(*) AS frequency
FROM fct_hourly_aqi
WHERE timestamp_utc >= NOW() - INTERVAL 24 HOUR
GROUP BY dominant_pollutant
ORDER BY frequency DESC
""".strip(),
        },
    ]


# ---------------------------------------------------------------------------
# EPA Color Scale
# ---------------------------------------------------------------------------

EPA_COLOR_SCHEME = [
    {"min": 0, "max": 50, "color": "#00E400", "label": "Good"},
    {"min": 51, "max": 100, "color": "#FFFF00", "label": "Moderate"},
    {"min": 101, "max": 150, "color": "#FF7E00", "label": "Unhealthy for Sensitive"},
    {"min": 151, "max": 200, "color": "#FF0000", "label": "Unhealthy"},
    {"min": 201, "max": 300, "color": "#8F3F97", "label": "Very Unhealthy"},
    {"min": 301, "max": 500, "color": "#7E0023", "label": "Hazardous"},
]


def epa_color_params() -> dict[str, Any]:
    """Trả về Superset chart params cho EPA color scale trên AQI."""
    return {
        "color_scheme": "superset_sequential_AQI",
        "linear_color_scheme": "blue_white_yellow",
        "country_field": "city",
        "color_endpoint": "value",
        "d3_format": ".1f",
        "format_bytes": False,
        "color_bounds": [0, 50, 100, 150, 200, 300, 500],
        "tick_spacing": 1,
        "show_bubbles": True,
        "chart_type": "country",
    }


# ---------------------------------------------------------------------------
# Chart Definitions per Dashboard
# ---------------------------------------------------------------------------


@dataclass
class ChartDef:
    """Định nghĩa một chart slice."""

    name: str
    viz_type: str
    sql: str
    params: dict[str, Any] = field(default_factory=dict)
    dataset_id: int = 0  # filled by setup


DASHBOARD_CHARTS: dict[str, list[ChartDef]] = {
    # ── Dashboard 1: AQI Overview ────────────────────────────────────────
    "01_aqi_overview": [
        ChartDef(
            name="AQI by City (Table)",
            viz_type="table",
            sql="""
SELECT
    city,
    station_name,
    normalized_aqi,
    dominant_pollutant,
    timestamp_utc
FROM fct_hourly_aqi f
JOIN dim_locations l ON f.station_id = l.station_id
WHERE f.timestamp_utc >= NOW() - INTERVAL 1 HOUR
  AND l.latitude BETWEEN 8 AND 24
  AND l.longitude BETWEEN 102 AND 110
ORDER BY normalized_aqi DESC
LIMIT 100
""".strip(),
            params={
                "table_filter": False,
                "row_limit": 100,
                "color_scheme": "superset_sequential_AQI",
                "conditional_formatting": [
                    {
                        "operator": "between",
                        "target": {"column": "normalized_aqi"},
                        "min_val": 0,
                        "max_val": 50,
                        "color": "#00E400",
                    },
                    {
                        "operator": "between",
                        "target": {"column": "normalized_aqi"},
                        "min_val": 51,
                        "max_val": 100,
                        "color": "#FFFF00",
                    },
                    {
                        "operator": "between",
                        "target": {"column": "normalized_aqi"},
                        "min_val": 101,
                        "max_val": 150,
                        "color": "#FF7E00",
                    },
                    {
                        "operator": "between",
                        "target": {"column": "normalized_aqi"},
                        "min_val": 151,
                        "max_val": 200,
                        "color": "#FF0000",
                    },
                    {
                        "operator": "between",
                        "target": {"column": "normalized_aqi"},
                        "min_val": 201,
                        "max_val": 500,
                        "color": "#8F3F97",
                    },
                ],
            },
        ),
        ChartDef(
            name="Current AQI — Max (Big Number)",
            viz_type="big_number",
            sql="""
SELECT max(normalized_aqi) AS max_aqi
FROM fct_hourly_aqi
WHERE timestamp_utc >= NOW() - INTERVAL 1 HOUR
""".strip(),
            params={
                "compare_lag": "10",
                "compare_suffix": "vs 1h ago",
                "d3_format": ".0f",
                "color_scheme": "superset_sequential_AQI",
            },
        ),
        ChartDef(
            name="Vietnam — AQI Stations Map",
            viz_type="country_map",
            sql="""
SELECT
    l.station_name AS "Station",
    l.latitude AS "Latitude",
    l.longitude AS "Longitude",
    l.city AS "City",
    f.normalized_aqi AS "AQI",
    l.source AS "Source"
FROM dim_locations l
LEFT JOIN fct_hourly_aqi f
      ON l.station_id = f.station_id
      AND f.timestamp_utc >= NOW() - INTERVAL 1 HOUR
WHERE l.latitude BETWEEN 8 AND 24
  AND l.longitude BETWEEN 102 AND 110
ORDER BY f.normalized_aqi DESC NULLS LAST
""".strip(),
            params={
                "entity": "Country Map",
                "country_field": "City",
                "color_scheme": "superset_sequential_AQI",
                "show_bubbles": True,
                "color_bounds": [0, 50, 100, 150, 200, 300, 500],
                "d3_format": ".0f",
            },
        ),
        ChartDef(
            name="Stations — Active Count",
            viz_type="big_number_total",
            sql="""
SELECT count(DISTINCT station_id) AS active_stations
FROM dim_locations
WHERE is_active = 1
  AND latitude BETWEEN 8 AND 24
  AND longitude BETWEEN 102 AND 110
""".strip(),
            params={
                "d3_format": ".0f",
                "color_scheme": "superset_colors",
            },
        ),
        ChartDef(
            name="Top 10 Worst AQI Stations",
            viz_type="bar",
            sql="""
SELECT
    f.station_name,
    f.normalized_aqi,
    l.city
FROM fct_hourly_aqi f
JOIN dim_locations l ON f.station_id = l.station_id
WHERE f.timestamp_utc >= NOW() - INTERVAL 1 HOUR
  AND l.latitude BETWEEN 8 AND 24
  AND l.longitude BETWEEN 102 AND 110
ORDER BY f.normalized_aqi DESC
LIMIT 10
""".strip(),
            params={
                "viz_type": "bar",
                "metrics": [{"expressionType": "SQL", "column": {"column_name": "normalized_aqi"}, "sqlExpression": "normalized_aqi", "label": "AQI"}],
                "groupby": ["station_name"],
                "row_limit": 10,
                "color_scheme": "superset_sequential_AQI",
                "bar_and_line_opacity": 0.8,
            },
        ),
    ],
    # ── Dashboard 2: Trends ───────────────────────────────────────────────
    "02_trends": [
        ChartDef(
            name="AQI — 24h Trend",
            viz_type="line",
            sql="""
SELECT
    toStartOfHour(datetime_hour) AS hour_bucket,
    avg(normalized_aqi) AS avg_aqi,
    l.city
FROM fct_hourly_aqi f
JOIN dim_locations l ON f.station_id = l.station_id
WHERE f.datetime_hour >= NOW() - INTERVAL 24 HOUR
  AND l.latitude BETWEEN 8 AND 24
  AND l.longitude BETWEEN 102 AND 110
GROUP BY hour_bucket, l.city
ORDER BY hour_bucket
""".strip(),
            params={
                "viz_type": "line",
                "metrics": [{"expressionType": "SQL", "sqlExpression": "avg_aqi", "label": "Avg AQI"}],
                "groupby": ["city"],
                "row_limit": 5000,
                "color_scheme": "superset_sequential_AQI",
                "line_interpolation": "spline",
                "show_legend": True,
                "x_axis_format": "%H:%M",
            },
        ),
        ChartDef(
            name="AQI — 7-Day Daily Trend",
            viz_type="line",
            sql="""
SELECT
    date,
    avg_aqi,
    max_aqi,
    city
FROM fct_daily_aqi_summary
WHERE date >= today() - INTERVAL 7 DAY
  AND city IS NOT NULL
ORDER BY date
""".strip(),
            params={
                "viz_type": "line",
                "metrics": [
                    {"expressionType": "SQL", "sqlExpression": "avg_aqi", "label": "Avg Daily AQI"},
                    {"expressionType": "SQL", "sqlExpression": "max_aqi", "label": "Max Daily AQI"},
                ],
                "groupby": ["city"],
                "row_limit": 5000,
                "color_scheme": "superset_sequential_AQI",
                "line_interpolation": "spline",
            },
        ),
        ChartDef(
            name="PM2.5 — 24h Trend",
            viz_type="area",
            sql="""
SELECT
    datetime_hour,
    avg_value AS pm25_value,
    station_name
FROM fct_hourly_aqi
WHERE datetime_hour >= NOW() - INTERVAL 24 HOUR
  AND pollutant = 'pm25'
ORDER BY datetime_hour
""".strip(),
            params={
                "viz_type": "area",
                "metrics": [{"expressionType": "SQL", "sqlExpression": "avg(pm25_value)", "label": "PM2.5"}],
                "groupby": ["station_name"],
                "row_limit": 5000,
                "color_scheme": "superset_colors",
                "line_interpolation": "spline",
            },
        ),
        ChartDef(
            name="All Pollutants — 24h Comparison",
            viz_type="line",
            sql="""
SELECT
    datetime_hour,
    pollutant,
    avg(avg_value) AS avg_value
FROM fct_hourly_aqi
WHERE datetime_hour >= NOW() - INTERVAL 24 HOUR
  AND pollutant IN ('pm25', 'pm10', 'o3', 'no2', 'so2', 'co')
GROUP BY datetime_hour, pollutant
ORDER BY datetime_hour
""".strip(),
            params={
                "viz_type": "line",
                "metrics": [{"expressionType": "SQL", "sqlExpression": "avg(avg_value)", "label": "Avg Value"}],
                "groupby": ["pollutant"],
                "row_limit": 5000,
                "color_scheme": "superset_colors",
                "line_interpolation": "spline",
            },
        ),
        ChartDef(
            name="PM2.5 Distribution (Histogram)",
            viz_type="histogram",
            sql="""
SELECT avg_value
FROM fct_hourly_aqi
WHERE pollutant = 'pm25'
  AND timestamp_utc >= NOW() - INTERVAL 24 HOUR
  AND avg_value IS NOT NULL
""".strip(),
            params={
                "viz_type": "histogram",
                "row_limit": 1000,
                "color_scheme": "superset_sequential_AQI",
            },
        ),
        ChartDef(
            name="Top 10 Highest PM2.5 Stations",
            viz_type="bar",
            sql="""
SELECT
    f.station_name,
    avg(f.avg_value) AS avg_pm25,
    l.city
FROM fct_hourly_aqi f
JOIN dim_locations l ON f.station_id = l.station_id
WHERE f.datetime_hour >= NOW() - INTERVAL 24 HOUR
  AND f.pollutant = 'pm25'
  AND l.latitude BETWEEN 8 AND 24
  AND l.longitude BETWEEN 102 AND 110
GROUP BY f.station_name, l.city
ORDER BY avg_pm25 DESC
LIMIT 10
""".strip(),
            params={
                "viz_type": "bar",
                "metrics": [{"expressionType": "SQL", "sqlExpression": "avg(avg_pm25)", "label": "Avg PM2.5"}],
                "groupby": ["station_name"],
                "row_limit": 10,
                "color_scheme": "superset_sequential_AQI",
                "bar_and_line_opacity": 0.8,
            },
        ),
    ],
    # ── Dashboard 3: Pollutant Analysis ─────────────────────────────────
    "03_pollutant_analysis": [
        ChartDef(
            name="Dominant Pollutant Frequency",
            viz_type="pie",
            sql="""
SELECT
    dominant_pollutant,
    count(*) AS frequency
FROM fct_hourly_aqi
WHERE timestamp_utc >= NOW() - INTERVAL 24 HOUR
GROUP BY dominant_pollutant
ORDER BY frequency DESC
""".strip(),
            params={
                "viz_type": "pie",
                "metric": {"expressionType": "SQL", "sqlExpression": "count(*)", "label": "Count"},
                "groupby": ["dominant_pollutant"],
                "row_limit": 10,
                "color_scheme": "superset_colors",
                "show_legend": True,
                "donut": True,
            },
        ),
        ChartDef(
            name="Average Pollutant Levels by City",
            viz_type="bar",
            sql="""
SELECT
    l.city,
    f.pollutant,
    avg(f.avg_value) AS avg_level
FROM fct_hourly_aqi f
JOIN dim_locations l ON f.station_id = l.station_id
WHERE f.timestamp_utc >= NOW() - INTERVAL 24 HOUR
  AND l.latitude BETWEEN 8 AND 24
  AND l.longitude BETWEEN 102 AND 110
GROUP BY l.city, f.pollutant
ORDER BY l.city
""".strip(),
            params={
                "viz_type": "bar",
                "metrics": [{"expressionType": "SQL", "sqlExpression": "avg(avg_level)", "label": "Avg Level"}],
                "groupby": ["city", "pollutant"],
                "row_limit": 500,
                "color_scheme": "superset_colors",
            },
        ),
        ChartDef(
            name="PM2.5 Distribution (Histogram)",
            viz_type="histogram",
            sql="""
SELECT avg_value
FROM fct_hourly_aqi
WHERE pollutant = 'pm25'
  AND timestamp_utc >= NOW() - INTERVAL 24 HOUR
  AND avg_value IS NOT NULL
""".strip(),
            params={
                "viz_type": "histogram",
                "row_limit": 1000,
                "color_scheme": "superset_sequential_AQI",
            },
        ),
        ChartDef(
            name="Top 10 Stations by AQI",
            viz_type="bar",
            sql="""
SELECT
    f.station_name,
    f.normalized_aqi,
    l.city
FROM fct_hourly_aqi f
JOIN dim_locations l ON f.station_id = l.station_id
WHERE f.timestamp_utc >= NOW() - INTERVAL 1 HOUR
ORDER BY f.normalized_aqi DESC
LIMIT 10
""".strip(),
            params={
                "viz_type": "bar",
                "metrics": [{"expressionType": "SQL", "sqlExpression": "normalized_aqi", "label": "AQI"}],
                "groupby": ["station_name"],
                "row_limit": 10,
                "color_scheme": "superset_sequential_AQI",
            },
        ),
        ChartDef(
            name="Sensor Quality Tier Distribution",
            viz_type="pie",
            sql="""
SELECT
    sensor_quality_tier,
    count(*) AS station_count
FROM dim_locations
WHERE latitude BETWEEN 8 AND 24
  AND longitude BETWEEN 102 AND 110
GROUP BY sensor_quality_tier
ORDER BY station_count DESC
""".strip(),
            params={
                "viz_type": "pie",
                "metric": {"expressionType": "SQL", "sqlExpression": "count(*)", "label": "Count"},
                "groupby": ["sensor_quality_tier"],
                "row_limit": 10,
                "color_scheme": "superset_colors",
                "donut": True,
            },
        ),
    ],
    # ── Dashboard 4: Source Comparison ───────────────────────────────────
    "04_source_comparison": [
        ChartDef(
            name="AQI by Source (Grouped Bar)",
            viz_type="bar",
            sql="""
SELECT
    l.city,
    multiIf(
        l.station_id LIKE 'AQICN%', 'AQICN',
        l.station_id LIKE 'OPENWEATHER%', 'OpenWeather',
        l.station_id LIKE 'SENSORSCM%', 'Sensors.Community',
        'Other'
    ) AS source_name,
    avg(f.normalized_aqi) AS avg_aqi
FROM fct_hourly_aqi f
JOIN dim_locations l ON f.station_id = l.station_id
WHERE f.timestamp_utc >= NOW() - INTERVAL 24 HOUR
  AND l.latitude BETWEEN 8 AND 24
  AND l.longitude BETWEEN 102 AND 110
GROUP BY l.city, source_name
HAVING source_name != 'Other'
ORDER BY l.city, source_name
""".strip(),
            params={
                "viz_type": "bar",
                "metrics": [{"expressionType": "SQL", "sqlExpression": "avg(avg_aqi)", "label": "Avg AQI"}],
                "groupby": ["city", "source_name"],
                "row_limit": 500,
                "color_scheme": "superset_colors",
                "bar_stacked": False,
            },
        ),
        ChartDef(
            name="Data Count by Source (7-Day)",
            viz_type="bar",
            sql="""
SELECT
    toDate(datetime_hour) AS date,
    multiIf(
        station_id LIKE 'AQICN%', 'AQICN',
        station_id LIKE 'OPENWEATHER%', 'OpenWeather',
        station_id LIKE 'SENSORSCM%', 'Sensors.Community',
        'Other'
    ) AS source_name,
    count(*) AS record_count
FROM fct_hourly_aqi
WHERE datetime_hour >= NOW() - INTERVAL 7 DAY
GROUP BY date, source_name
HAVING source_name != 'Other'
ORDER BY date
""".strip(),
            params={
                "viz_type": "bar",
                "metrics": [{"expressionType": "SQL", "sqlExpression": "count(*)", "label": "Record Count"}],
                "groupby": ["date", "source_name"],
                "row_limit": 500,
                "color_scheme": "superset_colors",
                "bar_stacked": True,
            },
        ),
        ChartDef(
            name="Source Coverage Map",
            viz_type="country_map",
            sql="""
SELECT
    l.station_name,
    l.latitude,
    l.longitude,
    l.city,
    l.source,
    l.is_active
FROM dim_locations l
WHERE l.latitude BETWEEN 8 AND 24
  AND l.longitude BETWEEN 102 AND 110
ORDER BY l.city
""".strip(),
            params={
                "viz_type": "country_map",
                "entity": "Country Map",
                "country_field": "city",
                "color_scheme": "superset_colors",
                "show_bubbles": True,
            },
        ),
        ChartDef(
            name="Source Count per City",
            viz_type="pivot_table",
            sql="""
SELECT
    l.city,
    multiIf(
        l.station_id LIKE 'AQICN%', 'AQICN',
        l.station_id LIKE 'OPENWEATHER%', 'OpenWeather',
        l.station_id LIKE 'SENSORSCM%', 'Sensors.Community',
        'Other'
    ) AS source_name,
    count(*) AS station_count
FROM dim_locations l
WHERE l.latitude BETWEEN 8 AND 24
  AND l.longitude BETWEEN 102 AND 110
  AND l.is_active = 1
GROUP BY l.city, source_name
HAVING source_name != 'Other'
ORDER BY l.city
""".strip(),
            params={
                "viz_type": "pivot_table",
                "metrics": [{"expressionType": "SQL", "sqlExpression": "count(*)", "label": "Stations"}],
                "groupby": ["city"],
                "columns": ["source_name"],
                "row_limit": 200,
            },
        ),
        ChartDef(
            name="AQI Correlation — AQICN vs OpenWeather",
            viz_type="scatter",
            sql="""
SELECT
    aqicn.city,
    aqicn.avg_aqi AS aqicn_aqi,
    ow.avg_aqi AS openweather_aqi
FROM (
    SELECT l.city, avg(f.normalized_aqi) AS avg_aqi
    FROM fct_hourly_aqi f
    JOIN dim_locations l ON f.station_id = l.station_id
    WHERE f.timestamp_utc >= NOW() - INTERVAL 24 HOUR
      AND l.station_id LIKE 'AQICN%'
      AND l.latitude BETWEEN 8 AND 24
      AND l.longitude BETWEEN 102 AND 110
    GROUP BY l.city
) aqicn
JOIN (
    SELECT l.city, avg(f.normalized_aqi) AS avg_aqi
    FROM fct_hourly_aqi f
    JOIN dim_locations l ON f.station_id = l.station_id
    WHERE f.timestamp_utc >= NOW() - INTERVAL 24 HOUR
      AND l.station_id LIKE 'OPENWEATHER%'
      AND l.latitude BETWEEN 8 AND 24
      AND l.longitude BETWEEN 102 AND 110
    GROUP BY l.city
) ow ON aqicn.city = ow.city
ORDER BY aqicn.city
""".strip(),
            params={
                "viz_type": "scatter",
                "metrics": [
                    {"expressionType": "SQL", "sqlExpression": "aqicn_aqi", "label": "AQICN AQI"},
                    {"expressionType": "SQL", "sqlExpression": "openweather_aqi", "label": "OpenWeather AQI"},
                ],
                "groupby": ["city"],
                "row_limit": 200,
                "color_scheme": "superset_sequential_AQI",
            },
        ),
    ],
    # ── Dashboard 5: Forecast vs Actual ───────────────────────────────────
    "05_forecast_vs_actual": [
        ChartDef(
            name="Forecast vs Actual AQI (Dual Line)",
            viz_type="line",
            sql="""
SELECT
    toStartOfHour(f.forecast_time) AS forecast_hour,
    avg(f.aqi) AS forecast_aqi,
    avg(a.normalized_aqi) AS actual_aqi,
    l.city
FROM raw_aqicn_forecast f
JOIN dim_locations l ON f.station_name = l.station_name
LEFT JOIN fct_hourly_aqi a
      ON l.station_id = a.station_id
      AND toStartOfHour(f.forecast_time) = a.datetime_hour
WHERE f.forecast_time >= NOW() - INTERVAL 24 HOUR
  AND l.latitude BETWEEN 8 AND 24
  AND l.longitude BETWEEN 102 AND 110
GROUP BY forecast_hour, l.city
ORDER BY forecast_hour
""".strip(),
            params={
                "viz_type": "line",
                "metrics": [
                    {"expressionType": "SQL", "sqlExpression": "avg(forecast_aqi)", "label": "Forecast AQI"},
                    {"expressionType": "SQL", "sqlExpression": "avg(actual_aqi)", "label": "Actual AQI"},
                ],
                "groupby": ["city"],
                "row_limit": 5000,
                "color_scheme": "superset_colors",
                "line_interpolation": "spline",
                "show_legend": True,
            },
        ),
        ChartDef(
            name="Forecast Accuracy by City (%)",
            viz_type="bar",
            sql="""
SELECT
    l.city,
    countIf(abs(f.aqi - a.normalized_aqi) <= 20) * 100.0 / count(*) AS accuracy_pct,
    count(*) AS sample_count
FROM raw_aqicn_forecast f
JOIN dim_locations l ON f.station_name = l.station_name
JOIN fct_hourly_aqi a
      ON l.station_id = a.station_id
      AND toStartOfHour(f.forecast_time) = a.datetime_hour
WHERE f.forecast_time >= NOW() - INTERVAL 7 DAY
  AND l.latitude BETWEEN 8 AND 24
  AND l.longitude BETWEEN 102 AND 110
GROUP BY l.city
ORDER BY accuracy_pct DESC
""".strip(),
            params={
                "viz_type": "bar",
                "metrics": [
                    {"expressionType": "SQL", "sqlExpression": "accuracy_pct", "label": "Accuracy %"},
                    {"expressionType": "SQL", "sqlExpression": "sample_count", "label": "Samples"},
                ],
                "groupby": ["city"],
                "row_limit": 100,
                "color_scheme": "superset_sequential_AQI",
            },
        ),
        ChartDef(
            name="Forecast Error Distribution",
            viz_type="histogram",
            sql="""
SELECT abs(f.aqi - a.normalized_aqi) AS abs_error
FROM raw_aqicn_forecast f
JOIN dim_locations l ON f.station_name = l.station_name
JOIN fct_hourly_aqi a
      ON l.station_id = a.station_id
      AND toStartOfHour(f.forecast_time) = a.datetime_hour
WHERE f.forecast_time >= NOW() - INTERVAL 7 DAY
  AND abs(f.aqi - a.normalized_aqi) IS NOT NULL
  AND abs(f.aqi - a.normalized_aqi) < 200
""".strip(),
            params={
                "viz_type": "histogram",
                "row_limit": 1000,
                "color_scheme": "superset_colors",
            },
        ),
        ChartDef(
            name="Forecast Coverage (Table)",
            viz_type="table",
            sql="""
SELECT
    l.city,
    min(f.forecast_time) AS earliest_forecast,
    max(f.forecast_time) AS latest_forecast,
    count(*) AS forecast_count
FROM raw_aqicn_forecast f
JOIN dim_locations l ON f.station_name = l.station_name
WHERE f.forecast_time >= NOW() - INTERVAL 24 HOUR
  AND l.latitude BETWEEN 8 AND 24
  AND l.longitude BETWEEN 102 AND 110
GROUP BY l.city
ORDER BY forecast_count DESC
""".strip(),
            params={
                "viz_type": "table",
                "row_limit": 100,
            },
        ),
        ChartDef(
            name="AQI Forecast Bias by City",
            viz_type="bar",
            sql="""
SELECT
    l.city,
    avg(f.aqi - a.normalized_aqi) AS avg_bias,
    avg(abs(f.aqi - a.normalized_aqi)) AS avg_abs_error
FROM raw_aqicn_forecast f
JOIN dim_locations l ON f.station_name = l.station_name
JOIN fct_hourly_aqi a
      ON l.station_id = a.station_id
      AND toStartOfHour(f.forecast_time) = a.datetime_hour
WHERE f.forecast_time >= NOW() - INTERVAL 7 DAY
  AND l.latitude BETWEEN 8 AND 24
  AND l.longitude BETWEEN 102 AND 110
GROUP BY l.city
ORDER BY avg_bias DESC
""".strip(),
            params={
                "viz_type": "bar",
                "metrics": [
                    {"expressionType": "SQL", "sqlExpression": "avg_bias", "label": "Avg Bias (overestimate+)"},
                    {"expressionType": "SQL", "sqlExpression": "avg_abs_error", "label": "Avg Absolute Error"},
                ],
                "groupby": ["city"],
                "row_limit": 100,
                "color_scheme": "superset_colors",
            },
        ),
    ],
}


# ---------------------------------------------------------------------------
# Dashboard Metadata
# ---------------------------------------------------------------------------

DASHBOARD_META: dict[str, dict[str, str]] = {
    "01_aqi_overview": {
        "title": "AQI Overview — Vietnam",
        "slug": "aqi-overview-vietnam",
        "description": "Current AQI by city/station, Vietnam map, EPA color coding. Near-real-time data from AQICN, OpenWeather, Sensors.Community.",
    },
    "02_trends": {
        "title": "AQI & Pollutant Trends — Vietnam",
        "slug": "trends-vietnam",
        "description": "AQI and pollutant trends over 24h and 7-day periods. PM2.5, PM10, O₃, NO₂, SO₂, CO comparison.",
    },
    "03_pollutant_analysis": {
        "title": "Pollutant Analysis — Vietnam",
        "slug": "pollutant-analysis-vietnam",
        "description": "Dominant pollutant identification, pollutant contribution to overall AQI, distribution analysis.",
    },
    "04_source_comparison": {
        "title": "Source Comparison — Vietnam",
        "slug": "source-comparison-vietnam",
        "description": "AQI comparison across different data sources (AQICN, OpenWeather, Sensors.Community) for the same cities.",
    },
    "05_forecast_vs_actual": {
        "title": "AQICN Forecast Accuracy — Vietnam",
        "slug": "forecast-vs-actual-vietnam",
        "description": "AQICN forecast AQI vs actual AQI, forecast accuracy by city, error distribution and bias analysis.",
    },
}


# ---------------------------------------------------------------------------
# Layout helpers
# ---------------------------------------------------------------------------

def make_positions(
    chart_ids: list[int],
    *,
    cols: int = 12,
    rows_per_chart: int = 8,
    header_rows: int = 0,
) -> list[dict[str, Any]]:
    """
    Tạo dashboard positions layout.
    Đặt charts trong lưới bootstrap cols x rows.
    """
    positions = []
    row_height = 50  # pixels per row unit
    current_y = 0 + header_rows * row_height

    for i, chart_id in enumerate(chart_ids):
        col = (i % 2) * (cols // 2)
        width = cols // 2
        positions.append(
            {
                "id": chart_id,
                "type": "CHART",
                "meta": {
                    "width": width,
                    "height": rows_per_chart,
                    "x": col,
                    "y": current_y,
                },
            }
        )
        if i % 2 == 1:
            current_y += rows_per_chart
    return positions


# ---------------------------------------------------------------------------
# ZIP export helper
# ---------------------------------------------------------------------------

def create_stub_zip(name: str, dashboard_key: str) -> Path:
    """
    Tạo một stub ZIP với metadata.yaml — placeholder cho dashboard.
    Khi Superset chạy và có dữ liệu, chạy setup_dashboards.py để tạo dashboard thực sự.
    """
    dashboards_dir = Path(__file__).parent / "dashboards"
    zip_path = dashboards_dir / f"{name}.zip"

    meta = DASHBOARD_META.get(dashboard_key, {})
    dashboard_title = meta.get("title", name)
    slug = meta.get("slug", name)
    description = meta.get("description", "")

    metadata_content = {
        "version": "1.0",
        "type": "dashboard",
        "uuid": str(__import__("uuid").uuid4()),
        "dashboard_title": dashboard_title,
        "slug": slug,
        "description": description,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "stub",
        "note": (
            "Stub ZIP. Run `python setup_dashboards.py` after Superset starts "
            "and mart tables are populated to create charts and dashboards."
        ),
        "charts": list(chart.name for chart in DASHBOARD_CHARTS.get(dashboard_key, [])),
    }

    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "metadata.yaml",
            json.dumps(metadata_content, indent=2, ensure_ascii=False),
        )
        zf.writestr(
            "README.txt",
            f"""Dashboard: {dashboard_title}
Slug: {slug}

This ZIP is a Superset dashboard stub.

To create the actual dashboard:
1. Start Superset: docker compose up -d superset
2. Run mart ingestion: docker compose run airflow-scheduler airflow dags trigger dag_transform
3. Wait for data: check ClickHouse tables fct_hourly_aqi, dim_locations have rows
4. Run setup: docker compose exec superset python /app/setup_dashboards.py

Charts defined: {len(DASHBOARD_CHARTS.get(dashboard_key, []))}
Query tables: fct_hourly_aqi, dim_locations, fct_daily_aqi_summary, mart_air_quality__dashboard, raw_aqicn_forecast
Vietnam filter: latitude 8-24, longitude 102-110
EPA color scale: Good(0-50) Green, Moderate(51-100) Yellow, Unhealthy for Sensitive(101-150) Orange,
  Unhealthy(151-200) Red, Very Unhealthy(201-300) Purple, Hazardous(301-500) Maroon
""",
        )

    with open(zip_path, "wb") as f:
        f.write(buf.getvalue())

    logger.info("Created stub ZIP: %s", zip_path)
    return zip_path


# ---------------------------------------------------------------------------
# Main Setup
# ---------------------------------------------------------------------------


def run_setup(
    config: SupersetConfig,
    dry_run: bool = False,
    skip_existing: bool = True,
) -> None:
    """Thiết lập toàn bộ dashboards qua Superset REST API."""
    client = SupersetAPIClient(config)

    # Wait for Superset
    if not client.wait_for_healthy():
        raise RuntimeError("Superset is not healthy. Aborting setup.")

    # Login
    client.login()

    # ── 1. Create database ──────────────────────────────────────────────────
    db_id = client.create_database(
        name="ClickHouse AirQuality",
        sqlalchemy_uri=config.clickhouse_uri(),
    )

    # ── 2. Create datasets ──────────────────────────────────────────────────
    dataset_ids: dict[str, int] = {}
    for ds_def in get_dataset_definitions():
        name = ds_def["name"]
        try:
            ds_id = client.create_dataset(
                table_name=ds_def["table_name"],
                schema_name=ds_def["schema"],
                database_id=db_id,
                sql=ds_def.get("sql"),
            )
            dataset_ids[name] = ds_id
        except Exception as exc:
            logger.warning("Failed to create dataset '%s': %s", name, exc)

    # ── 3. Create charts per dashboard ─────────────────────────────────────
    chart_ids_by_dashboard: dict[str, list[int]] = {}

    for dash_key, charts in DASHBOARD_CHARTS.items():
        if dry_run:
            logger.info("[DRY RUN] Would create %d charts for %s", len(charts), dash_key)
            chart_ids_by_dashboard[dash_key] = list(range(1000, 1000 + len(charts)))
            continue

        dash_chart_ids = []
        for chart_def in charts:
            try:
                # Pick a reasonable dataset id for the chart
                # Charts that use multi-table joins use ds_virtual datasets
                ds_name = chart_def.name  # fallback
                if chart_def.sql and "JOIN" in chart_def.sql.upper():
                    ds_id = dataset_ids.get("fct_hourly_aqi", db_id)
                elif chart_def.sql and "raw_aqicn_forecast" in chart_def.sql:
                    ds_id = dataset_ids.get("raw_aqicn_forecast", db_id)
                elif chart_def.sql and "dim_locations" in chart_def.sql and "fct_hourly_aqi" not in chart_def.sql:
                    ds_id = dataset_ids.get("dim_locations", db_id)
                else:
                    ds_id = dataset_ids.get("fct_hourly_aqi", db_id)

                cid = client.create_chart(
                    slice_name=chart_def.name,
                    dataset_id=ds_id,
                    viz_type=chart_def.viz_type,
                    sql=chart_def.sql,
                    datasource_id=ds_id,
                    datasource_type="table",
                    config=chart_def.params,
                )
                dash_chart_ids.append(cid)
            except Exception as exc:
                logger.warning("Failed to create chart '%s': %s", chart_def.name, exc)

        chart_ids_by_dashboard[dash_key] = dash_chart_ids

    # ── 4. Create dashboards ────────────────────────────────────────────────
    dashboard_ids: dict[str, int] = {}

    for dash_key, meta in DASHBOARD_META.items():
        try:
            dash_id = client.create_dashboard(
                title=meta["title"],
                slug=meta["slug"],
                description=meta["description"],
            )
            dashboard_ids[dash_key] = dash_id
        except Exception as exc:
            logger.warning("Failed to create dashboard '%s': %s", meta["title"], exc)

    # ── 5. Set dashboard positions ─────────────────────────────────────────
    if not dry_run:
        for dash_key, dash_id in dashboard_ids.items():
            chart_ids = chart_ids_by_dashboard.get(dash_key, [])
            if chart_ids:
                try:
                    # Estimate layout: 2 cols, 8 rows per chart, header=2 rows
                    positions = make_positions(
                        chart_ids,
                        cols=12,
                        rows_per_chart=8,
                        header_rows=2,
                    )
                    client.set_dashboard_position(dash_id, positions)
                except Exception as exc:
                    logger.warning("Failed to set positions for dashboard %d: %s", dash_id, exc)

    # ── Summary ─────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Setup complete!")
    logger.info("Dashboards created:")
    for dash_key, dash_id in dashboard_ids.items():
        meta = DASHBOARD_META.get(dash_key, {})
        chart_count = len(chart_ids_by_dashboard.get(dash_key, []))
        logger.info(
            "  [%s] %s (id=%d) — %d charts",
            dash_key,
            meta.get("title", dash_key),
            dash_id,
            chart_count,
        )
    logger.info("=" * 60)
    logger.info("Visit: http://localhost:8088")
    logger.info(
        "Dashboards: http://localhost:8088/superset/dashboard/%s/",
        "/".join(dashboard_ids.values()),
    )


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Superset Dashboard Setup")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be created without making changes")
    parser.add_argument("--skip-existing", action="store_true", default=True, help="Skip datasets/dashboards that already exist")
    parser.add_argument("--base-url", default=os.environ.get("SUPERSET_BASE_URL", "http://localhost:8088"), help="Superset base URL")
    args = parser.parse_args()

    config = SupersetConfig(base_url=args.base_url)
    logger.info("Superset Dashboard Setup — base_url=%s", config.base_url)

    try:
        run_setup(config, dry_run=args.dry_run, skip_existing=args.skip_existing)
    except Exception as exc:
        logger.error("Setup failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
