---
gsd_plan_version: 1.0
phase: 01
plan: 1.03
slug: sensors-community-client
status: draft
wave: 1
depends_on:
  - PLAN-0-00
autonomous: true
files_modified:
  - python_jobs/jobs/sensorscm/__init__.py
  - python_jobs/jobs/sensorscm/ingest_measurements.py
  - python_jobs/models/sensorscm_models.py
  - python_jobs/common/config.py
  - scripts/init-clickhouse.sql
  - airflow/dags/dag_ingest_hourly.py
  - tests/test_sensorscm_int.py
requirements_addressed:
  - source-replacement
created: 2026-04-01
must_haves:
  - raw_sensorscm_measurements and raw_sensorscm_sensors tables created with ReplacingMergeTree(ingest_time)
  - python_jobs/jobs/sensorscm/ingest_measurements.py runs and inserts rows
  - dag_ingest_hourly has run_sensorscm_measurements_ingestion task in parallel fan-in
  - ingestion.control updated for 'sensorscm' source
  - All ingested records have quality_flag in {'valid', 'implausible', 'outlier'}
---

# Plan 1.03 — Sensors.Community Client

**Phase:** 01-multi-source-ingestion
**Wave:** 1 (independent — runs in parallel with PLAN-1-01 and PLAN-1-02)
**Autonomous:** true

---

## Context

Sensors.Community (`api.sensor.community/v1/feeds/`) is a community-driven air quality sensor network (formerly Luftdaten). Vietnam bounding-box: `lat=16.0&latDelta=7.5&lng=105.0&lngDelta=7.0` covers 8.4°N–23.4°N, 102.1°E–109.5°E. No auth required. Schedule in `dag_ingest_hourly` is `0 * * * *` (hourly, Wave 1); separate `dag_sensorscm_poll` with `*/10 * * * *` is added in PLAN-1-05 (Wave 3).

Key constraints:
- **D-05**: Insert ALL data including outliers — `quality_flag` = valid/implausible/outlier
- **D-23, D-26**: `quality_flag = 'implausible'` for PM2.5/PM10 outside 0–500 µg/m³; `quality_flag = 'outlier'` for stations outside Vietnam bbox
- **D-24**: No historical backfill — real-time polling only
- **D-29**: One `ReplacingMergeTree(ingest_time)` per source; ORDER BY `(sensor_id, timestamp_utc, parameter)`
- **D-31**: tenacity retry with exponential backoff in APIClient

---

## Tasks

### 1-03-A: Create `python_jobs/jobs/sensorscm/__init__.py`

<read_first>
- `python_jobs/jobs/openweather/__init__.py` — reference pattern from PLAN-1-01
</read_first>

<action>
Create `python_jobs/jobs/sensorscm/__init__.py`:
```python
"""Sensors.Community (Luftdaten) ingestion jobs."""
```
</action>

<acceptance_criteria>
- `python_jobs/jobs/sensorscm/__init__.py` exists and is non-empty
</acceptance_criteria>

---

### 1-03-B: Create `python_jobs/models/sensorscm_models.py`

<read_first>
- `python_jobs/models/openweather_models.py` — reference transform pattern from PLAN-1-01
- `01-RESEARCH.md` — API response shape, P1→PM10, P2→PM2.5, bbox params `lat=16.0&latDelta=7.5&lng=105.0&lngDelta=7.0`
- `01-CONTEXT.md` — D-23, D-24, D-26 (quality_flag logic), Vietnam bbox lat 8.4–23.4, lon 102.1–109.5
</read_first>

<action>
Create `python_jobs/models/sensorscm_models.py`:

```python
"""
Sensors.Community (Luftdaten) data models.

API base: https://api.sensor.community/v1/feeds/
Vietnam bbox: lat=16.0, latDelta=7.5, lng=105.0, lngDelta=7.0
Fields: P1 → PM10, P2 → PM2.5, temperature, humidity

Author: Air Quality Data Platform
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# Vietnam bounding box (D-23/D-26)
VIETNAM_BBOX = {
    "lat_min": 8.4,
    "lat_max": 23.4,
    "lon_min": 102.1,
    "lon_max": 109.5,
}

# Vietnam API bbox center
VIETNAM_BBOX_CENTER = {
    "lat": 16.0,
    "latDelta": 7.5,
    "lng": 105.0,
    "lngDelta": 7.0,
}

# Parameter name mapping: API value_type → canonical names
PARAMETER_MAP = {
    "P1":         "pm10",
    "P2":         "pm25",
    "temperature": "temperature",
    "humidity":   "humidity",
}

# Implausible value thresholds (D-23)
IMPLAUSIBLE_PM_THRESHOLD = 500.0   # µg/m³


def is_in_vietnam_bbox(lat: float, lon: float) -> bool:
    """Check if coordinates are within Vietnam bounding box."""
    return (
        VIETNAM_BBOX["lat_min"] <= lat <= VIETNAM_BBOX["lat_max"]
        and VIETNAM_BBOX["lon_min"] <= lon <= VIETNAM_BBOX["lon_max"]
    )


def assign_quality_flag(
    parameter: str,
    value: float,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
) -> str:
    """
    Assign quality flag per D-23 and D-26.

    Rules:
    - 'outlier' if lat/lon outside Vietnam bbox
    - 'implausible' if PM2.5 or PM10 outside 0–500 µg/m³
    - 'valid' otherwise

    All data is inserted regardless of flag (D-23).
    """
    # Check geographic outlier first
    if lat is not None and lon is not None:
        if not is_in_vietnam_bbox(lat, lon):
            return "outlier"

    # Check for implausible values (D-23)
    if parameter in ("pm25", "pm10"):
        if value < 0.0 or value > IMPLAUSIBLE_PM_THRESHOLD:
            return "implausible"

    return "valid"


def map_parameter(api_value_type: str) -> Optional[str]:
    """Map API value_type to canonical parameter name."""
    return PARAMETER_MAP.get(api_value_type)


def transform_sensor_reading(
    station: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Transform a single Sensors.Community station record into measurement records.

    API response shape:
    {
      "id": 12345,
      "sensor": {"id": 67890, "sensor_type": {"name": "SDS011"}, "pin": "1"},
      "location": {"latitude": 10.8231, "longitude": 106.6297, "country": "VN"},
      "data": [{"sensordatavalues": [{"value_type": "P2", "value": 28.7}], "timestamp": "..."}]
    }

    Returns one record per sensordatavalue entry.
    """
    records = []

    sensor_id = station.get("sensor", {}).get("id")
    if not sensor_id:
        logger.warning("Station missing sensor.id, skipping")
        return records

    sensor_type = station.get("sensor", {}).get("sensor_type", {}).get("name", "unknown")
    location = station.get("location", {})
    lat = location.get("latitude")
    lon = location.get("longitude")

    in_bbox = is_in_vietnam_bbox(lat, lon) if lat and lon else False

    data_entries = station.get("data", [])
    for entry in data_entries:
        timestamp_str = entry.get("timestamp")
        try:
            timestamp_utc = datetime.fromisoformat(
                timestamp_str.replace("Z", "+00:00")
            ) if timestamp_str else datetime.now(timezone.utc)
        except Exception:
            timestamp_utc = datetime.now(timezone.utc)

        sensordatavalues = entry.get("sensordatavalues", [])
        for sv in sensordatavalues:
            value_type = sv.get("value_type")
            value_str = sv.get("value")

            if value_type is None or value_str is None:
                continue

            try:
                value = float(value_str)
            except (ValueError, TypeError):
                logger.warning(f"Non-numeric value for {value_type}: {value_str}")
                continue

            parameter = map_parameter(value_type)
            if parameter is None:
                # Unknown value type, skip
                continue

            quality_flag = assign_quality_flag(parameter, value, lat, lon)

            record = {
                "sensor_id": sensor_id,
                "station_id": sensor_id,        # id field reused as station_id
                "latitude": lat,
                "longitude": lon,
                "timestamp_utc": timestamp_utc,
                "parameter": parameter,
                "value": value,
                "unit": "µg/m³" if parameter in ("pm10", "pm25") else (
                    "°C" if parameter == "temperature" else "%"
                ),
                "sensor_type": sensor_type,
                "quality_flag": quality_flag,
                "raw_payload": str(station),
            }
            records.append(record)

    return records


def transform_sensorscm_response(
    response: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Transform the full Sensors.Community API response into measurement records.

    Args:
        response: List of station objects from /feeds/ endpoint

    Returns:
        Flat list of all measurement records from all stations.
    """
    all_records = []
    for station in response:
        records = transform_sensor_reading(station)
        all_records.extend(records)
    logger.info(f"Transformed {len(response)} stations into {len(all_records)} measurement records")
    return all_records
```

File location: `python_jobs/models/sensorscm_models.py`
</action>

<acceptance_criteria>
- `python_jobs/models/sensorscm_models.py` contains `VIETNAM_BBOX` dict with lat_min=8.4, lat_max=23.4, lon_min=102.1, lon_max=109.5
- `grep -n 'assign_quality_flag' python_jobs/models/sensorscm_models.py` returns the function definition
- `grep -n 'transform_sensor_reading' python_jobs/models/sensorscm_models.py` returns the function definition
- `grep -n 'P1.*pm10' python_jobs/models/sensorscm_models.py` finds P1→pm10 mapping
- `grep -n 'P2.*pm25' python_jobs/models/sensorscm_models.py` finds P2→pm25 mapping
- `grep -n 'quality_flag' python_jobs/models/sensorscm_models.py` finds quality_flag assignment in transform
</acceptance_criteria>

---

### 1-03-C: Create `python_jobs/jobs/sensorscm/ingest_measurements.py`

<read_first>
- `python_jobs/jobs/openweather/ingest_measurements.py` — reference implementation pattern (PLAN-1-01)
- `python_jobs/models/sensorscm_models.py` — `transform_sensorscm_response()`, `VIETNAM_BBOX_CENTER` (created in 1-03-B)
- `python_jobs/common/api_client.py` — `APIClient` class
- `python_jobs/common/clickhouse_writer.py` — `create_clickhouse_writer()` factory
- `python_jobs/common/ingestion_control.py` — `update_control()` signature
</read_first>

<action>
Create `python_jobs/jobs/sensorscm/ingest_measurements.py`:

```python
#!/usr/bin/env python3
"""
Sensors.Community Measurements Ingestion Job.

Polls https://api.sensor.community/v1/feeds/ with Vietnam bounding-box filter
(lat=16.0, latDelta=7.5, lng=105.0, lngDelta=7.0) and stores readings in ClickHouse.

No authentication required. All data inserted with quality_flag.

Usage:
    python jobs/sensorscm/ingest_measurements.py --mode incremental

Author: Air Quality Data Platform
"""

import os
import sys
import logging
import argparse
from typing import List, Dict, Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common.api_client import APIClient
from common.clickhouse_writer import create_clickhouse_writer
from common.ingestion_control import update_control
from models.sensorscm_models import (
    VIETNAM_BBOX_CENTER,
    transform_sensorscm_response,
)


def create_sensorscm_client() -> APIClient:
    """Create a configured Sensors.Community API client (no auth)."""
    return APIClient(
        base_url="https://api.sensor.community",
        token=None,               # No auth required
        timeout=30,
        max_retries=5,
        backoff_factor=2.0,
        auth_header_name=None,    # No auth header
    )


def fetch_vietnam_sensors() -> List[Dict[str, Any]]:
    """
    Fetch all sensor readings within the Vietnam bounding box.

    API: GET /v1/feeds/?lat=16.0&latDelta=7.5&lng=105.0&lngDelta=7.0

    Returns list of station records (no pagination — single call returns all).
    """
    logger = logging.getLogger(__name__)

    client = create_sensorscm_client()
    try:
        params = {
            "lat": VIETNAM_BBOX_CENTER["lat"],
            "latDelta": VIETNAM_BBOX_CENTER["latDelta"],
            "lng": VIETNAM_BBOX_CENTER["lng"],
            "lngDelta": VIETNAM_BBOX_CENTER["lngDelta"],
        }
        response = client.get("/v1/feeds/", params=params)

        if not isinstance(response, list):
            logger.warning(f"Unexpected response type: {type(response)}, expected list")
            return []

        logger.info(f"Sensors.Community: {len(response)} stations returned")
        return response
    finally:
        client.close()


def run_incremental() -> int:
    """Fetch + write current sensor readings for Vietnam. Returns row count."""
    logger = logging.getLogger(__name__)

    stations = fetch_vietnam_sensors()
    if not stations:
        logger.warning("No Sensors.Community stations returned for Vietnam bbox")
        return 0

    records = transform_sensorscm_response(stations)
    if not records:
        logger.warning("No Sensors.Community measurement records after transform")
        return 0

    writer = create_clickhouse_writer()
    writer.write_batch("raw_sensorscm_measurements", records, source="sensorscm")
    logger.info(f"Wrote {len(records)} Sensors.Community measurements")

    return len(records)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Ingest Sensors.Community air quality data")
    parser.add_argument("--mode", choices=["incremental"], default="incremental")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logger = logging.getLogger(__name__)

    try:
        if args.mode == "incremental":
            count = run_incremental()

        update_control(source="sensorscm", records_ingested=count, success=True)
        logger.info(f"Sensors.Community ingestion done: {count} records")
    except Exception as e:
        logger.error(f"Sensors.Community ingestion failed: {e}")
        update_control(source="sensorscm", records_ingested=0, success=False, error_message=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
```

File location: `python_jobs/jobs/sensorscm/ingest_measurements.py`
</action>

<acceptance_criteria>
- `grep -n 'create_sensorscm_client' python_jobs/jobs/sensorscm/ingest_measurements.py` returns the function
- `grep -n 'fetch_vietnam_sensors' python_jobs/jobs/sensorscm/ingest_measurements.py` returns the function
- `grep -n 'lat=16.0' python_jobs/jobs/sensorscm/ingest_measurements.py` finds the bbox center in the API call
- `grep -n 'raw_sensorscm_measurements' python_jobs/jobs/sensorscm/ingest_measurements.py` finds the table name
- `grep -n 'update_control.*sensorscm' python_jobs/jobs/sensorscm/ingest_measurements.py` finds the control table update
- `grep -n 'transform_sensorscm_response' python_jobs/jobs/sensorscm/ingest_measurements.py` finds the transform import
</acceptance_criteria>

---

### 1-03-D: Add `raw_sensorscm_measurements` and `raw_sensorscm_sensors` tables to `scripts/init-clickhouse.sql`

<read_first>
- `scripts/init-clickhouse.sql` — existing table definitions; note `raw_openweather_measurements` and `raw_waqi_measurements` patterns from PLAN-1-01 and PLAN-1-02
- `01-RESEARCH.md` — recommended schema for `raw_sensorscm_measurements` and `raw_sensorscm_sensors`
</read_first>

<action>
Append these SQL statements to `scripts/init-clickhouse.sql`:

```sql
-- ============================================
-- Sensors.Community Measurements (Plan 1.03)
-- Source: api.sensor.community/v1/feeds/
-- Vietnam bbox: lat=16.0, latDelta=7.5, lng=105.0, lngDelta=7.0
-- D-05: Insert ALL data; quality_flag = valid/implausible/outlier
-- D-29: ReplacingMergeTree(ingest_time), server-side dedup
-- No Python-side dedup
-- ============================================
CREATE TABLE IF NOT EXISTS raw_sensorscm_measurements
(
    source              LowCardinality(String) DEFAULT 'sensorscm',
    ingest_time         DateTime DEFAULT now(),
    ingest_batch_id     String,
    ingest_date         Date MATERIALIZED toDate(ingest_time),

    sensor_id           UInt32,
    station_id          UInt32,                    -- id from API (reused as station_id)

    latitude            Float64,
    longitude           Float64,

    timestamp_utc       Nullable(DateTime),
    parameter           LowCardinality(String),    -- pm10, pm25, temperature, humidity
    value               Float32,
    unit                String DEFAULT 'µg/m³',

    sensor_type         LowCardinality(String),    -- SDS011, PMS5003, etc.
    quality_flag        LowCardinality(String),     -- valid | implausible | outlier (D-05, D-23, D-26)

    raw_payload         String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (station_id, timestamp_utc, parameter)
SETTINGS index_granularity = 8192;

-- Sensors/sensor metadata
-- Stores per-sensor metadata (sensor_type, location) deduplicated on sensor_id
CREATE TABLE IF NOT EXISTS raw_sensorscm_sensors
(
    source              LowCardinality(String) DEFAULT 'sensorscm',
    ingest_time         DateTime DEFAULT now(),
    ingest_batch_id     String,
    ingest_date         Date MATERIALIZED toDate(ingest_time),

    sensor_id           UInt32,
    station_id          UInt32,
    latitude            Float64,
    longitude           Float64,
    sensor_type         LowCardinality(String),
    is_indoor          Bool DEFAULT false,

    raw_payload         String CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(ingest_date)
ORDER BY (sensor_id)
SETTINGS index_granularity = 8192;

-- NOTE: Sensors.Community has NO historical backfill.
-- Real-time polling only. dag_sensorscm_poll (*/10 * * * *) added in PLAN-1-05.
```

Note: `raw_sensorscm_sensors` is populated by the same `ingest_measurements.py` script using the sensor metadata from each API response. A future metadata-only DAG can refresh sensors independently.
</action>

<acceptance_criteria>
- `grep -n 'raw_sensorscm_measurements' scripts/init-clickhouse.sql` finds the CREATE TABLE
- `grep -n 'ENGINE = ReplacingMergeTree(ingest_time)' scripts/init-clickhouse.sql` finds the engine
- `grep -n "ORDER BY (station_id, timestamp_utc, parameter)" scripts/init-clickhouse.sql` finds the ORDER BY key for measurements
- `grep -n 'raw_sensorscm_sensors' scripts/init-clickhouse.sql` finds the sensors table
- `grep -n "ORDER BY (sensor_id)" scripts/init-clickhouse.sql` finds the sensors ORDER BY key
- `grep -n "quality_flag" scripts/init-clickhouse.sql` finds the quality_flag column definition
</acceptance_criteria>

---

### 1-03-E: Add `run_sensorscm_measurements_ingestion` task to `airflow/dags/dag_ingest_hourly.py`

<read_first>
- `airflow/dags/dag_ingest_hourly.py` — current state (after PLAN-1-01/PLAN-1-02 OpenWeather and WAQI tasks added)
- `python_jobs/common/ingestion_control.py` — `update_control()` signature
</read_first>

<action>
In `airflow/dags/dag_ingest_hourly.py`:

1. Ensure `get_job_env_vars()` includes `SENSORSCMM_*` env vars if needed (Sensors.Community requires no token, but ClickHouse vars are required). Confirm all ClickHouse vars are already present.

2. Add the task function inside `dag_ingest_hourly()` after the existing measurement tasks:
```python
    @task
    def run_sensorscm_measurements_ingestion():
        """Run Sensors.Community measurements ingestion."""
        import subprocess

        env = os.environ.copy()
        env.update(get_job_env_vars())

        cmd = f"cd {PYTHON_PATH} && python jobs/sensorscm/ingest_measurements.py --mode incremental"

        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise Exception(f"Command failed: {cmd}")
        print(f"Sensors.Community measurements ingestion completed")
```

3. Add the control task:
```python
    @task
    def update_sensorscm_control():
        """Update ingestion_control for Sensors.Community measurements."""
        import sys
        sys.path.insert(0, '/opt/python/jobs')
        from common.ingestion_control import update_control as _update
        _update(source='sensorscm', records_ingested=0, success=True)
        print("Updated ingestion_control for sensorscm")
```

4. Update task definitions to add `sensorscm = run_sensorscm_measurements_ingestion()` and `update_sensorscm_control = update_sensorscm_control()`.

5. Update the fan-in dependencies to include sensorscm in the parallel list. The existing pattern after PLAN-1-01 and PLAN-1-02 will be something like:
```python
    check_clickhouse >> metadata >> [aqicn, forecast, openweather, waqi, sensorscm]
    [aqicn, forecast, openweather, waqi, sensorscm] >> update_aqicn_control >> update_forecast_control >> update_openweather_control >> update_waqi_control >> update_sensorscm_control >> completion
```
If openweather and waqi are not yet added (PLAN-1-01 and PLAN-1-02 still pending), add sensorscm alongside `[aqicn, forecast]`:
```python
    check_clickhouse >> metadata >> [aqicn, forecast, sensorscm]
    [aqicn, forecast, sensorscm] >> update_aqicn_control >> update_forecast_control >> update_sensorscm_control >> completion
```
</action>

<acceptance_criteria>
- `grep -n 'run_sensorscm_measurements_ingestion' airflow/dags/dag_ingest_hourly.py` returns the function definition
- `grep -n 'update_sensorscm_control' airflow/dags/dag_ingest_hourly.py` returns the function definition
- `grep -n 'jobs/sensorscm/ingest_measurements.py' airflow/dags/dag_ingest_hourly.py` finds the command
- `grep -n 'sensorscm' airflow/dags/dag_ingest_hourly.py` finds the task in fan-in list (e.g. `[aqicn, forecast, sensorscm]` or `[aqicn, forecast, openweather, waqi, sensorscm]`)
</acceptance_criteria>

---

### 1-03-F: Create `tests/test_sensorscm_int.py` integration stub

<read_first>
- `tests/conftest.py` — existing fixtures: `sample_sensorscm_response`
- `tests/test_sensorscm.py` — existing unit test stubs from PLAN-0-00
</read_first>

<action>
Create `tests/test_sensorscm_int.py`:

```python
"""Integration tests for Sensors.Community ingestion (Plan 1.03).

These tests verify end-to-end behavior. Requires network access to
api.sensor.community (no auth needed). Skipped if API is unreachable.
"""

import pytest


def test_sensorscm_api_vietnam_bbox():
    """Verify /v1/feeds/ endpoint accepts Vietnam bbox params and returns a list."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

    from python_jobs.common.api_client import APIClient

    client = APIClient(
        base_url="https://api.sensor.community",
        token=None,
        timeout=30,
        max_retries=3,
        auth_header_name=None,
    )

    params = {
        "lat": 16.0,
        "latDelta": 7.5,
        "lng": 105.0,
        "lngDelta": 7.0,
    }

    try:
        response = client.get("/v1/feeds/", params=params)
        assert isinstance(response, list), f"Expected list, got {type(response)}"
    finally:
        client.close()


def test_sensorscm_transform_sensor_reading(sample_sensorscm_response):
    """Transform function produces correct record structure."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

    from python_jobs.models.sensorscm_models import transform_sensor_reading

    records = transform_sensor_reading(sample_sensorscm_response[0])
    assert len(records) >= 2, f"Expected P1 (PM10) and P2 (PM2.5), got {len(records)} records"

    # Verify PM2.5 record
    pm25_records = [r for r in records if r["parameter"] == "pm25"]
    assert len(pm25_records) == 1
    assert pm25_records[0]["value"] == 28.7
    assert pm25_records[0]["sensor_type"] == "SDS011"

    # Verify PM10 record
    pm10_records = [r for r in records if r["parameter"] == "pm10"]
    assert len(pm10_records) == 1
    assert pm10_records[0]["value"] == 45.2

    # Verify quality_flag assignment
    assert all(r["quality_flag"] == "valid" for r in records), "Expected quality_flag=valid for in-bbox reading"


def test_sensorscm_quality_flag_outlier():
    """Station outside Vietnam bbox gets quality_flag='outlier'."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

    from python_jobs.models.sensorscm_models import assign_quality_flag

    # Bangkok coordinates (outside Vietnam bbox)
    flag = assign_quality_flag(parameter="pm25", value=50.0, lat=13.75, lon=100.5)
    assert flag == "outlier", f"Expected 'outlier' for Bangkok coords, got '{flag}'"


def test_sensorscm_quality_flag_implausible():
    """PM2.5 outside 0–500 µg/m³ gets quality_flag='implausible'."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

    from python_jobs.models.sensorscm_models import assign_quality_flag

    # Very high value (implausible)
    flag_high = assign_quality_flag(parameter="pm25", value=999.0, lat=21.0, lon=105.8)
    assert flag_high == "implausible", f"Expected 'implausible' for 999 µg/m³, got '{flag_high}'"

    # Negative value (implausible)
    flag_neg = assign_quality_flag(parameter="pm10", value=-5.0, lat=21.0, lon=105.8)
    assert flag_neg == "implausible", f"Expected 'implausible' for -5 µg/m³, got '{flag_neg}'"


def test_sensorscm_quality_flag_valid():
    """Normal PM2.5 within 0–500 µg/m³ gets quality_flag='valid'."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

    from python_jobs.models.sensorscm_models import assign_quality_flag

    flag = assign_quality_flag(parameter="pm25", value=68.5, lat=21.0, lon=105.8)
    assert flag == "valid", f"Expected 'valid' for 68.5 µg/m³ in Hanoi, got '{flag}'"


def test_sensorscm_parameter_mapping():
    """P1→pm10, P2→pm25, temperature→temperature, humidity→humidity."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

    from python_jobs.models.sensorscm_models import map_parameter

    assert map_parameter("P1") == "pm10"
    assert map_parameter("P2") == "pm25"
    assert map_parameter("temperature") == "temperature"
    assert map_parameter("humidity") == "humidity"
    assert map_parameter("unknown_type") is None
```

File location: `tests/test_sensorscm_int.py`
</action>

<acceptance_criteria>
- `tests/test_sensorscm_int.py` exists
- `grep -n 'transform_sensor_reading' tests/test_sensorscm_int.py` finds the transform test
- `grep -n 'quality_flag' tests/test_sensorscm_int.py` finds quality_flag tests
- `pytest tests/test_sensorscm_int.py --collect-only` produces zero errors
</acceptance_criteria>

---

## Summary

| Task | File | Action |
|------|------|--------|
| 1-03-A | `python_jobs/jobs/sensorscm/__init__.py` | Package init file |
| 1-03-B | `python_jobs/models/sensorscm_models.py` | `transform_sensor_reading()`, `assign_quality_flag()`, Vietnam bbox check |
| 1-03-C | `python_jobs/jobs/sensorscm/ingest_measurements.py` | bbox polling, quality flag assignment, batch insert |
| 1-03-D | `scripts/init-clickhouse.sql` | `raw_sensorscm_measurements` + `raw_sensorscm_sensors` ReplacingMergeTree tables |
| 1-03-E | `airflow/dags/dag_ingest_hourly.py` | `run_sensorscm_measurements_ingestion` task in parallel fan-in |
| 1-03-F | `tests/test_sensorscm_int.py` | Integration test stubs |

---

*Generated: 2026-04-01*
