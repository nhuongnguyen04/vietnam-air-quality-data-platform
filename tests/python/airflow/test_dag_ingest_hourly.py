"""File-based checks for the hourly ingestion DAG."""

from __future__ import annotations

from pathlib import Path

import pytest


DAG_PATH = Path("airflow/dags/dag_ingest_hourly.py")


@pytest.mark.integration
def test_dag_ingest_hourly_contains_expected_job_tasks() -> None:
    content = DAG_PATH.read_text(encoding="utf-8")

    assert "def run_aqiin_measurements_ingestion()" in content
    assert "def run_openweather_unified_ingestion()" in content
    assert "def run_traffic_processing(**context)" in content
    assert "python jobs/aqiin/ingest_measurements.py --mode incremental" in content
    assert "python jobs/openweather/ingest_openweather_unified.py" in content


@pytest.mark.integration
def test_dag_ingest_hourly_triggers_transform_after_update_control() -> None:
    content = DAG_PATH.read_text(encoding="utf-8")

    assert "trigger_dag_id='dag_transform'" in content
    assert "_update(source='dag_ingest_hourly', records_ingested=0, success=True)" in content
    assert "check_ch >> [aqiin, ow_unified, tt_processing]" in content
    assert "[aqiin, ow_unified, tt_processing] >> update_control >> completion" in content
