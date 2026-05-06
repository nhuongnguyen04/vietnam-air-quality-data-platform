"""File-based checks for the Google Drive sync DAG."""

from __future__ import annotations

from pathlib import Path

import pytest


DAG_PATH = Path("airflow/dags/dag_sync_gdrive.py")


@pytest.mark.integration
def test_dag_sync_gdrive_updates_ingestion_control() -> None:
    content = DAG_PATH.read_text(encoding="utf-8")

    assert "def update_sync_control()" in content
    assert "@task(trigger_rule='all_done')" in content
    assert "_update(" in content
    assert "source='dag_sync_gdrive'" in content
    assert "ti.xcom_pull(task_ids='sync_data')" in content
