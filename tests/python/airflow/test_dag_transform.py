"""File-based checks for the transform DAG."""

from __future__ import annotations

from pathlib import Path

import pytest


DAG_PATH = Path("airflow/dags/dag_transform.py")


@pytest.mark.integration
def test_dag_transform_updates_ingestion_control_from_final_task_states() -> None:
    content = DAG_PATH.read_text(encoding="utf-8")

    assert "def update_transform_control()" in content
    assert "@task(trigger_rule='all_done')" in content
    assert "source='dag_transform'" in content
    assert "ti.xcom_pull(task_ids='log_dbt_stats')" in content
    assert "failed_states = {'failed', 'upstream_failed'}" in content
