"""File-based checks for the transform DAG."""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

DAG_PATH = Path("airflow/dags/dag_transform.py")


def load_artifact_helpers() -> dict[str, object]:
    tree = ast.parse(DAG_PATH.read_text(encoding="utf-8"))
    helper_nodes = []
    for node in tree.body:
        if isinstance(node, ast.Import) and any(
            alias.name in {"json", "os", "tempfile"} for alias in node.names
        ):
            helper_nodes.append(node)
        if isinstance(node, ast.FunctionDef) and node.name in {
            "_load_json_document",
            "_write_json_atomic",
        }:
            helper_nodes.append(node)

    helper_module = ast.Module(body=helper_nodes, type_ignores=[])
    ast.fix_missing_locations(helper_module)
    namespace: dict[str, object] = {}
    exec(compile(helper_module, str(DAG_PATH), "exec"), namespace)
    return namespace


@pytest.mark.integration
def test_dag_transform_updates_ingestion_control_from_final_task_states() -> None:
    content = DAG_PATH.read_text(encoding="utf-8")

    assert "def update_transform_control()" in content
    assert "@task(trigger_rule='all_done')" in content
    assert "source='dag_transform'" in content
    assert "ti.xcom_pull(task_ids='log_dbt_stats')" in content
    assert "if ti.xcom_pull(task_ids=task_id) is None" in content
    assert "get_task_instances(" not in content


@pytest.mark.integration
def test_dag_transform_recovers_dbt_artifacts_with_trailing_json_data(tmp_path: Path) -> None:
    helpers = load_artifact_helpers()
    load_json_document = helpers["_load_json_document"]
    write_json_atomic = helpers["_write_json_atomic"]

    artifact = tmp_path / "manifest.json"
    artifact.write_text(
        '{"nodes":{"model.air_quality.test":{"database":""}},"sources":{}}{"extra":true}',
        encoding="utf-8",
    )

    data, needs_rewrite = load_json_document(str(artifact))
    assert needs_rewrite is True

    data["nodes"]["model.air_quality.test"]["database"] = "air_quality"
    write_json_atomic(str(artifact), data)

    cleaned = json.loads(artifact.read_text(encoding="utf-8"))
    assert cleaned["nodes"]["model.air_quality.test"]["database"] == "air_quality"
    assert list(tmp_path.glob(".*.tmp")) == []
