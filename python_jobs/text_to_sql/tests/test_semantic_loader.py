from __future__ import annotations

import json

import yaml

from python_jobs.text_to_sql.semantic_loader import (
    SemanticValidationError,
    build_table_prompt_context,
    load_allowed_tables,
    load_semantic_bundle,
)


def test_load_allowed_semantic_assets(semantic_dir):
    allowed_tables = load_allowed_tables(semantic_dir)

    assert "dm_air_quality_overview_daily" in allowed_tables
    assert "fct_air_quality_summary_hourly" in allowed_tables

    bundle = load_semantic_bundle(semantic_dir)
    assert "dm_aqi_current_status" in bundle.table_docs
    assert {question["lang"] for question in bundle.example_questions} == {"vi", "en"}
    assert len(bundle.schema_snapshot["tables"]) == len(bundle.allowed_tables)


def test_build_prompt_context_uses_snapshot_columns(semantic_dir):
    context = build_table_prompt_context(semantic_dir)
    first_table = next(item for item in context if item["table"] == "dm_aqi_current_status")

    assert "business_purpose" in first_table
    assert "lineage_summary" in first_table
    assert "columns" in first_table


def test_reject_forbidden_tables(temp_semantic_dir):
    (temp_semantic_dir / "allowed_tables.yml").write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "tables": [
                    "dm_aqi_current_status",
                    "raw_aqiin_measurements",
                    "stg_openweather__meteorology",
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    try:
        load_allowed_tables(temp_semantic_dir)
    except SemanticValidationError as exc:
        assert "raw_aqiin_measurements" in str(exc) or "stg_openweather__meteorology" in str(exc)
    else:
        raise AssertionError("Forbidden tables should fail validation")


def test_dashboard_metadata_tables_are_covered(dashboard_metadata_path, semantic_dir):
    dashboard_metadata = yaml.safe_load(dashboard_metadata_path.read_text(encoding="utf-8"))
    bundle = load_semantic_bundle(semantic_dir)

    metadata_tables = {
        table_name
        for page in dashboard_metadata["dashboard"]["pages"]
        for table_name in page.get("source_tables", [])
    }

    missing = sorted(metadata_tables - bundle.allowed_tables)
    assert missing == []

    docs_with_business_context = {
        table_name
        for table_name, table_doc in bundle.table_docs.items()
        if table_doc["business_purpose"] and table_doc["lineage_summary"]
    }
    assert metadata_tables.issubset(docs_with_business_context)


def test_schema_snapshot_contains_only_allowlisted_tables(semantic_dir):
    snapshot = json.loads((semantic_dir / "generated_schema_snapshot.json").read_text(encoding="utf-8"))
    table_names = {table["name"] for table in snapshot["tables"]}

    assert table_names == load_allowed_tables(semantic_dir)
