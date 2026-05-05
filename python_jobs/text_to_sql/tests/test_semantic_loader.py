from __future__ import annotations

import yaml

from python_jobs.text_to_sql.semantic_loader import (
    SemanticValidationError,
    build_table_prompt_context,
    load_allowed_tables,
    load_dbt_model_docs,
    load_example_questions,
)


def test_load_allowed_semantic_assets(semantic_dir):
    allowed_tables = load_allowed_tables(semantic_dir)

    assert "dm_air_quality_overview_daily" in allowed_tables
    assert "fct_air_quality_summary_hourly" in allowed_tables

    example_questions = load_example_questions(semantic_dir)
    assert {question["lang"] for question in example_questions} == {"vi", "en"}
    assert {
        table_name
        for question in example_questions
        for table_name in question["tables"]
    }.issubset(allowed_tables)

    dbt_docs = load_dbt_model_docs()
    assert "dm_aqi_current_status" in dbt_docs
    assert dbt_docs["dm_aqi_current_status"]["description"].strip()
    assert "province" in dbt_docs["dm_aqi_current_status"]["columns"]


def test_build_prompt_context_uses_injected_clickhouse_schema(semantic_dir):
    context = build_table_prompt_context(
        semantic_dir,
        clickhouse_schema={
            "dm_aqi_current_status": [
                {"name": "province", "type": "String"},
                {"name": "ward_code", "type": "String"},
                {"name": "current_aqi_vn", "type": "Float64"},
            ]
        },
    )
    first_table = next(item for item in context if item["table"] == "dm_aqi_current_status")

    assert first_table["description"].strip()
    assert first_table["columns"] == ["province", "ward_code", "current_aqi_vn"]
    assert first_table["column_types"]["current_aqi_vn"] == "Float64"
    assert "province" in first_table["column_docs"]

    context_tables = {item["table"] for item in context}
    assert context_tables == load_allowed_tables(semantic_dir)


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
    allowed_tables = load_allowed_tables(semantic_dir)
    dbt_docs = load_dbt_model_docs(project_root=dashboard_metadata_path.parents[2])

    metadata_tables = {
        table_name
        for page in dashboard_metadata["dashboard"]["pages"]
        for table_name in page.get("source_tables", [])
    }

    missing = sorted(metadata_tables - allowed_tables)
    assert missing == []

    docs_with_business_context = {
        table_name
        for table_name, table_doc in dbt_docs.items()
        if table_doc["description"].strip()
    }
    assert metadata_tables.issubset(docs_with_business_context)
