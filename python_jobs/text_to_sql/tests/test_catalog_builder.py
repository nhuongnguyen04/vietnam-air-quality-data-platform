from __future__ import annotations

from python_jobs.text_to_sql.catalog_builder import build_vanna_catalog_bundle
from python_jobs.text_to_sql.semantic_loader import load_allowed_tables


def test_catalog_builder_returns_mart_only_tables(
    semantic_dir,
    dashboard_metadata_path,
    fake_clickhouse_schema,
):
    bundle = build_vanna_catalog_bundle(
        semantic_dir=semantic_dir,
        dashboard_metadata_path=dashboard_metadata_path,
        clickhouse_schema=fake_clickhouse_schema(load_allowed_tables(semantic_dir)),
    )

    assert bundle["query_surface"] == "mart_only"
    assert bundle["tables"]
    assert all(
        entry["table"].startswith(("dm_", "fct_")) for entry in bundle["tables"]
    )


def test_catalog_builder_includes_dashboard_and_example_context(
    semantic_dir,
    dashboard_metadata_path,
    fake_clickhouse_schema,
):
    bundle = build_vanna_catalog_bundle(
        semantic_dir=semantic_dir,
        dashboard_metadata_path=dashboard_metadata_path,
        clickhouse_schema=fake_clickhouse_schema(load_allowed_tables(semantic_dir)),
    )
    current_status = next(
        entry for entry in bundle["tables"] if entry["table"] == "dm_aqi_current_status"
    )

    filenames = {page["filename"] for page in current_status["dashboard_pages"]}
    languages = {question["lang"] for question in current_status["example_questions"]}

    assert "5_Alerts.py" in filenames
    assert "10_Ask_Data.py" in filenames
    assert languages == {"en", "vi"}
    assert current_status["description"].strip()
    assert "province" in current_status["column_docs"]
    assert current_status["columns"]
