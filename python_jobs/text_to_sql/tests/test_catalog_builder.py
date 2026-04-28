from __future__ import annotations

from python_jobs.text_to_sql.catalog_builder import build_vanna_catalog_bundle


def test_catalog_builder_returns_mart_only_tables(semantic_dir, dashboard_metadata_path):
    bundle = build_vanna_catalog_bundle(
        semantic_dir=semantic_dir,
        dashboard_metadata_path=dashboard_metadata_path,
    )

    assert bundle["query_surface"] == "mart_only"
    assert bundle["tables"]
    assert all(
        entry["table"].startswith(("dm_", "fct_")) for entry in bundle["tables"]
    )


def test_catalog_builder_includes_dashboard_and_example_context(
    semantic_dir,
    dashboard_metadata_path,
):
    bundle = build_vanna_catalog_bundle(
        semantic_dir=semantic_dir,
        dashboard_metadata_path=dashboard_metadata_path,
    )
    current_status = next(
        entry for entry in bundle["tables"] if entry["table"] == "dm_aqi_current_status"
    )

    filenames = {page["filename"] for page in current_status["dashboard_pages"]}
    languages = {question["lang"] for question in current_status["example_questions"]}

    assert "5_Alerts.py" in filenames
    assert "10_Ask_Data.py" in filenames
    assert languages == {"en", "vi"}
    assert current_status["background_lineage"]
    assert current_status["columns"]

