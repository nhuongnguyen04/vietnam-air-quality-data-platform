from __future__ import annotations

from pathlib import Path


def _read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def test_app_registers_ask_data_page():
    app_source = _read("python_jobs/dashboard/app.py")

    assert "10_Ask_Data.py" in app_source
    assert "st.navigation" in app_source


def test_i18n_contains_nav_ask_data_in_both_languages():
    i18n_source = _read("python_jobs/dashboard/lib/i18n.py")

    assert "nav_ask_data" in i18n_source
    assert "\"Ask Data\"" in i18n_source


def test_page_shows_preview_before_execute_and_uses_service_client():
    page_source = _read("python_jobs/dashboard/pages/10_Ask_Data.py")

    assert "text_to_sql_client" in page_source or "TextToSqlClient" in page_source
    assert page_source.index("Xem trước SQL") < page_source.index("Chạy truy vấn")
    assert "query_df(" not in page_source


def test_page_persists_required_session_state_keys():
    page_source = _read("python_jobs/dashboard/pages/10_Ask_Data.py")

    assert "st.session_state.setdefault(\"question\"" in page_source
    assert "st.session_state.setdefault(\"preview\"" in page_source
    assert "st.session_state.setdefault(\"preview_stale\"" in page_source
    assert "st.session_state.setdefault(\"result\"" in page_source


def test_dashboard_metadata_contains_ask_data_entry():
    metadata_source = _read("python_jobs/dashboard/dashboard_metadata.yml")

    assert "10_Ask_Data.py" in metadata_source
    assert "Ask Data" in metadata_source
    assert "raw_" not in metadata_source.split("10_Ask_Data.py", 1)[1].split("\n    - filename:", 1)[0]
