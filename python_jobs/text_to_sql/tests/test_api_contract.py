from __future__ import annotations

from fastapi import HTTPException
import pytest

from python_jobs.text_to_sql.app import AskRequest, ExecuteRequest, create_app


def _route_endpoint(app, path: str):
    for route in app.routes:
        if getattr(route, "path", None) == path:
            return route.endpoint
    raise AssertionError(f"Route {path} not found")


@pytest.mark.unit
def test_health_returns_ok(text_to_sql_app):
    response = _route_endpoint(text_to_sql_app, "/health")()

    assert response == {"status": "ok"}


@pytest.mark.unit
def test_ask_returns_preview_and_referenced_tables(text_to_sql_app):
    preview_store = text_to_sql_app.state.preview_store
    response = _route_endpoint(text_to_sql_app, "/ask")(
        AskRequest(
            question="Tinh nao co AQI cao nhat?",
            lang="vi",
            standard="TCVN",
            session_id="session-1",
        ),
        text_to_sql_app.state.runtime,
        preview_store,
    )

    payload = response.model_dump()
    assert payload["sql"]
    assert payload["preview_token"]
    assert payload["referenced_tables"] == ["dm_aqi_current_status"]
    assert "warnings" in payload


@pytest.mark.unit
def test_execute_rejects_missing_preview_token(text_to_sql_app):
    with pytest.raises(HTTPException) as exc:
        _route_endpoint(text_to_sql_app, "/execute")(
            ExecuteRequest(
                sql="SELECT * FROM dm_aqi_current_status LIMIT 10",
                preview_token="missing-token",
            ),
            text_to_sql_app.state.executor,
            text_to_sql_app.state.preview_store,
        )

    assert exc.value.status_code == 400
    assert "preview" in exc.value.detail.lower()


@pytest.mark.unit
def test_execute_rejects_mismatched_preview_token(text_to_sql_app):
    ask_response = _route_endpoint(text_to_sql_app, "/ask")(
        AskRequest(
            question="Tinh nao co AQI cao nhat?",
            lang="vi",
            standard="TCVN",
            session_id="session-2",
        ),
        text_to_sql_app.state.runtime,
        text_to_sql_app.state.preview_store,
    )

    with pytest.raises(HTTPException) as exc:
        _route_endpoint(text_to_sql_app, "/execute")(
            ExecuteRequest(
                sql="SELECT * FROM dm_air_quality_overview_daily LIMIT 10",
                preview_token=ask_response.preview_token,
            ),
            text_to_sql_app.state.executor,
            text_to_sql_app.state.preview_store,
        )

    assert exc.value.status_code == 400
    assert "preview token" in exc.value.detail.lower()


@pytest.mark.unit
def test_execute_revalidates_sql_even_after_preview(
    semantic_dir,
    fake_vanna_runtime,
    fake_clickhouse_executor,
):
    app = create_app(
        runtime=fake_vanna_runtime,
        executor=fake_clickhouse_executor,
        semantic_dir=str(semantic_dir),
    )
    ask_response = _route_endpoint(app, "/ask")(
        AskRequest(
            question="show current AQI",
            lang="en",
            standard="TCVN",
            session_id="session-3",
        ),
        app.state.runtime,
        app.state.preview_store,
    )

    with pytest.raises(HTTPException) as exc:
        _route_endpoint(app, "/execute")(
            ExecuteRequest(
                sql="DROP TABLE dm_aqi_current_status",
                preview_token=ask_response.preview_token,
            ),
            app.state.executor,
            app.state.preview_store,
        )

    assert exc.value.status_code == 400
    assert "preview token" in exc.value.detail.lower()


@pytest.mark.unit
def test_execute_returns_required_response_fields(text_to_sql_app):
    ask_response = _route_endpoint(text_to_sql_app, "/ask")(
        AskRequest(
            question="show current AQI",
            lang="en",
            standard="TCVN",
            session_id="session-4",
        ),
        text_to_sql_app.state.runtime,
        text_to_sql_app.state.preview_store,
    )

    response = _route_endpoint(text_to_sql_app, "/execute")(
        ExecuteRequest(
            sql=ask_response.sql,
            preview_token=ask_response.preview_token,
        ),
        text_to_sql_app.state.executor,
        text_to_sql_app.state.preview_store,
    )

    payload = response.model_dump()
    assert payload["columns"] == ["province", "current_aqi_vn"]
    assert payload["rows"]
    assert payload["row_count"] == 2
    assert payload["truncated"] is False
    assert payload["execution_ms"] == 18
