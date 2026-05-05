from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import yaml

os.environ.setdefault("TEXT_TO_SQL_PREVIEW_SECRET", "test-preview-secret")

from python_jobs.text_to_sql.app import create_app
from python_jobs.text_to_sql import semantic_loader
from python_jobs.text_to_sql.clickhouse_executor import QueryExecutionResult
from python_jobs.text_to_sql.vanna_runtime import GeneratedSql


@pytest.fixture(autouse=True)
def text_to_sql_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEXT_TO_SQL_PREVIEW_SECRET", "test-preview-secret")


@pytest.fixture
def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


@pytest.fixture
def semantic_dir(repo_root: Path) -> Path:
    return repo_root / "python_jobs" / "text_to_sql" / "semantic"


@pytest.fixture
def dashboard_metadata_path(repo_root: Path) -> Path:
    return repo_root / "python_jobs" / "dashboard" / "dashboard_metadata.yml"


@pytest.fixture
def temp_semantic_dir(tmp_path: Path) -> Path:
    semantic_dir = tmp_path / "semantic"
    semantic_dir.mkdir()

    (semantic_dir / "allowed_tables.yml").write_text(
        yaml.safe_dump({"version": 1, "tables": ["dm_aqi_current_status"]}, sort_keys=False),
        encoding="utf-8",
    )
    (semantic_dir / "table_docs.yml").write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "tables": {
                    "dm_aqi_current_status": {
                        "business_purpose": "Current AQI status",
                        "grain": "province, ward_code",
                        "key_dimensions": ["province", "ward_code"],
                        "safe_filters": ["province"],
                        "lineage_summary": "Approved current-state mart",
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (semantic_dir / "example_questions.yml").write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "questions": [
                    {
                        "id": "vi-1",
                        "lang": "vi",
                        "topic": "aqi_status",
                        "question": "Tinh nao co AQI cao nhat?",
                        "tables": ["dm_aqi_current_status"],
                    },
                    {
                        "id": "en-1",
                        "lang": "en",
                        "topic": "aqi_status",
                        "question": "Which province has the highest AQI?",
                        "tables": ["dm_aqi_current_status"],
                    },
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (semantic_dir / "generated_schema_snapshot.json").write_text(
        json.dumps(
            {
                "version": 1,
                "tables": [
                    {
                        "name": "dm_aqi_current_status",
                        "grain": "province, ward_code",
                        "columns": ["province", "ward_code", "current_aqi_vn"],
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return semantic_dir


class FakeVannaRuntime:
    def __init__(self, sql: str = "SELECT * FROM dm_aqi_current_status LIMIT 10") -> None:
        self.sql = sql

    def generate_sql(self, *, question: str, lang: str, standard: str, session_id: str) -> GeneratedSql:
        return GeneratedSql(
            sql=self.sql,
            explanation=f"Preview for {question} in {lang} under {standard}",
            referenced_tables=["dm_aqi_current_status"],
            generator_metadata={
                "model": "fake-model",
                "collection": "fake-collection",
                "semantic_fingerprint": "fake-fingerprint",
            },
        )


class FakeClickHouseExecutor:
    def execute_query(self, sql: str) -> QueryExecutionResult:
        return QueryExecutionResult(
            columns=["province", "current_aqi_vn"],
            rows=[["Ha Noi", 88], ["Da Nang", 72]],
            row_count=2,
            truncated=False,
            execution_ms=18,
            sql=sql,
        )


@pytest.fixture
def fake_vanna_runtime() -> FakeVannaRuntime:
    return FakeVannaRuntime()


@pytest.fixture
def fake_clickhouse_executor() -> FakeClickHouseExecutor:
    return FakeClickHouseExecutor()


@pytest.fixture
def text_to_sql_app(semantic_dir: Path, fake_vanna_runtime: FakeVannaRuntime, fake_clickhouse_executor: FakeClickHouseExecutor):
    return create_app(
        runtime=fake_vanna_runtime,
        executor=fake_clickhouse_executor,
        semantic_dir=str(semantic_dir),
    )


@pytest.fixture
def load_text():
    def _load_text(path: str) -> str:
        return Path(path).read_text(encoding="utf-8")

    return _load_text


@pytest.fixture
def fake_clickhouse_schema():
    def _build(allowed_tables: set[str]) -> dict[str, list[dict[str, str]]]:
        schema = {
            table_name: [
                {"name": "province", "type": "String"},
                {"name": "date", "type": "Date"},
            ]
            for table_name in allowed_tables
        }
        schema.update(
            {
                "dm_aqi_current_status": [
                    {"name": "province", "type": "String"},
                    {"name": "ward_code", "type": "String"},
                    {"name": "current_aqi_vn", "type": "Float64"},
                    {"name": "pm25", "type": "Float64"},
                ],
                "dm_air_quality_overview_hourly": [
                    {"name": "province", "type": "String"},
                    {"name": "datetime_hour", "type": "DateTime"},
                    {"name": "avg_aqi_vn", "type": "Float64"},
                    {"name": "pm25_avg", "type": "Float64"},
                ],
                "dm_aqi_compliance_standards": [
                    {"name": "province", "type": "String"},
                    {"name": "date", "type": "Date"},
                    {"name": "who_pm25_breach", "type": "UInt8"},
                    {"name": "pm25_avg", "type": "Float64"},
                ],
                "dm_aqi_health_impact_summary": [
                    {"name": "province", "type": "String"},
                    {"name": "date", "type": "Date"},
                    {"name": "pm25_population_exposure", "type": "Float64"},
                ],
                "dm_regional_health_risk_ranking": [
                    {"name": "province", "type": "String"},
                    {"name": "date", "type": "Date"},
                    {"name": "health_risk_rank", "type": "UInt32"},
                    {"name": "health_risk_score", "type": "Float64"},
                ],
                "dm_traffic_pollution_correlation_daily": [
                    {"name": "province", "type": "String"},
                    {"name": "date", "type": "Date"},
                    {"name": "congestion_daily_avg", "type": "Float64"},
                    {"name": "pm25_daily_avg", "type": "Float64"},
                    {"name": "pm25_congestion_uplift", "type": "Float64"},
                ],
                "dm_weather_hourly_trend": [
                    {"name": "province", "type": "String"},
                    {"name": "datetime_hour", "type": "DateTime"},
                    {"name": "avg_humidity", "type": "Float64"},
                    {"name": "avg_wind_speed", "type": "Float64"},
                ],
                "dm_weather_pollution_correlation_daily": [
                    {"name": "province", "type": "String"},
                    {"name": "date", "type": "Date"},
                    {"name": "avg_humidity", "type": "Float64"},
                    {"name": "avg_wind_speed", "type": "Float64"},
                    {"name": "pm25_daily_avg", "type": "Float64"},
                ],
            }
        )
        return {
            table_name: schema[table_name]
            for table_name in allowed_tables
            if table_name in schema
        }

    return _build


@pytest.fixture(autouse=True)
def stub_clickhouse_schema(monkeypatch: pytest.MonkeyPatch, fake_clickhouse_schema) -> None:
    monkeypatch.setattr(
        semantic_loader,
        "fetch_clickhouse_schema",
        lambda allowed_tables, **kwargs: fake_clickhouse_schema(allowed_tables),
    )
