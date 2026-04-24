from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml


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
