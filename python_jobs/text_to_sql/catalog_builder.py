"""Build reviewable mart-only catalog and training assets for text-to-SQL."""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml

try:
    from python_jobs.text_to_sql.semantic_loader import (
        build_table_prompt_context,
        get_semantic_dir,
        load_example_questions,
    )
except ModuleNotFoundError:  # pragma: no cover - container import fallback
    from semantic_loader import build_table_prompt_context, get_semantic_dir, load_example_questions  # type: ignore


DEFAULT_DASHBOARD_METADATA_PATH = (
    Path(__file__).resolve().parents[1] / "dashboard" / "dashboard_metadata.yml"
)


def get_dashboard_metadata_path(path: str | Path | None = None) -> Path:
    return Path(path) if path else DEFAULT_DASHBOARD_METADATA_PATH


def load_dashboard_metadata(path: str | Path | None = None) -> dict[str, Any]:
    metadata_path = get_dashboard_metadata_path(path)
    with metadata_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _dashboard_pages_by_table(
    dashboard_metadata: dict[str, Any],
) -> dict[str, list[dict[str, str]]]:
    pages_by_table: dict[str, list[dict[str, str]]] = defaultdict(list)
    for page in dashboard_metadata.get("dashboard", {}).get("pages", []):
        page_info = {
            "filename": str(page.get("filename", "")).strip(),
            "name": str(page.get("name", "")).strip(),
            "display_name": str(page.get("display_name", "")).strip(),
        }
        for table_name in page.get("source_tables", []):
            pages_by_table[str(table_name).strip()].append(page_info)
    return dict(pages_by_table)


def _example_questions_by_table(
    example_questions: list[dict[str, Any]],
) -> dict[str, list[dict[str, str]]]:
    questions_by_table: dict[str, list[dict[str, str]]] = defaultdict(list)
    for question in example_questions:
        compact = {
            "id": str(question.get("id", "")).strip(),
            "lang": str(question.get("lang", "")).strip(),
            "question": str(question.get("question", "")).strip(),
            "topic": str(question.get("topic", "")).strip(),
        }
        for table_name in question.get("tables", []):
            questions_by_table[str(table_name).strip()].append(compact)
    return dict(questions_by_table)


def build_vanna_catalog_bundle(
    semantic_dir: str | Path | None = None,
    dashboard_metadata_path: str | Path | None = None,
) -> dict[str, Any]:
    """Return a mart-only bundle ready for Vanna prep and quality evaluation."""
    resolved_semantic_dir = get_semantic_dir(semantic_dir)
    dashboard_metadata = load_dashboard_metadata(dashboard_metadata_path)
    table_context = build_table_prompt_context(resolved_semantic_dir)
    example_questions = load_example_questions(resolved_semantic_dir)
    pages_by_table = _dashboard_pages_by_table(dashboard_metadata)
    questions_by_table = _example_questions_by_table(example_questions)

    catalog_tables = []
    for table in table_context:
        table_name = table["table"]
        catalog_tables.append(
            {
                "table": table_name,
                "business_purpose": table["business_purpose"],
                "grain": table["grain"],
                "key_dimensions": list(table["key_dimensions"]),
                "safe_filters": list(table["safe_filters"]),
                "columns": list(table["columns"]),
                "background_lineage": table["lineage_summary"],
                "dashboard_pages": pages_by_table.get(table_name, []),
                "example_questions": questions_by_table.get(table_name, []),
            }
        )

    return {
        "version": 1,
        "query_surface": "mart_only",
        "semantic_dir": str(resolved_semantic_dir),
        "tables": catalog_tables,
        "question_examples": example_questions,
    }
