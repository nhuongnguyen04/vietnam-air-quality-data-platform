"""Build mart-only catalog and training assets for text-to-SQL.

Sources of truth:
  - allowed_tables.yml          : security allowlist
  - dbt mart YAML files         : model + column descriptions
  - ClickHouse system.columns   : actual column types
  - example_questions.yml       : Q+SQL training pairs
"""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

try:
    from python_jobs.text_to_sql.semantic_loader import (
        get_semantic_dir,
        load_allowed_tables,
        load_example_questions,
        load_dbt_model_docs,
        fetch_clickhouse_schema,
        build_table_prompt_context,
    )
except ModuleNotFoundError:  # pragma: no cover - container import fallback
    from semantic_loader import (  # type: ignore
        get_semantic_dir,
        load_allowed_tables,
        load_example_questions,
        load_dbt_model_docs,
        fetch_clickhouse_schema,
        build_table_prompt_context,
    )


DEFAULT_DASHBOARD_METADATA_PATH = (
    Path(__file__).resolve().parents[1] / "dashboard" / "dashboard_metadata.yml"
)


def _load_dashboard_metadata(path: str | Path | None = None) -> dict[str, Any]:
    import yaml
    metadata_path = Path(path) if path else DEFAULT_DASHBOARD_METADATA_PATH
    try:
        with metadata_path.open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}
    except FileNotFoundError:
        return {}


def _dashboard_pages_by_table(
    dashboard_metadata: dict[str, Any],
) -> dict[str, list[dict[str, str]]]:
    pages_by_table: dict[str, list[dict[str, str]]] = defaultdict(list)
    for page in dashboard_metadata.get("dashboard", {}).get("pages", []):
        page_info = {
            "filename": str(page.get("filename", "")).strip(),
            "name": str(page.get("name", "")).strip(),
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
        }
        for table_name in question.get("tables", []):
            questions_by_table[str(table_name).strip()].append(compact)
    return dict(questions_by_table)


def build_vanna_catalog_bundle(
    semantic_dir: str | Path | None = None,
    dashboard_metadata_path: str | Path | None = None,
    project_root: Path | None = None,
    clickhouse_schema: dict[str, list[dict[str, str]]] | None = None,
) -> dict[str, Any]:
    """Return a mart-only bundle ready for Vanna training.

    Data flow:
      allowed_tables.yml → which tables to include
      dbt YAML           → description, grain, column descriptions
      ClickHouse         → real column names + types
      example_questions  → Q+SQL training pairs
    """
    resolved_semantic_dir = get_semantic_dir(semantic_dir)
    example_questions = load_example_questions(resolved_semantic_dir)
    dashboard_metadata = _load_dashboard_metadata(dashboard_metadata_path)
    pages_by_table = _dashboard_pages_by_table(dashboard_metadata)
    questions_by_table = _example_questions_by_table(example_questions)

    # Build table context from dbt + ClickHouse (single call, fetches schema once)
    table_context = build_table_prompt_context(
        resolved_semantic_dir,
        project_root=project_root,
        clickhouse_schema=clickhouse_schema,
    )

    catalog_tables = []
    for table in table_context:
        table_name = table["table"]
        catalog_tables.append(
            {
                "table": table_name,
                "description": table["description"],
                "grain": table["grain"],
                "columns": table["columns"],          # ordered list of column names
                "column_types": table["column_types"],  # {col: "Nullable(Float64)", ...}
                "column_docs": table["column_docs"],    # {col: "description text", ...}
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
