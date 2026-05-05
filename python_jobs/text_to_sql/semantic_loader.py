"""Helpers for loading and validating text-to-SQL semantic assets.

Sources of truth (new architecture):
  - allowed_tables.yml     : security allowlist (unchanged)
  - dbt schema YAMLs       : business descriptions for models + columns
  - ClickHouse system.columns : actual DDL types at runtime
  - example_questions.yml  : Q+SQL training pairs (unchanged)

Deprecated (kept for backward compatibility only, not used by catalog_builder):
  - table_docs.yml
  - generated_schema_snapshot.json
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

import yaml


ALLOWED_PREFIXES = ("dm_", "fct_")
FORBIDDEN_PATTERNS = (
    "raw_",
    "stg_",
    "int_",
    "dim_",
    "system.",
    "information_schema",
)
DEFAULT_SEMANTIC_DIR = Path(__file__).resolve().parent / "semantic"

# dbt mart YAML files — two search strategies:
#   1. Local dev: relative to project root (monorepo layout)
#   2. Container: copied into the semantic/ directory at build time
DBT_MART_YAML_RELATIVE = [
    "dbt/dbt_tranform/models/marts/core/_mart_core__models.yml",
    "dbt/dbt_tranform/models/marts/analytics/_mart_analytics__models.yml",
]
DBT_MART_YAML_BASENAMES = [
    "_mart_core__models.yml",
    "_mart_analytics__models.yml",
]


class SemanticValidationError(ValueError):
    """Raised when semantic assets violate the mart-only contract."""


def get_semantic_dir(semantic_dir: str | Path | None = None) -> Path:
    return Path(semantic_dir) if semantic_dir else DEFAULT_SEMANTIC_DIR


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _normalize_table_name(table_name: str) -> str:
    cleaned = table_name.strip().lower()
    if "." in cleaned:
        cleaned = cleaned.split(".")[-1]
    return cleaned


def validate_table_name(table_name: str) -> str:
    normalized = _normalize_table_name(table_name)
    if any(pattern in normalized for pattern in FORBIDDEN_PATTERNS):
        raise SemanticValidationError(f"Forbidden table reference: {table_name}")
    if not normalized.startswith(ALLOWED_PREFIXES):
        raise SemanticValidationError(
            f"Only dm_* and fct_* tables are allowed: {table_name}"
        )
    if not re.fullmatch(r"(dm|fct)_[a-z0-9_]+", normalized):
        raise SemanticValidationError(f"Invalid table name: {table_name}")
    return normalized


def load_allowed_tables(semantic_dir: str | Path | None = None) -> set[str]:
    path = get_semantic_dir(semantic_dir) / "allowed_tables.yml"
    payload = _load_yaml(path)
    tables = payload.get("tables", [])
    if not isinstance(tables, list) or not tables:
        raise SemanticValidationError("allowed_tables.yml must define a non-empty tables list")
    return {validate_table_name(table_name) for table_name in tables}


def load_example_questions(semantic_dir: str | Path | None = None) -> list[dict[str, Any]]:
    path = get_semantic_dir(semantic_dir) / "example_questions.yml"
    payload = _load_yaml(path)
    questions = payload.get("questions", [])
    if not isinstance(questions, list) or not questions:
        raise SemanticValidationError("example_questions.yml must define questions")
    for question in questions:
        if question.get("lang") not in {"vi", "en"}:
            raise SemanticValidationError("Example questions must use vi or en")
        for table_name in question.get("tables", []):
            validate_table_name(table_name)
    return questions


# ── dbt schema YAML loader ────────────────────────────────────────────────────

def _find_dbt_yaml_paths(project_root: Path | None = None) -> list[Path]:
    """Resolve dbt mart YAML paths.

    Search order:
      1. Project root relative paths (local dev / monorepo)
      2. Semantic directory (container — files copied during build/deploy)
    """
    root = project_root
    if root is None:
        try:
            root = Path(__file__).resolve().parents[2]
        except IndexError:
            root = None  # shallow path (e.g. /app/), skip strategy 1

    paths = []
    # Strategy 1: project root (local dev)
    if root is not None:
        for rel in DBT_MART_YAML_RELATIVE:
            p = root / rel
            if p.exists():
                paths.append(p)

    # Strategy 2: semantic directory (container deployment)
    if not paths:
        semantic_dir = DEFAULT_SEMANTIC_DIR
        for basename in DBT_MART_YAML_BASENAMES:
            p = semantic_dir / basename
            if p.exists():
                paths.append(p)

    return paths


def load_dbt_model_docs(
    project_root: Path | None = None,
) -> dict[str, dict[str, Any]]:
    """Load model descriptions and column descriptions from dbt mart YAML files.

    Returns a dict keyed by model name:
    {
      "fct_air_quality_province_level_daily": {
        "description": "...",
        "meta": {...},
        "columns": {
          "date": "Date of the observation.",
          "province": "Province name.",
          ...
        }
      },
      ...
    }
    """
    docs: dict[str, dict[str, Any]] = {}
    for yaml_path in _find_dbt_yaml_paths(project_root):
        payload = _load_yaml(yaml_path)
        for model in payload.get("models", []):
            name = _normalize_table_name(model.get("name", ""))
            if not name.startswith(ALLOWED_PREFIXES):
                continue
            column_docs: dict[str, str] = {}
            for col in model.get("columns", []):
                col_name = (col.get("name") or "").strip()
                col_desc = (col.get("description") or "").strip()
                if col_name:
                    column_docs[col_name] = col_desc
            docs[name] = {
                "description": (model.get("description") or "").strip(),
                "meta": model.get("meta") or {},
                "columns": column_docs,
            }
    return docs


# ── ClickHouse live schema loader ─────────────────────────────────────────────

def fetch_clickhouse_schema(
    allowed_tables: set[str],
    *,
    host: str = "clickhouse",
    port: int = 8123,
    user: str = "admin",
    password: str = "",
    database: str = "air_quality",
) -> dict[str, list[dict[str, str]]]:
    """Query system.columns for the approved tables and return real ClickHouse types.

    Returns:
    {
      "fct_air_quality_province_level_daily": [
        {"name": "date", "type": "Date"},
        {"name": "province", "type": "String"},
        {"name": "pm25_avg", "type": "Nullable(Float64)"},
        ...
      ],
      ...
    }
    """
    import os

    try:
        import clickhouse_connect
    except ImportError as exc:  # pragma: no cover
        raise SemanticValidationError(
            "clickhouse_connect is required for live schema fetching"
        ) from exc

    host = os.environ.get("CLICKHOUSE_HOST", host)
    port = int(os.environ.get("CLICKHOUSE_PORT", port))
    # text-to-sql container uses TEXT_TO_SQL_CLICKHOUSE_* vars; fall back to generic
    user = (
        os.environ.get("TEXT_TO_SQL_CLICKHOUSE_USER")
        or os.environ.get("CLICKHOUSE_USER")
        or user
    )
    password = (
        os.environ.get("TEXT_TO_SQL_CLICKHOUSE_PASSWORD")
        or os.environ.get("CLICKHOUSE_PASSWORD")
        or password
    )
    database = os.environ.get("CLICKHOUSE_DB", database)

    client = clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password, database=database
    )
    try:
        table_list = ", ".join(f"'{t}'" for t in sorted(allowed_tables))
        result = client.query(
            f"SELECT table, name, type FROM system.columns "
            f"WHERE database = '{database}' AND table IN ({table_list}) "
            f"ORDER BY table, position"
        )
        schema: dict[str, list[dict[str, str]]] = {}
        for row in result.result_rows:
            table, col_name, col_type = row
            schema.setdefault(table, []).append({"name": col_name, "type": col_type})
        return schema
    finally:
        client.close()


# ── High-level context builder ────────────────────────────────────────────────

def build_table_prompt_context(
    semantic_dir: str | Path | None = None,
    *,
    project_root: Path | None = None,
    clickhouse_schema: dict[str, list[dict[str, str]]] | None = None,
) -> list[dict[str, Any]]:
    """Build training context for each allowed table.

    Data sources (in priority order):
      1. allowed_tables.yml        — which tables are approved
      2. dbt YAML                  — business description, column descriptions
      3. ClickHouse system.columns — real types (passed in or fetched live)

    Returns list of dicts, one per table, ready for Vanna training.
    """
    allowed = load_allowed_tables(semantic_dir)
    dbt_docs = load_dbt_model_docs(project_root)

    if clickhouse_schema is None:
        clickhouse_schema = fetch_clickhouse_schema(allowed)

    context = []
    for table_name in sorted(allowed):
        dbt = dbt_docs.get(table_name, {})
        ch_cols = clickhouse_schema.get(table_name, [])
        col_names = [c["name"] for c in ch_cols]
        col_types = {c["name"]: c["type"] for c in ch_cols}
        col_descs = dbt.get("columns", {})
        meta = dbt.get("meta", {})

        context.append(
            {
                "table": table_name,
                "description": dbt.get("description", ""),
                "grain": meta.get("grain", ""),
                "columns": col_names,
                "column_types": col_types,    # {"date": "Date", "pm25_avg": "Nullable(Float64)", ...}
                "column_docs": col_descs,      # {"date": "Date of observation", ...}
            }
        )
    return context
