"""Helpers for loading and validating text-to-SQL semantic assets."""
from __future__ import annotations

import json
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


class SemanticValidationError(ValueError):
    """Raised when semantic assets violate the mart-only contract."""


@dataclass(frozen=True)
class SemanticBundle:
    allowed_tables: set[str]
    table_docs: dict[str, dict[str, Any]]
    example_questions: list[dict[str, Any]]
    schema_snapshot: dict[str, Any]


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


def load_table_docs(semantic_dir: str | Path | None = None) -> dict[str, dict[str, Any]]:
    path = get_semantic_dir(semantic_dir) / "table_docs.yml"
    payload = _load_yaml(path)
    docs = payload.get("tables", {})
    if not isinstance(docs, dict) or not docs:
        raise SemanticValidationError("table_docs.yml must define table documentation")
    validated_docs: dict[str, dict[str, Any]] = {}
    for table_name, table_doc in docs.items():
        normalized = validate_table_name(table_name)
        if not isinstance(table_doc, dict):
            raise SemanticValidationError(f"Documentation for {table_name} must be a mapping")
        required_fields = ("business_purpose", "grain", "key_dimensions", "safe_filters", "lineage_summary")
        missing = [field for field in required_fields if field not in table_doc]
        if missing:
            raise SemanticValidationError(
                f"Documentation for {table_name} missing fields: {', '.join(missing)}"
            )
        validated_docs[normalized] = table_doc
    return validated_docs


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


def load_schema_snapshot(semantic_dir: str | Path | None = None) -> dict[str, Any]:
    path = get_semantic_dir(semantic_dir) / "generated_schema_snapshot.json"
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    tables = payload.get("tables", [])
    if not isinstance(tables, list) or not tables:
        raise SemanticValidationError("generated_schema_snapshot.json must define tables")
    for table in tables:
        validate_table_name(table["name"])
        for column in table.get("columns", []):
            if any(pattern in column.lower() for pattern in FORBIDDEN_PATTERNS):
                raise SemanticValidationError(
                    f"Forbidden column metadata found in schema snapshot: {column}"
                )
    return payload


def load_semantic_bundle(semantic_dir: str | Path | None = None) -> SemanticBundle:
    allowed_tables = load_allowed_tables(semantic_dir)
    table_docs = load_table_docs(semantic_dir)
    example_questions = load_example_questions(semantic_dir)
    schema_snapshot = load_schema_snapshot(semantic_dir)

    if allowed_tables != set(table_docs):
        missing_docs = sorted(allowed_tables - set(table_docs))
        extra_docs = sorted(set(table_docs) - allowed_tables)
        raise SemanticValidationError(
            "Semantic docs mismatch allowlist: "
            f"missing={missing_docs or '[]'} extra={extra_docs or '[]'}"
        )

    snapshot_tables = {table["name"] for table in schema_snapshot["tables"]}
    if snapshot_tables != allowed_tables:
        missing_snapshot = sorted(allowed_tables - snapshot_tables)
        extra_snapshot = sorted(snapshot_tables - allowed_tables)
        raise SemanticValidationError(
            "Schema snapshot mismatch allowlist: "
            f"missing={missing_snapshot or '[]'} extra={extra_snapshot or '[]'}"
        )

    languages = {question["lang"] for question in example_questions}
    if languages != {"en", "vi"}:
        raise SemanticValidationError("Example questions must cover both vi and en")

    return SemanticBundle(
        allowed_tables=allowed_tables,
        table_docs=table_docs,
        example_questions=example_questions,
        schema_snapshot=schema_snapshot,
    )


def build_table_prompt_context(
    semantic_dir: str | Path | None = None,
) -> list[dict[str, Any]]:
    bundle = load_semantic_bundle(semantic_dir)
    context = []
    snapshot_by_name = {table["name"]: table for table in bundle.schema_snapshot["tables"]}
    for table_name in sorted(bundle.allowed_tables):
        table_doc = bundle.table_docs[table_name]
        snapshot = snapshot_by_name[table_name]
        context.append(
            {
                "table": table_name,
                "business_purpose": table_doc["business_purpose"],
                "grain": table_doc["grain"],
                "key_dimensions": table_doc["key_dimensions"],
                "safe_filters": table_doc["safe_filters"],
                "lineage_summary": table_doc["lineage_summary"],
                "columns": snapshot.get("columns", []),
            }
        )
    return context
