"""Validate generated SQL against the mart-only Ask Data contract."""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
import re
from typing import Iterable

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError
import yaml

try:
    from python_jobs.text_to_sql.semantic_loader import (
        SemanticValidationError,
        load_allowed_tables,
        validate_table_name,
    )
except ModuleNotFoundError:  # pragma: no cover - container import fallback
    from semantic_loader import (  # type: ignore
        SemanticValidationError,
        load_allowed_tables,
        validate_table_name,
    )


FORBIDDEN_KEYWORDS = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "TRUNCATE",
    "CREATE",
    "RENAME",
    "EXCHANGE",
    "ATTACH",
    "DETACH",
    "SYSTEM",
    "OPTIMIZE",
    "KILL",
    "GRANT",
    "REVOKE",
    "SET",
    "USE",
    "SHOW",
    "DESCRIBE",
    "EXPLAIN",
    "CALL",
}
FORBIDDEN_TABLE_PREFIXES = ("raw_", "stg_", "int_", "dim_")
FORBIDDEN_SCHEMAS = {"system", "information_schema"}
TABLE_REGEX = re.compile(r"\b(?:from|join)\s+([a-zA-Z0-9_.]+)", re.IGNORECASE)


class SqlValidationError(ValueError):
    """Raised when SQL is outside the preview/execute contract."""


@dataclass(frozen=True)
class ValidationResult:
    sql: str
    referenced_tables: list[str]
    warnings: list[str]


def _normalize_sql(sql: str) -> str:
    return sql.strip().rstrip(";")


# Hardcoded ClickHouse rewrite rules
_REWRITE_RULES = [
    (re.compile(r'\bCURRENT_TIMESTAMP\s*(?:\(\s*\))?', re.IGNORECASE), 'now()'),
    (re.compile(r'\bCURRENT_DATE\s*(?:\(\s*\))?', re.IGNORECASE), 'today()'),
    (re.compile(r'\bNOW\s*\(\s*\)', re.IGNORECASE), 'now()'),
    (re.compile(r'\b(toInterval(?:Hour|Day|Minute|Second|Week|Month|Year))\s*\(\s*\'(\d+)\'\s*\)', re.IGNORECASE), r'\1(\2)'),
    (re.compile(r'\bDATE_SUB\s*\(\s*(?:CURDATE|CURRENT_DATE)\s*\(\s*\)\s*,\s*INTERVAL\s+(\d+)\s+DAY\s*\)', re.IGNORECASE), r'today() - toIntervalDay(\1)'),
    (re.compile(r'\bDATEDIFF\s*\(\s*NOW\s*\(\s*\)\s*,\s*([^)]+)\s*\)', re.IGNORECASE), r"dateDiff('day', \1, now())"),
    (re.compile(r'\bEXTRACT\s*\(\s*YEAR\s+FROM\s+([^)]+)\)', re.IGNORECASE), r'toYear(\1)'),
    (re.compile(r'\bEXTRACT\s*\(\s*MONTH\s+FROM\s+([^)]+)\)', re.IGNORECASE), r'toMonth(\1)'),
    (re.compile(r'\bIFNULL\s*\(', re.IGNORECASE), 'ifNull('),
    (re.compile(r'\bNVL\s*\(', re.IGNORECASE), 'ifNull('),
]


def _regex_rewrite(sql: str, dialect_config: str | None = None) -> str:
    """Apply hardcoded regex rewrites for known ClickHouse incompatibilities."""
    for pattern, replacement in _REWRITE_RULES:
        sql = pattern.sub(replacement, sql)
    return sql


def _rewrite_for_clickhouse(sql: str, dialect_config: str | None = None) -> str:
    """Rewrite *sql* to be compatible with ClickHouse.

    Step 1: YAML-configured regex pre-pass (semantic/clickhouse_dialect.yml).
    Step 2: sqlglot transpile — structural AST-level dialect normalization.

    Falls back silently so downstream validation can surface a meaningful error.
    To add a new rewrite rule: edit clickhouse_dialect.yml, no Python changes needed.
    """
    sql = _regex_rewrite(sql, dialect_config)
    for source_dialect in ("", "mysql", "postgres", "duckdb"):
        try:
            results = sqlglot.transpile(
                sql,
                read=source_dialect,
                write="clickhouse",
                error_level=sqlglot.ErrorLevel.RAISE,
            )
            if results:
                return results[0]
        except Exception:  # noqa: BLE001 — try next dialect silently
            continue
    return sql  # fallback: return as-is, let validator surface the error


def _ensure_single_statement(sql: str) -> list[exp.Expression]:
    parsed = sqlglot.parse(sql, read="clickhouse")
    statements = [statement for statement in parsed if statement is not None]
    if len(statements) != 1:
        raise SqlValidationError("Only a single statement is allowed")
    return statements


def _extract_cte_names(statement: exp.Expression) -> set[str]:
    return {
        cte.alias_or_name.lower()
        for cte in statement.find_all(exp.CTE)
        if cte.alias_or_name
    }


def _normalize_table_reference(table: exp.Table) -> str:
    table_name = table.name.lower()
    schema_name = (table.db or "").lower()
    if schema_name in FORBIDDEN_SCHEMAS:
        raise SqlValidationError(f"Schema {schema_name} is not allowed")
    normalized = validate_table_name(table_name)
    if schema_name and schema_name != "air_quality":
        raise SqlValidationError(f"Schema {schema_name} is not allowed")
    return normalized


def _extract_referenced_tables(statement: exp.Expression) -> list[str]:
    cte_names = _extract_cte_names(statement)
    referenced_tables: set[str] = set()
    for table in statement.find_all(exp.Table):
        table_name = table.name.lower()
        if table_name in cte_names:
            continue
        referenced_tables.add(_normalize_table_reference(table))
    return sorted(referenced_tables)


def _contains_forbidden_keyword(sql: str) -> str | None:
    for keyword in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{keyword}\b", sql, flags=re.IGNORECASE):
            return keyword
    return None


def _fallback_referenced_tables(sql: str) -> list[str]:
    referenced_tables = set()
    for match in TABLE_REGEX.findall(sql):
        table_name = match.split(".")[-1]
        referenced_tables.add(validate_table_name(table_name))
    return sorted(referenced_tables)


def _ensure_allowed_statement(statement: exp.Expression) -> None:
    if not isinstance(statement, exp.Select):
        raise SqlValidationError("Only SELECT or WITH ... SELECT queries are allowed")


def _build_warnings(sql: str, statement: exp.Expression | None) -> list[str]:
    warnings: list[str] = []
    if "select *" in sql.lower():
        warnings.append("SELECT * may return unnecessary columns.")
    has_limit = statement.args.get("limit") if statement is not None else None
    if has_limit is None:
        warnings.append("No LIMIT clause detected; executor will enforce a row cap.")
    return warnings


def _ensure_allowlisted_tables(
    referenced_tables: Iterable[str],
    semantic_dir: str | None = None,
) -> None:
    allowed_tables = load_allowed_tables(semantic_dir)
    missing = sorted(set(referenced_tables) - allowed_tables)
    if missing:
        raise SqlValidationError(f"Query references non-allowlisted tables: {', '.join(missing)}")


def validate_sql(sql: str, semantic_dir: str | None = None) -> ValidationResult:
    normalized_sql = _normalize_sql(sql)
    if not normalized_sql:
        raise SqlValidationError("SQL cannot be empty")

    if ";" in normalized_sql:
        raise SqlValidationError("Multi-statement SQL is not allowed")

    # Rewrite standard SQL → ClickHouse dialect before any validation.
    # This converts CURRENT_TIMESTAMP → now(), CURRENT_DATE → today(), etc.
    normalized_sql = _rewrite_for_clickhouse(normalized_sql)

    lowered_sql = normalized_sql.lower()
    if "system." in lowered_sql:
        raise SqlValidationError("system schema access is not allowed")
    if "information_schema" in lowered_sql:
        raise SqlValidationError("information_schema access is not allowed")

    forbidden_keyword = _contains_forbidden_keyword(normalized_sql)
    if forbidden_keyword:
        raise SqlValidationError(f"{forbidden_keyword} statements are not allowed")

    try:
        statements = _ensure_single_statement(normalized_sql)
        statement = statements[0]
        _ensure_allowed_statement(statement)
        referenced_tables = _extract_referenced_tables(statement)
        formatted_sql = statement.sql(dialect="clickhouse", pretty=True)
    except ParseError:
        if not re.match(r"^\s*(select|with)\b", normalized_sql, flags=re.IGNORECASE):
            raise SqlValidationError("Only SELECT or WITH ... SELECT queries are allowed")
        referenced_tables = _fallback_referenced_tables(normalized_sql)
        statement = None
        formatted_sql = normalized_sql
    except SemanticValidationError as exc:
        raise SqlValidationError(str(exc)) from exc

    if not referenced_tables:
        raise SqlValidationError("Query must reference at least one approved mart or fact table")

    for table_name in referenced_tables:
        if table_name.startswith(FORBIDDEN_TABLE_PREFIXES):
            raise SqlValidationError(f"Forbidden table reference: {table_name}")

    _ensure_allowlisted_tables(referenced_tables, semantic_dir)
    warnings = _build_warnings(normalized_sql, statement)

    return ValidationResult(
        sql=formatted_sql,
        referenced_tables=referenced_tables,
        warnings=warnings,
    )
