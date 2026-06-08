"""Validate generated SQL against the mart-only Ask Data contract."""
from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

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


_AGGREGATE_CLASSES = (exp.Max, exp.Min, exp.Avg, exp.Sum, exp.Count)


def _is_aggregate_function(node: exp.Expression) -> bool:
    if isinstance(node, _AGGREGATE_CLASSES):
        return True
    if isinstance(node, exp.Anonymous) and node.name.upper() in {"MAX", "MIN", "AVG", "SUM", "COUNT"}:
        return True
    return False


def _has_aggregate_function(select_node: exp.Select) -> bool:
    for part in [
        select_node.expressions,
        select_node.args.get("having"),
        select_node.args.get("order"),
    ]:
        if part is None:
            continue
        nodes = part if isinstance(part, list) else [part]
        for node in nodes:
            for child in node.walk():
                if _is_aggregate_function(child):
                    return True
    return False


def _get_non_aggregated_columns(select_node: exp.Select) -> set[str]:
    non_agg_cols = set()

    def walk(node: exp.Expression):
        if _is_aggregate_function(node):
            return
        if isinstance(node, exp.Column):
            non_agg_cols.add(node.name.lower())
            return
        for child in node.args.values():
            if isinstance(child, list):
                for item in child:
                    if isinstance(item, exp.Expression):
                        walk(item)
            elif isinstance(child, exp.Expression):
                walk(child)

    # Collect from projections
    for expr in select_node.expressions:
        walk(expr)

    # Collect from HAVING
    having = select_node.args.get("having")
    if having:
        walk(having)

    # Collect from ORDER BY, but skip aliases defined in projections
    aliases = {
        expr.alias.lower()
        for expr in select_node.expressions
        if isinstance(expr, exp.Alias) and expr.alias
    }
    order = select_node.args.get("order")
    if order:
        def walk_order(node_order: exp.Expression):
            if _is_aggregate_function(node_order):
                return
            if isinstance(node_order, exp.Column):
                col_name = node_order.name.lower()
                if col_name not in aliases:
                    non_agg_cols.add(col_name)
                return
            for child in node_order.args.values():
                if isinstance(child, list):
                    for item in child:
                        if isinstance(item, exp.Expression):
                            walk_order(item)
                elif isinstance(child, exp.Expression):
                    walk_order(child)

        walk_order(order)

    return non_agg_cols


def _get_group_by_columns(select_node: exp.Select) -> set[str]:
    group = select_node.args.get("group")
    if not group:
        return set()
    cols = set()
    for expr in group.expressions:
        for child in expr.walk():
            if isinstance(child, exp.Column):
                cols.add(child.name.lower())
    return cols


def _check_clickhouse_aggregation_rule(statement: exp.Expression) -> None:
    for select_node in statement.find_all(exp.Select):
        if _has_aggregate_function(select_node):
            non_agg = _get_non_aggregated_columns(select_node)
            grouped = _get_group_by_columns(select_node)
            missing = non_agg - grouped
            if missing:
                missing_str = ", ".join(sorted(missing))
                raise SqlValidationError(
                    f"Selecting non-aggregated columns alongside aggregate functions requires a GROUP BY clause. "
                    f"Missing columns in GROUP BY: {missing_str}"
                )


_REGION_TRANSLATIONS = {
    # region_3
    "miền bắc": "Northern",
    "mien bac": "Northern",
    "miền trung": "Central",
    "mien trung": "Central",
    "miền nam": "Southern",
    "mien nam": "Southern",
    # region_8
    "tây bắc": "Northwest",
    "tay bac": "Northwest",
    "tây bắc bộ": "Northwest",
    "tay bac bo": "Northwest",
    "đông bắc": "Northeast",
    "dong bac": "Northeast",
    "đông bắc bộ": "Northeast",
    "dong bac bo": "Northeast",
    "đồng bằng sông hồng": "Red River Delta",
    "dong bang song hồng": "Red River Delta",
    "dong bang song hong": "Red River Delta",
    "bắc trung bộ": "North Central",
    "bac trung bo": "North Central",
    "nam trung bộ": "South Central Coast",
    "nam trung bo": "South Central Coast",
    "duyên hải nam trung bộ": "South Central Coast",
    "duyen hai nam trung bo": "South Central Coast",
    "tây nguyên": "Central Highlands",
    "tay nguyen": "Central Highlands",
    "đông nam bộ": "Southeast",
    "dong nam bo": "Southeast",
    "đồng bằng sông cửu long": "Mekong Delta",
    "dong bang song cuu long": "Mekong Delta",
    "đbscl": "Mekong Delta",
}


def _rewrite_vietnamese_regions(statement: exp.Expression) -> exp.Expression:
    def transform_fn(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Literal) and node.is_string:
            val = node.this
            if val:
                normalized_val = re.sub(r"\s+", " ", val.strip().lower())
                translated = _REGION_TRANSLATIONS.get(normalized_val)
                if translated:
                    return exp.Literal.string(translated)
        return node
    return statement.transform(transform_fn)


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
        statement = _rewrite_vietnamese_regions(statement)
        _check_clickhouse_aggregation_rule(statement)
        referenced_tables = _extract_referenced_tables(statement)
        formatted_sql = statement.sql(dialect="clickhouse", pretty=True)
    except ParseError as exc:
        if not re.match(r"^\s*(select|with)\b", normalized_sql, flags=re.IGNORECASE):
            raise SqlValidationError("Only SELECT or WITH ... SELECT queries are allowed") from exc
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
