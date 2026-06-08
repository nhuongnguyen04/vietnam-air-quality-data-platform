"""Vanna client training logic and prompt rendering helpers."""
from __future__ import annotations

from typing import Any


def render_training_ddl(table: dict[str, Any]) -> str:
    """Generate CREATE TABLE DDL using real ClickHouse types from system.columns."""
    col_types = table.get("column_types", {})
    column_defs = ",\n  ".join(
        f"{col} {col_types.get(col, 'String')}"
        for col in table["columns"]
    )
    return f"CREATE TABLE {table['table']} (\n  {column_defs}\n)"


def render_training_documentation(table: dict[str, Any]) -> str:
    parts = [
        f"Table: {table['table']}",
        f"Description: {table.get('description', '')}",
        f"Grain: {table.get('grain', '')}",
        f"Columns: {', '.join(table['columns'])}",
        "Policy: use only approved dm_* and fct_* tables.",
    ]
    # Column-level docs from dbt
    col_docs = table.get("column_docs", {})
    if col_docs:
        col_doc_lines = "\n  ".join(
            f"{col}: {desc}"
            for col, desc in col_docs.items()
            if desc
        )
        if col_doc_lines:
            parts.append(f"Column descriptions:\n  {col_doc_lines}")
    if table.get("dashboard_pages"):
        pages = ", ".join(p["filename"] for p in table["dashboard_pages"])
        parts.append(f"Used in dashboard pages: {pages}")
    return "\n".join(parts)


def render_policy_documentation(bundle: dict[str, Any]) -> str:
    allowed_tables = ", ".join(table["table"] for table in bundle["tables"])
    return (
        "Ask Data SQL generation policy:\n"
        f"- Approved tables: {allowed_tables}\n"
        "- Only SELECT or WITH ... SELECT queries are allowed.\n"
        "- Stay on mart/fact analytics surfaces only.\n"
        "- Favor province/ward level reporting marts before lower-level internals.\n"
        "- Reuse the bilingual examples and dashboard context when mapping user intent.\n"
    )


def train_vanna_client(client: Any, bundle: dict[str, Any]) -> None:
    # 1. Access policy
    client.train(documentation=render_policy_documentation(bundle))
    # 2. Domain documentation (date functions, column conventions)
    system_docs = """
    ClickHouse date and time functions for SQL generation:
    - yesterday()                             : Date of yesterday (Date type)
    - today()                                 : Date of today (Date type)
    - now()                                   : Current datetime (DateTime type)
    - today() - toIntervalDay(N)              : N days ago as Date
    - now() - toIntervalHour(N)               : N hours ago as DateTime
    - toStartOfMonth(today())                 : First day of current month
    - toStartOfMonth(today()) - toIntervalMonth(1) : First day of last month
    - toStartOfHour(now())                    : Current hour truncated

    Column naming conventions:
    - date columns        : "date"  (Date type, filter with =, >=, <=)
    - hour columns        : "datetime_hour"  (DateTime type)
    - month columns       : "month"  (Date type, first day of each month)
    - daily PM2.5 average : "pm25_avg"
    - hourly/current PM2.5: "pm25"
    - region columns      : "region_3" (3 main regions: Northern, Central, Southern) or "region_8" (8 economic/ecological regions)
      IMPORTANT: region_3 and region_8 values must always be in English.
      For "Miền Bắc", use region_3 = 'Northern'.
      For "Miền Trung", use region_3 = 'Central'.
      For "Miền Nam", use region_3 = 'Southern'.

    CRITICAL RULES:
    - You are writing SQL queries specifically for ClickHouse database.
    - STRICT CLICKHOUSE AGGREGATION RULE: Selecting any non-aggregated column alongside aggregate functions (e.g. SELECT province, MAX(pm25_avg) ...) requires a GROUP BY clause containing all non-aggregated columns. NEVER omit GROUP BY when selecting non-aggregate columns and aggregate functions together. If no grouping is needed, omit the non-aggregate column from the SELECT list (e.g. SELECT MAX(pm25_avg) ...).
    - PROVINCE/REGION COMPARISON RULE: When comparing values between two provinces or regions (e.g., calculating differences), use subqueries/CTEs or cross joins, and make sure any subquery containing aggregate functions does not select non-aggregated columns without a GROUP BY. If a subquery selects a non-aggregate column (like `province`) alongside `MAX(...)` or `AVG(...)`, it MUST have a `GROUP BY province` clause. Refer to the examples for how to structure CROSS JOINs for comparisons.
    - NEVER use CURRENT_TIMESTAMP, CURRENT_DATE, NOW() -- use now(), today(), yesterday() instead.
    - NEVER hardcode dates like '2023-11-13'. Always use dynamic date functions.
    - For "hom qua" (yesterday): WHERE date = yesterday()
    - For "hom nay" (today): WHERE date = today()
    - For "trong N gio qua" (last N hours): WHERE datetime_hour >= now() - toIntervalHour(N)
    - For "trong N ngay qua" (last N days): WHERE date >= today() - toIntervalDay(N)
    - For "thang nay" (this month): WHERE date >= toStartOfMonth(today())
    - For province-level PM2.5/AQI: prefer fct_air_quality_province_level_daily over ward/summary tables.
    - For a scalar question about one province, such as "AQI cao nhat cua Ha Noi hom qua",
      return one aggregate row: SELECT MAX(max_aqi_vn) ... WHERE date = yesterday() AND province = ...
    - Do not answer scalar province questions by selecting max_aqi_vn directly from
      dm_air_quality_overview_daily; that table is ward-level and will return many rows.
    """
    client.train(documentation=system_docs.strip())
    # 3. DDL + business documentation per table
    for table in bundle["tables"]:
        client.train(ddl=render_training_ddl(table))
        client.train(documentation=render_training_documentation(table))
    # 4. Question+SQL pairs — the most powerful training signal
    sql_pairs_trained = 0
    for question in bundle["question_examples"]:
        sql = (question.get("sql") or "").strip()
        if sql:
            client.train(question=question["question"], sql=sql)
            sql_pairs_trained += 1
    print(f"Vanna trained with {sql_pairs_trained} question+SQL pairs.")
