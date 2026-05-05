"""Read-only ClickHouse execution for preview-approved SQL."""
from __future__ import annotations

from dataclasses import dataclass
import os
import re
import time

import clickhouse_connect


@dataclass(frozen=True)
class QueryExecutionResult:
    columns: list[str]
    rows: list[list[object]]
    row_count: int
    truncated: bool
    execution_ms: int
    sql: str


class ClickHouseExecutor:
    def __init__(
        self,
        max_rows: int = 500,
        timeout_seconds: int = 30,
        database: str | None = None,
    ) -> None:
        self.max_rows = max_rows
        self.timeout_seconds = timeout_seconds
        self.database = database or os.environ.get("CLICKHOUSE_DB", "air_quality")

    def _get_client(self):
        username = os.environ.get("TEXT_TO_SQL_CLICKHOUSE_USER")
        password = os.environ.get("TEXT_TO_SQL_CLICKHOUSE_PASSWORD")
        if not username or not password:
            raise ValueError(
                "Dedicated TEXT_TO_SQL_CLICKHOUSE_USER and TEXT_TO_SQL_CLICKHOUSE_PASSWORD are required"
            )

        return clickhouse_connect.get_client(
            host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
            port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
            username=username,
            password=password,
            database=self.database,
        )

    def _apply_default_limit(self, sql: str) -> tuple[str, bool]:
        if re.search(r"\blimit\b", sql, flags=re.IGNORECASE):
            return sql, False
        return f"{sql.rstrip()} LIMIT {self.max_rows}", True

    def execute_query(self, sql: str) -> QueryExecutionResult:
        bounded_sql, limit_was_added = self._apply_default_limit(sql)
        start = time.perf_counter()
        client = self._get_client()
        result = client.query(
            bounded_sql,
            settings={
                "max_execution_time": self.timeout_seconds,
                "max_result_rows": self.max_rows,
                "result_overflow_mode": "break",
            },
        )
        execution_ms = int((time.perf_counter() - start) * 1000)
        rows = [list(row) for row in result.result_rows]
        row_count = len(rows)
        truncated = limit_was_added or row_count >= self.max_rows
        return QueryExecutionResult(
            columns=list(result.column_names),
            rows=rows,
            row_count=row_count,
            truncated=truncated,
            execution_ms=execution_ms,
            sql=bounded_sql,
        )
