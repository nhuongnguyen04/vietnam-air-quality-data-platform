"""FastAPI app for Ask Data SQL preview and guarded execution."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import os
import time

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field

try:
    from python_jobs.text_to_sql.clickhouse_executor import ClickHouseExecutor
    from python_jobs.text_to_sql.semantic_loader import build_table_prompt_context
    from python_jobs.text_to_sql.sql_validator import SqlValidationError, validate_sql
    from python_jobs.text_to_sql.vanna_runtime import (
        GeneratedSql,
        RuntimeGenerationError,
        RuntimeNotConfiguredError,
        VannaRuntime,
    )
except ModuleNotFoundError:  # pragma: no cover - container import fallback
    from clickhouse_executor import ClickHouseExecutor  # type: ignore
    from semantic_loader import build_table_prompt_context  # type: ignore
    from sql_validator import SqlValidationError, validate_sql  # type: ignore
    from vanna_runtime import (  # type: ignore
        GeneratedSql,
        RuntimeGenerationError,
        RuntimeNotConfiguredError,
        VannaRuntime,
    )


@dataclass
class PreviewRecord:
    sql_hash: str
    sql: str
    created_at: float


class PreviewStore:
    def __init__(self, ttl_seconds: int = 900) -> None:
        self.ttl_seconds = ttl_seconds
        self._records: dict[str, PreviewRecord] = {}

    def _secret(self) -> bytes:
        secret = os.environ.get("TEXT_TO_SQL_PREVIEW_SECRET", "phase-10-preview-secret")
        return secret.encode("utf-8")

    def issue(self, sql: str) -> str:
        sql_hash = hashlib.sha256(sql.encode("utf-8")).hexdigest()
        token = hmac.new(self._secret(), sql_hash.encode("utf-8"), hashlib.sha256).hexdigest()
        self._records[token] = PreviewRecord(
            sql_hash=sql_hash,
            sql=sql,
            created_at=time.time(),
        )
        return token

    def validate(self, token: str, sql: str) -> bool:
        record = self._records.get(token)
        if record is None:
            return False
        if time.time() - record.created_at > self.ttl_seconds:
            self._records.pop(token, None)
            return False
        sql_hash = hashlib.sha256(sql.encode("utf-8")).hexdigest()
        return hmac.compare_digest(record.sql_hash, sql_hash)


class AskRequest(BaseModel):
    question: str = Field(min_length=3)
    lang: str = Field(default="vi")
    standard: str = Field(default="TCVN")
    session_id: str = Field(min_length=1)


class AskResponse(BaseModel):
    sql: str
    explanation: str
    warnings: list[str]
    referenced_tables: list[str]
    preview_token: str


class ExecuteRequest(BaseModel):
    sql: str = Field(min_length=1)
    preview_token: str = Field(min_length=1)


class ExecuteResponse(BaseModel):
    columns: list[str]
    rows: list[list[object]]
    row_count: int
    truncated: bool
    execution_ms: int


def create_app(
    *,
    runtime: VannaRuntime | None = None,
    executor: ClickHouseExecutor | None = None,
    semantic_dir: str | None = None,
) -> FastAPI:
    app = FastAPI(title="Vietnam Air Quality Text-to-SQL", version="0.1.0")
    app.state.runtime = runtime or VannaRuntime(semantic_dir=semantic_dir)
    app.state.executor = executor or ClickHouseExecutor()
    app.state.preview_store = PreviewStore()
    app.state.semantic_dir = semantic_dir

    def get_runtime() -> VannaRuntime:
        return app.state.runtime

    def get_executor() -> ClickHouseExecutor:
        return app.state.executor

    def get_preview_store() -> PreviewStore:
        return app.state.preview_store

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metadata/tables")
    def metadata_tables() -> dict[str, list[dict[str, object]]]:
        return {"tables": build_table_prompt_context(app.state.semantic_dir)}

    @app.post("/ask", response_model=AskResponse)
    def ask(
        request: AskRequest,
        runtime_dependency: VannaRuntime = Depends(get_runtime),
        preview_store: PreviewStore = Depends(get_preview_store),
    ) -> AskResponse:
        try:
            generated: GeneratedSql = runtime_dependency.generate_sql(
                question=request.question,
                lang=request.lang,
                standard=request.standard,
                session_id=request.session_id,
            )
            validation = validate_sql(generated.sql, app.state.semantic_dir)
        except RuntimeNotConfiguredError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except RuntimeGenerationError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except SqlValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        preview_token = preview_store.issue(validation.sql)
        return AskResponse(
            sql=validation.sql,
            explanation=generated.explanation,
            warnings=validation.warnings,
            referenced_tables=validation.referenced_tables,
            preview_token=preview_token,
        )

    @app.post("/execute", response_model=ExecuteResponse)
    def execute(
        request: ExecuteRequest,
        executor_dependency: ClickHouseExecutor = Depends(get_executor),
        preview_store: PreviewStore = Depends(get_preview_store),
    ) -> ExecuteResponse:
        if not preview_store.validate(request.preview_token, request.sql):
            raise HTTPException(
                status_code=400,
                detail="Preview token is missing, expired, or does not match the previewed SQL",
            )

        try:
            validation = validate_sql(request.sql, app.state.semantic_dir)
        except SqlValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        result = executor_dependency.execute_query(validation.sql)
        return ExecuteResponse(
            columns=result.columns,
            rows=result.rows,
            row_count=result.row_count,
            truncated=result.truncated,
            execution_ms=result.execution_ms,
        )

    return app


app = create_app()
