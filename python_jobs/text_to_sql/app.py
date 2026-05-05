"""FastAPI app for Ask Data SQL preview and guarded execution."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import hashlib
import hmac
import os
import time
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Response
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
        self._secret_bytes = self._load_secret()

    def _load_secret(self) -> bytes:
        secret = os.environ.get("TEXT_TO_SQL_PREVIEW_SECRET")
        if not secret:
            raise RuntimeError("TEXT_TO_SQL_PREVIEW_SECRET environment variable is required")
        return secret.encode("utf-8")

    def issue(self, sql: str) -> str:
        sql_hash = hashlib.sha256(sql.encode("utf-8")).hexdigest()
        token = hmac.new(self._secret_bytes, sql_hash.encode("utf-8"), hashlib.sha256).hexdigest()
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
    generator_metadata: dict[str, str]
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


# ---------------------------------------------------------------------------
# Simple in-memory SQL response cache
# ---------------------------------------------------------------------------

_CACHE_TTL_SECONDS = int(os.environ.get("TEXT_TO_SQL_CACHE_TTL", "300"))


@dataclass
class _CacheEntry:
    result: Any
    expires_at: float


class SqlResponseCache:
    """Thread-safe, TTL-based cache keyed on (question, lang, standard)."""

    def __init__(self, ttl_seconds: int = _CACHE_TTL_SECONDS) -> None:
        self._ttl = ttl_seconds
        self._store: dict[str, _CacheEntry] = {}

    @staticmethod
    def _key(question: str, lang: str, standard: str) -> str:
        raw = f"{lang}|{standard}|{question.strip().lower()}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def get(self, question: str, lang: str, standard: str) -> Any | None:
        entry = self._store.get(self._key(question, lang, standard))
        if entry is None or time.time() > entry.expires_at:
            return None
        return entry.result

    def set(self, question: str, lang: str, standard: str, result: Any) -> None:
        self._store[self._key(question, lang, standard)] = _CacheEntry(
            result=result,
            expires_at=time.time() + self._ttl,
        )

    def invalidate_expired(self) -> None:
        now = time.time()
        expired = [k for k, v in self._store.items() if now > v.expires_at]
        for k in expired:
            self._store.pop(k, None)


# ---------------------------------------------------------------------------


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
    app.state.sql_cache = SqlResponseCache()
    app.state.semantic_dir = semantic_dir
    app.state.vanna_ready = False  # flips to True after startup warmup

    def get_runtime() -> VannaRuntime:
        return app.state.runtime

    def get_executor() -> ClickHouseExecutor:
        return app.state.executor

    def get_preview_store() -> PreviewStore:
        return app.state.preview_store

    @app.on_event("startup")
    async def warmup_vanna() -> None:
        """Pre-initialise the Vanna client so the first /ask request is fast.

        All heavy work (ChromaDB init, DDL/doc embedding, manifest resolution)
        happens here at boot time rather than on the first user request.
        Flips app.state.vanna_ready to True so /health/ready can gate traffic.
        """
        runtime: VannaRuntime = app.state.runtime
        try:
            loop = asyncio.get_running_loop()
            # Run blocking init in a thread pool so we don't block the event loop.
            await loop.run_in_executor(None, runtime._get_vanna_client)
            app.state.vanna_ready = True
            print("Vanna warm-up complete — first /ask request will be fast.")
        except Exception as exc:  # pragma: no cover
            # Do NOT crash the app; /ask will surface the error with HTTP 503.
            print(f"Vanna warm-up failed (non-fatal): {exc.__class__.__name__}: {exc}")

    @app.get("/health")
    def health() -> dict[str, str]:
        """Liveness probe — always returns 200 once the process is up."""
        return {"status": "ok"}

    @app.get("/health/ready")
    def health_ready(response: Response) -> dict[str, str]:
        """Readiness probe — returns 503 until Vanna warm-up is complete.

        Use this in Docker healthcheck / dashboard depends_on so downstream
        services only route traffic when the service is truly ready.
        """
        if not app.state.vanna_ready:
            response.status_code = 503
            return {"status": "warming_up"}
        return {"status": "ready"}

    @app.get("/metadata/tables")
    def metadata_tables() -> dict[str, list[dict[str, object]]]:
        return {"tables": build_table_prompt_context(app.state.semantic_dir)}

    def get_sql_cache() -> SqlResponseCache:
        return app.state.sql_cache

    @app.post("/ask", response_model=AskResponse)
    def ask(
        request: AskRequest,
        runtime_dependency: VannaRuntime = Depends(get_runtime),
        preview_store: PreviewStore = Depends(get_preview_store),
        sql_cache: SqlResponseCache = Depends(get_sql_cache),
    ) -> AskResponse:
        # --- Cache hit: skip LLM entirely for repeated questions ---------------
        cached = sql_cache.get(request.question, request.lang, request.standard)
        if cached is not None:
            # Re-issue a fresh preview token so the client can always /execute.
            preview_token = preview_store.issue(cached.sql)
            return AskResponse(
                sql=cached.sql,
                explanation=cached.explanation,
                warnings=cached.warnings,
                referenced_tables=cached.referenced_tables,
                generator_metadata={**cached.generator_metadata, "cache": "hit"},
                preview_token=preview_token,
            )

        # --- Cache miss: generate via Vanna/Groq --------------------------------
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

        if sorted(generated.referenced_tables) != validation.referenced_tables:
            raise HTTPException(
                status_code=502,
                detail=(
                    "Runtime referenced tables did not match validator output: "
                    f"runtime={generated.referenced_tables} validator={validation.referenced_tables}"
                ),
            )

        response = AskResponse(
            sql=validation.sql,
            explanation=generated.explanation,
            warnings=validation.warnings,
            referenced_tables=validation.referenced_tables,
            generator_metadata=generated.generator_metadata,
            preview_token=preview_store.issue(validation.sql),
        )
        # Store in cache for subsequent identical questions.
        sql_cache.set(request.question, request.lang, request.standard, response)
        # Opportunistically evict stale entries (lightweight, O(n) on cache size).
        sql_cache.invalidate_expired()
        return response

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
