"""Single integration boundary for Vanna-backed SQL generation without execution."""
from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any

try:
    from python_jobs.text_to_sql.catalog_builder import build_vanna_catalog_bundle
except ModuleNotFoundError:  # pragma: no cover - container import fallback
    from catalog_builder import build_vanna_catalog_bundle  # type: ignore


class RuntimeNotConfiguredError(RuntimeError):
    """Raised when the runtime wrapper has no configured provider."""


class RuntimeGenerationError(RuntimeError):
    """Raised when the provider response cannot be used safely."""


@dataclass(frozen=True)
class GeneratedSql:
    sql: str
    explanation: str


class VannaRuntime:
    """Vanna-backed SQL generation wrapper that never executes SQL."""

    def __init__(self, semantic_dir: str | None = None) -> None:
        self.semantic_dir = semantic_dir
        self._vanna_client: Any | None = None

    def _resolve_vanna_config(self) -> dict[str, Any]:
        groq_api_key = os.environ.get("GROQ_API_KEY")
        if not groq_api_key:
            raise RuntimeNotConfiguredError(
                "GROQ_API_KEY is required for the Vanna runtime. Set GROQ_API_KEY and optionally GROQ_MODEL."
            )
        return {
            "api_key": groq_api_key,
            "model": os.environ.get("GROQ_MODEL", "qwen/qwen3-32b"),
            "base_url": "https://api.groq.com/openai/v1",
            "client": os.environ.get("TEXT_TO_SQL_VANNA_CLIENT", "in-memory"),
            "collection_name": os.environ.get(
                "TEXT_TO_SQL_VANNA_COLLECTION", "air_quality_ask_data"
            ),
        }

    def _load_vanna_dependencies(self):
        try:
            from openai import OpenAI
        except ImportError as exc:  # pragma: no cover - dependency gate
            raise RuntimeNotConfiguredError(
                "OpenAI client dependency missing for Vanna runtime. Install text-to-sql requirements."
            ) from exc

        try:
            from vanna.legacy.chromadb import ChromaDB_VectorStore
            from vanna.legacy.openai import OpenAI_Chat
        except ImportError:
            try:
                from vanna.chromadb import ChromaDB_VectorStore  # type: ignore
                from vanna.openai import OpenAI_Chat  # type: ignore
            except ImportError as exc:  # pragma: no cover - dependency gate
                raise RuntimeNotConfiguredError(
                    "Vanna dependency missing. Install `vanna[openai,chromadb]` for text-to-sql."
                ) from exc

        return OpenAI, ChromaDB_VectorStore, OpenAI_Chat

    def _render_training_ddl(self, table: dict[str, Any]) -> str:
        column_defs = ",\n  ".join(f"{column} String" for column in table["columns"])
        return f"CREATE TABLE {table['table']} (\n  {column_defs}\n)"

    def _render_training_documentation(self, table: dict[str, Any]) -> str:
        parts = [
            f"Table: {table['table']}",
            f"Business purpose: {table['business_purpose']}",
            f"Grain: {table['grain']}",
            f"Safe filters: {', '.join(table['safe_filters'])}",
            f"Columns: {', '.join(table['columns'])}",
            f"Background lineage: {table['background_lineage']}",
        ]
        if table.get("dashboard_pages"):
            page_names = ", ".join(page["filename"] for page in table["dashboard_pages"])
            parts.append(f"Dashboard pages: {page_names}")
        if table.get("example_questions"):
            examples = " | ".join(
                f"[{question['lang']}] {question['question']}"
                for question in table["example_questions"]
            )
            parts.append(f"Example questions: {examples}")
        return "\n".join(parts)

    def _create_vanna_client(self) -> Any:
        config = self._resolve_vanna_config()
        OpenAI, ChromaDB_VectorStore, OpenAI_Chat = self._load_vanna_dependencies()
        openai_client = OpenAI(api_key=config["api_key"], base_url=config["base_url"])

        class GroqVanna(ChromaDB_VectorStore, OpenAI_Chat):
            def __init__(self, *, client: Any, runtime_config: dict[str, Any]) -> None:
                ChromaDB_VectorStore.__init__(self, config=runtime_config)
                OpenAI_Chat.__init__(self, client=client, config=runtime_config)

        return GroqVanna(client=openai_client, runtime_config=config)

    def _train_vanna_client(self, client: Any) -> None:
        bundle = build_vanna_catalog_bundle(self.semantic_dir)
        for table in bundle["tables"]:
            client.train(ddl=self._render_training_ddl(table))
            client.train(documentation=self._render_training_documentation(table))
        for question in bundle["question_examples"]:
            client.train(
                documentation=(
                    f"Question example [{question['lang']}] {question['question']} -> "
                    f"tables: {', '.join(question['tables'])}"
                )
            )

    def _get_vanna_client(self) -> Any:
        if self._vanna_client is None:
            self._vanna_client = self._create_vanna_client()
            self._train_vanna_client(self._vanna_client)
        return self._vanna_client

    def metadata_context(self) -> list[dict[str, Any]]:
        return build_vanna_catalog_bundle(self.semantic_dir)["tables"]

    def generate_sql(
        self,
        *,
        question: str,
        lang: str,
        standard: str,
        session_id: str,
    ) -> GeneratedSql:
        _ = {"lang": lang, "standard": standard, "session_id": session_id}
        client = self._get_vanna_client()
        try:
            sql = client.generate_sql(question=question)
        except Exception as exc:
            raise RuntimeGenerationError("Vanna SQL generation failed") from exc
        if not sql:
            raise RuntimeGenerationError("Vanna did not return SQL")
        return GeneratedSql(
            sql=str(sql).strip(),
            explanation="Generated by Vanna OSS using the approved mart-only catalog.",
        )
