"""Single integration boundary for SQL generation without execution."""
from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any

from python_jobs.text_to_sql.semantic_loader import build_table_prompt_context


class RuntimeNotConfiguredError(RuntimeError):
    """Raised when the runtime wrapper has no configured provider."""


@dataclass(frozen=True)
class GeneratedSql:
    sql: str
    explanation: str


class VannaRuntime:
    """Provider wrapper that exposes SQL generation only."""

    def __init__(self, semantic_dir: str | None = None) -> None:
        self.semantic_dir = semantic_dir

    def generate_sql(
        self,
        *,
        question: str,
        lang: str,
        standard: str,
        session_id: str,
    ) -> GeneratedSql:
        _ = {
            "question": question,
            "lang": lang,
            "standard": standard,
            "session_id": session_id,
            "semantic_context": self.metadata_context(),
        }

        if not (os.environ.get("OPENAI_API_KEY") or os.environ.get("VANNA_API_KEY")):
            raise RuntimeNotConfiguredError(
                "No cloud model credentials configured for text-to-SQL generation"
            )

        raise RuntimeNotConfiguredError(
            "Provider-specific SQL generation is not wired yet; keep usage behind this wrapper"
        )

    def metadata_context(self) -> list[dict[str, Any]]:
        return build_table_prompt_context(self.semantic_dir)
