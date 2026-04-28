"""Dashboard-side client for the internal text-to-SQL service."""
from __future__ import annotations

import json
import os
from typing import Any
from urllib import error, request


class TextToSqlClientError(RuntimeError):
    """Raised when the internal text-to-SQL service request fails."""


class TextToSqlClient:
    def __init__(self, base_url: str | None = None, timeout_seconds: int = 30) -> None:
        self.base_url = (base_url or os.environ.get("TEXT_TO_SQL_URL", "http://localhost:8000")).rstrip("/")
        self.timeout_seconds = timeout_seconds

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(
            url=f"{self.base_url}{path}",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8")
            raise TextToSqlClientError(detail or f"Service request failed with HTTP {exc.code}") from exc
        except error.URLError as exc:
            raise TextToSqlClientError(
                f"Service unavailable at {self.base_url}: {exc.reason}. "
                "Start or rebuild the text-to-sql service and verify TEXT_TO_SQL_URL."
            ) from exc

    def preview(
        self,
        *,
        question: str,
        lang: str,
        standard: str,
        session_id: str,
    ) -> dict[str, Any]:
        return self._post(
            "/ask",
            {
                "question": question,
                "lang": lang,
                "standard": standard,
                "session_id": session_id,
            },
        )

    def execute(self, *, sql: str, preview_token: str) -> dict[str, Any]:
        return self._post(
            "/execute",
            {
                "sql": sql,
                "preview_token": preview_token,
            },
        )
