"""Dashboard-side client for the internal text-to-SQL service."""
from __future__ import annotations

import json
import os
import socket
from typing import Any
from urllib import error, request


class TextToSqlClientError(RuntimeError):
    """Raised when the internal text-to-SQL service request fails."""


class TextToSqlClient:
    def __init__(
        self,
        base_url: str | None = None,
        timeout_seconds: int | None = None,
    ) -> None:
        self.base_url = (
            base_url or os.environ.get("TEXT_TO_SQL_URL", "http://localhost:8000")
        ).rstrip("/")
        self.timeout_seconds = timeout_seconds or self._resolve_timeout_seconds()

    def _resolve_timeout_seconds(self) -> int:
        configured_timeout = os.environ.get("TEXT_TO_SQL_TIMEOUT_SECONDS", "90")
        try:
            return int(configured_timeout)
        except ValueError:
            return 90

    def _format_http_error(self, exc: error.HTTPError) -> str:
        detail = exc.read().decode("utf-8")
        try:
            parsed = json.loads(detail)
        except json.JSONDecodeError:
            return detail or f"Service request failed with HTTP {exc.code}"
        if isinstance(parsed, dict) and parsed.get("detail"):
            return str(parsed["detail"])
        return detail or f"Service request failed with HTTP {exc.code}"

    def _format_timeout_error(self) -> str:
        return (
            f"Text-to-SQL service timed out after {self.timeout_seconds}s. "
            "Try again, or increase TEXT_TO_SQL_TIMEOUT_SECONDS if the first "
            "Vanna/Groq request is still warming up."
        )

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
            raise TextToSqlClientError(self._format_http_error(exc)) from exc
        except (TimeoutError, socket.timeout) as exc:
            raise TextToSqlClientError(self._format_timeout_error()) from exc
        except error.URLError as exc:
            if isinstance(exc.reason, (TimeoutError, socket.timeout)):
                raise TextToSqlClientError(self._format_timeout_error()) from exc
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
