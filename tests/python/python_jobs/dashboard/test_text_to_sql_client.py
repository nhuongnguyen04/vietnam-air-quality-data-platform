from __future__ import annotations

import io
from urllib import error

import pytest

from python_jobs.dashboard.lib.text_to_sql_client import (
    TextToSqlClient,
    TextToSqlClientError,
)


def test_timeout_is_wrapped_as_client_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def raise_timeout(*_args, **_kwargs):
        raise TimeoutError("timed out")

    monkeypatch.setattr(
        "python_jobs.dashboard.lib.text_to_sql_client.request.urlopen",
        raise_timeout,
    )

    client = TextToSqlClient(base_url="http://text-to-sql:8000", timeout_seconds=5)

    with pytest.raises(TextToSqlClientError, match="timed out after 5s"):
        client.preview(
            question="Tinh nao co AQI cao nhat?",
            lang="vi",
            standard="TCVN",
            session_id="test-session",
        )


def test_urlerror_timeout_is_wrapped_as_client_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def raise_urlerror_timeout(*_args, **_kwargs):
        raise error.URLError(TimeoutError("timed out"))

    monkeypatch.setattr(
        "python_jobs.dashboard.lib.text_to_sql_client.request.urlopen",
        raise_urlerror_timeout,
    )

    client = TextToSqlClient(base_url="http://text-to-sql:8000", timeout_seconds=5)

    with pytest.raises(TextToSqlClientError, match="timed out after 5s"):
        client.preview(
            question="Tinh nao co AQI cao nhat?",
            lang="vi",
            standard="TCVN",
            session_id="test-session",
        )


def test_http_error_detail_is_unwrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    def raise_http_error(*_args, **_kwargs):
        raise error.HTTPError(
            url="http://text-to-sql:8000/ask",
            code=503,
            msg="Service Unavailable",
            hdrs=None,
            fp=io.BytesIO(b'{"detail":"GROQ_API_KEY is required"}'),
        )

    monkeypatch.setattr(
        "python_jobs.dashboard.lib.text_to_sql_client.request.urlopen",
        raise_http_error,
    )

    client = TextToSqlClient(base_url="http://text-to-sql:8000", timeout_seconds=5)

    with pytest.raises(TextToSqlClientError, match="GROQ_API_KEY is required"):
        client.preview(
            question="Tinh nao co AQI cao nhat?",
            lang="vi",
            standard="TCVN",
            session_id="test-session",
        )
