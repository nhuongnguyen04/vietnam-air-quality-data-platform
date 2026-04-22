"""Live Telegram delivery checks."""

from __future__ import annotations

import os

import pytest

from python_jobs.jobs.alerting.telegram_client import send_message, send_severity_message


pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("RUN_LIVE_TESTS") != "1",
        reason="RUN_LIVE_TESTS=1 is required for live tests",
    ),
    pytest.mark.skipif(
        not os.environ.get("TELEGRAM_BOT_TOKEN") or not os.environ.get("TELEGRAM_CHAT_ID"),
        reason="Telegram credentials are required for live tests",
    ),
]


def test_send_message_live() -> None:
    result = send_message("Live test: telegram_client delivery verified")

    assert result["ok"] is True
    assert "message_id" in result["result"]


def test_send_severity_message_live() -> None:
    result = send_severity_message(
        "warning",
        "Live Integration Test",
        "This message confirms Telegram delivery is working.",
    )

    assert result["ok"] is True
    assert "message_id" in result["result"]
