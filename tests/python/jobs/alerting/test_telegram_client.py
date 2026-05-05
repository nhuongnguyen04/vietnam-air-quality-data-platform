"""Unit tests for the Telegram alerting client."""

from __future__ import annotations

from importlib import reload
from unittest.mock import MagicMock

import pytest
import requests

import python_jobs.jobs.alerting.telegram_client as telegram_client


def _make_mock_response(ok: bool = True, message_id: int = 999, extra: dict | None = None) -> MagicMock:
    response = MagicMock()
    response.raise_for_status = MagicMock()
    response.json = MagicMock(
        return_value={"ok": ok, "result": {"message_id": message_id, **(extra or {})}}
    )
    return response


@pytest.mark.unit
def test_send_message_posts_expected_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_post = MagicMock(return_value=_make_mock_response())
    monkeypatch.setattr(requests, "post", mock_post)
    monkeypatch.setenv("TELEGRAM_AQ_BOT_TOKEN", "test_token")
    monkeypatch.setenv("TELEGRAM_AQ_CHAT_ID", "123456")

    module = reload(telegram_client)
    result = module.send_message("hello")

    assert result["ok"] is True
    call = mock_post.call_args
    assert call.args[0] == "https://api.telegram.org/bottest_token/sendMessage"
    assert call.kwargs["json"] == {
        "chat_id": "123456",
        "text": "hello",
        "parse_mode": "HTML",
    }
    assert call.kwargs["timeout"] == 30


@pytest.mark.unit
def test_send_message_raises_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    response = MagicMock()
    response.raise_for_status.side_effect = requests.HTTPError("403 Forbidden")
    monkeypatch.setattr(requests, "post", MagicMock(return_value=response))
    monkeypatch.setenv("TELEGRAM_AQ_BOT_TOKEN", "bad_token")
    monkeypatch.setenv("TELEGRAM_AQ_CHAT_ID", "123456")

    module = reload(telegram_client)

    with pytest.raises(requests.HTTPError):
        module.send_message("fail")


@pytest.mark.unit
def test_send_message_requires_aq_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TELEGRAM_AQ_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_AQ_CHAT_ID", raising=False)

    module = reload(telegram_client)

    with pytest.raises(RuntimeError, match="TELEGRAM_AQ_BOT_TOKEN and TELEGRAM_AQ_CHAT_ID"):
        module.send_message("fail")


@pytest.mark.unit
def test_send_severity_message_formats_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_send = MagicMock(return_value={"ok": True})
    module = reload(telegram_client)
    monkeypatch.setattr(module, "send_message", mock_send)

    result = module.send_severity_message("critical", "AQI High", "PM2.5 exceeded threshold")

    assert result == {"ok": True}
    mock_send.assert_called_once_with("🔴 CRITICAL: AQI High\n\nPM2.5 exceeded threshold")
