"""Unit + integration tests for Telegram alerting client."""

import pytest
import sys
import os
from unittest.mock import MagicMock
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def _make_mock_response(ok=True, message_id=999, extra=None):
    """Return a mock requests.Response with raise_for_status and json()."""
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json = MagicMock(return_value={
        "ok": ok,
        "result": {"message_id": message_id, **(extra or {})}
    })
    return resp


class TestSendMessage:
    """Tests for send_message()."""

    def test_send_message_parses_response_correctly(self, monkeypatch):
        """send_message() returns parsed JSON dict on HTTP 200."""
        mock_resp = _make_mock_response(
            ok=True, message_id=999,
            extra={"chat": {"id": 123456789, "type": "private"}, "text": "test"}
        )
        mock_post = MagicMock(return_value=mock_resp)
        monkeypatch.setattr(requests, "post", mock_post)
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "your_test_bot_token")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "123456789")

        from importlib import reload
        import python_jobs.jobs.alerting.telegram_client as tc
        reload(tc)

        result = tc.send_message("test")

        assert result["ok"] is True
        assert result["result"]["message_id"] == 999
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert "sendMessage" in call_args[0][0]
        assert call_args[1]["json"]["chat_id"] == "123456789"
        assert call_args[1]["json"]["text"] == "test"
        assert call_args[1]["json"]["parse_mode"] == "HTML"

    def test_send_message_uses_html_parse_mode_by_default(self, monkeypatch):
        """send_message() defaults to parse_mode=HTML."""
        mock_resp = _make_mock_response()
        mock_post = MagicMock(return_value=mock_resp)
        monkeypatch.setattr(requests, "post", mock_post)
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test_token")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "123456")

        from importlib import reload
        import python_jobs.jobs.alerting.telegram_client as tc
        reload(tc)

        tc.send_message("hello")
        call_args = mock_post.call_args
        assert call_args[1]["json"]["parse_mode"] == "HTML"

    def test_send_message_supports_markdownv2(self, monkeypatch):
        """send_message(parse_mode='MarkdownV2') overrides default to MarkdownV2."""
        mock_resp = _make_mock_response()
        mock_post = MagicMock(return_value=mock_resp)
        monkeypatch.setattr(requests, "post", mock_post)
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test_token")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "123456")

        from importlib import reload
        import python_jobs.jobs.alerting.telegram_client as tc
        reload(tc)

        tc.send_message("hello", parse_mode="MarkdownV2")
        call_args = mock_post.call_args
        assert call_args[1]["json"]["parse_mode"] == "MarkdownV2"

    def test_send_message_raises_on_http_error(self, monkeypatch):
        """send_message() raises requests.HTTPError on non-200 response."""
        def raise_error(*a, **kw):
            resp = MagicMock()
            resp.raise_for_status.side_effect = requests.HTTPError("403 Forbidden")
            return resp

        monkeypatch.setattr(requests, "post", raise_error)
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "bad_token")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "123456")

        from importlib import reload
        import python_jobs.jobs.alerting.telegram_client as tc
        reload(tc)

        with pytest.raises(requests.HTTPError):
            tc.send_message("test")

    def test_send_message_timeout_is_30_seconds(self, monkeypatch):
        """send_message() sets timeout=30 on the request."""
        mock_resp = _make_mock_response()
        captured_kwargs = {}

        def capture_post(*a, **kw):
            captured_kwargs.update(kw)
            return mock_resp

        monkeypatch.setattr(requests, "post", capture_post)
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test_token")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "123456")

        from importlib import reload
        import python_jobs.jobs.alerting.telegram_client as tc
        reload(tc)

        tc.send_message("test")
        assert captured_kwargs.get("timeout") == 30

    def test_send_message_url_contains_bot_token(self, monkeypatch):
        """send_message() constructs URL with the bot token."""
        mock_resp = _make_mock_response()
        captured_url = []

        def capture_url(url, **kw):
            captured_url.append(url)
            return mock_resp

        monkeypatch.setattr(requests, "post", capture_url)
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "testbot:ABC123xyz")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "123456")

        from importlib import reload
        import python_jobs.jobs.alerting.telegram_client as tc
        reload(tc)

        tc.send_message("test")
        assert "testbot:ABC123xyz" in captured_url[0]
        assert "sendMessage" in captured_url[0]


class TestSendSeverityMessage:
    """Tests for send_severity_message()."""

    def test_critical_prefix(self, monkeypatch):
        """severity='critical' → 🔴 CRITICAL: prefix."""
        captured = {}

        def capture_post(url, **kw):
            captured.update({"url": url, "payload": kw.get("json", {})})
            return _make_mock_response()

        monkeypatch.setattr(requests, "post", capture_post)
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test_token")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "123456")

        from importlib import reload
        import python_jobs.jobs.alerting.telegram_client as tc
        reload(tc)

        tc.send_severity_message("critical", "AQI High", "PM2.5 exceeded threshold")
        assert "🔴 CRITICAL:" in captured["payload"]["text"]
        assert "AQI High" in captured["payload"]["text"]

    def test_warning_prefix(self, monkeypatch):
        """severity='warning' → 🟡 WARNING: prefix."""
        captured = {}

        def capture_post(url, **kw):
            captured.update({"url": url, "payload": kw.get("json", {})})
            return _make_mock_response()

        monkeypatch.setattr(requests, "post", capture_post)
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test_token")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "123456")

        from importlib import reload
        import python_jobs.jobs.alerting.telegram_client as tc
        reload(tc)

        tc.send_severity_message("warning", "Sensor Offline", "No data for 30 min")
        assert "🟡 WARNING:" in captured["payload"]["text"]
        assert "Sensor Offline" in captured["payload"]["text"]

    def test_body_on_newline_after_title(self, monkeypatch):
        """Body appears after a blank line following the title."""
        captured = {}

        def capture_post(url, **kw):
            captured["text"] = kw.get("json", {}).get("text", "")
            return _make_mock_response()

        monkeypatch.setattr(requests, "post", capture_post)
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test_token")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "123456")

        from importlib import reload
        import python_jobs.jobs.alerting.telegram_client as tc
        reload(tc)

        tc.send_severity_message("warning", "Title Line", "Body content here")
        # Title and body separated by \n\n
        assert "\n\n" in captured["text"]
        parts = captured["text"].split("\n\n")
        assert "Title Line" in parts[0]
        assert "Body content here" in parts[1]


@pytest.mark.skipif(
    os.environ.get("SKIP_LIVE_TELEGRAM") != "0",
    reason="SKIP_LIVE_TELEGRAM must be '0' to run live Telegram API tests"
)
class TestLiveTelegram:
    """Live integration tests — calls the real Telegram API.

    Run with:  SKIP_LIVE_TELEGRAM=0 pytest tests/test_telegram_client.py::TestLiveTelegram -v
    """

    def test_send_message_live(self):
        """send_message() delivers a real message to the configured chat."""
        import python_jobs.jobs.alerting.telegram_client as tc
        result = tc.send_message("✅ Live test passed — telegram_client delivery verified")
        assert result["ok"] is True
        assert "message_id" in result["result"]

    def test_severity_message_live(self):
        """send_severity_message() delivers a real severity message."""
        import python_jobs.jobs.alerting.telegram_client as tc
        result = tc.send_severity_message(
            "warning",
            "Live Integration Test",
            "This message confirms real Telegram delivery is working."
        )
        assert result["ok"] is True
