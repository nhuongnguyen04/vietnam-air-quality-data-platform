"""Telegram client for sending air quality alerts and reports."""
import os
import requests

TELEGRAM_API = "https://api.telegram.org"
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]


def send_message(text: str, parse_mode: str = "HTML") -> dict:
    """Send a message to the configured Telegram chat.

    Args:
        text: Message body. Supports HTML formatting when parse_mode="HTML".
        parse_mode: "HTML" or "MarkdownV2". Defaults to "HTML".

    Returns:
        Telegram API response dict.

    Raises:
        requests.HTTPError: If the bot token or chat ID is invalid.
    """
    url = f"{TELEGRAM_API}/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": parse_mode}
    resp = requests.post(url, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def send_severity_message(severity: str, title: str, body: str) -> dict:
    """Send an alert with severity emoji prefix.

    Args:
        severity: "critical" → 🔴 CRITICAL: prefix
                  "warning" → 🟡 WARNING: prefix
        title: Alert title line (appears after prefix)
        body: Alert description (appears after blank line)

    Returns:
        Telegram API response dict.
    """
    prefix = "🔴 CRITICAL:" if severity == "critical" else "🟡 WARNING:"
    text = f"{prefix} {title}\n\n{body}"
    return send_message(text)
