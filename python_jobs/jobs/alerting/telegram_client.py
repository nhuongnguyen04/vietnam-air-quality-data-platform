"""Telegram client for sending air quality alerts and reports."""
import os
import requests
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_API = "https://api.telegram.org"


def _get_credentials() -> tuple[str, str]:
    """Return the AQ Telegram bot credentials used by reports and AQ alerts."""
    bot_token = os.getenv("TELEGRAM_AQ_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_AQ_CHAT_ID")

    if not bot_token or not chat_id:
        raise RuntimeError(
            "TELEGRAM_AQ_BOT_TOKEN and TELEGRAM_AQ_CHAT_ID environment variables are required"
        )

    return bot_token, chat_id


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
    bot_token, chat_id = _get_credentials()
    url = f"{TELEGRAM_API}/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
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
