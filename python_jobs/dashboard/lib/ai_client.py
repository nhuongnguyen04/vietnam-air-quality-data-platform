"""Thin wrapper around the ckey.vn OpenAI-compatible API."""
import os
import logging
from openai import OpenAI

logger = logging.getLogger(__name__)
_client: OpenAI | None = None

def get_ai_client() -> OpenAI | None:
    """Lazy-init singleton. Returns None if CKEY_API_KEY not set."""
    global _client
    if _client is not None:
        return _client
    api_key = os.environ.get("CKEY_API_KEY")
    if not api_key:
        logger.warning("CKEY_API_KEY not set — AI analysis disabled")
        return None
    _client = OpenAI(
        api_key=api_key,
        base_url=os.environ.get("CKEY_BASE_URL", "https://api.xah.io/v1"),
    )
    return _client

def get_analysis_model() -> str:
    return os.environ.get("CKEY_MODEL_ANALYSIS", "gpt-5.5")
