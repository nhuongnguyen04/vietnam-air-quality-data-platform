"""Regex-based SQL extraction logic from LLM response."""
from __future__ import annotations

import re


def _starts_like_sql(text: str) -> bool:
    stripped = text.strip()
    if re.match(r"^select\b", stripped, flags=re.IGNORECASE):
        return True
    return bool(
        re.match(
            r"^with\s+(?:recursive\s+)?[a-z_][a-z0-9_]*\s+as\s*\(",
            stripped,
            flags=re.IGNORECASE,
        )
    )


def _trim_sql_candidate(candidate: str) -> str:
    cleaned = candidate.strip()
    if not _starts_like_sql(cleaned):
        return ""
    if ";" in cleaned:
        cleaned = cleaned.split(";", 1)[0]
    return cleaned.strip()


def extract_sql_statement(response: object) -> str:
    text = str(response).strip()
    if not text:
        return ""

    fenced_blocks = re.findall(
        r"```(?:sql)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL
    )
    for block in reversed(fenced_blocks):
        candidate = _trim_sql_candidate(block)
        if candidate:
            return candidate

    without_thinking = re.sub(
        r"<think>.*?</think>",
        "",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    lines = without_thinking.splitlines()
    for index in range(len(lines) - 1, -1, -1):
        if _starts_like_sql(lines[index]):
            candidate = _trim_sql_candidate("\n".join(lines[index:]))
            if candidate:
                return candidate

    return _trim_sql_candidate(without_thinking)
