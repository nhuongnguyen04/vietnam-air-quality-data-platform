from __future__ import annotations

import os

import pytest

from python_jobs.text_to_sql.vanna_runtime import VannaRuntime


@pytest.mark.integration
def test_live_vanna_runtime_generates_allowlisted_sql(monkeypatch, semantic_dir, tmp_path):
    if os.environ.get("RUN_LIVE_VANNA_TESTS") != "1":
        pytest.skip("Set RUN_LIVE_VANNA_TESTS=1 to enable live Vanna runtime checks")
    if not os.environ.get("GROQ_API_KEY"):
        pytest.skip("GROQ_API_KEY is required for live Vanna runtime checks")

    monkeypatch.setenv("TEXT_TO_SQL_VANNA_PERSIST_DIRECTORY", str(tmp_path / "vanna"))
    runtime = VannaRuntime(str(semantic_dir))
    result = runtime.generate_sql(
        question="Which provinces had the worst AQI in the last 24 hours?",
        lang="en",
        standard="TCVN",
        session_id="live-vanna-integration",
    )

    assert result.sql.startswith(("SELECT", "WITH"))
    assert result.referenced_tables
    assert result.generator_metadata["collection"]
