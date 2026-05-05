from __future__ import annotations

import json

import pytest

from python_jobs.text_to_sql.vanna_runtime import (
    RuntimeGenerationError,
    RuntimeNotConfiguredError,
    VannaRuntime,
)


class FakeVanna:
    def __init__(self):
        self.training_calls = []
        self.questions = []

    def train(self, **kwargs):
        self.training_calls.append(kwargs)
        return "trained"

    def generate_sql(self, *, question):
        self.questions.append(question)
        return (
            "SELECT province, current_aqi_vn "
            "FROM dm_aqi_current_status "
            "ORDER BY current_aqi_vn DESC "
            "LIMIT 100"
        )

    def get_related_ddl(self, question, **kwargs):
        return []

    def get_related_documentation(self, question, **kwargs):
        return []


@pytest.mark.unit
def test_runtime_requires_groq_key_for_vanna(monkeypatch, semantic_dir):
    monkeypatch.delenv("GROQ_API_KEY", raising=False)

    runtime = VannaRuntime(str(semantic_dir))

    with pytest.raises(RuntimeNotConfiguredError):
        runtime.generate_sql(
            question="Which provinces had the worst AQI?",
            lang="en",
            standard="TCVN",
            session_id="session-1",
        )


@pytest.mark.unit
def test_runtime_defaults_to_persistent_chromadb(monkeypatch, semantic_dir):
    monkeypatch.setenv("GROQ_API_KEY", "test-groq-key")
    monkeypatch.delenv("TEXT_TO_SQL_VANNA_CLIENT", raising=False)
    monkeypatch.delenv("TEXT_TO_SQL_VANNA_PERSIST_DIRECTORY", raising=False)

    runtime = VannaRuntime(str(semantic_dir))
    config = runtime._resolve_vanna_config()

    assert config.client == "chromadb"
    assert config.persist_directory == "/data/vanna"
    assert config.rebuild is False


@pytest.mark.unit
def test_runtime_uses_vanna_with_groq_backing(monkeypatch, semantic_dir, tmp_path):
    fake_vanna = FakeVanna()
    monkeypatch.setenv("GROQ_API_KEY", "test-groq-key")
    monkeypatch.setenv("GROQ_MODEL", "qwen/qwen3-32b")
    monkeypatch.setenv("TEXT_TO_SQL_VANNA_PERSIST_DIRECTORY", str(tmp_path / "vanna"))
    monkeypatch.setattr(
        VannaRuntime,
        "_create_vanna_client",
        lambda self: fake_vanna,
    )

    runtime = VannaRuntime(str(semantic_dir))
    result = runtime.generate_sql(
        question="Tinh nao co AQI cao nhat trong 24 gio qua?",
        lang="vi",
        standard="TCVN",
        session_id="session-2",
    )

    assert fake_vanna.questions == ["Tinh nao co AQI cao nhat trong 24 gio qua?"]
    assert any("ddl" in call for call in fake_vanna.training_calls)
    assert any("documentation" in call for call in fake_vanna.training_calls)
    assert result.sql.startswith("SELECT")
    assert "province" in result.sql
    assert result.referenced_tables == ["dm_aqi_current_status"]
    assert result.explanation.startswith("Mapped the request to approved marts/facts:")
    assert result.generator_metadata["model"] == "qwen/qwen3-32b"
    assert result.generator_metadata["collection"].startswith("air_quality_ask_data__")


@pytest.mark.unit
def test_runtime_reuses_manifest_without_retraining(monkeypatch, semantic_dir, tmp_path):
    persist_directory = tmp_path / "vanna"
    monkeypatch.setenv("GROQ_API_KEY", "test-groq-key")
    monkeypatch.setenv("TEXT_TO_SQL_VANNA_PERSIST_DIRECTORY", str(persist_directory))

    first_client = FakeVanna()
    monkeypatch.setattr(VannaRuntime, "_create_vanna_client", lambda self: first_client)
    first_runtime = VannaRuntime(str(semantic_dir))
    first_result = first_runtime.generate_sql(
        question="AQI cao nhat?",
        lang="vi",
        standard="TCVN",
        session_id="session-manifest-1",
    )
    assert first_client.training_calls

    second_client = FakeVanna()
    monkeypatch.setattr(VannaRuntime, "_create_vanna_client", lambda self: second_client)
    second_runtime = VannaRuntime(str(semantic_dir))
    second_result = second_runtime.generate_sql(
        question="AQI cao nhat?",
        lang="vi",
        standard="TCVN",
        session_id="session-manifest-2",
    )

    assert second_client.training_calls == []
    assert first_result.generator_metadata["collection"] == second_result.generator_metadata["collection"]


@pytest.mark.unit
def test_runtime_retrains_when_semantic_fingerprint_changes(monkeypatch, temp_semantic_dir, tmp_path):
    persist_directory = tmp_path / "vanna"
    monkeypatch.setenv("GROQ_API_KEY", "test-groq-key")
    monkeypatch.setenv("TEXT_TO_SQL_VANNA_PERSIST_DIRECTORY", str(persist_directory))

    first_client = FakeVanna()
    monkeypatch.setattr(VannaRuntime, "_create_vanna_client", lambda self: first_client)
    first_runtime = VannaRuntime(str(temp_semantic_dir))
    first_result = first_runtime.generate_sql(
        question="Tinh nao co AQI cao nhat?",
        lang="vi",
        standard="TCVN",
        session_id="session-fingerprint-1",
    )

    questions_path = temp_semantic_dir / "example_questions.yml"
    payload = json.loads(json.dumps({"noop": 1}))
    del payload["noop"]
    payload = {
        "version": 1,
        "questions": [
            {
                "id": "vi-1",
                "lang": "vi",
                "topic": "aqi_status",
                "question": "Tinh nao co AQI cao nhat?",
                "tables": ["dm_aqi_current_status"],
            },
            {
                "id": "en-1",
                "lang": "en",
                "topic": "aqi_status",
                "question": "Which province has the highest AQI?",
                "tables": ["dm_aqi_current_status"],
            },
            {
                "id": "en-2",
                "lang": "en",
                "topic": "aqi_status",
                "question": "Show current AQI by province",
                "tables": ["dm_aqi_current_status"],
            },
        ],
    }
    questions_path.write_text(json.dumps(payload), encoding="utf-8")

    second_client = FakeVanna()
    monkeypatch.setattr(VannaRuntime, "_create_vanna_client", lambda self: second_client)
    second_runtime = VannaRuntime(str(temp_semantic_dir))
    second_result = second_runtime.generate_sql(
        question="Tinh nao co AQI cao nhat?",
        lang="vi",
        standard="TCVN",
        session_id="session-fingerprint-2",
    )

    assert second_client.training_calls
    assert (
        first_result.generator_metadata["semantic_fingerprint"]
        != second_result.generator_metadata["semantic_fingerprint"]
    )
    assert first_result.generator_metadata["collection"] != second_result.generator_metadata["collection"]


@pytest.mark.unit
def test_runtime_extracts_sql_from_reasoning_response(monkeypatch, semantic_dir, tmp_path):
    class ChattyVanna(FakeVanna):
        def generate_sql(self, *, question):
            self.questions.append(question)
            return """
            with the available context, I should use the hourly table.

            SELECT province, MAX(current_aqi_vn) AS max_aqi
            FROM dm_aqi_current_status
            ORDER BY max_aqi DESC
            LIMIT 1;
            """

    fake_vanna = ChattyVanna()
    monkeypatch.setenv("GROQ_API_KEY", "test-groq-key")
    monkeypatch.setenv("TEXT_TO_SQL_VANNA_PERSIST_DIRECTORY", str(tmp_path / "vanna"))
    monkeypatch.setattr(
        VannaRuntime,
        "_create_vanna_client",
        lambda self: fake_vanna,
    )

    runtime = VannaRuntime(str(semantic_dir))
    result = runtime.generate_sql(
        question="AQI cao nhat?",
        lang="vi",
        standard="TCVN",
        session_id="session-2",
    )

    assert result.sql.startswith("SELECT")
    assert "province" in result.sql
    assert "with the available context" not in result.sql
    assert "LIMIT 1" in result.sql


@pytest.mark.unit
def test_runtime_extracts_sql_from_markdown_fence(monkeypatch, semantic_dir, tmp_path):
    class FencedVanna(FakeVanna):
        def generate_sql(self, *, question):
            self.questions.append(question)
            return """
            <think>I should not leak this reasoning.</think>

            ```sql
            SELECT province, current_aqi_vn
            FROM dm_aqi_current_status
            LIMIT 10;
            ```
            """

    fake_vanna = FencedVanna()
    monkeypatch.setenv("GROQ_API_KEY", "test-groq-key")
    monkeypatch.setenv("TEXT_TO_SQL_VANNA_PERSIST_DIRECTORY", str(tmp_path / "vanna"))
    monkeypatch.setattr(
        VannaRuntime,
        "_create_vanna_client",
        lambda self: fake_vanna,
    )

    runtime = VannaRuntime(str(semantic_dir))
    result = runtime.generate_sql(
        question="AQI cao nhat?",
        lang="vi",
        standard="TCVN",
        session_id="session-4",
    )

    assert result.sql.startswith("SELECT")
    assert "province" in result.sql
    assert "<think>" not in result.sql


@pytest.mark.unit
def test_runtime_rejects_sql_that_misses_eval_shape(monkeypatch, semantic_dir, tmp_path):
    class WrongShapeVanna(FakeVanna):
        def generate_sql(self, *, question):
            self.questions.append(question)
            return """
            SELECT province, MAX(avg_aqi_vn) AS max_aqi
            FROM dm_air_quality_overview_hourly
            WHERE datetime_hour >= now() - INTERVAL 24 HOUR
            GROUP BY province
            """

    fake_vanna = WrongShapeVanna()
    monkeypatch.setenv("GROQ_API_KEY", "test-groq-key")
    monkeypatch.setenv("TEXT_TO_SQL_VANNA_PERSIST_DIRECTORY", str(tmp_path / "vanna"))
    monkeypatch.setattr(VannaRuntime, "_create_vanna_client", lambda self: fake_vanna)

    runtime = VannaRuntime(str(semantic_dir))
    with pytest.raises(RuntimeGenerationError, match="expected SQL shapes"):
        runtime.generate_sql(
            question="Tinh nao co AQI cao nhat trong 24 gio qua?",
            lang="vi",
            standard="TCVN",
            session_id="session-shape-1",
        )


@pytest.mark.unit
def test_runtime_wraps_vanna_generation_errors(monkeypatch, semantic_dir, tmp_path):
    class BrokenVanna:
        def train(self, **kwargs):
            return "trained"

        def generate_sql(self, *, question):
            raise RuntimeError("boom")

        def get_related_ddl(self, question, **kwargs):
            return []

        def get_related_documentation(self, question, **kwargs):
            return []

    monkeypatch.setenv("GROQ_API_KEY", "test-groq-key")
    monkeypatch.setenv("TEXT_TO_SQL_VANNA_PERSIST_DIRECTORY", str(tmp_path / "vanna"))
    monkeypatch.setattr(
        VannaRuntime,
        "_create_vanna_client",
        lambda self: BrokenVanna(),
    )

    runtime = VannaRuntime(str(semantic_dir))

    with pytest.raises(RuntimeGenerationError, match="Vanna SQL generation failed"):
        runtime.generate_sql(
            question="AQI cao nhat?",
            lang="vi",
            standard="TCVN",
            session_id="session-3",
        )
