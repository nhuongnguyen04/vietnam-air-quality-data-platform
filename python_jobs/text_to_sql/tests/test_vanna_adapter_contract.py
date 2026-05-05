from __future__ import annotations

from python_jobs.text_to_sql.vanna_runtime import GeneratedSql, VannaRuntime


def test_runtime_reuses_cached_vanna_client(monkeypatch, semantic_dir, tmp_path):
    class FakeVanna:
        def __init__(self):
            self.generate_calls = 0
            self.train_calls = 0

        def train(self, **kwargs):
            self.train_calls += 1
            return "trained"

        def generate_sql(self, *, question):
            self.generate_calls += 1
            return "SELECT * FROM dm_aqi_current_status LIMIT 10"

        def get_related_ddl(self, question, **kwargs):
            return []

        def get_related_documentation(self, question, **kwargs):
            return []

    fake = FakeVanna()
    monkeypatch.setenv("GROQ_API_KEY", "test-groq-key")
    monkeypatch.setenv("TEXT_TO_SQL_VANNA_PERSIST_DIRECTORY", str(tmp_path / "vanna"))
    monkeypatch.setattr(VannaRuntime, "_create_vanna_client", lambda self: fake)

    runtime = VannaRuntime(str(semantic_dir))
    first = runtime.generate_sql(
        question="q1",
        lang="vi",
        standard="TCVN",
        session_id="1",
    )
    second = runtime.generate_sql(
        question="q2",
        lang="vi",
        standard="TCVN",
        session_id="2",
    )

    assert isinstance(first, GeneratedSql)
    assert isinstance(second, GeneratedSql)
    assert fake.generate_calls == 2
    assert fake.train_calls > 0
    assert first.referenced_tables == ["dm_aqi_current_status"]
    assert "collection" in first.generator_metadata


def test_runtime_metadata_context_uses_catalog_builder(semantic_dir):
    runtime = VannaRuntime(str(semantic_dir))
    context = runtime.metadata_context()

    assert context
    assert all(entry["table"].startswith(("dm_", "fct_")) for entry in context)
