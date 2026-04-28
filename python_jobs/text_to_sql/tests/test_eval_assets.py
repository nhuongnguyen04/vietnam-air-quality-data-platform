from __future__ import annotations

from pathlib import Path

import yaml

from python_jobs.text_to_sql.semantic_loader import load_allowed_tables


def test_eval_assets_are_bilingual(semantic_dir, repo_root):
    eval_path = repo_root / "python_jobs" / "text_to_sql" / "evals" / "ask_data_eval_cases.yml"
    payload = yaml.safe_load(eval_path.read_text(encoding="utf-8"))

    languages = {case["lang"] for case in payload["cases"]}

    assert payload["version"] == 1
    assert languages == {"en", "vi"}


def test_eval_assets_reference_allowlisted_tables(semantic_dir, repo_root):
    eval_path = repo_root / "python_jobs" / "text_to_sql" / "evals" / "ask_data_eval_cases.yml"
    payload = yaml.safe_load(eval_path.read_text(encoding="utf-8"))
    allowed_tables = load_allowed_tables(semantic_dir)

    for case in payload["cases"]:
        assert case["expected_tables"]
        assert set(case["expected_tables"]).issubset(allowed_tables)
        assert case["forbidden_targets"]
        assert case["expected_sql_shape"]
        assert case["expected_business_intent"]
