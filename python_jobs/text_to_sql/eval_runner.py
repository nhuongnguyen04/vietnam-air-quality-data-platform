"""Evaluate Vanna SQL generation against the bilingual Ask Data corpus."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

import yaml

try:
    from python_jobs.text_to_sql.sql_validator import ValidationResult, validate_sql
except ModuleNotFoundError:  # pragma: no cover - container import fallback
    from sql_validator import ValidationResult, validate_sql  # type: ignore


DEFAULT_EVAL_PATH = Path(__file__).resolve().parent / "evals" / "ask_data_eval_cases.yml"


class EvalValidationError(ValueError):
    """Raised when generated SQL misses the corpus expectations for a matched eval case."""


@dataclass(frozen=True)
class EvalCase:
    id: str
    lang: str
    question: str
    expected_tables: list[str]
    expected_sql_shape: list[str]
    forbidden_targets: list[str]
    expected_business_intent: str


@dataclass(frozen=True)
class EvalResult:
    case_id: str
    referenced_tables: list[str]
    matched_shapes: list[str]


def get_eval_path(eval_path: str | Path | None = None) -> Path:
    return Path(eval_path) if eval_path else DEFAULT_EVAL_PATH


def _normalize_question(question: str) -> str:
    return re.sub(r"\s+", " ", question).strip().lower()


def load_eval_cases(eval_path: str | Path | None = None) -> list[EvalCase]:
    payload = yaml.safe_load(get_eval_path(eval_path).read_text(encoding="utf-8")) or {}
    cases = []
    for case in payload.get("cases", []):
        cases.append(
            EvalCase(
                id=str(case["id"]),
                lang=str(case["lang"]),
                question=str(case["question"]),
                expected_tables=list(case["expected_tables"]),
                expected_sql_shape=list(case["expected_sql_shape"]),
                forbidden_targets=list(case["forbidden_targets"]),
                expected_business_intent=str(case["expected_business_intent"]),
            )
        )
    return cases


def find_matching_eval_case(
    *,
    question: str,
    lang: str,
    eval_path: str | Path | None = None,
) -> EvalCase | None:
    normalized_question = _normalize_question(question)
    for case in load_eval_cases(eval_path):
        if case.lang == lang and _normalize_question(case.question) == normalized_question:
            return case
    return None


def _matches_shape(shape: str, sql: str, referenced_tables: list[str]) -> bool:
    lowered_sql = sql.lower()
    referenced_tables_set = set(referenced_tables)

    if shape == "top_n":
        return bool(re.search(r"order\s+by[\s\S]+limit\s+\d+", lowered_sql))
    if shape == "recent_24_hours":
        return (
            "dm_aqi_current_status" in referenced_tables_set
            or bool(re.search(r"\b24\b", lowered_sql)) and ("hour" in lowered_sql or "day" in lowered_sql)
        )
    if shape == "order_by_desc":
        return bool(re.search(r"order\s+by[\s\S]+desc", lowered_sql))
    if shape == "daily_compliance_filter":
        return "dm_aqi_compliance_standards" in referenced_tables_set and bool(
            re.search(r"\bwhere\b", lowered_sql)
        )
    if shape == "who_threshold":
        return "dm_aqi_compliance_standards" in referenced_tables_set and "who" in lowered_sql
    if shape == "recent_7_days":
        return "dm_aqi_compliance_standards" in referenced_tables_set and (
            bool(re.search(r"\b7\b", lowered_sql)) or "week" in lowered_sql
        )
    if shape == "correlation_or_join":
        return (
            any("correlation" in table for table in referenced_tables_set)
            or " join " in f" {lowered_sql} "
            or "corr(" in lowered_sql
        )
    if shape == "province_grouping":
        return bool(
            re.search(r"group\s+by\s+(?:[a-z0-9_]+\.)?province\b", lowered_sql)
            or re.search(r"select[\s\S]+(?:^|[\s,(])(?:[a-z0-9_]+\.)?province\b", lowered_sql)
        )
    if shape == "traffic_vs_pm25":
        return "pm25" in lowered_sql and ("traffic" in lowered_sql or "congestion" in lowered_sql)
    if shape == "monthly_filter":
        return (
            "month" in lowered_sql
            or "addmonths" in lowered_sql
            or "interval 1 month" in lowered_sql
            or "tostartofmonth" in lowered_sql
        )
    if shape == "ranking":
        return bool(
            re.search(r"order\s+by", lowered_sql)
            or "rank(" in lowered_sql
            or "dense_rank" in lowered_sql
        )
    if shape == "recent_30_days":
        return bool(re.search(r"\b30\b", lowered_sql)) and "day" in lowered_sql
    if shape == "weather_vs_pm25":
        return "pm25" in lowered_sql and (
            ("humidity" in lowered_sql and "wind" in lowered_sql)
            or any("weather" in table for table in referenced_tables_set)
        )
    return True


def evaluate_sql_against_case(
    sql: str,
    case: EvalCase,
    *,
    semantic_dir: str | None = None,
) -> EvalResult:
    validation: ValidationResult = validate_sql(sql, semantic_dir)
    referenced_tables_set = set(validation.referenced_tables)
    matched_expected_tables = sorted(referenced_tables_set.intersection(case.expected_tables))
    if not matched_expected_tables:
        raise EvalValidationError(
            f"Eval case {case.id} did not use any expected tables: {', '.join(case.expected_tables)}"
        )

    lowered_sql = validation.sql.lower()
    hit_forbidden_targets = [
        target
        for target in case.forbidden_targets
        if target.lower() in lowered_sql
    ]
    if hit_forbidden_targets:
        raise EvalValidationError(
            f"Eval case {case.id} touched forbidden targets: {', '.join(hit_forbidden_targets)}"
        )

    missing_shapes = [
        shape
        for shape in case.expected_sql_shape
        if not _matches_shape(shape, validation.sql, validation.referenced_tables)
    ]
    if missing_shapes:
        raise EvalValidationError(
            f"Eval case {case.id} is missing expected SQL shapes: {', '.join(missing_shapes)}"
        )

    return EvalResult(
        case_id=case.id,
        referenced_tables=validation.referenced_tables,
        matched_shapes=list(case.expected_sql_shape),
    )
