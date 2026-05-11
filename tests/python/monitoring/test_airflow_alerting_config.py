"""File-based checks for Airflow failure alerting via Prometheus/Grafana."""

from __future__ import annotations

from pathlib import Path

import pytest

POSTGRES_QUERIES = Path("monitoring/prometheus/queries-postgresql.yaml")
PROMETHEUS_RULES = Path("monitoring/prometheus/rules/alerts.yml")
GRAFANA_RULES = Path("monitoring/grafana/provisioning/alerting/system-prometheus-rules.yml")
CONTACT_POINTS = Path("monitoring/grafana/provisioning/alerting/contact-points.yml")


@pytest.mark.integration
def test_postgres_exporter_queries_use_supported_mapping_format() -> None:
    content = POSTGRES_QUERIES.read_text(encoding="utf-8")

    assert not content.startswith("queries:")
    assert "airflow_dag_run:" in content
    assert "airflow_dag_run_count:" in content
    assert "usage: \"LABEL\"" in content
    assert "usage: \"GAUGE\"" in content
    assert "COUNT(*)::float AS count" in content
    assert "AS seconds_ago" in content


@pytest.mark.integration
def test_dag_failure_rules_evaluate_exported_gauge_directly() -> None:
    prometheus_rules = PROMETHEUS_RULES.read_text(encoding="utf-8")
    grafana_rules = GRAFANA_RULES.read_text(encoding="utf-8")

    assert 'sum(airflow_dag_run_count{state="failed"}) > 0' in prometheus_rules
    assert 'expr: sum(airflow_dag_run_count{state="failed"})' in grafana_rules
    assert "increase(airflow_dag_run_count" not in prometheus_rules
    assert "increase(airflow_dag_run_count" not in grafana_rules


@pytest.mark.integration
def test_telegram_contact_points_use_environment_chat_ids() -> None:
    content = CONTACT_POINTS.read_text(encoding="utf-8")

    assert 'chatid: "-1003977230036"' in content
    assert 'chatid: "5602934306"' in content
    assert "chatid: ${TELEGRAM_AQ_CHAT_ID}" not in content
    assert "chatid: ${TELEGRAM_SYS_CHAT_ID}" not in content
