"""File-based checks for Airflow failure alerting via Prometheus/Grafana."""

from __future__ import annotations

from pathlib import Path

import pytest

POSTGRES_QUERIES = Path("monitoring/prometheus/queries-postgresql.yaml")
PROMETHEUS_RULES = Path("monitoring/prometheus/rules/alerts.yml")
GRAFANA_RULES = Path("monitoring/grafana/provisioning/alerting/system-prometheus-rules.yml")
GRAFANA_V3_RULES = Path("monitoring/grafana/provisioning/alerting/v3-alert-rules.yml")
CONTACT_POINTS = Path("monitoring/grafana/provisioning/alerting/contact-points.yml")


@pytest.mark.integration
def test_postgres_exporter_queries_use_supported_mapping_format() -> None:
    content = POSTGRES_QUERIES.read_text(encoding="utf-8")

    assert not content.startswith("queries:")
    assert "airflow_dag_run:" in content
    assert "airflow_dag_run_count:" in content
    assert "dag_id," in content
    assert "GROUP BY dag_id, state" in content
    assert "usage: \"LABEL\"" in content
    assert "usage: \"GAUGE\"" in content
    assert "COUNT(*)::float AS count" in content
    assert "AS seconds_ago" in content


@pytest.mark.integration
def test_dag_failure_rules_evaluate_exported_gauge_directly() -> None:
    prometheus_rules = PROMETHEUS_RULES.read_text(encoding="utf-8")
    grafana_rules = GRAFANA_RULES.read_text(encoding="utf-8")

    assert 'airflow_dag_run_count{state="failed"} > 0' in prometheus_rules
    assert 'expr: airflow_dag_run_count{state="failed"}' in grafana_rules
    assert "{{ $labels.dag_id }}" in prometheus_rules
    assert "{{ $labels.dag_id }}" in grafana_rules
    assert 'sum(airflow_dag_run_count{state="failed"})' not in prometheus_rules
    assert 'sum(airflow_dag_run_count{state="failed"})' not in grafana_rules
    assert "increase(airflow_dag_run_count" not in prometheus_rules
    assert "increase(airflow_dag_run_count" not in grafana_rules


@pytest.mark.integration
def test_no_recent_success_rules_use_seconds_ago_gauge_directly() -> None:
    prometheus_rules = PROMETHEUS_RULES.read_text(encoding="utf-8")
    grafana_rules = GRAFANA_RULES.read_text(encoding="utf-8")

    assert 'airflow_dag_run_count_seconds_ago{state="success"} > 7200' in prometheus_rules
    assert 'expr: airflow_dag_run_count_seconds_ago{state="success"}' in grafana_rules
    assert "time() - airflow_dag_run_count_seconds_ago" not in prometheus_rules
    assert "time() - airflow_dag_run_count_seconds_ago" not in grafana_rules


@pytest.mark.integration
def test_system_alerts_fire_without_pending_window() -> None:
    prometheus_rules = PROMETHEUS_RULES.read_text(encoding="utf-8")
    grafana_rules = GRAFANA_RULES.read_text(encoding="utf-8")
    grafana_v3_rules = GRAFANA_V3_RULES.read_text(encoding="utf-8")

    assert "interval: 30s" in grafana_rules
    assert "system-data-freshness" in grafana_v3_rules
    assert "interval: 30s" in grafana_v3_rules
    assert "for: 0m" in prometheus_rules
    assert "for: 0m" in grafana_rules
    assert "for: 0m" in grafana_v3_rules
    assert "for: 5m" not in prometheus_rules
    assert "for: 10m" not in prometheus_rules
    assert "for: 5m" not in grafana_rules
    assert "for: 10m" not in grafana_rules


@pytest.mark.integration
def test_telegram_contact_points_use_environment_chat_ids() -> None:
    content = CONTACT_POINTS.read_text(encoding="utf-8")

    assert 'chatid: "-1003977230036"' in content
    assert 'chatid: "5602934306"' in content
    assert "chatid: ${TELEGRAM_AQ_CHAT_ID}" not in content
    assert "chatid: ${TELEGRAM_SYS_CHAT_ID}" not in content
