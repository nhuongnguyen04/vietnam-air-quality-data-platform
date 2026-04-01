"""Tests for OpenAQ decommission (Plan 1.04)."""

import pytest
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def test_openaq_directory_removal():
    """python_jobs/jobs/openaq/ directory no longer exists after decommission."""
    openaq_dir = os.path.join(
        os.path.dirname(__file__), '..',
        'python_jobs', 'jobs', 'openaq'
    )
    assert not os.path.exists(openaq_dir), \
        f"OpenAQ directory still exists at {openaq_dir}"


def test_dag_ingest_hourly_no_openaq_tasks():
    """dag_ingest_hourly.py contains no OpenAQ task references."""
    dag_path = os.path.join(
        os.path.dirname(__file__), '..',
        'airflow', 'dags', 'dag_ingest_hourly.py'
    )
    with open(dag_path) as f:
        content = f.read().lower()

    assert "openaq" not in content, "OpenAQ references still in dag_ingest_hourly"
    assert "ingest_openaq" not in content
    assert "openaq_api_token" not in content
    assert "raw_openaq" not in content


def test_openaq_tables_renamed_to_archived():
    """OpenAQ tables are renamed to raw_openaq_*_archived, not dropped."""
    init_sql_path = os.path.join(
        os.path.dirname(__file__), '..',
        'scripts', 'init-clickhouse.sql'
    )
    with open(init_sql_path) as f:
        sql = f.read().lower()

    # Verify all 4 RENAME TABLE statements are present
    assert "rename table raw_openaq_measurements to raw_openaq_measurements_archived" in sql, \
        "RENAME TABLE for measurements not found"
    assert "rename table raw_openaq_locations to raw_openaq_locations_archived" in sql, \
        "RENAME TABLE for locations not found"
    assert "rename table raw_openaq_parameters to raw_openaq_parameters_archived" in sql, \
        "RENAME TABLE for parameters not found"
    assert "rename table raw_openaq_sensors to raw_openaq_sensors_archived" in sql, \
        "RENAME TABLE for sensors not found"

    # Verify no active CREATE TABLE for raw_openaq_* (they should be disabled/commented)
    lines = sql.split('\n')
    active_create_lines = [
        l for l in lines
        if "create table" in l and "raw_openaq" in l and "disabled" not in l and "-- " not in l
    ]
    assert len(active_create_lines) == 0, \
        f"Active CREATE TABLE for OpenAQ tables found: {active_create_lines}"


def test_dag_metadata_update_no_openaq():
    """dag_metadata_update.py contains no OpenAQ task references."""
    dag_path = os.path.join(
        os.path.dirname(__file__), '..',
        'airflow', 'dags', 'dag_metadata_update.py'
    )
    with open(dag_path) as f:
        content = f.read().lower()

    assert "openaq" not in content, "OpenAQ references still in dag_metadata_update"
    assert "refresh_openaq" not in content
    assert "raw_openaq" not in content
    assert "openaq_api_token" not in content


def test_ensure_metadata_no_longer_calls_openaq():
    """ensure_metadata task in dag_ingest_hourly no longer calls OpenAQ metadata scripts."""
    dag_path = os.path.join(
        os.path.dirname(__file__), '..',
        'airflow', 'dags', 'dag_ingest_hourly.py'
    )
    with open(dag_path) as f:
        content = f.read()

    assert "ingest_parameters.py" not in content, \
        "ingest_parameters.py still referenced in dag_ingest_hourly"
    assert "ingest_locations.py" not in content, \
        "ingest_locations.py still referenced in dag_ingest_hourly"
    assert "ingest_sensors.py" not in content, \
        "ingest_sensors.py still referenced in dag_ingest_hourly"


def test_openaq_token_removed_from_env():
    """OPENAQ_API_TOKEN no longer present in .env or docker-compose.yml."""
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    compose_path = os.path.join(os.path.dirname(__file__), '..', 'docker-compose.yml')

    with open(env_path) as f:
        env_content = f.read()

    assert "OPENAQ_API_TOKEN" not in env_content, \
        "OPENAQ_API_TOKEN still in .env"

    with open(compose_path) as f:
        compose_content = f.read()

    assert "OPENAQ_API_TOKEN" not in compose_content, \
        "OPENAQ_API_TOKEN still in docker-compose.yml"
