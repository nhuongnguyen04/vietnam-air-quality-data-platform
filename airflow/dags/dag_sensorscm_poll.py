"""
Sensors.Community Poll DAG for Air Quality Data Platform (Airflow 3 TaskFlow API).

Polls https://api.sensor.community/v1/feeds/ every 5 minutes for Vietnam bbox
and stores readings in ClickHouse.

Schedule: Every 5 minutes (*/5 * * * *)

This DAG runs independently from dag_ingest_hourly. Both may write to
raw_sensorscm_measurements; ReplacingMergeTree handles dedup server-side (D-01).
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
import os

# Default arguments
default_args = {
    'owner': 'air-quality-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Python jobs directory in Airflow container
PYTHON_JOBS_DIR = os.environ.get('PYTHON_JOBS_DIR', '/opt/python/jobs')
PYTHON_PATH = PYTHON_JOBS_DIR


def get_job_env_vars() -> dict:
    """Get environment variables at execution time (not parse time)."""
    return {
        'CLICKHOUSE_HOST': os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        'CLICKHOUSE_PORT': os.environ.get('CLICKHOUSE_PORT', '8123'),
        'CLICKHOUSE_USER': os.environ.get('CLICKHOUSE_USER', 'admin'),
        'CLICKHOUSE_PASSWORD': os.environ.get('CLICKHOUSE_PASSWORD', 'admin'),
        'CLICKHOUSE_DB': os.environ.get('CLICKHOUSE_DB', 'air_quality'),
        'AQICN_API_TOKEN': os.environ.get('AQICN_API_TOKEN', ''),
        # Note: Sensors.Community requires no API token
    }


@dag(
    default_args=default_args,
    description='Poll Sensors.Community API every 5 minutes for Vietnam air quality data',
    schedule='*/5 * * * *',     # Every 5 minutes — increased from 10 min for better real-time data
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=5,
    tags=['ingestion', 'sensorscm', '5min', 'air-quality'],
)
def dag_sensorscm_poll():
    """Poll Sensors.Community every 5 minutes."""

    @task
    def check_clickhouse_connection():
        """Check if ClickHouse is accessible."""
        import requests

        clickhouse_host = os.environ.get('CLICKHOUSE_HOST', 'clickhouse')
        clickhouse_port = os.environ.get('CLICKHOUSE_PORT', '8123')

        url = f"http://{clickhouse_host}:{clickhouse_port}/ping"

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            print(f"ClickHouse connection successful: {response.status_code}")
            return True
        except Exception as e:
            print(f"ClickHouse connection failed: {e}")
            raise

    @task
    def run_sensorscm_poll():
        """Poll Sensors.Community Vietnam bbox and ingest measurements."""
        import subprocess

        env = os.environ.copy()
        env.update(get_job_env_vars())

        cmd = f"cd {PYTHON_PATH} && python jobs/sensorscm/ingest_measurements.py --mode incremental"

        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise Exception(f"Command failed: {cmd}")
        print(f"Sensors.Community poll completed successfully")

    @task
    def update_sensorscm_control():
        """Update ingestion_control for Sensors.Community (5-min poll)."""
        import sys
        sys.path.insert(0, '/opt/python/jobs')
        from common.ingestion_control import update_control as _update
        _update(source='sensorscm', records_ingested=0, success=True)
        print("Updated ingestion_control for sensorscm")

    @task
    def log_completion():
        """Log completion message."""
        print("dag_sensorscm_poll completed")

    # Define task dependencies
    check_clickhouse = check_clickhouse_connection()
    poll = run_sensorscm_poll()
    update_control = update_sensorscm_control()
    completion = log_completion()

    check_clickhouse >> poll >> update_control >> completion


dag_sensorscm_poll = dag_sensorscm_poll()
