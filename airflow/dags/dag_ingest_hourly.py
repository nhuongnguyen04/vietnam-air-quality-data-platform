"""
Hourly Ingestion DAG for Air Quality Data Platform (Airflow 3 TaskFlow API).

This DAG runs every 15 minutes to ingest the latest measurements from:
- AQI.in (~540 Vietnam monitoring stations via widget scraper)
- OpenWeather Air Pollution API (62 Vietnam provinces)

D-AQI-02 (Phase 6): AQICN and Sensors.Community removed.
Sources: AQI.in + OpenWeather (2 sources, both running in parallel).

Schedule: Every 15 minutes (*/15 * * * *)
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    """Get environment variables at execution time (not parse time).

    This must be a function, not a module-level dict, because Airflow 3
    parses DAGs in the dag-processor but executes tasks in a separate
    task runner process. Module-level dicts capture env vars at parse time,
    which may differ from the execution environment.
    """
    return {
        'CLICKHOUSE_HOST': os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        'CLICKHOUSE_PORT': os.environ.get('CLICKHOUSE_PORT', '8123'),
        'CLICKHOUSE_USER': os.environ.get('CLICKHOUSE_USER', 'admin'),
        'CLICKHOUSE_PASSWORD': os.environ.get('CLICKHOUSE_PASSWORD', 'admin123456'),
        'CLICKHOUSE_DB': os.environ.get('CLICKHOUSE_DB', 'air_quality'),
        'OPENWEATHER_API_TOKEN': os.environ.get('OPENWEATHER_API_TOKEN', ''),
    }


@dag(
    default_args=default_args,
    description='Ingestion of air quality measurements from AQI.in and OpenWeather every 15 minutes — triggers dag_transform on completion',
    schedule='*/15 * * * *',
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    tags=['ingestion', '15min', 'triggers-transform', 'air-quality', 'aqiin'],
)
def dag_ingest_hourly():
    """15-minute ingestion DAG using Airflow 3 TaskFlow API — AQI.in + OpenWeather only."""

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
    def run_aqiin_measurements_ingestion():
        """Run AQI.in measurements ingestion (~540 Vietnam stations via widget scraper)."""
        import subprocess

        env = os.environ.copy()
        env.update(get_job_env_vars())

        cmd = f"cd {PYTHON_PATH} && python jobs/aqiin/ingest_measurements.py --mode incremental"

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
        print(f"AQI.in measurements ingestion completed")

    @task
    def update_aqiin_control():
        """Update ingestion_control for AQI.in measurements."""
        import sys
        sys.path.insert(0, '/opt/python/jobs')
        from common.ingestion_control import update_control as _update
        _update(source='aqiin', records_ingested=0, success=True)
        print("Updated ingestion_control for aqiin")

    @task
    def run_openweather_measurements_ingestion():
        """Run OpenWeather measurements ingestion (62 Vietnam provinces)."""
        import subprocess

        env = os.environ.copy()
        env.update(get_job_env_vars())

        cmd = f"cd {PYTHON_PATH} && python jobs/openweather/ingest_measurements.py --mode incremental"

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
        print("OpenWeather measurements ingestion completed")

    @task
    def update_openweather_control():
        """Update ingestion_control for OpenWeather measurements."""
        import sys
        sys.path.insert(0, '/opt/python/jobs')
        from common.ingestion_control import update_control as _update
        _update(source='openweather', records_ingested=0, success=True)
        print("Updated ingestion_control for openweather")

    @task
    def log_completion():
        """Log completion message."""
        print("Hourly ingestion completed (AQI.in + OpenWeather)")

    # Trigger dag_transform after ingestion completes
    trigger_transform = TriggerDagRunOperator(
        task_id='trigger_transform',
        trigger_dag_id='dag_transform',
        wait_for_completion=False,
        poke_interval=30,
        reset_dag_run=True,
        allowed_states=['success'],
        failed_states=['failed'],
    )

    # Define task dependencies
    check_clickhouse = check_clickhouse_connection()

    aqiin = run_aqiin_measurements_ingestion()
    openweather = run_openweather_measurements_ingestion()
    update_aqiin_control = update_aqiin_control()
    update_openweather_control = update_openweather_control()
    completion = log_completion()

    # Fan-out: 2 sources run in parallel after connection check
    check_clickhouse >> [aqiin, openweather]

    # Fan-in per source
    aqiin >> update_aqiin_control
    openweather >> update_openweather_control

    # Fan-in all control updates to completion
    [update_aqiin_control, update_openweather_control] >> completion

    # Trigger dag_transform after ingestion completes
    completion >> trigger_transform


dag_ingest_hourly = dag_ingest_hourly()
