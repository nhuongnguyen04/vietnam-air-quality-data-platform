"""
Legacy manual ingestion DAG for the Air Quality Data Platform (Airflow 3 TaskFlow API).

This DAG can be triggered manually to ingest the latest measurements from:
- AQI.in (~540 Vietnam monitoring stations via widget scraper)
- OpenWeather Air Pollution API (62 Vietnam provinces)
- TomTom Traffic Flow API (3-hourly sampling)
- Traffic Pattern Engine (1-hourly interpolation via Python)

Schedule: None (manual fallback only)
"""

from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
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


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value in (None, ''):
        raise RuntimeError(f"{name} environment variable is required")
    return value


def get_job_env_vars() -> dict:
    """Get environment variables at execution time (not parse time)."""
    return {
        'CLICKHOUSE_HOST': os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        'CLICKHOUSE_PORT': os.environ.get('CLICKHOUSE_PORT', '8123'),
        'CLICKHOUSE_USER': os.environ.get('CLICKHOUSE_USER', 'admin'),
        'CLICKHOUSE_PASSWORD': _require_env('CLICKHOUSE_PASSWORD'),
        'CLICKHOUSE_DB': os.environ.get('CLICKHOUSE_DB', 'air_quality'),
        'OPENWEATHER_API_TOKENS': os.environ.get('OPENWEATHER_API_TOKENS', os.environ.get('OPENWEATHER_API_TOKEN', '')),
        'TOMTOM_API_KEY': os.environ.get('TOMTOM_API_KEY', ''),
    }


@dag(
    default_args=default_args,
    description='Legacy manual fallback for direct ingestion into ClickHouse; GitHub Actions remains the primary scheduler',
    schedule=None,
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    tags=['ingestion', 'hourly', 'triggers-transform', 'air-quality', 'weather', 'traffic'],
)
def dag_ingest_hourly():
    """Manual fallback DAG for AQI.in, OpenWeather (AQI + Weather), and TomTom (Traffic)."""

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
        """Run AQI.in measurements ingestion."""
        import subprocess

        env = os.environ.copy()
        env.update(get_job_env_vars())

        cmd = f"cd {PYTHON_PATH} && python jobs/aqiin/ingest_measurements.py --mode incremental"

        result = subprocess.run(cmd, shell=True, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise Exception(f"Command failed: {cmd}")
        print("AQI.in measurements ingestion completed")

    @task
    def run_openweather_unified_ingestion():
        """Run Unified OpenWeather ingestion (AQI + Weather) for 653 points."""
        import subprocess

        env = os.environ.copy()
        env.update(get_job_env_vars())

        cmd = f"cd {PYTHON_PATH} && python jobs/openweather/ingest_openweather_unified.py"

        result = subprocess.run(cmd, shell=True, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise Exception(f"Command failed: {cmd}")
        print("OpenWeather Unified ingestion completed (AQI + Weather)")

    @task
    def run_traffic_processing(**context):
        """Run TomTom Traffic Ingestion (Peak) or Off-peak Generation."""
        import subprocess
        from datetime import datetime
        import os
        
        # Get Vietnam Hour
        data_interval_start = context.get('data_interval_start')
        # Airflow data_interval_start is in UTC, convert to VN (UTC+7)
        hour = (data_interval_start.hour + 7) % 24 if data_interval_start else (datetime.now().hour + 7) % 24
        
        env = os.environ.copy()
        env.update(get_job_env_vars())
        
        if 7 <= hour <= 20:
            print(f"Peak Hour ({hour}:00 VN): Running TomTom Ingestion...")
            cmd = f"cd {PYTHON_PATH} && python jobs/traffic/ingest_tomtom.py"
        else:
            print(f"Off-Peak Hour ({hour}:00 VN): Running Simulated Traffic Generation...")
            cmd = f"cd {PYTHON_PATH} && python jobs/traffic/generate_offpeak_traffic.py"

        result = subprocess.run(cmd, shell=True, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise Exception(f"Command failed: {cmd}")
            
        print(f"Traffic processing completed for hour {hour}")
        return True

    @task
    def update_ingestion_control():
        """Update ingestion_control for all sources."""
        import sys
        sys.path.insert(0, '/opt/python/jobs')
        from common.ingestion_control import update_control as _update
        
        _update(source='dag_ingest_hourly', records_ingested=0, success=True)
        print("Updated ingestion_control summary")

    @task
    def log_completion():
        """Log completion message."""
        print("Manual ingestion fallback cycle completed (AQI + Weather + Traffic)")

    trigger_transform = TriggerDagRunOperator(
        task_id='trigger_transform',
        trigger_dag_id='dag_transform',
        wait_for_completion=False,
    )

    check_ch = check_clickhouse_connection()
    
    aqiin = run_aqiin_measurements_ingestion()
    ow_unified = run_openweather_unified_ingestion()
    tt_processing = run_traffic_processing()
    
    update_control = update_ingestion_control()
    completion = log_completion()

    check_ch >> [aqiin, ow_unified, tt_processing]
    [aqiin, ow_unified, tt_processing] >> update_control >> completion
    completion >> trigger_transform


dag_ingest_hourly = dag_ingest_hourly()
