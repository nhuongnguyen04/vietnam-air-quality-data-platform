"""
Hourly Ingestion DAG for Air Quality Data Platform (Airflow 3 TaskFlow API).

This DAG runs hourly to ingest the latest measurements from:
- AQICN / World Air Quality Index (api.waqi.info — primary source)
- OpenWeather Air Pollution API
- Sensors.Community

Note: AQICN and WAQI are the same API (api.waqi.info). The pipeline uses AQICN
as the canonical source; do NOT add WAQI as a separate ingestion source.

Schedule: Every 15 minutes (*/15 * * * *) — increased from hourly for near-real-time air quality data
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
        'AQICN_API_TOKEN': os.environ.get('AQICN_API_TOKEN', ''),
        'OPENWEATHER_API_TOKEN': os.environ.get('OPENWEATHER_API_TOKEN', ''),
    }


def build_env_command() -> str:
    """Build environment variable export commands."""
    job_env = get_job_env_vars()
    return ' && '.join([f"export {k}='{v}'" for k, v in job_env.items()])


@dag(
    default_args=default_args,
    description='Ingestion of air quality measurements from AQICN, Sensors.Community, and OpenWeather every 15 minutes — triggers dag_transform on completion',
    schedule='*/15 * * * *',
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    tags=['ingestion', '15min', 'triggers-transform', 'air-quality'],
)
def dag_ingest_hourly():
    """15-minute ingestion DAG using Airflow 3 TaskFlow API — triggers dag_transform on completion."""

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
    def ensure_metadata():
        """
        Ensure metadata exists in ClickHouse. If not, run metadata ingestion.

        This task always succeeds so that downstream measurement/forecast
        tasks are never skipped (unlike the previous @task.branch approach).

        Note: External metadata ingestion removed (Plan 1.04). AQICN stations are
        populated automatically during measurement/forecast ingestion.
        """
        import requests

        clickhouse_host = os.environ.get('CLICKHOUSE_HOST', 'clickhouse')
        clickhouse_port = os.environ.get('CLICKHOUSE_PORT', '8123')
        clickhouse_user = os.environ.get('CLICKHOUSE_USER', 'admin')
        clickhouse_password = os.environ.get('CLICKHOUSE_PASSWORD', 'admin123456')

        url = f"http://{clickhouse_host}:{clickhouse_port}/?user={clickhouse_user}&password={clickhouse_password}"

        try:
            # Check if AQICN stations table has data
            # Note: External metadata ingestion removed (Plan 1.04)
            query = "SELECT count(*) FROM raw_aqicn_stations"
            response = requests.get(f"{url}&query={query}", timeout=10)
            if response.status_code == 200:
                count = int(response.text.strip())
                if count > 0:
                    print(f"Metadata exists ({count} AQICN stations), proceeding to measurements ingestion")
                    return

        except Exception as e:
            print(f"Error checking metadata: {e}")

        # No metadata — proceed anyway; AQICN stations will be populated
        # automatically during first measurement/forecast ingestion run
        print("Metadata check complete; AQICN stations will be populated on first ingestion")

    @task
    def run_aqicn_measurements_ingestion():
        """Run AQICN measurements ingestion."""
        import subprocess
        
        env = os.environ.copy()
        env.update(get_job_env_vars())
        
        cmd = f"cd {PYTHON_PATH} && python jobs/aqicn/ingest_measurements.py --mode incremental"
        
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
        print(f"AQICN measurements ingestion completed")

    @task
    def update_aqicn_control():
        """Update ingestion_control for AQICN measurements."""
        import sys
        sys.path.insert(0, '/opt/python/jobs')
        from common.ingestion_control import update_control as _update
        _update(source='aqicn', records_ingested=0, success=True)
        print("Updated ingestion_control for aqicn")

    @task
    def run_sensorscm_measurements_ingestion():
        """Run Sensors.Community measurements ingestion."""
        import subprocess

        env = os.environ.copy()
        env.update(get_job_env_vars())

        cmd = f"cd {PYTHON_PATH} && python jobs/sensorscm/ingest_measurements.py --mode incremental"

        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise Exception(f"Command failed: {cmd}")
        print("Sensors.Community measurements ingestion completed")

    @task
    def update_sensorscm_control():
        """Update ingestion_control for Sensors.Community measurements."""
        import sys
        sys.path.insert(0, '/opt/python/jobs')
        from common.ingestion_control import update_control as _update
        _update(source='sensorscm', records_ingested=0, success=True)
        print("Updated ingestion_control for sensorscm")

    @task
    def run_openweather_measurements_ingestion():
        """Run OpenWeather measurements ingestion."""
        import subprocess

        env = os.environ.copy()
        env.update(get_job_env_vars())

        cmd = f"cd {PYTHON_PATH} && python jobs/openweather/ingest_measurements.py --mode incremental"

        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True,
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
        print("Hourly ingestion completed")

    # Trigger dag_transform after ingestion completes
    trigger_transform = TriggerDagRunOperator(
        task_id='trigger_transform',
        trigger_dag_id='dag_transform',
        wait_for_completion=False,  # Don't wait for dag_transform to finish
        poke_interval=30,
        reset_dag_run=True,  # Allow re-triggering even if previous run is running
        allowed_states=['success'],  # Only trigger if ingestion succeeded
        failed_states=['failed'],  # upstream_failed removed in Airflow 3.x (DagRunState enum)
    )

    # Define task dependencies
    check_clickhouse = check_clickhouse_connection()
    metadata = ensure_metadata()

    aqicn = run_aqicn_measurements_ingestion()
    sensorscm = run_sensorscm_measurements_ingestion()
    openweather = run_openweather_measurements_ingestion()
    update_aqicn_control = update_aqicn_control()
    update_sensorscm_control = update_sensorscm_control()
    update_openweather_control = update_openweather_control()
    completion = log_completion()

    # Fan-out: all 3 sources run in parallel after metadata
    check_clickhouse >> metadata >> [aqicn, sensorscm, openweather]

    # Fan-in per source, then fan-in to completion
    aqicn >> update_aqicn_control
    sensorscm >> update_sensorscm_control
    openweather >> update_openweather_control

    # Fan-in all control updates to completion
    [update_aqicn_control, update_sensorscm_control,
     update_openweather_control] >> completion

    # Trigger dag_transform after ingestion completes
    completion >> trigger_transform


dag_ingest_hourly = dag_ingest_hourly()

