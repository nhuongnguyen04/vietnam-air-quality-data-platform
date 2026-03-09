"""
Historical Data Ingestion DAG for Air Quality Data Platform (Airflow 3 TaskFlow API).

This DAG is used for one-time historical data backfill from:
- OpenAQ API (full historical measurements for Vietnam sensors)
- AQICN API (historical measurements for Vietnam stations)

This DAG should be manually triggered with configuration:
- start_date: Start date for backfill (YYYY-MM-DD)
- end_date: End date for backfill (YYYY-MM-DD)
- days_back: Number of days to backfill (alternative to start_date/end_date)

Schedule: Manual trigger only
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task

import os

# Default arguments
default_args = {
    'owner': 'air-quality-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
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
        'CLICKHOUSE_PASSWORD': os.environ.get('CLICKHOUSE_PASSWORD', 'admin'),
        'CLICKHOUSE_DB': os.environ.get('CLICKHOUSE_DB', 'airquality'),
        'OPENAQ_API_TOKEN': os.environ.get('OPENAQ_API_TOKEN', ''),
        'AQICN_API_TOKEN': os.environ.get('AQICN_API_TOKEN', ''),
    }


def build_env_command() -> str:
    """Build environment variable export commands."""
    job_env = get_job_env_vars()
    return ' && '.join([f"export {k}='{v}'" for k, v in job_env.items()])


@dag(
    default_args=default_args,
    description='One-time historical data backfill from OpenAQ and AQICN',
    schedule=None,  # Manual trigger only
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    tags=['ingestion', 'historical', 'backfill', 'air-quality'],
)
def dag_ingest_historical():
    """Historical data ingestion DAG using Airflow 3 TaskFlow API."""

    @task
    def configure_dates(**context):
        """Extract and configure start/end dates from DAG run conf or use defaults."""
        conf = context['dag_run'].conf if context.get('dag_run') else {}
        
        # Get dates from config or use defaults
        start_date = conf.get('start_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        end_date = conf.get('end_date', datetime.now().strftime('%Y-%m-%d'))
        days_back = conf.get('days_back', 30)
        
        print(f"Historical backfill configuration: start_date={start_date}, end_date={end_date}, days_back={days_back}")
        
        return {'start_date': start_date, 'end_date': end_date, 'days_back': days_back}

    @task
    def check_clickhouse_connection():
        """Check if ClickHouse is accessible."""
        import requests
        
        clickhouse_host = os.environ.get('CLICKHOUSE_HOST', 'clickhouse')
        clickhouse_port = os.environ.get('CLICKHOUSE_PORT', '8123')
        clickhouse_user = os.environ.get('CLICKHOUSE_USER', 'admin')
        clickhouse_password = os.environ.get('CLICKHOUSE_PASSWORD', 'admin')
        
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
    def ingest_openaq_parameters():
        """Ingest OpenAQ parameters (one-time)."""
        import subprocess
        
        env = os.environ.copy()
        env.update(get_job_env_vars())
        
        cmd = f"cd {PYTHON_PATH} && python jobs/openaq/ingest_parameters.py"
        
        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True,
            timeout=600
        )
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise Exception(f"Command failed: {cmd}")
        print(f"OpenAQ parameters ingestion completed")

    @task
    def ingest_openaq_locations():
        """Ingest OpenAQ locations (one-time)."""
        import subprocess
        
        env = os.environ.copy()
        env.update(get_job_env_vars())
        
        cmd = f"cd {PYTHON_PATH} && python jobs/openaq/ingest_locations.py"
        
        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True,
            timeout=1800
        )
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise Exception(f"Command failed: {cmd}")
        print(f"OpenAQ locations ingestion completed")

    @task
    def ingest_openaq_sensors():
        """Ingest OpenAQ sensors (one-time)."""
        import subprocess
        
        env = os.environ.copy()
        env.update(get_job_env_vars())
        
        cmd = f"cd {PYTHON_PATH} && python jobs/openaq/ingest_sensors.py"
        
        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True,
            timeout=3600
        )
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise Exception(f"Command failed: {cmd}")
        print(f"OpenAQ sensors ingestion completed")

    @task
    def ingest_aqicn_stations():
        """Ingest AQICN stations (one-time)."""
        import subprocess
        
        env = os.environ.copy()
        env.update(get_job_env_vars())
        
        cmd = f"cd {PYTHON_PATH} && python jobs/aqicn/ingest_stations.py"
        
        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True,
            timeout=1800
        )
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise Exception(f"Command failed: {cmd}")
        print(f"AQICN stations ingestion completed")

    @task
    def backfill_openaq_measurements(dates_config: dict):
        """Backfill OpenAQ measurements (parallel sensors)."""
        import subprocess
        
        env = os.environ.copy()
        env.update(get_job_env_vars())
        
        start_date = dates_config.get('start_date')
        end_date = dates_config.get('end_date')
        
        cmd = f"cd {PYTHON_PATH} && python jobs/openaq/ingest_measurements.py --mode historical --start-date {start_date} --end-date {end_date}"
        
        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True,
            timeout=86400
        )
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise Exception(f"Command failed: {cmd}")
        print(f"OpenAQ measurements backfill completed")

    @task
    def backfill_aqicn_measurements(dates_config: dict):
        """Backfill AQICN measurements (parallel stations)."""
        import subprocess
        
        env = os.environ.copy()
        env.update(get_job_env_vars())
        
        days_back = dates_config.get('days_back', 30)
        
        cmd = f"cd {PYTHON_PATH} && python jobs/aqicn/ingest_measurements.py --mode historical --days-back {days_back}"
        
        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True,
            timeout=86400
        )
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise Exception(f"Command failed: {cmd}")
        print(f"AQICN measurements backfill completed")

    @task
    def backfill_aqicn_forecast():
        """Backfill AQICN forecasts (if available)."""
        import subprocess
        
        env = os.environ.copy()
        env.update(get_job_env_vars())
        
        cmd = f"cd {PYTHON_PATH} && python jobs/aqicn/ingest_forecast.py"
        
        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True,
            timeout=3600
        )
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise Exception(f"Command failed: {cmd}")
        print(f"AQICN forecast backfill completed")

    @task
    def log_completion():
        """Log completion message."""
        print("Historical ingestion completed")

    # Define task dependencies
    
    configure_dates_task = configure_dates()
    check_clickhouse = check_clickhouse_connection()

    ingest_params = ingest_openaq_parameters()
    ingest_locations = ingest_openaq_locations()
    ingest_sensors = ingest_openaq_sensors()
    ingest_stations = ingest_aqicn_stations()

    backfill_openaq_task = backfill_openaq_measurements(configure_dates_task)
    backfill_aqicn_task = backfill_aqicn_measurements(configure_dates_task)
    backfill_forecast_task = backfill_aqicn_forecast()
    log_task = log_completion()

    configure_dates_task >> check_clickhouse

    check_clickhouse >> ingest_params >> ingest_locations >> ingest_sensors
    check_clickhouse >> ingest_stations

    ingest_sensors >> backfill_openaq_task
    ingest_stations >> backfill_aqicn_task
    ingest_stations >> backfill_forecast_task
    [backfill_openaq_task, backfill_aqicn_task, backfill_forecast_task] >> log_task


dag_ingest_historical = dag_ingest_historical()
