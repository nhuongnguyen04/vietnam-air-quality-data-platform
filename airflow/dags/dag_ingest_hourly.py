"""
Hourly Ingestion DAG for Air Quality Data Platform (Airflow 3 TaskFlow API).

This DAG runs hourly to ingest the latest measurements from:
- OpenAQ API (measurements for Vietnam sensors)
- AQICN API (measurements for Vietnam stations)

The DAG checks for metadata updates periodically and runs the ingestion jobs.

Schedule: Hourly (every hour at minute 0)
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
        'CLICKHOUSE_DB': os.environ.get('CLICKHOUSE_DB', 'air_quality'),
        'OPENAQ_API_TOKEN': os.environ.get('OPENAQ_API_TOKEN', ''),
        'AQICN_API_TOKEN': os.environ.get('AQICN_API_TOKEN', ''),
    }


def build_env_command() -> str:
    """Build environment variable export commands."""
    job_env = get_job_env_vars()
    return ' && '.join([f"export {k}='{v}'" for k, v in job_env.items()])


@dag(
    default_args=default_args,
    description='Hourly ingestion of air quality measurements from OpenAQ and AQICN',
    schedule='0 * * * *',
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    tags=['ingestion', 'hourly', 'air-quality'],
)
def dag_ingest_hourly():
    """Hourly ingestion DAG using Airflow 3 TaskFlow API."""

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
        """
        import requests
        import subprocess
        
        clickhouse_host = os.environ.get('CLICKHOUSE_HOST', 'clickhouse')
        clickhouse_port = os.environ.get('CLICKHOUSE_PORT', '8123')
        clickhouse_user = os.environ.get('CLICKHOUSE_USER', 'admin')
        clickhouse_password = os.environ.get('CLICKHOUSE_PASSWORD', 'admin')
        
        url = f"http://{clickhouse_host}:{clickhouse_port}/?user={clickhouse_user}&password={clickhouse_password}"
        
        metadata_exists = False
        
        try:
            # Check if openaq_locations table has data
            # Note: AQICN stations check removed — stations are now populated
            # during measurement/forecast ingestion via feed API
            query = "SELECT count(*) FROM raw_openaq_locations"
            response = requests.get(f"{url}&query={query}", timeout=10)
            if response.status_code == 200:
                count = int(response.text.strip())
                if count > 0:
                    metadata_exists = True
                    
        except Exception as e:
            print(f"Error checking metadata: {e}")
        
        if metadata_exists:
            print("Metadata exists, proceeding to measurements ingestion")
            return
        
        # Metadata missing — run metadata ingestion
        print("Metadata missing, running metadata ingestion first")
        
        env = os.environ.copy()
        env.update(get_job_env_vars())
        
        commands = [
            f"cd {PYTHON_PATH} && python jobs/openaq/ingest_parameters.py --mode rewrite",
            f"cd {PYTHON_PATH} && python jobs/openaq/ingest_locations.py --mode rewrite",
            f"cd {PYTHON_PATH} && python jobs/openaq/ingest_sensors.py --mode rewrite",
            # NOTE: AQICN stations are now updated automatically during
            # measurement/forecast ingestion via feed API + crawl.html
        ]
        
        for cmd in commands:
            result = subprocess.run(
                cmd,
                shell=True,
                env=env,
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                print(f"Error running: {cmd}")
                print(f"stderr: {result.stderr}")
                raise Exception(f"Command failed: {cmd}")
            print(f"Completed: {cmd}")

    # @task
    # def run_openaq_measurements_ingestion():
    #     """Run OpenAQ measurements ingestion. DISABLED for Plan 0.4 AQICN-only baseline."""
    #     import subprocess
    #
    #     env = os.environ.copy()
    #     env.update(get_job_env_vars())
    #
    #     cmd = f"cd {PYTHON_PATH} && python jobs/openaq/ingest_measurements.py --mode incremental"
    #
    #     result = subprocess.run(
    #         cmd,
    #         shell=True,
    #         env=env,
    #         capture_output=True,
    #         text=True
    #     )
    #     if result.returncode != 0:
    #         print(f"Error: {result.stderr}")
    #         raise Exception(f"Command failed: {cmd}")
    #     print(f"OpenAQ measurements ingestion completed")

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
    def run_aqicn_forecast_ingestion():
        """Run AQICN forecast ingestion."""
        import subprocess
        
        env = os.environ.copy()
        env.update(get_job_env_vars())
        
        cmd = f"cd {PYTHON_PATH} && python jobs/aqicn/ingest_forecast.py"
        
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
        print(f"AQICN forecast ingestion completed")

    @task
    def log_completion():
        """Log completion message."""
        print("Hourly ingestion completed")

    # Define task dependencies — linear, no branching
    check_clickhouse = check_clickhouse_connection()
    metadata = ensure_metadata()

    # openaq = run_openaq_measurements_ingestion()  # DISABLED for 0.4 baseline
    aqicn = run_aqicn_measurements_ingestion()
    forecast = run_aqicn_forecast_ingestion()

    completion = log_completion()

    # [openaq, aqicn, forecast] — DISABLED openaq for Plan 0.4 baseline
    check_clickhouse >> metadata >> [aqicn, forecast] >> completion


dag_ingest_hourly = dag_ingest_hourly()

