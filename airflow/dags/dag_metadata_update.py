"""
Metadata Update DAG for Air Quality Data Platform (Airflow 3 TaskFlow API).

This DAG runs daily to refresh metadata:
- AQI.in stations (auto-populated during measurement ingestion)

D-AQI-02 (Phase 6): AQICN and Sensors.Community removed.
AQI.in stations are populated automatically during measurement ingestion.

Schedule: Daily at 01:00
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
PYTHON_JOBS_DIR = os.environ.get('PYTHON_JOBS_DIR', '/opt/python/jobs/')
PYTHON_PATH = PYTHON_JOBS_DIR


def get_job_env_vars() -> dict:
    """Get environment variables at execution time (not parse time)."""
    return {
        'CLICKHOUSE_HOST': os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        'CLICKHOUSE_PORT': os.environ.get('CLICKHOUSE_PORT', '8123'),
        'CLICKHOUSE_USER': os.environ.get('CLICKHOUSE_USER', 'admin'),
        'CLICKHOUSE_PASSWORD': os.environ.get('CLICKHOUSE_PASSWORD', 'admin123456'),
        'CLICKHOUSE_DB': os.environ.get('CLICKHOUSE_DB', 'air_quality'),
    }


@dag(
    default_args=default_args,
    description='Daily metadata refresh for AQI.in stations (D-AQI-02: AQICN + Sensors.Community removed)',
    schedule='0 1 * * *',  # Daily at 01:00
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    tags=['metadata', 'daily', 'refresh', 'air-quality', 'aqiin'],
)
def dag_metadata_update():
    """Metadata update DAG using Airflow 3 TaskFlow API.

    D-AQI-02 (Phase 6): Only AQI.in stations metadata refresh.
    OpenWeather city metadata is static (62 provinces).
    """

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
    def log_metadata_stats():
        """Log metadata statistics after refresh."""
        import requests

        clickhouse_host = os.environ.get('CLICKHOUSE_HOST', 'clickhouse')
        clickhouse_port = os.environ.get('CLICKHOUSE_PORT', '8123')
        clickhouse_user = os.environ.get('CLICKHOUSE_USER', 'admin')
        clickhouse_password = os.environ.get('CLICKHOUSE_PASSWORD', 'admin123456')

        url = f"http://{clickhouse_host}:{clickhouse_port}/?user={clickhouse_user}&password={clickhouse_password}"

        stats = {}

        try:
            # Get AQI.in stations count
            query = "SELECT count(*) FROM raw_aqiin_stations"
            response = requests.get(f"{url}&query={query}", timeout=30)
            if response.status_code == 200:
                lines = response.text.strip().split('\n')
                if len(lines) > 1:
                    stats['aqiin_stations'] = int(lines[1].strip())

            # Get AQI.in measurements count (recent)
            query = "SELECT count(*) FROM raw_aqiin_measurements WHERE ingest_time >= now() - INTERVAL 1 DAY"
            response = requests.get(f"{url}&query={query}", timeout=30)
            if response.status_code == 200:
                lines = response.text.strip().split('\n')
                if len(lines) > 1:
                    stats['aqiin_measurements_24h'] = int(lines[1].strip())

            print(f"Metadata statistics: {stats}")

        except Exception as e:
            print(f"Error getting metadata stats: {e}")

        return stats

    @task
    def log_completion():
        """Log completion message."""
        print("Metadata update completed")

    # Define task dependencies
    check_clickhouse = check_clickhouse_connection()
    log_stats = log_metadata_stats()
    log_done = log_completion()

    check_clickhouse >> log_stats >> log_done


dag_metadata_update = dag_metadata_update()
