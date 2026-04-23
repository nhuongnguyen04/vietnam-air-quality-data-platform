"""
Google Drive Sync DAG for Air Quality Data Platform.

This DAG runs on the local Airflow instance to sync CSV files 
ingested by GitHub Actions from Google Drive to ClickHouse.
"""

from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
import os
import subprocess

# Default arguments
default_args = {
    'owner': 'air-quality-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Directories
SCRIPTS_DIR = os.environ.get('SCRIPTS_DIR', '/opt/python/jobs/scripts')
if not os.path.exists(SCRIPTS_DIR):
    # Fallback for local dev
    SCRIPTS_DIR = '/opt/python/jobs/scripts'

@dag(
    default_args=default_args,
    description='Syncs ingested CSV data from Google Drive to ClickHouse',
    schedule='*/5 * * * *',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=['sync', 'gdrive', 'ingestion'],
)
def dag_sync_gdrive():
    """DAG to sync Google Drive data to ClickHouse."""

    @task
    def sync_data():
        """Execute the gdrive_sync.py script."""
        script_path = os.path.join('/opt/python/jobs', 'jobs/sync/gdrive_sync.py')

        # Ensure we have the necessary environment variables
        env = os.environ.copy()

        # Note: These should be set in docker-compose.yml or Airflow Connections
        # GDRIVE_SERVICE_ACCOUNT
        # GDRIVE_ROOT_FOLDER_ID
        # CLICKHOUSE_HOST, etc.

        result = subprocess.run(
            ['python', script_path],
            env=env,
            cwd='/opt/python/jobs',
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print(f"Sync Script Error: {result.stderr}")
            raise Exception(f"Sync script failed with return code {result.returncode}")

        print(f"Sync Script Output: {result.stdout}")

        # Parse sync counters from output. Partial failures are allowed: valid files
        # should still trigger downstream transforms while failed files remain in
        # landing_zone for the next retry.
        counters = {
            "FILES_FOUND": 0,
            "FILES_SYNCED": 0,
            "FILES_FAILED": 0,
        }
        for line in result.stdout.splitlines():
            for key in counters:
                if line.startswith(f"{key}="):
                    counters[key] = int(line.split("=", 1)[1])

        if counters["FILES_FAILED"] > 0:
            print(
                "Sync completed with partial failures: "
                f"found={counters['FILES_FOUND']} "
                f"synced={counters['FILES_SYNCED']} "
                f"failed={counters['FILES_FAILED']}. "
                "Failed files remain in landing_zone for retry."
            )

        return counters["FILES_SYNCED"] > 0

    @task
    def log_completion():
        print("Google Drive to ClickHouse sync completed successfully")

    @task
    def log_no_data():
        print("No new data synced from Google Drive — skipping downstream tasks")

    trigger_transform = TriggerDagRunOperator(
        task_id='trigger_transform',
        trigger_dag_id='dag_transform',
        wait_for_completion=False,
    )

    # Dependencies
    sync_has_data = sync_data()

    check_sync = ShortCircuitOperator(
        task_id='check_sync_data',
        python_callable=lambda x: bool(x),
        op_args=[sync_has_data],
    )

    # Branch: proceed only if data was synced
    check_sync >> log_completion() >> trigger_transform
    check_sync >> log_no_data()

dag_sync_gdrive = dag_sync_gdrive()
