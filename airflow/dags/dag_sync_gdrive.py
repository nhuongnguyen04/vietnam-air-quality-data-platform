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
        
        # Parse number of files synced from output
        for line in result.stdout.splitlines():
            if line.startswith("FILES_SYNCED="):
                success_count = int(line.split("=")[1])
                return success_count > 0
        
        return False

    # Dependencies
    sync_has_data = sync_data()

    check_sync = ShortCircuitOperator(
        task_id='check_sync_data',
        python_callable=lambda x: x,
        op_args=[sync_has_data],
    )

    @task
    def log_completion():
        print("Google Drive to ClickHouse sync completed successfully")

    trigger_transform = TriggerDagRunOperator(
        task_id='trigger_transform',
        trigger_dag_id='dag_transform',
        wait_for_completion=False,
    )

    completion = log_completion()

    # Linear flow: Only proceed if check_sync passes
    sync_has_data >> check_sync >> completion >> trigger_transform

dag_sync_gdrive = dag_sync_gdrive()
