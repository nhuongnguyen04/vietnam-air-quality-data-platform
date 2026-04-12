"""
Google Drive Sync DAG for Air Quality Data Platform.

This DAG runs on the local Airflow instance to sync CSV files 
ingested by GitHub Actions from Google Drive to ClickHouse.
"""

from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
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
        return True

    @task
    def run_traffic_calculation():
        """Run Traffic Pattern Enrichment calculation in Python."""
        import subprocess
        
        script_path = os.path.join('/opt/python/jobs', 'jobs/traffic/calculate_hourly_traffic.py')
        
        # Ensure we have the necessary environment variables
        env = os.environ.copy()
        env['CLICKHOUSE_HOST'] = os.environ.get('CLICKHOUSE_HOST', 'clickhouse')
        env['CLICKHOUSE_PORT'] = os.environ.get('CLICKHOUSE_PORT', '8123')
        env['CLICKHOUSE_USER'] = os.environ.get('CLICKHOUSE_USER', 'admin')
        env['CLICKHOUSE_PASSWORD'] = os.environ.get('CLICKHOUSE_PASSWORD', 'admin123456')
        env['CLICKHOUSE_DB'] = os.environ.get('CLICKHOUSE_DB', 'air_quality')

        result = subprocess.run(
            ['python', script_path],
            env=env,
            cwd='/opt/python/jobs',
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"Calculation Script Error: {result.stderr}")
            raise Exception(f"Calculation script failed with return code {result.returncode}")
            
        print(f"Calculation Script Output: {result.stdout}")
        return True

    @task
    def log_completion():
        print("Google Drive to ClickHouse sync completed successfully")

    trigger_transform = TriggerDagRunOperator(
        task_id='trigger_transform',
        trigger_dag_id='dag_transform',
        wait_for_completion=False,
    )

    # Dependencies
    sync = sync_data()
    calc = run_traffic_calculation()
    completion = log_completion()
    
    sync >> calc >> completion >> trigger_transform

dag_sync_gdrive = dag_sync_gdrive()
