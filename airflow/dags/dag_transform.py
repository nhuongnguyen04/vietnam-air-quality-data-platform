"""
dbt Transformation DAG for Air Quality Data Platform (Airflow 3 TaskFlow API).

This DAG runs after hourly ingestion to transform raw data into analytics-ready models:
- dbt deps: Install required dbt packages
- dbt seed: Load seed data (if any)
- dbt run: Execute staging, intermediate, and marts models

Schedule: Every hour at minute 45 — runs after ingestion (15-min cycle) completes
"""

from datetime import datetime, timedelta
from airflow.sdk import dag, task
import os

# Default arguments
default_args = {
    'owner': 'air-quality-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# dbt configuration
DBT_PROFILES_DIR = os.environ.get('DBT_PROFILES_DIR', '/opt/dbt/dbt_tranform')
DBT_PROJECT_DIR = os.environ.get('DBT_PROJECT_DIR', '/opt/dbt/dbt_tranform')
DBT_TARGET = os.environ.get('DBT_TARGET', 'production')

# Environment variables for dbt
DBT_ENV_VARS = {
    'CLICKHOUSE_HOST': os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
    'CLICKHOUSE_PORT': os.environ.get('CLICKHOUSE_PORT', '8123'),
    'CLICKHOUSE_USER': os.environ.get('CLICKHOUSE_USER', 'admin'),
    'CLICKHOUSE_PASSWORD': os.environ.get('CLICKHOUSE_PASSWORD', 'admin'),
    'CLICKHOUSE_DB': os.environ.get('CLICKHOUSE_DB', 'airquality'),
    'DBT_LOG_PATH': '/tmp/dbt_logs',
    'DBT_TARGET_PATH': '/tmp/dbt_target',
    'DBT_PACKAGES_INSTALL_PATH': '/tmp/dbt_packages',
}


def build_env_command() -> str:
    """Build environment variable export commands."""
    return ' && '.join([f"export {k}='{v}'" for k, v in DBT_ENV_VARS.items()])


@dag(
    default_args=default_args,
    description='dbt transformation for air quality data — runs hourly at minute 45 after ingestion completes',
    schedule='45 * * * *',  # Every hour at minute 45 (after 15-min ingestion cycle)
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    tags=['transform', 'dbt', 'hourly', 'air-quality'],
)
def dag_transform():
    """dbt transformation DAG — runs hourly at minute 45."""

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
    def check_dbt_ready():
        """Check if dbt project is ready and dependencies are installed."""
        import os
        
        # Check if dbt project exists
        if not os.path.exists(DBT_PROJECT_DIR):
            raise FileNotFoundError(f"DBT project directory not found: {DBT_PROJECT_DIR}")
        
        # Check if dbt_project.yml exists
        dbt_project_file = os.path.join(DBT_PROJECT_DIR, 'dbt_project.yml')
        if not os.path.exists(dbt_project_file):
            raise FileNotFoundError(f"dbt_project.yml not found: {dbt_project_file}")
        
        print(f"DBT project is ready at: {DBT_PROJECT_DIR}")
        return True

    @task
    def dbt_deps():
        """dbt deps - Install required packages."""
        import subprocess
        
        env = os.environ.copy()
        env.update(DBT_ENV_VARS)
        
        cmd = f"""
{build_env_command()} && \
cd {DBT_PROJECT_DIR} && \
dbt deps --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}
"""
        
        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True,
            timeout=600
        )
        if result.returncode != 0:
            print(f"Error:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
            raise Exception("dbt deps failed")
        print("dbt deps completed")

    @task
    def dbt_seed():
        """dbt seed - Load seed data (if any)."""
        import subprocess
        
        env = os.environ.copy()
        env.update(DBT_ENV_VARS)
        
        cmd = f"""
{build_env_command()} && \
cd {DBT_PROJECT_DIR} && \
dbt seed --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET} --select seeds
"""
        
        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True,
            timeout=600
        )
        if result.returncode != 0:
            print(f"Error:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
            raise Exception("dbt seed failed")
        print("dbt seed completed")

    @task
    def dbt_run_staging():
        """dbt run staging models."""
        import subprocess
        
        env = os.environ.copy()
        env.update(DBT_ENV_VARS)
        
        cmd = f"""
{build_env_command()} && \
cd {DBT_PROJECT_DIR} && \
dbt run --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET} --select staging
"""
        
        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True,
            timeout=1800
        )
        if result.returncode != 0:
            print(f"Error:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
            raise Exception("dbt run staging failed")
        print("dbt run staging completed")

    @task
    def dbt_run_intermediate():
        """dbt run intermediate models."""
        import subprocess
        
        env = os.environ.copy()
        env.update(DBT_ENV_VARS)
        
        cmd = f"""
{build_env_command()} && \
cd {DBT_PROJECT_DIR} && \
dbt run --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET} --select intermediate
"""
        
        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True,
            timeout=1800
        )
        if result.returncode != 0:
            print(f"Error:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
            raise Exception("dbt run intermediate failed")
        print("dbt run intermediate completed")

    @task
    def dbt_run_marts():
        """dbt run marts models."""
        import subprocess
        
        env = os.environ.copy()
        env.update(DBT_ENV_VARS)
        
        cmd = f"""
{build_env_command()} && \
cd {DBT_PROJECT_DIR} && \
dbt run --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET} --select marts
"""
        
        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True,
            timeout=1800
        )
        if result.returncode != 0:
            print(f"Error:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
            raise Exception("dbt run marts failed")
        print("dbt run marts completed")

    @task
    def dbt_test():
        """dbt test - Run tests."""
        import subprocess
        
        env = os.environ.copy()
        env.update(DBT_ENV_VARS)
        
        cmd = f"""
{build_env_command()} && \
cd {DBT_PROJECT_DIR} && \
dbt test --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}
"""
        
        result = subprocess.run(
            cmd,
            shell=True,
            env=env,
            capture_output=True,
            text=True,
            timeout=1800
        )
        if result.returncode != 0:
            print(f"Error:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
            raise Exception("dbt test failed")
        print("dbt test completed")

    @task
    def log_dbt_stats():
        """Log dbt transformation statistics."""
        import requests
        
        clickhouse_host = os.environ.get('CLICKHOUSE_HOST', 'clickhouse')
        clickhouse_port = os.environ.get('CLICKHOUSE_PORT', '8123')
        clickhouse_user = os.environ.get('CLICKHOUSE_USER', 'admin')
        clickhouse_password = os.environ.get('CLICKHOUSE_PASSWORD', 'admin')
        clickhouse_db = os.environ.get('CLICKHOUSE_DB', 'airquality')
        
        url = f"http://{clickhouse_host}:{clickhouse_port}/?user={clickhouse_user}&password={clickhouse_password}"
        
        stats = {}
        
        try:
            # Get staging tables count
            query = f"SELECT count(*) FROM system.tables WHERE database = '{clickhouse_db}' AND name LIKE 'stg_%'"
            response = requests.get(f"{url}&query={query}", timeout=30)
            if response.status_code == 200:
                lines = response.text.strip().split('\n')
                if len(lines) > 1:
                    stats['staging_tables'] = int(lines[1].strip())
            
            # Get intermediate tables count
            query = f"SELECT count(*) FROM system.tables WHERE database = '{clickhouse_db}' AND name LIKE 'int_%'"
            response = requests.get(f"{url}&query={query}", timeout=30)
            if response.status_code == 200:
                lines = response.text.strip().split('\n')
                if len(lines) > 1:
                    stats['intermediate_tables'] = int(lines[1].strip())
            
            # Get marts tables count
            query = f"SELECT count(*) FROM system.tables WHERE database = '{clickhouse_db}' AND name LIKE 'fct_%'"
            response = requests.get(f"{url}&query={query}", timeout=30)
            if response.status_code == 200:
                lines = response.text.strip().split('\n')
                if len(lines) > 1:
                    stats['marts_tables'] = int(lines[1].strip())
            
            print(f"dbt transformation statistics: {stats}")
            
        except Exception as e:
            print(f"Error getting dbt stats: {e}")
        
        return stats

    @task
    def update_transform_control():
        """Update ingestion_control for dbt transformation run."""
        import sys
        sys.path.insert(0, '/opt/python/jobs')
        from common.ingestion_control import update_control as _update
        _update(source='dbt_transform', records_ingested=0, success=True)
        print("Updated ingestion_control for dbt_transform")

    @task
    def log_completion():
        """Log completion message."""
        print("dbt transformation completed")

    # Define task dependencies
    check_clickhouse = check_clickhouse_connection()
    check_dbt = check_dbt_ready()
    deps = dbt_deps()
    seed = dbt_seed()
    staging = dbt_run_staging()
    intermediate = dbt_run_intermediate()
    marts = dbt_run_marts()
    test = dbt_test()
    stats = log_dbt_stats()
    update_transform_control = update_transform_control()
    completion = log_completion()

    check_clickhouse >> check_dbt >> deps >> seed >> staging >> intermediate >> marts >> test >> stats >> update_transform_control >> completion

dag_transform = dag_transform()
