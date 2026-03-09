"""
Metadata Update DAG for Air Quality Data Platform (Airflow 3 TaskFlow API).

This DAG runs daily to refresh metadata:
- OpenAQ parameters (pollutants/measurements types)
- OpenAQ locations (air quality monitoring locations in Vietnam)
- OpenAQ sensors (sensors associated with locations)
- AQICN stations (air quality stations in Vietnam)

Schedule: Daily at 01:00
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
    'retry_delay': timedelta(minutes=5),
}

# Python jobs directory in Airflow container
PYTHON_JOBS_DIR = os.environ.get('PYTHON_JOBS_DIR', '/opt/python/jobs/')
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
    description='Daily refresh of air quality metadata from OpenAQ and AQICN',
    schedule='0 1 * * *',  # Daily at 01:00
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    tags=['metadata', 'daily', 'refresh', 'air-quality'],
)
def dag_metadata_update():
    """Metadata update DAG using Airflow 3 TaskFlow API."""

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
    def refresh_openaq_parameters():
        """Refresh OpenAQ parameters."""
        import subprocess
        
        env = os.environ.copy()
        env.update(get_job_env_vars())
        
        cmd = f"cd {PYTHON_PATH} && python jobs/openaq/ingest_parameters.py --mode rewrite"
        
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
        print(f"OpenAQ parameters refresh completed")

    @task
    def refresh_openaq_locations():
        """Refresh OpenAQ locations."""
        import subprocess
        
        env = os.environ.copy()
        env.update(get_job_env_vars())
        
        cmd = f"cd {PYTHON_PATH} && python jobs/openaq/ingest_locations.py --mode rewrite"
        
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
        print(f"OpenAQ locations refresh completed")

    @task
    def refresh_openaq_sensors():
        """Refresh OpenAQ sensors."""
        import subprocess
        
        env = os.environ.copy()
        env.update(get_job_env_vars())
        
        cmd = f"cd {PYTHON_PATH} && python jobs/openaq/ingest_sensors.py --mode rewrite"
        
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
        print(f"OpenAQ sensors refresh completed")

    @task
    def refresh_aqicn_stations():
        """Refresh AQICN stations."""
        import subprocess
        
        env = os.environ.copy()
        env.update(get_job_env_vars())
        
        cmd = f"cd {PYTHON_PATH} && python jobs/aqicn/ingest_stations.py --mode rewrite"
        
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
        print(f"AQICN stations refresh completed")

    @task
    def log_metadata_stats():
        """Log metadata statistics after refresh."""
        import requests
        
        clickhouse_host = os.environ.get('CLICKHOUSE_HOST', 'clickhouse')
        clickhouse_port = os.environ.get('CLICKHOUSE_PORT', '8123')
        clickhouse_user = os.environ.get('CLICKHOUSE_USER', 'admin')
        clickhouse_password = os.environ.get('CLICKHOUSE_PASSWORD', 'admin')
        
        url = f"http://{clickhouse_host}:{clickhouse_port}/?user={clickhouse_user}&password={clickhouse_password}"
        
        stats = {}
        
        try:
            # Get OpenAQ parameters count
            query = "SELECT count(*) FROM raw_openaq_parameters"
            response = requests.get(f"{url}&query={query}", timeout=30)
            if response.status_code == 200:
                lines = response.text.strip().split('\n')
                if len(lines) > 1:
                    stats['openaq_parameters'] = int(lines[1].strip())
            
            # Get OpenAQ locations count
            query = "SELECT count(*) FROM raw_openaq_locations"
            response = requests.get(f"{url}&query={query}", timeout=30)
            if response.status_code == 200:
                lines = response.text.strip().split('\n')
                if len(lines) > 1:
                    stats['openaq_locations'] = int(lines[1].strip())
            
            # Get OpenAQ sensors count
            query = "SELECT count(*) FROM raw_openaq_sensors"
            response = requests.get(f"{url}&query={query}", timeout=30)
            if response.status_code == 200:
                lines = response.text.strip().split('\n')
                if len(lines) > 1:
                    stats['openaq_sensors'] = int(lines[1].strip())
            
            # Get AQICN stations count
            query = "SELECT count(*) FROM raw_aqicn_stations"
            response = requests.get(f"{url}&query={query}", timeout=30)
            if response.status_code == 200:
                lines = response.text.strip().split('\n')
                if len(lines) > 1:
                    stats['aqicn_stations'] = int(lines[1].strip())
            
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

    refresh_params = refresh_openaq_parameters()
    refresh_locations = refresh_openaq_locations()
    refresh_sensors = refresh_openaq_sensors()
    refresh_stations = refresh_aqicn_stations()

    log_stats = log_metadata_stats()
    log_done = log_completion()

    # OpenAQ chain
    check_clickhouse >> refresh_params >> refresh_locations >> refresh_sensors

    # AQICN parallel branch
    check_clickhouse >> refresh_stations

    # Join
    [refresh_sensors, refresh_stations] >> log_stats >> log_done


dag_metadata_update = dag_metadata_update()
