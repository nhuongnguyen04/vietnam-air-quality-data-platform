"""
Hourly Ingestion DAG for Air Quality Data Platform (Airflow 3 TaskFlow API).

This DAG runs every hour to ingest the latest measurements from:
- AQI.in (~540 Vietnam monitoring stations via widget scraper)
- OpenWeather Air Pollution API (62 Vietnam provinces)
- TomTom Traffic Flow API (3-hourly sampling)
- Traffic Pattern Engine (1-hourly interpolation via Python)

Schedule: Every hour (0 * * * *)
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
    """Get environment variables at execution time (not parse time)."""
    return {
        'CLICKHOUSE_HOST': os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        'CLICKHOUSE_PORT': os.environ.get('CLICKHOUSE_PORT', '8123'),
        'CLICKHOUSE_USER': os.environ.get('CLICKHOUSE_USER', 'admin'),
        'CLICKHOUSE_PASSWORD': os.environ.get('CLICKHOUSE_PASSWORD', 'admin123456'),
        'CLICKHOUSE_DB': os.environ.get('CLICKHOUSE_DB', 'air_quality'),
        'OPENWEATHER_API_TOKEN': os.environ.get('OPENWEATHER_API_TOKEN', ''),
        'TOMTOM_API_KEY': os.environ.get('TOMTOM_API_KEY', ''),
    }


@dag(
    default_args=default_args,
    description='Ingestion of air quality, weather, and traffic data every hour — triggers dag_transform on completion',
    schedule='0 * * * *',
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    tags=['ingestion', 'hourly', 'triggers-transform', 'air-quality', 'weather', 'traffic'],
)
def dag_ingest_hourly():
    """Hourly ingestion DAG for AQI.in, OpenWeather (AQI + Weather), and TomTom (Traffic)."""

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
    def run_openweather_aqi_ingestion():
        """Run OpenWeather air pollution ingestion."""
        import subprocess

        env = os.environ.copy()
        env.update(get_job_env_vars())

        cmd = f"cd {PYTHON_PATH} && python jobs/openweather/ingest_measurements.py --mode incremental"

        result = subprocess.run(cmd, shell=True, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise Exception(f"Command failed: {cmd}")
        print("OpenWeather AQI ingestion completed")

    @task
    def run_openweather_weather_ingestion():
        """Run OpenWeather meteorology ingestion (Temp, Wind, Hum)."""
        import subprocess

        env = os.environ.copy()
        env.update(get_job_env_vars())

        cmd = f"cd {PYTHON_PATH} && python jobs/openweather/ingest_weather.py"

        result = subprocess.run(cmd, shell=True, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise Exception(f"Command failed: {cmd}")
        print("OpenWeather Weather ingestion completed")

    @task
    def run_tomtom_traffic_ingestion(**context):
        """Run TomTom Traffic Ingestion (Every 3 hours)."""
        import subprocess
        from datetime import datetime
        
        # Logic to run only every 3 hours to stay within API limits
        data_interval_start = context.get('data_interval_start')
        hour = data_interval_start.hour if data_interval_start else datetime.now().hour
        
        if hour % 3 == 0:
            env = os.environ.copy()
            env.update(get_job_env_vars())

            cmd = f"cd {PYTHON_PATH} && python jobs/traffic/ingest_tomtom_traffic.py"

            result = subprocess.run(cmd, shell=True, env=env, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"Error: {result.stderr}")
                raise Exception(f"Command failed: {cmd}")
            print("TomTom Traffic ingestion completed")
            return True
        else:
            print(f"Skipping traffic ingestion (Hour {hour} not divisible by 3)")
            return False

    @task
    def run_traffic_calculation(should_run_calc: bool):
        """Run Traffic Pattern Enrichment calculation in Python."""
        import subprocess
        
        # We always run the calculation to ensure we interpolate against last 24h
        # even if we didn't ingest new raw data this hour (to fill the 1h/2h slots)
        env = os.environ.copy()
        env.update(get_job_env_vars())

        cmd = f"cd {PYTHON_PATH} && python jobs/traffic/calculate_hourly_traffic.py"

        result = subprocess.run(cmd, shell=True, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
            raise Exception(f"Command failed: {cmd}")
        print("Traffic Hourly Calculation completed")

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
        print("Hourly ingestion cycle completed (AQI + Weather + Traffic)")

    trigger_transform = TriggerDagRunOperator(
        task_id='trigger_transform',
        trigger_dag_id='dag_transform',
        wait_for_completion=False,
    )

    # Dependencies
    check_ch = check_clickhouse_connection()
    
    aqiin = run_aqiin_measurements_ingestion()
    ow_aqi = run_openweather_aqi_ingestion()
    ow_weather = run_openweather_weather_ingestion()
    tt_traffic = run_tomtom_traffic_ingestion()
    
    # We pass the result of traffic ingestion, but calculation script 
    # handles lookback itself, so it can run every hour.
    traffic_calc = run_traffic_calculation(tt_traffic)
    
    update_control = update_ingestion_control()
    completion = log_completion()

    check_ch >> [aqiin, ow_aqi, ow_weather, tt_traffic]
    tt_traffic >> traffic_calc
    [aqiin, ow_aqi, ow_weather, traffic_calc] >> update_control >> completion
    completion >> trigger_transform


dag_ingest_hourly = dag_ingest_hourly()
