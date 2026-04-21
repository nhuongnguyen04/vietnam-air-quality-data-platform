"""
OpenMetadata Catalog Curation DAG.

Runs after dag_transform (every hour at minute 30):
- dag_transform: dbt run + dbt test (minute 30)
- dag_openmetadata_curation: OM curation sync (minute 35)

Schedule: 35 * * * * (runs 5 minutes after dag_transform)
Owner: air-quality-team
"""

from datetime import datetime, timedelta
from airflow.sdk import dag, task
import os


default_args = {
    'owner': 'air-quality-team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    description='OpenMetadata catalog curation — owners, tags, descriptions, glossary',
    schedule='35 * * * *',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    tags=['openmetadata', 'curation', 'catalog'],
)
def dag_openmetadata_curation():
    """Sync catalog curation to OpenMetadata after dbt transformation."""

    OM_URL = os.environ.get('OPENMETADATA_URL', 'http://openmetadata:8585/api')
    OM_USER = os.environ.get('OM_ADMIN_USER')
    OM_PASS = os.environ.get('OM_ADMIN_PASSWORD')
    if not OM_USER or not OM_PASS:
        raise RuntimeError("OM_ADMIN_USER and OM_ADMIN_PASSWORD must be set")

    @task
    def check_openmetadata_health():
        """Check if OpenMetadata server is healthy before running curation."""
        import requests

        url = f"{OM_URL.rstrip('/api')}/"
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            print(f"OpenMetadata server healthy: {resp.status_code}")
            return True
        except Exception as e:
            print(f"⚠️  OpenMetadata health check failed: {e} — curation will continue anyway")
            return False

    @task
    def openmetadata_curation_sync():
        """Run catalog curation bulk script against OM REST API."""
        import subprocess
        import sys

        script = "/opt/python/jobs/jobs/openmetadata/curation_bulk.py"
        if not os.path.exists(script):
            print(f"⚠️  Curation script not found at {script}, skipping")
            return

        env = os.environ.copy()
        env.update({
            'OM_URL': OM_URL,
            'OM_ADMIN_USER': OM_USER,
            'OM_ADMIN_PASSWORD': OM_PASS,
        })

        result = subprocess.run(
            [sys.executable, script],
            env=env,
            capture_output=True,
            text=True,
            timeout=300,
        )
        # Non-fatal: log but don't block on failure
        if result.returncode != 0:
            print(f"⚠️  Curation sync STDOUT: {result.stdout}")
            print(f"⚠️  Curation sync STDERR: {result.stderr}")
            # Don't raise — OM curation failures should not block pipeline
            return
        print(f"✅ OpenMetadata curation sync completed:\n{result.stdout}")

    @task
    def openmetadata_glossary_sync():
        """Ensure AQI glossary terms exist in OM (idempotent — safe to run repeatedly)."""
        import subprocess
        import sys

        script = "/opt/python/jobs/jobs/openmetadata/setup_glossary.py"
        if not os.path.exists(script):
            print(f"⚠️  Glossary script not found at {script}, skipping")
            return

        env = os.environ.copy()
        env.update({
            'OM_URL': OM_URL,
            'OM_ADMIN_USER': OM_USER,
            'OM_ADMIN_PASSWORD': OM_PASS,
        })

        result = subprocess.run(
            [sys.executable, script],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            print(f"⚠️  Glossary sync STDOUT: {result.stdout}")
            print(f"⚠️  Glossary sync STDERR: {result.stderr}")
            return
        print(f"✅ Glossary sync completed:\n{result.stdout}")

    @task
    def log_completion():
        """Log completion."""
        print("dag_openmetadata_curation completed — catalog curated")

    # Task dependencies
    check_om = check_openmetadata_health()
    curation = openmetadata_curation_sync()
    glossary = openmetadata_glossary_sync()
    done = log_completion()

    check_om >> [curation, glossary] >> done


dag_openmetadata_curation = dag_openmetadata_curation()
