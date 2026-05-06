"""
dbt transformation DAG for the warehouse build pipeline.

This DAG is trigger-driven. In the current architecture it is primarily invoked
by `dag_sync_gdrive` after GitHub Actions land new CSV files in Google Drive.
It can also be triggered manually for ad hoc rebuilds.
"""

from datetime import datetime, timedelta
import json
from pathlib import Path
import os
import subprocess

from airflow.sdk import dag, task, get_current_context


default_args = {
    'owner': 'air-quality-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

DBT_PROFILES_DIR = os.environ.get('DBT_PROFILES_DIR', '/opt/dbt/dbt_tranform')
DBT_PROJECT_DIR = os.environ.get('DBT_PROJECT_DIR', '/opt/dbt/dbt_tranform')
DBT_TARGET = os.environ.get('DBT_TARGET', 'production')


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if value in (None, ''):
        raise RuntimeError(f"{name} environment variable is required")
    return value


def get_dbt_env_vars() -> dict[str, str]:
    return {
        'CLICKHOUSE_HOST': os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        'CLICKHOUSE_PORT': os.environ.get('CLICKHOUSE_PORT', '8123'),
        'CLICKHOUSE_USER': os.environ.get('CLICKHOUSE_USER', 'admin'),
        'CLICKHOUSE_PASSWORD': _require_env('CLICKHOUSE_PASSWORD'),
        'CLICKHOUSE_DB': os.environ.get('CLICKHOUSE_DB', 'air_quality'),
        'DBT_LOG_PATH': os.path.join(DBT_PROJECT_DIR, 'logs'),
        'DBT_TARGET_PATH': os.path.join(DBT_PROJECT_DIR, 'target'),
        'DBT_PACKAGES_INSTALL_PATH': os.environ.get(
            'DBT_PACKAGES_INSTALL_PATH', '/opt/dbt/.cache/dbt_packages'
        ),
    }


def get_clickhouse_settings() -> tuple[str, int, str, str, str]:
    return (
        os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        int(os.environ.get('CLICKHOUSE_PORT', '8123')),
        os.environ.get('CLICKHOUSE_USER', 'admin'),
        _require_env('CLICKHOUSE_PASSWORD'),
        os.environ.get('CLICKHOUSE_DB', 'air_quality'),
    )


def run_dbt_command(
    command: list[str],
    *,
    timeout: int,
    dbt_vars: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update(get_dbt_env_vars())
    final_command = list(command)
    if dbt_vars:
        final_command.extend(['--vars', json.dumps(dbt_vars)])
    return subprocess.run(
        final_command,
        cwd=DBT_PROJECT_DIR,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def print_dbt_result(
    result: subprocess.CompletedProcess[str],
    error_message: str,
    success_message: str,
    *,
    allow_failure: bool = False,
) -> bool:
    if result.returncode != 0:
        print(f"Error:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
        if allow_failure:
            print(error_message)
            return False
        raise Exception(error_message)
    print(success_message)
    return True


@dag(
    default_args=default_args,
    description='dbt transformation DAG — primarily triggered by dag_sync_gdrive after new landing-zone data arrives',
    schedule=None,
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    tags=['transform', 'dbt', 'triggered', 'air-quality', 'gdrive-sync'],
)
def dag_transform():
    """dbt transformation DAG for trigger-based warehouse builds."""

    @task
    def check_clickhouse_connection():
        """Check if ClickHouse is accessible."""
        import requests

        clickhouse_host = os.environ.get('CLICKHOUSE_HOST', 'clickhouse')
        clickhouse_port = os.environ.get('CLICKHOUSE_PORT', '8123')
        url = f"http://{clickhouse_host}:{clickhouse_port}/ping"

        response = requests.get(url, timeout=10)
        response.raise_for_status()
        print(f"ClickHouse connection successful: {response.status_code}")
        return True

    @task
    def check_dbt_ready():
        """Check if the dbt project directory and project file exist."""
        dbt_project_file = os.path.join(DBT_PROJECT_DIR, 'dbt_project.yml')
        if not os.path.exists(DBT_PROJECT_DIR):
            raise FileNotFoundError(f"DBT project directory not found: {DBT_PROJECT_DIR}")
        if not os.path.exists(dbt_project_file):
            raise FileNotFoundError(f"dbt_project.yml not found: {dbt_project_file}")

        print(f"DBT project is ready at: {DBT_PROJECT_DIR}")
        return True

    @task
    def dbt_deps():
        """Run dbt deps only when packages.yml has changed or packages are missing."""
        packages_file = Path(DBT_PROJECT_DIR) / 'packages.yml'
        lock_file = Path(DBT_PROJECT_DIR) / 'package-lock.yml'
        packages_dir = Path(get_dbt_env_vars()['DBT_PACKAGES_INSTALL_PATH'])

        if not packages_file.exists():
            print("dbt deps skipped; packages.yml is not present")
            return False

        if (
            lock_file.exists()
            and packages_dir.exists()
            and lock_file.stat().st_mtime >= packages_file.stat().st_mtime
        ):
            print("dbt deps skipped; package-lock.yml is current and packages cache exists")
            return False

        result = run_dbt_command(
            ['dbt', 'deps', '--profiles-dir', DBT_PROFILES_DIR, '--target', DBT_TARGET],
            timeout=600,
        )
        print_dbt_result(result, "dbt deps failed", "dbt deps completed")
        return True

    @task
    def dbt_seed():
        """Load seed data."""
        result = run_dbt_command(
            ['dbt', 'seed', '--profiles-dir', DBT_PROFILES_DIR, '--target', DBT_TARGET, '--select', 'seeds'],
            timeout=600,
        )
        print_dbt_result(result, "dbt seed failed", "dbt seed completed")

    @task
    def dbt_run_staging():
        """Run staging models."""
        context = get_current_context()
        dag_run = context.get('dag_run')
        dag_conf = dag_run.conf if dag_run else {}
        dbt_vars = {}
        if dag_conf and dag_conf.get('raw_sync_run_id'):
            dbt_vars['staging_sync_run_id'] = dag_conf['raw_sync_run_id']

        result = run_dbt_command(
            ['dbt', 'run', '--profiles-dir', DBT_PROFILES_DIR, '--target', DBT_TARGET, '--select', 'staging'],
            timeout=1800,
            dbt_vars=dbt_vars or None,
        )
        print_dbt_result(result, "dbt run staging failed", "dbt run staging completed")

    @task
    def dbt_run_intermediate():
        """Run intermediate models."""
        context = get_current_context()
        dag_run = context.get('dag_run')
        dag_conf = dag_run.conf if dag_run else {}
        dbt_vars = {}
        if dag_conf and dag_conf.get('raw_sync_run_id'):
            dbt_vars['staging_sync_run_id'] = dag_conf['raw_sync_run_id']

        result = run_dbt_command(
            ['dbt', 'run', '--profiles-dir', DBT_PROFILES_DIR, '--target', DBT_TARGET, '--select', 'intermediate'],
            timeout=1800,
            dbt_vars=dbt_vars or None,
        )
        print_dbt_result(result, "dbt run intermediate failed", "dbt run intermediate completed")

    @task
    def dbt_run_marts():
        """Run mart models."""
        context = get_current_context()
        dag_run = context.get('dag_run')
        dag_conf = dag_run.conf if dag_run else {}
        dbt_vars = {}
        if dag_conf and dag_conf.get('raw_sync_run_id'):
            dbt_vars['staging_sync_run_id'] = dag_conf['raw_sync_run_id']

        result = run_dbt_command(
            ['dbt', 'run', '--profiles-dir', DBT_PROFILES_DIR, '--target', DBT_TARGET, '--select', 'marts'],
            timeout=1800,
            dbt_vars=dbt_vars or None,
        )
        print_dbt_result(result, "dbt run marts failed", "dbt run marts completed")

    @task
    def dbt_test():
        """Run dbt tests as the blocking validation branch."""
        result = run_dbt_command(
            ['dbt', 'test', '--profiles-dir', DBT_PROFILES_DIR, '--target', DBT_TARGET],
            timeout=1800,
        )
        print_dbt_result(result, "dbt test failed", "dbt test completed")

    @task
    def dbt_docs_generate():
        """Generate docs artifacts without blocking the rest of the DAG on failure."""
        result = run_dbt_command(
            ['dbt', 'docs', 'generate', '--profiles-dir', DBT_PROFILES_DIR, '--target', DBT_TARGET],
            timeout=1800,
        )
        print_dbt_result(
            result,
            "dbt docs generate failed, continuing",
            "dbt docs generate completed",
            allow_failure=True,
        )

    @task
    def patch_dbt_artifacts():
        """Ensure generated artifacts include the ClickHouse database name."""
        import json

        target_dir = os.path.join(DBT_PROJECT_DIR, 'target')
        db_name = os.environ.get('CLICKHOUSE_DB', 'air_quality')

        def patch_json(filepath: str, item_key: str | None):
            if not os.path.exists(filepath):
                return
            with open(filepath, 'r', encoding='utf-8') as handle:
                data = json.load(handle)
            changed = False
            for group in ['nodes', 'sources']:
                for node in data.get(group, {}).values():
                    if item_key is None:
                        if node.get('database') == '':
                            node['database'] = db_name
                            changed = True
                    else:
                        if node.get(item_key, {}).get('database') == '':
                            node[item_key]['database'] = db_name
                            changed = True
            if changed:
                with open(filepath, 'w', encoding='utf-8') as handle:
                    json.dump(data, handle)
                print(f"Patched {filepath} database to {db_name}")

        patch_json(os.path.join(target_dir, 'manifest.json'), None)
        patch_json(os.path.join(target_dir, 'catalog.json'), 'metadata')
        print("DBT artifacts patch completed")

    @task
    def log_dbt_stats():
        """Log dbt transformation statistics via the ClickHouse client."""
        import clickhouse_connect

        clickhouse_host, clickhouse_port, clickhouse_user, clickhouse_password, clickhouse_db = (
            get_clickhouse_settings()
        )
        stats: dict[str, int] = {}

        client = clickhouse_connect.get_client(
            host=clickhouse_host,
            port=clickhouse_port,
            username=clickhouse_user,
            password=clickhouse_password,
            database=clickhouse_db,
        )
        try:
            table_groups = {
                'staging_tables': "stg_%",
                'intermediate_tables': "int_%",
                'mart_tables': "dm_%",
                'fact_tables': "fct_%",
            }
            for key, pattern in table_groups.items():
                result = client.query(
                    f"SELECT count(*) FROM system.tables WHERE database = '{clickhouse_db}' AND name LIKE '{pattern}'"
                )
                stats[key] = int(result.result_rows[0][0])
            stats['warehouse_tables_built'] = (
                stats['staging_tables']
                + stats['intermediate_tables']
                + stats['mart_tables']
                + stats['fact_tables']
            )
            print(f"dbt transformation statistics: {stats}")
        finally:
            client.close()

        return stats

    @task(trigger_rule='all_done')
    def update_transform_control():
        """Update ingestion_control for the transform DAG using the final task states."""
        import sys

        sys.path.insert(0, '/opt/python/jobs')
        from common.ingestion_control import update_control as _update

        context = get_current_context()
        ti = context['ti']
        dag_run = context['dag_run']
        stats = ti.xcom_pull(task_ids='log_dbt_stats') or {}
        transformed_units = 0
        if isinstance(stats, dict):
            transformed_units = int(stats.get('warehouse_tables_built', 0) or 0)

        critical_task_ids = [
            'check_clickhouse_connection',
            'check_dbt_ready',
            'dbt_deps',
            'dbt_seed',
            'dbt_run_staging',
            'dbt_run_intermediate',
            'dbt_run_marts',
            'dbt_test',
            'log_dbt_stats',
        ]
        failed_states = {'failed', 'upstream_failed'}
        task_states = {
            task_instance.task_id: task_instance.state
            for task_instance in dag_run.get_task_instances()
        }
        failed_tasks = [
            task_id for task_id in critical_task_ids
            if task_states.get(task_id) in failed_states
        ]

        success = not failed_tasks
        error_message = ''
        if failed_tasks:
            error_message = "Failed tasks: " + ", ".join(sorted(failed_tasks))

        _update(
            source='dag_transform',
            records_ingested=transformed_units,
            success=success,
            error_message=error_message,
        )
        print(
            "Updated ingestion_control for dag_transform with "
            f"records_ingested={transformed_units}, failed_tasks={failed_tasks}"
        )

    @task
    def log_completion():
        """Log completion message."""
        print("dbt transformation completed")

    check_clickhouse = check_clickhouse_connection()
    check_dbt = check_dbt_ready()
    deps = dbt_deps()
    seed = dbt_seed()
    staging = dbt_run_staging()
    intermediate = dbt_run_intermediate()
    marts = dbt_run_marts()
    test = dbt_test()
    docs = dbt_docs_generate()
    patch = patch_dbt_artifacts()
    stats = log_dbt_stats()
    update_control = update_transform_control()
    completion = log_completion()

    check_clickhouse >> check_dbt >> deps >> seed >> staging >> intermediate >> marts
    marts >> test
    marts >> docs >> patch >> stats
    [test, stats] >> update_control >> completion


dag_transform = dag_transform()
