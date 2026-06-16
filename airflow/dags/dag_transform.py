"""
dbt transformation DAG for the warehouse build pipeline.

This DAG is trigger-driven. In the current architecture it is primarily invoked
by `dag_sync_gdrive` after GitHub Actions land new CSV files in Google Drive.
It can also be triggered manually for ad hoc rebuilds.
"""

import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, get_current_context, task

# Add python_jobs to path for imports in container / local fallback
PYTHON_JOBS_DIR = os.environ.get('PYTHON_JOBS_DIR', '/opt/python/jobs')
if not os.path.exists(PYTHON_JOBS_DIR):
    PYTHON_JOBS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../python_jobs'))
sys.path.insert(0, PYTHON_JOBS_DIR)

from common.config import require_env, get_clickhouse_env_vars

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


def _load_json_document(filepath: str) -> tuple[dict, bool]:
    """Load a JSON object, recovering dbt artifacts with trailing data."""
    with open(filepath, encoding='utf-8') as handle:
        content = handle.read()

    try:
        return json.loads(content), False
    except json.JSONDecodeError as exc:
        start = len(content) - len(content.lstrip())
        try:
            data, end = json.JSONDecoder().raw_decode(content, start)
        except json.JSONDecodeError as recovery_exc:
            raise exc from recovery_exc

        if not content[end:].strip():
            raise exc from None

        print(
            f"{filepath} contains extra data after the first JSON document; "
            "rewriting a clean artifact"
        )
        return data, True


def _write_json_atomic(filepath: str, data: dict) -> None:
    """Write a JSON artifact atomically to avoid leaving partial files."""
    directory = os.path.dirname(filepath)
    temp_path = ''
    try:
        with tempfile.NamedTemporaryFile(
            'w',
            encoding='utf-8',
            dir=directory,
            prefix=f".{os.path.basename(filepath)}.",
            suffix='.tmp',
            delete=False,
        ) as handle:
            temp_path = handle.name
            json.dump(data, handle)
            handle.write('\n')
        os.replace(temp_path, filepath)
    finally:
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)


def get_dbt_env_vars() -> dict[str, str]:
    env = get_clickhouse_env_vars()
    env.update({
        'DBT_LOG_PATH': os.path.join(DBT_PROJECT_DIR, 'logs'),
        'DBT_TARGET_PATH': os.path.join(DBT_PROJECT_DIR, 'target'),
        'DBT_PACKAGES_INSTALL_PATH': os.environ.get(
            'DBT_PACKAGES_INSTALL_PATH', '/opt/dbt/.cache/dbt_packages'
        ),
    })
    return env


def get_clickhouse_settings() -> tuple[str, int, str, str, str]:
    env = get_clickhouse_env_vars()
    return (
        env['CLICKHOUSE_HOST'],
        int(env['CLICKHOUSE_PORT']),
        env['CLICKHOUSE_USER'],
        env['CLICKHOUSE_PASSWORD'],
        env['CLICKHOUSE_DB'],
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
    description='dbt transformation DAG — runs hourly to build the warehouse',
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
    def dbt_build():
        """Run seed, compile, run, and test models in a single unified build step with full concurrency."""
        context = get_current_context()
        dag_run = context.get('dag_run')
        dag_conf = dag_run.conf if dag_run else {}
        dbt_vars = {}
        if dag_conf and dag_conf.get('raw_sync_run_id'):
            dbt_vars['staging_sync_run_id'] = dag_conf['raw_sync_run_id']

        result = run_dbt_command(
            ['dbt', 'build', '--profiles-dir', DBT_PROFILES_DIR, '--target', DBT_TARGET],
            timeout=3600,
            dbt_vars=dbt_vars or None,
        )
        print_dbt_result(result, "dbt build failed", "dbt build completed")
        return True

    @task
    def dbt_docs_generate():
        """Generate docs artifacts without blocking the rest of the DAG on failure."""
        context = get_current_context()
        dag_run = context.get('dag_run')
        if dag_run and getattr(dag_run, 'run_type', None) == 'scheduled':
            print("Skipping dbt docs generate for scheduled runs to save resources.")
            return False

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
        target_dir = os.path.join(DBT_PROJECT_DIR, 'target')
        db_name = os.environ.get('CLICKHOUSE_DB', 'air_quality')

        def patch_json(filepath: str, item_key: str | None):
            if not os.path.exists(filepath):
                return
            data, needs_rewrite = _load_json_document(filepath)
            database_changed = False
            for group in ['nodes', 'sources']:
                for node in data.get(group, {}).values():
                    if item_key is None:
                        if node.get('database') == '':
                            node['database'] = db_name
                            database_changed = True
                    else:
                        if node.get(item_key, {}).get('database') == '':
                            node[item_key]['database'] = db_name
                            database_changed = True
            if needs_rewrite or database_changed:
                _write_json_atomic(filepath, data)
            if database_changed:
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

            # Query previous last_success for dag_transform to compute processed raw records count
            try:
                last_success_query = f"SELECT max(last_success) FROM {clickhouse_db}.ingestion_control WHERE source = 'dag_transform'"
                last_success_res = client.query(last_success_query)
                last_success = last_success_res.result_rows[0][0] if last_success_res.result_rows else None
                last_success_str = last_success.strftime('%Y-%m-%d %H:%M:%S') if (last_success and hasattr(last_success, 'year') and last_success.year > 1970) else '1970-01-01 00:00:00'

                raw_tables = [
                    'raw_aqiin_measurements',
                    'raw_waqi_measurements',
                    'raw_openweather_measurements',
                    'raw_tomtom_traffic',
                    'raw_openweather_meteorology'
                ]
                processed_records = 0
                for tbl in raw_tables:
                    check_tbl = client.query(f"SELECT count() FROM system.tables WHERE database = '{clickhouse_db}' AND name = '{tbl}'")
                    if check_tbl.result_rows[0][0] > 0:
                        cnt_res = client.query(f"SELECT count() FROM {clickhouse_db}.{tbl} WHERE raw_loaded_at > '{last_success_str}'")
                        processed_records += int(cnt_res.result_rows[0][0])
                stats['processed_records'] = processed_records
            except Exception as e:
                print(f"Error calculating processed raw records: {e}")
                stats['processed_records'] = 0

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
        stats = ti.xcom_pull(task_ids='log_dbt_stats') or {}
        transformed_units = 0
        if isinstance(stats, dict):
            transformed_units = int(stats.get('processed_records', 0) or 0)

        critical_task_ids = [
            'check_clickhouse_connection',
            'check_dbt_ready',
            'dbt_deps',
            'dbt_build',
            'log_dbt_stats',
        ]
        failed_tasks = [
            task_id for task_id in critical_task_ids
            if ti.xcom_pull(task_ids=task_id) is None
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
    build = dbt_build()
    docs = dbt_docs_generate()
    patch = patch_dbt_artifacts()
    stats = log_dbt_stats()
    update_control = update_transform_control()
    completion = log_completion()

    check_clickhouse >> check_dbt >> deps >> build
    build >> docs >> patch >> stats
    [build, stats] >> update_control >> completion


dag_transform = dag_transform()
