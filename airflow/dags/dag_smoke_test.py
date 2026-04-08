"""
E2E Smoke Test DAG for Air Quality Alert Pipeline.

schedule: None  (triggered manually via Airflow UI or API)
max_active_runs: 1

Tests the alert pipeline without any external dependencies:
1. Inserts a test row into mart_air_quality__alerts
2. Verifies the row is readable
3. Cleans up the test row

Does NOT send Telegram messages (D-12 compliance).
"""

from datetime import datetime, timedelta
from airflow.sdk import dag, task
import os
import time
import random

# ClickHouse connection — shared by all tasks in this DAG
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "admin123456")
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "air_quality")

default_args = {
    "owner": "air-quality-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _get_client():
    """Return a clickhouse_connect HttpClient connected to the air_quality DB."""
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


@dag(
    default_args=default_args,
    description="E2E smoke test — insert test alert into mart_air_quality__alerts, verify readability, cleanup",
    schedule=None,          # On-demand only (D-11)
    start_date=datetime(2026, 4, 6),
    catchup=False,
    max_active_runs=1,     # Prevent parallel runs (D-11)
    tags=["smoke-test", "e2e", "alert"],
)
def dag_smoke_test():
    """Smoke test the alert pipeline end-to-end."""

    @task
    def insert_test_alert():
        """Insert a synthetic test alert row into mart_air_quality__alerts.

        Uses a randomised station ID to avoid collisions on rapid re-runs.
        Row values are deliberately high-AQI to simulate a real threshold breach.
        """
        import clickhouse_connect

        client = _get_client()

        # Random suffix avoids collisions on close re-runs
        test_id = f"SMOKE_TEST_{int(time.time()) % 100000:05d}_{random.randint(100, 999)}"
        now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        sql = f"""
        INSERT INTO mart_air_quality__alerts (
            station_id,
            datetime_hour,
            normalized_aqi,
            threshold_breached,
            dominant_pollutant,
            source,
            sensor_quality_tier
        ) VALUES (
            '{test_id}',
            '{now_str}',
            250,
            '150',
            'pm25',
            'smoke_test',
            'government'
        )
        """

        client.command(sql)
        print(f"Inserted test alert: station_id={test_id}, normalized_aqi=250, threshold_breached='150'")

        return {"station_id": test_id, "aqi": 250, "threshold": "150"}

    @task
    def verify_alert_created(task_instance):
        """SELECT from mart_air_quality__alerts to confirm the test row exists and is readable."""
        import clickhouse_connect

        # Receive station_id from insert_test_alert via xcom (Airflow TaskFlow pattern)
        test_station_id = task_instance.xcom_pull(task_ids="insert_test_alert")["station_id"]

        client = _get_client()

        result = client.query(f"""
            SELECT
                station_id,
                normalized_aqi,
                threshold_breached,
                dominant_pollutant,
                datetime_hour
            FROM mart_air_quality__alerts
            WHERE station_id = '{test_station_id}'
              AND threshold_breached = '150'
            LIMIT 1
        """)

        rows = result.result_rows
        if not rows:
            raise AssertionError(
                f"Alert not found in mart_air_quality__alerts for station_id={test_station_id}"
            )

        row = rows[0]
        print(
            f"Verified alert readable: station_id={row[0]}, "
            f"normalized_aqi={row[1]}, threshold_breached={row[2]}, "
            f"dominant_pollutant={row[3]}, datetime_hour={row[4]}"
        )

        return {
            "verified": True,
            "station_id": row[0],
            "normalized_aqi": row[1],
            "threshold_breached": row[2],
        }

    @task
    def cleanup_test_alert(task_instance):
        """Delete the test row from mart_air_quality__alerts to leave the mart clean after test."""
        import clickhouse_connect

        test_station_id = task_instance.xcom_pull(task_ids="insert_test_alert")["station_id"]

        client = _get_client()
        client.command(
            f"ALTER TABLE mart_air_quality__alerts DELETE WHERE station_id = '{test_station_id}'"
        )
        print(f"Cleaned up test alert: station_id={test_station_id}")

        return {"cleaned": True, "station_id": test_station_id}

    # Task chain: insert → verify → cleanup
    insert_result = insert_test_alert()
    verify_result = verify_alert_created(insert_result)
    cleanup_test_alert(verify_result)


dag_smoke_test = dag_smoke_test()