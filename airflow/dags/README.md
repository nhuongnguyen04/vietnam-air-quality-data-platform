# Airflow DAGs Documentation

## Overview

This directory contains the checked-in Airflow orchestration layer for the Vietnam Air Quality Data Platform. In the current architecture, the primary ingest path is:

`Google Apps Script -> GitHub Actions -> Google Drive landing zone -> dag_sync_gdrive -> dag_transform`

Airflow also owns manual fallback ingestion, OpenMetadata curation, the weekly Telegram report, and an on-demand smoke test.

## DAGs Available

### 1. `dag_ingest_hourly.py` - Legacy Manual Ingestion Fallback

Manual fallback DAG for direct ingestion into ClickHouse. It is not the source-of-truth scheduled path, but it remains useful for ad hoc recovery runs.

**Schedule**: `None` (manual only)

**Tasks**:
1. Check ClickHouse connectivity
2. Run AQI.in measurements ingestion
3. Run unified OpenWeather ingestion
4. Run TomTom peak-hour ingestion or off-peak traffic generation
5. Update `ingestion_control`
6. Trigger `dag_transform`

### 2. `dag_sync_gdrive.py` - Google Drive Landing-Zone Sync

Primary Airflow entrypoint for the checked-in ingest flow. Downloads CSV files uploaded by `.github/workflows/scheduled_ingestion.yml`, loads them into ClickHouse raw tables, and triggers downstream transforms only when new files were synced.

**Schedule**: `*/5 * * * *`

**Tasks**:
1. Run `python_jobs/jobs/sync/gdrive_sync.py`
2. Parse `FILES_FOUND`, `FILES_SYNCED`, and `FILES_FAILED`
3. Update `ingestion_control` for the sync step
4. Branch on `FILES_SYNCED`
5. Trigger `dag_transform` when new data arrived

### 3. `dag_transform.py` - dbt Warehouse Build

Trigger-driven dbt pipeline for staged, intermediate, mart, and analytics models.

**Schedule**: `None` (triggered by `dag_sync_gdrive` or run manually)

**Tasks**:
1. Check ClickHouse connectivity
2. Verify the dbt project is present
3. Run `dbt deps` only when packages are missing or stale
4. Run `dbt seed`
5. Run `dbt run --select staging`
6. Run `dbt run --select intermediate`
7. Run `dbt run --select marts`
8. Run blocking `dbt test`
9. Run non-blocking `dbt docs generate`
10. Patch dbt artifacts and log warehouse build statistics
11. Update `ingestion_control` with final DAG success/failure state

### 4. `dag_openmetadata_curation.py` - OpenMetadata Governance Sync

Keeps governance entities and glossary terms aligned in OpenMetadata after warehouse updates.

**Schedule**: `35 * * * *`

**Tasks**:
1. Check OpenMetadata health
2. Run governance bootstrap sync
3. Run glossary bootstrap sync
4. Log completion

### 5. `dag_weekly_report.py` - Weekly Telegram Report

Builds the weekly reporting message from warehouse marts and sends it through the legacy Python Telegram client.

**Schedule**: `0 2 * * 1` (Monday 02:00 UTC / 09:00 UTC+7)

**Tasks**:
1. Query city-level AQI averages
2. Query top 5 worst wards
3. Query week-over-week trend deltas
4. Query dominant pollutants
5. Build and send the report message

### 6. `dag_smoke_test.py` - Alert-Pipeline Smoke Test

On-demand E2E validation for the alert mart without sending any Telegram messages.

**Schedule**: `None` (manual only)

**Tasks**:
1. Insert a synthetic test row into `mart_air_quality__alerts`
2. Verify the row is readable
3. Delete the test row

## How to Trigger DAGs

### Via Airflow Web UI

1. Open Airflow at `http://localhost:8090`
2. Log in with the credentials configured for the Compose stack
3. Find the DAG in the list
4. Click the trigger button for manual runs

### Via CLI

```bash
# Trigger the manual ingestion fallback
docker compose exec airflow-webserver \
  airflow dags trigger dag_ingest_hourly

# Trigger a warehouse rebuild directly
docker compose exec airflow-webserver \
  airflow dags trigger dag_transform

# Trigger the smoke test
docker compose exec airflow-webserver \
  airflow dags trigger dag_smoke_test
```

## Environment Variables

The DAG layer depends on a mix of shared Compose variables and DAG-specific settings:

```bash
# ClickHouse
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=admin
CLICKHOUSE_PASSWORD=replace-with-clickhouse-password
CLICKHOUSE_DB=air_quality

# Airflow
AIRFLOW_ADMIN_USERNAME=airflow_admin
AIRFLOW_ADMIN_PASSWORD=replace-with-a-strong-password
AIRFLOW__CORE__FERNET_KEY=replace-with-a-generated-fernet-key
AIRFLOW_API_SECRET_KEY=change-me
AIRFLOW_API_AUTH_JWT_SECRET=change-me
AIRFLOW_WEBSERVER_SECRET_KEY=change-me

# dbt
DBT_PROFILES_DIR=/opt/dbt/dbt_tranform
DBT_TARGET=production
DBT_PACKAGES_INSTALL_PATH=/opt/dbt/.cache/dbt_packages

# Google Drive sync
GDRIVE_CLIENT_ID=
GDRIVE_CLIENT_SECRET=
GDRIVE_REFRESH_TOKEN=
GDRIVE_ROOT_FOLDER_ID=

# OpenMetadata curation
OPENMETADATA_URL=http://openmetadata:8585/api
OM_ADMIN_USER=admin@open-metadata.org
OM_ADMIN_PASSWORD=admin

# Direct-ingestion fallback
OPENWEATHER_API_TOKEN=
OPENWEATHER_API_TOKENS=
TOMTOM_API_KEY=
```

`dag_weekly_report.py` uses `python_jobs/jobs/alerting/telegram_client.py`, which reads `TELEGRAM_AQ_BOT_TOKEN` and `TELEGRAM_AQ_CHAT_ID`.

## Dependencies

The DAGs rely on:

- Airflow 3 TaskFlow APIs plus standard operators/providers
- `dbt-core` and `dbt-clickhouse`
- `clickhouse-connect`
- `requests`
- Repository-local Python jobs mounted at `/opt/python/jobs`

## File Structure

```text
airflow/dags/
├── __init__.py
├── dag_ingest_hourly.py         # Manual fallback ingestion
├── dag_openmetadata_curation.py # Governance and glossary sync
├── dag_smoke_test.py            # E2E alert smoke test
├── dag_sync_gdrive.py           # Google Drive landing-zone sync
├── dag_transform.py             # Trigger-based dbt transformation
└── dag_weekly_report.py         # Weekly Telegram report
```

## Monitoring

- Airflow UI: `http://localhost:8090`
- Airflow logs: `airflow/logs/`
- Scheduler and task logs: `docker compose logs airflow-scheduler`

## Troubleshooting

### DAG not appearing in Airflow

1. Confirm the file exists under `airflow/dags/`
2. Check `docker compose logs airflow-dag-processor`
3. Check `docker compose logs airflow-scheduler`

### Sync DAG failures

1. Verify Google Drive OAuth variables are set
2. Review `gdrive_sync.py` output in the task log
3. Confirm failed files remain in the landing zone for retry

### Transform DAG failures

1. Verify ClickHouse credentials and connectivity
2. Check the dbt project mount at `/opt/dbt/dbt_tranform`
3. Review `dbt test` failures separately from the non-blocking docs branch

### OpenMetadata curation failures

1. Verify `OPENMETADATA_URL`, `OM_ADMIN_USER`, and `OM_ADMIN_PASSWORD`
2. Check `docker compose logs openmetadata`
3. Re-run `dag_openmetadata_curation` after OpenMetadata is healthy
