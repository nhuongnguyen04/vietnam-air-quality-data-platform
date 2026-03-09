# Airflow DAGs Documentation

## Overview

This directory contains Airflow DAGs for the Vietnam Air Quality Data Platform. These DAGs orchestrate the data ingestion, transformation, and scheduling of the entire ETL pipeline.

## DAGs Available

### 1. `dag_ingest_hourly.py` - Hourly Ingestion DAG

Runs every hour to ingest the latest measurements from:
- **OpenAQ API**: Measurements for Vietnam sensors
- **AQICN API**: Measurements for Vietnam stations

**Schedule**: `0 * * * *` (Every hour at minute 0)

**Tasks**:
1. Check ClickHouse connection
2. Check if metadata exists (if not, ingest metadata first)
3. Run OpenAQ measurements ingestion
4. Run AQICN measurements ingestion  
5. Run AQICN forecast ingestion

### 2. `dag_ingest_historical.py` - Historical Data Ingestion DAG

One-time historical data backfill. Must be manually triggered with configuration parameters.

**Schedule**: Manual trigger only

**Configuration Parameters** (via DAG run conf):
- `start_date`: Start date for backfill (YYYY-MM-DD), default: 30 days ago
- `end_date`: End date for backfill (YYYY-MM-DD), default: today
- `days_back`: Number of days to backfill (alternative to start_date/end_date)

**Tasks**:
1. Configure dates from DAG run conf
2. Check ClickHouse connection
3. Ingest OpenAQ parameters (one-time)
4. Ingest OpenAQ locations (one-time)
5. Ingest OpenAQ sensors (one-time)
6. Ingest AQICN stations (one-time)
7. Backfill OpenAQ measurements
8. Backfill AQICN measurements
9. Backfill AQICN forecasts

### 3. `dag_metadata_update.py` - Metadata Update DAG

Daily refresh of metadata from OpenAQ and AQICN APIs.

**Schedule**: `0 1 * * *` (Daily at 01:00)

**Tasks**:
1. Check ClickHouse connection
2. Refresh OpenAQ parameters
3. Refresh OpenAQ locations
4. Refresh OpenAQ sensors
5. Refresh AQICN stations
6. Log metadata statistics

### 4. `dag_transform.py` - dbt Transformation DAG

Runs dbt models to transform raw data into analytics-ready models.

**Schedule**: `30 * * * *` (Every hour at minute 30, after ingestion completes)

**Tasks**:
1. Check ClickHouse connection
2. Check dbt project is ready
3. dbt deps - Install required packages
4. dbt seed - Load seed data (if any)
5. dbt run staging - Run staging models
6. dbt run intermediate - Run intermediate models
7. dbt run marts - Run marts models
8. dbt test - Run tests
9. Log transformation statistics

## How to Trigger DAGs

### Via Airflow Web UI

1. Open Airflow Web UI at `http://localhost:8090`
2. Login with credentials (check environment variables)
3. Find the DAG in the list
4. Click the "Play" button to trigger manually (for historical ingestion)

### Via CLI

```bash
# Trigger historical ingestion with custom dates
airflow dags trigger \
    -c '{"start_date": "2024-01-01", "end_date": "2024-01-31"}' \
    dag_ingest_historical

# Trigger historical ingestion with days_back
airflow dags trigger \
    -c '{"days_back": 90}' \
    dag_ingest_historical
```

## Environment Variables

The DAGs require the following environment variables to be set:

```bash
# ClickHouse
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=admin
CLICKHOUSE_PASSWORD=admin
CLICKHOUSE_DB=airquality

# API Tokens
OPENAQ_API_TOKEN=your_openaq_token
AQICN_API_TOKEN=your_aqicn_token

# Airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow

# dbt
DBT_PROFILES_DIR=/opt/dbt/dbt_tranform
DBT_TARGET=production
```

## Dependencies

The DAGs have the following dependencies:

- **Airflow Providers**:
  - `apache-airflow-providers-http`
  - `apache-airflow-providers-sqlite`
  - `apache-airflow-providers-postgres`

- **Python Packages**:
  - `requests`
  - `dbt-core`
  - `dbt-clickhouse`
  - `clickhouse-connect`

## File Structure

```
airflow/dags/
├── __init__.py
├── dag_ingest_hourly.py      # Hourly ingestion
├── dag_ingest_historical.py  # Historical backfill
├── dag_metadata_update.py    # Daily metadata refresh
└── dag_transform.py          # dbt transformation
```

## Monitoring

- Check Airflow Web UI at `http://localhost:8090` for DAG status
- Check Airflow logs in `airflow/logs/`

## Troubleshooting

### DAG not appearing in Airflow

1. Check that the DAG file is in the correct location (`/opt/airflow/dags` in container)
2. Check Airflow scheduler logs
3. Verify DAG file has no syntax errors

### Task failures

1. Check ClickHouse connection
2. Verify API tokens are set
3. Check Python jobs are accessible at `/opt/python/jobs`
4. Review task logs in Airflow UI

### dbt transformation failures

1. Verify dbt project is at `/opt/dbt/dbt_tranform`
2. Check dbt profiles.yml configuration
3. Verify ClickHouse credentials
4. Check dbt run logs

