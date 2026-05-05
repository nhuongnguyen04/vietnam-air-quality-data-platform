<!-- generated-by: gsd-doc-writer -->
# Deployment

## Deployment Targets

This repository ships as a containerized stack managed by Docker Compose. No Kubernetes, Vercel, Netlify, Fly.io, Railway, or serverless deployment configuration is checked into the repository.

| Target | Config file | What it deploys | How it is used |
| --- | --- | --- | --- |
| Full local or single-host stack | `docker-compose.yml` | ClickHouse, Airflow, PostgreSQL, Streamlit dashboard, FastAPI text-to-SQL, Grafana, Prometheus, exporters, Elasticsearch, OpenMetadata, and OpenMetadata ingestion | Primary runtime deployment path |
| Minimal validation stack | `docker-compose.test.yml` | ClickHouse plus schema bootstrap | Lightweight local or CI validation only |
| GitHub-hosted ingestion job | `.github/workflows/scheduled_ingestion.yml` | Python ingestion scripts running on a GitHub Actions runner, outputting CSV files and uploading them to Google Drive | Source-of-truth ingestion path |

Start the full stack with Docker Compose:

```bash
docker compose up -d
docker compose ps
```

The main Compose file exposes these operator-facing endpoints:

- ClickHouse: `http://localhost:8123`
- Airflow API/UI: `http://localhost:8090`
- Streamlit dashboard: `http://localhost:8501`
- Text-to-SQL healthcheck: `http://localhost:8000/health`
- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090`
- OpenMetadata: `http://localhost:8585`

## Build Pipeline

The repository contains a CI validation pipeline, but no automated deployment job. Deployment is manual after CI passes.

### CI Workflow

Workflow file: `.github/workflows/ci.yml`

Trigger conditions:

1. `push` to `main`, `develop`, `feature/**`, and `fix/**`
2. `pull_request` targeting `main`

The CI workflow runs these stages:

1. **Lint**
   - `pip install dbt-core==1.10.13 dbt-clickhouse==1.10.0 sqlfluff==3.5.0 sqlfluff-templater-dbt==3.5.0 ruff==0.11.0`
   - `cd dbt/dbt_tranform && dbt deps`
   - `ruff check python_jobs/ airflow/dags/ --config .ruff.toml`
   - `sqlfluff lint dbt/dbt_tranform/ --format github-annotation`
2. **Python Unit**
   - `pip install -r requirements.txt`
   - `pytest tests/python -m "not integration and not live"`
3. **Python Integration**
   - `pip install -r requirements.txt`
   - `pytest tests/python -m integration`
4. **Compile**
   - `pip install dbt-core==1.10.13 dbt-clickhouse==1.10.0`
   - `cd dbt/dbt_tranform && dbt deps`
   - `cd dbt/dbt_tranform && dbt compile --target dev`
5. **Validate**
   - Starts a ClickHouse `25.12` service container
   - Creates the `air_quality` database
   - Runs `dbt seed --target dev`
   - Runs `dbt run --select +dm_air_quality_overview_daily +dm_aqi_current_status +dm_traffic_hourly_trend --target dev`
6. **Text-to-SQL Quality Gate**
   - `pip install -r requirements.txt`
   - `pytest python_jobs/text_to_sql/tests -m "not integration"`
   - Covers the eval runner, persistent-store config, API contract, and Vanna runtime safety checks
7. **Test**
   - Starts a fresh ClickHouse `25.12` service container
   - Runs `dbt seed --target dev`
   - Runs `dbt run --target dev`
   - Runs `dbt test --target dev`

### Deployment Step

No repo-owned CI/CD deployment workflow is checked in under `.github/workflows/`. After validation passes, the checked-in deployment path is to update the target host and run Docker Compose manually:

```bash
docker compose pull
docker compose up -d --build
```

### Scheduled Ingestion Workflow

Workflow file: `.github/workflows/scheduled_ingestion.yml`

This workflow is designed to be triggered externally by Google Apps Script and also supports manual `workflow_dispatch`. It is the source-of-truth ingestion path for new raw source data:

1. Checks out the repository
2. Sets up Python `3.11`
3. Installs `requirements-ingest.txt`
4. Creates `landing_zone/`
5. Runs AQI.in, OpenWeather, and time-dependent TomTom ingestion scripts in parallel with `INGEST_MODE=csv`
6. Uploads generated CSV files to Google Drive with `scripts/gdrive_uploader.py`

## Environment Setup

Use the root `.env` file for Docker Compose deployments. Start from `.env.example`, replace the placeholder values, and add any environment-specific overrides you need. See `docs/CONFIGURATION.md` for the full variable inventory and defaults.

```bash
cp .env.example .env
# edit .env
docker compose up -d
```

Minimum variables for the full Compose stack:

| Area | Required settings |
| --- | --- |
| ClickHouse | `CLICKHOUSE_DB`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` |
| Airflow auth and secrets | `AIRFLOW_ADMIN_USERNAME`, `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW__CORE__FERNET_KEY`, `AIRFLOW_API_SECRET_KEY`, `AIRFLOW_API_AUTH_JWT_SECRET`, `AIRFLOW_WEBSERVER_SECRET_KEY` |
| External ingestion APIs | `OPENWEATHER_API_TOKEN`, `TOMTOM_API_KEY` |
| Text-to-SQL | `GROQ_API_KEY`, `TEXT_TO_SQL_PREVIEW_SECRET`, `TEXT_TO_SQL_VANNA_PERSIST_DIRECTORY`, plus `TEXT_TO_SQL_CLICKHOUSE_USER` and `TEXT_TO_SQL_CLICKHOUSE_PASSWORD` for a dedicated read-only user |
| Google Drive integrations | `GDRIVE_CLIENT_ID`, `GDRIVE_CLIENT_SECRET`, `GDRIVE_REFRESH_TOKEN`, `GDRIVE_ROOT_FOLDER_ID` |
| OpenMetadata | `OM_ADMIN_USER`, `OM_ADMIN_PASSWORD`, `POSTGRES_OM_DB`, `POSTGRES_OM_USER`, `POSTGRES_OM_PASSWORD` |
| Grafana alerting | `TELEGRAM_AQ_BOT_TOKEN`, `TELEGRAM_SYS_BOT_TOKEN` |

Deployment-specific notes verified from the repository:

- `.env.example` is not a complete deployment manifest; treat it as a starting template and review `docker-compose.yml` plus `docs/CONFIGURATION.md` before promoting a deployment.
- Airflow uses checked-in config from `airflow/config/airflow.cfg` and the custom image in `airflow/Dockerfile`.
- dbt uses `dbt/dbt_tranform/profiles.yml` with `dev`, `production`, and `ci` outputs; the checked-in target is `production`.
- Grafana provisioning reads datasource, dashboard, and alerting files from `monitoring/grafana/provisioning/`.
- The current Grafana contact-point file hardcodes chat IDs in `monitoring/grafana/provisioning/alerting/contact-points.yml` while bot tokens come from environment variables.

There are no checked-in `.env.development`, `.env.production`, or `.env.test` files. Environment separation is currently handled through the root `.env`, dbt profile targets, and the separate `docker-compose.test.yml` file.

## Rollback Procedure

No automated rollback workflow is checked into CI. Rollback is manual and depends on which layer changed.

1. **Grafana alerting or provisioning changes**
   Restore the previous version of the affected file under `monitoring/grafana/provisioning/alerting/` or `monitoring/grafana/provisioning/datasources/`, then restart Grafana.

   ```bash
   docker compose restart grafana
   curl http://localhost:3000/api/health
   ```

2. **dbt model regressions**
   Inspect the scheduler logs, then rerun only the failed transform task or rebuild the broken model.

   ```bash
   docker compose logs airflow-scheduler --tail=50 | grep "dbt run"
   docker compose exec airflow-scheduler \
     dbt run --profiles-dir /opt/dbt/dbt_tranform \
     --target production --full-refresh \
     --select <broken_model>
   ```

3. **Application or service regressions**
   No service-level rollback script is checked in. The supported repository-level approach is to restore the previous known-good revision on the deployment host, then recreate the affected containers with Docker Compose.

   ```bash
   docker compose up -d --build <service>
   ```

4. **Emergency alert suppression**
   If Telegram alerts are flooding or misconfigured, mute the rules in Grafana or temporarily change the contact-point or notification-policy files under `monitoring/grafana/provisioning/alerting/`, then restart Grafana.

## Monitoring

The deployed stack is monitored with Prometheus, Grafana, and exporter containers provisioned directly from the repository.

### Metrics Collection

Prometheus is defined in `docker-compose.yml` and configured by `monitoring/prometheus/prometheus.yml`:

- Scrape interval: `30s`
- Evaluation interval: `30s`
- Retention: `15d`
- Scraped jobs:
  - `prometheus`
  - `node-exporter`
  - `docker-stats-exporter`
  - `postgres-exporter`
  - `clickhouse`

### Dashboards And Datasources

Grafana `11.3.1` is deployed from `docker-compose.yml` with provisioning from `monitoring/grafana/`:

- Anonymous viewer access is enabled in `monitoring/grafana/grafana.ini`
- Datasources are provisioned for:
  - Prometheus: `monitoring/grafana/provisioning/datasources/prometheus.yml`
  - ClickHouse: `monitoring/grafana/provisioning/datasources/clickhouse.yml`
  - Airflow metadata PostgreSQL: `monitoring/grafana/provisioning/datasources/airflow-postgres.yml`
- Dashboards are provisioned from `monitoring/grafana/provisioning/dashboard-files/`

### Alerting

Grafana-managed alerting is provisioned from `monitoring/grafana/provisioning/alerting/`:

- Contact points: `contact-points.yml`
- Notification policies: `notification-policies.yml`
- Templates: `templates.yml`
- Air-quality and system alert rules: `v3-alert-rules.yml`, `system-prometheus-rules.yml`

Checked-in alert rules include AQI, PM2.5, PM10, and data-freshness conditions. Telegram bot tokens are environment-driven, while the currently checked-in chat IDs are embedded in `contact-points.yml`.

### Health Checks

The Compose stack defines explicit health checks for:

- ClickHouse: `http://localhost:8123/ping`
- Airflow: `http://localhost:8080/api/v2/monitor/health` inside the container
- Streamlit: `http://localhost:8501/_stcore/health`
- Text-to-SQL: `http://localhost:8000/health`
- Grafana: `http://localhost:3000/api/health`
- Prometheus: `http://localhost:9090/-/healthy`
- OpenMetadata: `http://localhost:8586/healthcheck`
- OpenMetadata ingestion: `http://localhost:8080/`

The repository does not include Sentry, Datadog, New Relic, or OpenTelemetry application instrumentation. Monitoring is Compose-native and Grafana/Prometheus-based.
