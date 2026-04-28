<!-- generated-by: gsd-doc-writer -->
# Configuration

This project is configured through a mix of root-level environment variables, Docker Compose service definitions, dbt profile files, Airflow and Grafana config files, and OpenMetadata workflow YAMLs. The root `.env.example` is the starting point, but it is not a complete inventory of every variable referenced by `docker-compose.yml` and the Python code.

## Environment Variables

Populate a local `.env` file from `.env.example` for Docker Compose runs. Variables below are grouped by feature area; entries marked as documented-only or legacy are present in the repository but are not fully wired into the current runtime.

### Core Platform

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `CLICKHOUSE_HOST` | Required for non-default deployments | Varies: `localhost` in dashboard/dbt helpers, `clickhouse` in Airflow/Python helpers | ClickHouse host used by dbt, the dashboard, Airflow connection setup, text-to-SQL, and Grafana datasource provisioning. |
| `CLICKHOUSE_PORT` | Optional | `8123` | ClickHouse HTTP port. |
| `CLICKHOUSE_USER` | Required for local stack | Varies: `admin` in Python/dbt helpers | Primary ClickHouse username used across services. |
| `CLICKHOUSE_PASSWORD` | Required | No single safe default | Primary ClickHouse password. The dashboard refuses to start without it, and text-to-SQL falls back to it when `TEXT_TO_SQL_CLICKHOUSE_PASSWORD` is unset. |
| `CLICKHOUSE_DB` | Required for local stack | Varies: `air_quality` in Compose/dbt/OpenMetadata helpers, `airquality` in `python_jobs/common/config.py` and `python_jobs/config/job_config.yaml` | Main ClickHouse database name. Set this explicitly to avoid conflicting defaults. |
| `CLICKHOUSE_DATABASE` | Optional (documented-only) | `air_quality` in `.env.example` | Present in `.env.example`, but no repo-owned runtime code references it. |
| `CLICKHOUSE_SECURE` | Optional (documented-only) | `false` in `.env.example` | Present in `.env.example`, but no repo-owned runtime code references it. |
| `CLICKHOUSE_SERVICE` | Optional | `ClickHouse` | OpenMetadata Streamlit connector service name override for lineage FQNs. |
| `CLICKHOUSE_SCHEMA` | Optional | `air_quality` | OpenMetadata Streamlit connector schema override for lineage FQNs. |

### Airflow And Orchestration

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `AIRFLOW_ADMIN_USERNAME` | Required for Compose | None | Injected into `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS` for Airflow simple auth. Referenced by `docker-compose.yml` but missing from `.env.example`. |
| `AIRFLOW_ADMIN_PASSWORD` | Required for Compose | None | Airflow admin password paired with `AIRFLOW_ADMIN_USERNAME`. Referenced by `docker-compose.yml` but missing from `.env.example`. |
| `AIRFLOW_API_SECRET_KEY` | Required for Compose | None | Airflow API secret key. Passed to all Airflow services through Compose. |
| `AIRFLOW_API_AUTH_JWT_SECRET` | Required for Compose | None | Airflow JWT signing secret for API auth. |
| `AIRFLOW_WEBSERVER_SECRET_KEY` | Required for Compose | None | Airflow webserver session secret. |
| `AIRFLOW_HOME` | Optional | Image/runtime dependent | Used by `python_jobs/common/logging_config.py` to derive a log directory when `JOB_LOG_DIR` is unset. |
| `JOB_LOG_DIR` | Optional | Derived from `AIRFLOW_HOME` or `/opt/airflow/logs` | Explicit override for Python job log output. |
| `DBT_PROFILES_DIR` | Compose-managed | `/opt/dbt/dbt_tranform` | Exported by Airflow services so `dbt` commands resolve the checked-in profile. |
| `PYTHON_JOBS_DIR` | Compose-managed | `/opt/python/jobs` or `/opt/python/jobs/` | Exported by Airflow services to locate ingestion scripts. |

### Ingestion And Job Tuning

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `OPENWEATHER_API_TOKEN` | Required for OpenWeather ingestion | None | Primary OpenWeather API token. Used by Airflow DAGs and Python ingestion code. |
| `OPENWEATHER_API_TOKENS` | Optional | None | Comma-separated list of OpenWeather tokens for `python_jobs/jobs/openweather/ingest_openweather_unified.py`. |
| `OPENWEATHER_API_TOKEN_1` ... `OPENWEATHER_API_TOKEN_20` | Optional | None | Additional token slots scanned by the unified OpenWeather ingestion job. |
| `TOMTOM_API_KEY` | Required for traffic ingestion | None | TomTom traffic API key used by Airflow ingestion services. Referenced by `docker-compose.yml` but missing from `.env.example`. |
| `AQICN_API_TOKEN` | Optional (legacy) | None | Still read by `python_jobs/common/config.py`, but the main Compose stack comments that AQICN is no longer used. |
| `OPENAQ_API_TOKEN` | Optional (legacy) | None | Still supported by `python_jobs/common/config.py` and `python_jobs/config/job_config.yaml`. |
| `RATE_LIMIT_OPENAQ` | Optional | `0.8` | Overrides `JobConfig.rate_limit_openaq`. |
| `RATE_LIMIT_AQICN` | Optional | `1.0` | Overrides `JobConfig.rate_limit_aqicn`. |
| `BATCH_SIZE` | Optional | `1000` | Overrides Python job batch size. Must remain positive. |
| `MAX_WORKERS` | Optional | `4` | Overrides Python job worker count. Must remain positive. |
| `MAX_RETRIES` | Optional | `3` | Overrides Python job retry count. |
| `CSV_OUTPUT_DIR` | Optional | `landing_zone` | Output/input directory for CSV writers and the Google Drive uploader. |
| `MAX_SYNC_WORKERS` | Optional | `5` | Parallel worker limit for `python_jobs/jobs/sync/gdrive_sync.py`. |

### Text-to-SQL And Dashboard

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `TEXT_TO_SQL_URL` | Optional | `http://localhost:8000` in dashboard code; `http://text-to-sql:8000` in Compose | Dashboard-side base URL for the internal text-to-SQL service. |
| `GROQ_API_KEY` | Required for text-to-SQL generation | None | Required by `python_jobs/text_to_sql/vanna_runtime.py` before SQL generation can start. |
| `GROQ_MODEL` | Optional | `qwen/qwen3-32b` | Groq-hosted model name for the Vanna runtime. |
| `TEXT_TO_SQL_CLICKHOUSE_USER` | Recommended for text-to-SQL | Falls back to `CLICKHOUSE_USER` | Dedicated read-only ClickHouse username for text-to-SQL execution. |
| `TEXT_TO_SQL_CLICKHOUSE_PASSWORD` | Recommended for text-to-SQL | Falls back to `CLICKHOUSE_PASSWORD` | Dedicated read-only ClickHouse password for text-to-SQL execution. |
| `TEXT_TO_SQL_VANNA_CLIENT` | Optional | `in-memory` | Vanna vector-store client setting. |
| `TEXT_TO_SQL_VANNA_COLLECTION` | Optional | `air_quality_ask_data` | Vanna collection name. |
| `OPENAI_API_KEY` | Optional (placeholder) | None | Reserved for future OpenAI-compatible provider variants. |
| `VANNA_API_KEY` | Optional (placeholder) | None | Reserved for future provider variants; current runtime does not require it. |
| `MAPBOX_ACCESS_TOKEN` | Optional (documented-only) | None | Present in `.env.example`, but no repo-owned runtime code references it. |
| `STREAMLIT_SERVER_PORT` | Compose-managed | `8501` | Passed to the dashboard container by `docker-compose.yml`. |

### OpenMetadata

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `OPENMETADATA_URL` | Optional | `http://openmetadata:8585/api` | OpenMetadata API base URL used by Airflow connection setup. |
| `OM_ADMIN_USER` | Optional | `admin@open-metadata.org` | OpenMetadata admin username used by Airflow connection setup and Compose. |
| `OM_ADMIN_PASSWORD` | Optional | `admin` | OpenMetadata admin password used by Airflow connection setup and Compose. |
| `POSTGRES_OM_DB` | Optional | `openmetadata_db` | OpenMetadata PostgreSQL database name. |
| `POSTGRES_OM_USER` | Optional | `openmetadata_user` | OpenMetadata PostgreSQL username. |
| `POSTGRES_OM_PASSWORD` | Optional | `openmetadata_password` | OpenMetadata PostgreSQL password. |
| `POSTGRES_OM_AIRFLOW_DB` | Optional | `airflow_db` | OpenMetadata-bundled Airflow metadata database name. Referenced by Compose but missing from `.env.example`. |
| `OPENMETADATA_CLUSTER_NAME` | Optional | `openmetadata` | OpenMetadata cluster name for server and ingestion containers. |
| `SERVER_PORT` | Optional | `8585` | OpenMetadata server port inside the OpenMetadata containers. |
| `SERVER_ADMIN_PORT` | Optional | `8586` | OpenMetadata admin port inside the OpenMetadata containers. |
| `MIGRATION_LIMIT_PARAM` | Optional | `1200` | OpenMetadata migration timeout/limit parameter in Compose. |
| `CLICKHOUSE_OM_READER_USER` | Optional (documented-only) | `om_reader` in `.env.example` | Documented as connector credentials, but no repo-owned runtime code reads it directly. |
| `CLICKHOUSE_OM_READER_PASSWORD` | Optional (documented-only) | `om_reader_secure_pass` in `.env.example` | Documented as connector credentials, but no repo-owned runtime code reads it directly. |

### Google Drive And Metadata Sync

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `GDRIVE_ROOT_FOLDER_ID` | Optional for OpenMetadata; required for uploads/sync | None | Root Google Drive folder ID. Missing values cause sync/upload jobs to log and skip work instead of raising immediately. |
| `GDRIVE_CLIENT_ID` | Required for Google Drive sync/upload | None | OAuth client ID for `scripts/gdrive_uploader.py`, `python_jobs/jobs/sync/gdrive_sync.py`, and `openmetadata/custom_connectors/gdrive.py`. |
| `GDRIVE_CLIENT_SECRET` | Required for Google Drive sync/upload | None | OAuth client secret for Google Drive integration. |
| `GDRIVE_REFRESH_TOKEN` | Required for Google Drive sync/upload | None | OAuth refresh token for Google Drive integration. |

### Alerting And Monitoring

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `TELEGRAM_AQ_BOT_TOKEN` | Required for Grafana AQ alerts | None | Bot token injected into Grafana alerting contact points. |
| `TELEGRAM_AQ_CHAT_ID` | Optional in current checked-in provisioning | None | Exported by Compose, but `monitoring/grafana/provisioning/alerting/contact-points.yml` currently hardcodes the AQ chat ID instead of reading this variable. |
| `TELEGRAM_SYS_BOT_TOKEN` | Required for Grafana system alerts | None | Bot token injected into Grafana alerting contact points. |
| `TELEGRAM_SYS_CHAT_ID` | Optional in current checked-in provisioning | None | Exported by Compose, but the checked-in Grafana contact point file currently hardcodes the system chat ID instead of reading this variable. |
| `TELEGRAM_BOT_TOKEN` | Optional (legacy Python alert client) | None | Used by `python_jobs/jobs/alerting/telegram_client.py`. This naming does not match the split AQ/SYS Grafana variables. |
| `TELEGRAM_CHAT_ID` | Optional (legacy Python alert client) | None | Used by `python_jobs/jobs/alerting/telegram_client.py`. This naming does not match the split AQ/SYS Grafana variables. |

### dbt Advanced Paths

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `DBT_PACKAGES_INSTALL_PATH` | Optional | `dbt_packages` | Overrides the packages install path in `dbt/dbt_tranform/dbt_project.yml`. |
| `DBT_TARGET_PATH` | Optional | `target` | Overrides the dbt build target directory in `dbt/dbt_tranform/dbt_project.yml`. |

## Config File Format

The repository uses several checked-in config files in addition to environment variables:

| File | Format | Key sections | Purpose |
| --- | --- | --- | --- |
| `docker-compose.yml` | YAML | `services`, `volumes`, `networks` | Main local stack definition for ClickHouse, Airflow, dashboard, text-to-SQL, monitoring, and OpenMetadata. |
| `docker-compose.test.yml` | YAML | `services`, `networks` | Minimal CI/test stack for ClickHouse-backed tests. |
| `dbt/dbt_tranform/profiles.yml` | YAML | `dbt_tranform.outputs.dev`, `production`, `ci` | dbt connection targets and thread/timeouts. |
| `dbt/dbt_tranform/dbt_project.yml` | YAML | `model-paths`, `packages-install-path`, `clean-targets`, `models`, `seeds` | dbt project structure and output path overrides. |
| `python_jobs/config/job_config.yaml` | YAML | `clickhouse`, `api`, `job` | Default ingestion-job connection settings, API placeholders, and batch/retry/rate-limit settings. |
| `airflow/config/airflow.cfg` | INI | `[core]`, `[database]`, `[webserver]`, `[scheduler]`, `[logging]`, `[api]` | Base Airflow container configuration. |
| `monitoring/grafana/grafana.ini` | INI | `[anonymous]`, `[security]`, `[users]`, `[plugins]`, `[server]` | Grafana auth, plugin, and server settings. |
| `monitoring/grafana/provisioning/**` | YAML | `datasources`, `contactPoints`, alert rules, dashboards | Grafana provisioning for datasources, alerting, and dashboards. |
| `openmetadata/ingestion-configs/*.yaml` | YAML | `source`, `sink`, `workflowConfig` | OpenMetadata ingestion workflows for ClickHouse and dbt artifacts. |
| `python_jobs/dashboard/.streamlit/config.toml` | TOML | `[theme]`, `[server]`, `[browser]` | Streamlit theme and local server behavior. |

Minimal checked-in examples:

```yaml
# dbt/dbt_tranform/profiles.yml
dbt_tranform:
  target: production
  outputs:
    production:
      type: clickhouse
      host: "{{ env_var('CLICKHOUSE_HOST', 'localhost') }}"
      port: "{{ env_var('CLICKHOUSE_PORT', '8123') | int }}"
      user: "{{ env_var('CLICKHOUSE_USER', 'admin') }}"
      password: "{{ env_var('CLICKHOUSE_PASSWORD', 'admin123456') }}"
      database: "{{ env_var('CLICKHOUSE_DB', 'air_quality') }}"
      schema: "{{ env_var('CLICKHOUSE_DB', 'air_quality') }}"
```

```yaml
# python_jobs/config/job_config.yaml
clickhouse:
  host: "clickhouse"
  port: 8123
  database: "airquality"
  user: "admin"

job:
  batch_size: 1000
  max_workers: 4
  max_retries: 3
```

```ini
; airflow/config/airflow.cfg
[core]
executor = LocalExecutor
dags_folder = /opt/airflow/dags

[database]
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres/airflow
```

## Required vs Optional Settings

The repository does not have a single central settings validator, so requiredness depends on which services you run.

| Setting | Why it is required | Failure mode in repo-owned code |
| --- | --- | --- |
| `CLICKHOUSE_PASSWORD` | Required for the dashboard; also used as text-to-SQL fallback | `python_jobs/dashboard/lib/clickhouse_client.py` raises `CLICKHOUSE_PASSWORD environment variable is required.` |
| `TEXT_TO_SQL_CLICKHOUSE_PASSWORD` or `CLICKHOUSE_PASSWORD` | Required for text-to-SQL execution | `python_jobs/text_to_sql/clickhouse_executor.py` raises `TEXT_TO_SQL_CLICKHOUSE_PASSWORD or CLICKHOUSE_PASSWORD is required`. |
| `GROQ_API_KEY` | Required for SQL generation in the current Vanna runtime | `python_jobs/text_to_sql/vanna_runtime.py` raises `GROQ_API_KEY is required for the Vanna runtime.` |
| `GDRIVE_CLIENT_ID`, `GDRIVE_CLIENT_SECRET`, `GDRIVE_REFRESH_TOKEN` | Required for Google Drive uploader/sync and the custom OpenMetadata Google Drive connector | Google Drive helpers raise `Missing OAuth credentials...` when any of the three are absent. |
| `AIRFLOW_ADMIN_USERNAME`, `AIRFLOW_ADMIN_PASSWORD` | Required to render the checked-in Airflow Compose environment cleanly | Missing values leave `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS` incomplete in `docker-compose.yml`. |
| `AIRFLOW_API_SECRET_KEY`, `AIRFLOW_API_AUTH_JWT_SECRET`, `AIRFLOW_WEBSERVER_SECRET_KEY` | Required by the checked-in Airflow Compose stack | `docker-compose.yml` interpolates these without fallbacks. |
| `OPENWEATHER_API_TOKEN` | Required for the default OpenWeather ingestion path | `python_jobs/jobs/openweather/ingest_openweather_unified.py` exits with `No OpenWeather tokens found.` when no token variables are present. |
| `TOMTOM_API_KEY` | Required for traffic ingestion DAG tasks | Passed from `docker-compose.yml` into Airflow services without a fallback. |

Optional settings with repo-defined defaults include `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USER`, `OPENMETADATA_URL`, `OM_ADMIN_USER`, `OM_ADMIN_PASSWORD`, `GROQ_MODEL`, `TEXT_TO_SQL_URL`, `DBT_PACKAGES_INSTALL_PATH`, `DBT_TARGET_PATH`, `CSV_OUTPUT_DIR`, `MAX_SYNC_WORKERS`, and the Python job tuning variables.

Settings that are currently documented or legacy rather than strongly wired into the runtime include `CLICKHOUSE_DATABASE`, `CLICKHOUSE_SECURE`, `MAPBOX_ACCESS_TOKEN`, `AQICN_API_TOKEN`, `OPENAQ_API_TOKEN`, `CLICKHOUSE_OM_READER_USER`, and `CLICKHOUSE_OM_READER_PASSWORD`.

## Defaults

The most important defaults are split across several files and are not always consistent.

| Setting | Default | Where it is set |
| --- | --- | --- |
| `CLICKHOUSE_HOST` | `localhost` or `clickhouse` | `dbt/dbt_tranform/profiles.yml`, `python_jobs/dashboard/lib/clickhouse_client.py`, `python_jobs/text_to_sql/clickhouse_executor.py`, `python_jobs/common/config.py`, `airflow/config/setup_connections.py` |
| `CLICKHOUSE_PORT` | `8123` | dbt profiles, Python helpers, and Compose fallbacks |
| `CLICKHOUSE_USER` | `admin` | dbt profiles, Python helpers, and Compose fallbacks |
| `CLICKHOUSE_DB` | `air_quality` or `airquality` | `docker-compose.yml`, dbt profiles, OpenMetadata/Airflow helpers, and `python_jobs/config/job_config.yaml` |
| `GROQ_MODEL` | `qwen/qwen3-32b` | `.env.example`, `docker-compose.yml`, and `python_jobs/text_to_sql/vanna_runtime.py` |
| `TEXT_TO_SQL_URL` | `http://localhost:8000` or `http://text-to-sql:8000` | `python_jobs/dashboard/lib/text_to_sql_client.py` and `docker-compose.yml` |
| `TEXT_TO_SQL_VANNA_CLIENT` | `in-memory` | `python_jobs/text_to_sql/vanna_runtime.py` |
| `TEXT_TO_SQL_VANNA_COLLECTION` | `air_quality_ask_data` | `python_jobs/text_to_sql/vanna_runtime.py` |
| `OPENMETADATA_URL` | `http://openmetadata:8585/api` | `airflow/config/setup_connections.py` |
| `OM_ADMIN_USER` / `OM_ADMIN_PASSWORD` | `admin@open-metadata.org` / `admin` | `airflow/config/setup_connections.py` and `.env.example` |
| `OPENMETADATA_CLUSTER_NAME` | `openmetadata` | `docker-compose.yml` |
| `SERVER_PORT` / `SERVER_ADMIN_PORT` | `8585` / `8586` | `docker-compose.yml` |
| `MIGRATION_LIMIT_PARAM` | `1200` | `docker-compose.yml` |
| `POSTGRES_OM_DB` / `POSTGRES_OM_USER` / `POSTGRES_OM_PASSWORD` | `openmetadata_db` / `openmetadata_user` / `openmetadata_password` | `.env.example` and Compose fallbacks |
| `POSTGRES_OM_AIRFLOW_DB` | `airflow_db` | `docker-compose.yml` |
| `RATE_LIMIT_OPENAQ` / `RATE_LIMIT_AQICN` | `0.8` / `1.0` | `python_jobs/common/config.py` |
| `BATCH_SIZE` / `MAX_WORKERS` / `MAX_RETRIES` | `1000` / `4` / `3` | `python_jobs/common/config.py` |
| `CSV_OUTPUT_DIR` | `landing_zone` | `scripts/gdrive_uploader.py` and `python_jobs/common/writer_factory.py` |
| `MAX_SYNC_WORKERS` | `5` | `python_jobs/jobs/sync/gdrive_sync.py` |
| `DBT_PACKAGES_INSTALL_PATH` / `DBT_TARGET_PATH` | `dbt_packages` / `target` | `dbt/dbt_tranform/dbt_project.yml` |
| `CLICKHOUSE_SERVICE` / `CLICKHOUSE_SCHEMA` | `ClickHouse` / `air_quality` | `openmetadata/custom_connectors/streamlit.py` |

Set the core ClickHouse variables explicitly in `.env` instead of relying on file-local defaults. That avoids the `air_quality` versus `airquality` drift already present in the checked-in configuration.

## Per-environment Overrides

This repository does not include `.env.development`, `.env.production`, or `.env.test` files. The current environment strategy is file- and compose-driven:

1. Local Docker development uses a root `.env` file loaded by `docker-compose.yml`. Start from `.env.example` and add the extra variables Compose expects but the example file does not list yet, especially `AIRFLOW_ADMIN_USERNAME`, `AIRFLOW_ADMIN_PASSWORD`, `TOMTOM_API_KEY`, and the Google Drive OAuth variables if that integration is enabled.
2. CI and lightweight local verification use `docker-compose.test.yml`, which hardcodes a minimal ClickHouse-only test stack and does not depend on the full local `.env` surface.
3. dbt has its own target-level overrides in `dbt/dbt_tranform/profiles.yml` with `dev`, `production`, and `ci` outputs. The checked-in target is `production`.
4. Airflow, Grafana, OpenMetadata, and Streamlit use checked-in config files for service defaults. Environment variables override only the values those files or Compose definitions explicitly expose.
5. Production-like deployments should keep secrets in the deployment platform's secret store and inject them as environment variables at runtime rather than editing checked-in files.

Known gaps in the current checked-in configuration:

- `.env.example` does not list every variable interpolated by `docker-compose.yml`.
- Grafana alert bot tokens are env-driven, but the checked-in `contact-points.yml` currently hardcodes chat IDs instead of using the exported `TELEGRAM_AQ_CHAT_ID` and `TELEGRAM_SYS_CHAT_ID`.
- Legacy variables such as `AQICN_API_TOKEN` and `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` remain in Python code paths even though the main Compose stack has moved to newer names or workflows.
