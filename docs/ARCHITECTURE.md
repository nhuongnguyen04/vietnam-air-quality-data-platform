<!-- generated-by: gsd-doc-writer -->
# Architecture

## System Overview

The Vietnam Air Quality Data Platform is a containerized, batch-oriented analytics system built around ClickHouse as the serving warehouse, Apache Airflow as the orchestrator, dbt as the transformation layer, and Streamlit plus an internal FastAPI service as the user-facing query layer. Its primary inputs are external air-quality, weather, and traffic sources (`AQI.in`, `OpenWeather`, `TomTom`) plus an alternate Google Drive CSV landing-zone path; its primary outputs are ClickHouse marts, the Streamlit analytics dashboard, the guarded Ask Data text-to-SQL workflow, OpenMetadata catalog/lineage assets, and Prometheus/Grafana operational telemetry. Architecturally, the repository is layered: orchestration and ingestion, warehouse modeling, serving interfaces, and metadata/monitoring services.

## Component Diagram

```mermaid
graph TD
    Sources[External Sources<br/>AQI.in / OpenWeather / TomTom]
    BatchAlt[GitHub Actions + Google Drive Landing Zone]
    Airflow[Airflow DAGs]
    Jobs[Python Jobs]
    ClickHouse[ClickHouse]
    dbt[dbt Transform Layer]
    Serving[Serving Layer<br/>Streamlit + FastAPI Text-to-SQL]
    Metadata[OpenMetadata]
    Monitoring[Prometheus + Grafana + Exporters]
    Users[Analysts / Operators]

    Sources --> Jobs
    Sources --> BatchAlt
    BatchAlt --> Airflow
    Airflow --> Jobs
    Airflow --> dbt
    Jobs --> ClickHouse
    dbt --> ClickHouse
    ClickHouse --> Serving
    dbt --> Metadata
    ClickHouse --> Metadata
    ClickHouse --> Monitoring
    Airflow --> Monitoring
    Serving --> Users
    Metadata --> Users
    Monitoring --> Users
```

### Major Components

- `airflow/dags/dag_ingest_hourly.py` runs the main ingestion cycle and triggers `dag_transform` after successful source collection.
- `airflow/dags/dag_sync_gdrive.py` handles the alternate ingest path where CSV files are produced elsewhere, staged in Google Drive, then synchronized into ClickHouse.
- `python_jobs/jobs/` contains source-specific collectors for AQI.in, OpenWeather, TomTom, Google Drive sync, and OpenMetadata bootstrap tasks.
- `dbt/dbt_tranform/` converts raw warehouse tables into staged, intermediate, and mart-layer models, then emits `manifest.json`, `catalog.json`, and `run_results.json`.
- `python_jobs/dashboard/` is the interactive Streamlit dashboard that reads `dm_*` and `fct_*` models from ClickHouse.
- `python_jobs/text_to_sql/` is an internal FastAPI service that generates SQL, validates it against a mart-only contract, and executes it with a read-only ClickHouse client.
- `openmetadata/` and `python_jobs/jobs/openmetadata/` together provide metadata ingestion configs plus glossary/governance bootstrap code.
- `monitoring/` provisions Prometheus, Grafana, alerting rules, and custom exporters for runtime observability.

## Data Flow

1. **Source acquisition**
   Airflow starts in `dag_ingest_hourly()` and runs three parallel collectors: `jobs/aqiin/ingest_measurements.py`, `jobs/openweather/ingest_openweather_unified.py`, and either `jobs/traffic/ingest_tomtom.py` or `jobs/traffic/generate_offpeak_traffic.py`, depending on Vietnam local time. These jobs call external APIs through shared helpers in `python_jobs/common/`.

2. **Raw persistence**
   The ingestion jobs obtain a writer from `get_data_writer()` in `python_jobs/common/writer_factory.py`. In normal runtime they batch-write into ClickHouse raw tables through `ClickHouseWriter`; in CI or alternate batch flows they can emit CSV files instead.

3. **Alternate landing-zone path**
   `.github/workflows/scheduled_ingestion.yml` can run the ingestion scripts with `INGEST_MODE=csv` and upload CSV outputs to Google Drive. `dag_sync_gdrive()` then downloads those files, maps folder paths to target raw tables, inserts them into ClickHouse, and only triggers downstream transforms if new data was synchronized.

4. **Transformation and test execution**
   `dag_transform()` runs `dbt deps`, `dbt seed`, `dbt run` for `staging`, `intermediate`, and `marts`, followed by `dbt test` and `dbt docs generate`. The dbt project is explicitly layered in `dbt_project.yml`, with staging models materialized as tables and downstream layers materialized incrementally.

5. **Model unification**
   The intermediate layer merges source-specific measurements into common structures. For example, `int_core__measurements_unified.sql` unions AQI.in and OpenWeather measurements, applies source weighting and calibration, and enriches each record with Vietnamese regional dimensions before downstream marts consume it.

6. **Serving-model construction**
   Core and analytics marts build query-ready warehouse tables such as `fct_aqi_weather_traffic_unified.sql` and the `dm_air_quality_*` models. These marts join air quality, weather, traffic, and administrative dimensions into dashboard-facing and text-to-SQL-facing datasets.

7. **User query path**
   The Streamlit app (`python_jobs/dashboard/app.py`) renders multiple analytics pages and queries ClickHouse through dashboard library code. The Ask Data page calls the internal FastAPI service; that service uses `VannaRuntime` to generate SQL, `validate_sql()` to enforce the mart-only contract, issues a preview token, and executes only preview-approved SQL through `ClickHouseExecutor`.

8. **Metadata and governance**
   After dbt runs produce artifacts, OpenMetadata ingests warehouse objects and dbt lineage from the shared `dbt-target` volume. `dag_openmetadata_curation()` separately runs `setup_governance.py` and `setup_glossary.py` so dbt-owned domains, owners, and glossary terms resolve in the catalog.

9. **Observability**
   Prometheus scrapes ClickHouse, PostgreSQL, node, and container metrics; Grafana provisions dashboards and alerting from repo-managed configuration. This keeps operational monitoring separate from the analytical serving path while still observing Airflow, warehouse, and host/container health.

## Key Abstractions

| Abstraction | Location | Purpose |
| --- | --- | --- |
| `dag_ingest_hourly()` | `airflow/dags/dag_ingest_hourly.py` | Main orchestration entry point for hourly source ingestion and downstream transform triggering. |
| `dag_sync_gdrive()` | `airflow/dags/dag_sync_gdrive.py` | Alternate ingestion DAG that bridges Google Drive landing-zone files into ClickHouse raw tables. |
| `dag_transform()` | `airflow/dags/dag_transform.py` | Warehouse build pipeline for `dbt deps`, `seed`, layered runs, tests, docs generation, and artifact patching. |
| `APIClient` | `python_jobs/common/api_client.py` | Shared retrying HTTP client used by ingestion jobs to talk to external providers. |
| `TokenManager` | `python_jobs/common/token_manager.py` | Multi-token rotation and per-token rate-limit management for high-throughput API ingestion. |
| `DataWriter` / `get_data_writer()` | `python_jobs/common/base_writer.py`, `python_jobs/common/writer_factory.py` | Write abstraction that switches between ClickHouse persistence and CSV output depending on runtime mode. |
| `ClickHouseWriter` | `python_jobs/common/clickhouse_writer.py` | Batch insert implementation for raw table ingestion, including retry and batch-id decoration. |
| `update_control()` | `python_jobs/common/ingestion_control.py` | Writes operational run metadata into the `ingestion_control` warehouse table after pipeline steps complete. |
| `VannaRuntime` | `python_jobs/text_to_sql/vanna_runtime.py` | SQL-generation boundary for Ask Data; trains Vanna on the approved mart catalog but does not execute SQL. |
| `validate_sql()` | `python_jobs/text_to_sql/sql_validator.py` | Contract gate that rejects non-`SELECT` queries, non-allowlisted tables, and raw/staging/intermediate access. |
| `ClickHouseExecutor` | `python_jobs/text_to_sql/clickhouse_executor.py` | Read-only execution boundary that adds row caps and timeout settings to approved Ask Data queries. |

## Directory Structure Rationale

The repository keeps orchestration, source collection, warehouse modeling, serving interfaces, and operational infrastructure in separate top-level directories because each layer has a different runtime, lifecycle, and deployment boundary.

```text
airflow/         DAG definitions, Airflow image/config, and mounted runtime directories for orchestration
dbt/             ClickHouse transformation project with macros, seeds, staging, intermediate, and mart models
python_jobs/     Shared ingestion libraries, source collectors, Streamlit dashboard, and FastAPI text-to-SQL service
monitoring/      Prometheus scrape/rule config, Grafana provisioning, and the custom Docker stats exporter
openmetadata/    Catalog ingestion configs, custom connectors, and local OpenMetadata persistence mounts
tests/           Cross-cutting Python tests for Airflow, common utilities, ingestion jobs, and models
scripts/         Bootstrap and operational scripts such as ClickHouse initialization
docs/            Project documentation maintained alongside the implementation
postgres/        PostgreSQL initialization scripts for the shared Airflow/OpenMetadata metadata store
data/            Static supporting datasets such as OSM-derived inputs used by traffic enrichment
```

Additional stateful directories such as `clickhouse-data/`, `airflow/data/`, `airflow/logs/`, `openmetadata/elasticsearch-data/`, and `logs/` exist to back Docker volumes and local runtime state rather than to define application logic. Keeping them outside the main source directories helps preserve a clean separation between code, declarative infrastructure, and mutable local data.
