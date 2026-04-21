# Nền tảng Phân tích Chất lượng Không khí Việt Nam

Pipeline Data Engineering end-to-end: AQICN + Sensors.Community + OpenWeather → Python → ClickHouse → dbt → Streamlit

**Stack**: Python (Requests), ClickHouse, dbt, Apache Airflow, Streamlit, Prometheus + Grafana  
**Deployment**: Docker Compose

## Cấu trúc repository

- `dbt/`          → Data transformation models (staging → marts)
- `airflow/`      → DAGs & orchestration
- `python_jobs/`  → Python jobs for data ingestion
- `monitoring/grafana/`   → Grafana provisioning (dashboards, datasources, alerting)
- `monitoring/prometheus/` → Prometheus configuration & scrape rules
- `scripts/`      → Database initialization scripts

## Services

All services run via Docker Compose on the `air-quality-network` network.

| Service | Port | Purpose |
|---------|------|---------|
| ClickHouse | 8123 (HTTP), 9000 (native), 9440 (TLS) | Analytical database |
| PostgreSQL | 5432 | Airflow metadata database |
| Airflow Webserver | 8090 | Airflow UI + REST API |
| Airflow Scheduler | — | DAG scheduling |
| Airflow Dag-Processor | — | DAG file parsing |
| Airflow Triggerer | — | Deferred task execution |
| Streamlit Dashboard | 8501 | Analytics dashboards (5 pages) |
| Grafana | 3000 | Operational monitoring + alerting |
| Prometheus | 9090 | Metrics collection |
| Node Exporter | 9100 | Host-level metrics |
| Docker Stats Exporter | 9888 | Container metrics |
| PostgreSQL Exporter | 9187 | PostgreSQL metrics for Prometheus |

## Hardware Requirements

- **Minimum:** 16GB RAM, 4 CPU cores
- **Recommended:** 16GB RAM, 8 CPU cores
- Docker Desktop (Mac/Windows) or Docker Engine on Linux
- At least 20GB free disk space (ClickHouse data + logs)

### Resource Allocation

| Service | Memory | CPUs |
|---------|--------|------|
| ClickHouse | 3GB | 2 |
| PostgreSQL | 1GB | 1 |
| Airflow Scheduler | 512MB | 1 |
| Airflow Dag-Processor | 512MB | 1 |
| Airflow Triggerer | 512MB | 1 |
| Airflow Webserver | 512MB | 1 |
| Streamlit Dashboard | 512MB | 0.5 |
| Grafana | 512MB | 0.5 |
| Prometheus | 512MB | 0.25 |
| PostgreSQL Exporter | 256MB | 0.25 |
| Node Exporter | 128MB | 0.25 |
| Docker Stats Exporter | 128MB | 0.25 |
| **Total** | **~7.4GB** | **~7** |


---

## Streamlit Analytics Dashboard (Phase 3.2)

Real-time AQI analytics dashboard for Vietnam — Streamlit-based analytics (Phase 3.2).

### Running with Docker Compose

```bash
docker compose up -d dashboard
# Dashboard: http://localhost:8501
```

### Pages

| Page | Description |
|------|-------------|
| Overview | AQI trends, city comparison, current AQI metrics |
| Pollutants | PM2.5/PM10/O3/NO2 analysis, exceedance rates |
| Source Comparison | AQICN vs Sensors.Community vs OpenWeather comparison |
| Forecast | Forecast vs actual AQI, accuracy metrics |
| Alerts | Recent AQI alerts, frequency timeline |

## Grafana Operational Dashboards (Phase 3.3)

Operational monitoring dashboards (anonymous access — no login required).

### Running with Docker Compose

```bash
docker compose up -d grafana
# Grafana: http://localhost:3000
```

### Dashboards

| Dashboard | Purpose |
|-----------|---------|
| Pipeline Health | DAG success rate, task execution trends, records ingested per source per hour, API error rate |
| Data Freshness | Max timestamp per source, lag seconds, rows ingested per hour, active station count |

### Alerting

Grafana sends critical alerts to Telegram:
- AQI > 200 (Very Unhealthy)
- DAG failure
- ClickHouse down

Configure Telegram by setting `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in `.env`.

## Quick Start

```bash
# Start all services
docker compose up -d

# Check service status
docker compose ps

# View logs
docker compose logs -f clickhouse
docker compose logs -f airflow-scheduler

# Access services
# - Streamlit Dashboard: http://localhost:8501
# - Grafana: http://localhost:3000 (anonymous access)
# - Airflow: http://localhost:8090
# - Prometheus: http://localhost:9090
# - ClickHouse: http://localhost:8123
# - OpenMetadata: http://localhost:8585
```

## OpenMetadata Integration (Phase 4)

OpenMetadata 1.12.4 cung cấp data catalog, dbt lineage graph, và data quality governance.

### Access
- **URL:** http://localhost:8585
- **Credentials:** `admin@open-metadata.org` / `admin`
- **OM Bundled Airflow:** http://localhost:8080 (admin / admin)

### Architecture
- **OM Server:** `openmetadata/server:1.12.4` — catalog UI + API (port 8585)
- **OM Ingestion:** `openmetadata/ingestion:1.12.4` — pipeline runner (port 8080)
- **PostgreSQL:** dùng chung với Airflow metadata (database: `openmetadata_db`)
- **Elasticsearch:** `docker.elastic.co/elasticsearch/elasticsearch:7.16.3` — search index

### Services Connected
| Service | Connector | Schedule |
|---------|----------|----------|
| ClickHouse (air_quality) | OM ClickHouse connector | Hourly |
| dbt lineage | OM dbt connector | Hourly |
| Airflow DAGs | OM Airflow connector | Manual |

### Key Catalog Entities
- **Databases:** `air_quality` (ClickHouse)
- **Schemas:** `air_quality`
- **Tables:** 20+ (raw_* + stg_* + int_* + fct_* + mart_*)
- **Pipelines:** 3+ (dag_ingest_hourly, dag_transform, dag_openmetadata_curation)
- **Glossary:** AirQuality (7 terms: AQI, PM2.5, PM10, O₃, NO₂, SO₂, CO)

### Catalog Curation
Curation tự động qua `dag_openmetadata_curation` (chạy `35 * * * *`):
- Owners, tags, tier assignments cho tất cả mart + raw tables
- Glossary terms setup
- OM ClickHouse connector owners/tags limitation được workaround qua REST API

### Data Quality
- **Source of truth:** dbt tests (46 tests, Plan 2.3)
- OM đọc kết quả từ `target/run_results.json` qua dbt ingestion pipeline
- Quality dashboard trong OM hiển thị pass/fail status cho mỗi test

### Environment Variables
```bash
OPENMETADATA_URL=http://openmetadata:8585/api
OM_ADMIN_USER=admin@open-metadata.org
OM_ADMIN_PASSWORD=admin
POSTGRES_OM_DB=openmetadata_db
POSTGRES_OM_USER=openmetadata_user
POSTGRES_OM_PASSWORD=openmetadata_password
CLICKHOUSE_OM_READER_USER=om_reader
CLICKHOUSE_OM_READER_PASSWORD=om_reader_secure_pass
```

### Troubleshooting
- **OM server không lên:** Kiểm tra `docker compose ps openmetadata` — healthcheck phải healthy
- **Catalog trống:** Chạy OM ingestion thủ công (Settings → Services → Run)
- **dbt lineage không hiển thị:** Đảm bảo `dbt run` đã chạy và `target/manifest.json` tồn tại
- **Credentials sai:** OM credentials là `admin@open-metadata.org` / `admin` (KHÔNG phải `admin` / `admin`)

## Phase 5: Alerting & Reporting

**Status:** In progress

Phase 5 adds end-to-end alerting and automated weekly reporting to the platform.

### Alerting Architecture

All alerts are managed via **Grafana native alerting** (no separate DAG). Evaluated every 1 minute. Telegram is the sole notification channel.

**Alert flow:**
```
Grafana evaluates rules (every 1 min)
  → Threshold breached
    → Contact point: telegram-critical
      → Telegram Bot API
        → Chat ID 5602934306
```

### Alert Rules

| UID | Alert | Threshold | Severity | Re-alert |
|-----|-------|-----------|----------|---------|
| `aqi-critical-200` | AQI Critical | AQI > 200 | 🔴 CRITICAL | 1h |
| `aqi-warning-150` | AQI Warning | AQI > 150 | 🟡 WARNING | 1h |
| `pm25-warning-75` | PM2.5 Warning | PM2.5 > 75 µg/m³ | 🟡 WARNING | 1h |
| `multi-source-divergence` | Source Divergence | \|AQICN−OW\| > 50 | 🟡 WARNING | 1h |
| `dag-failure-critical` | DAG Failure | any DAG fails | 🔴 CRITICAL | 1h |
| `clickhouse-down-critical` | ClickHouse Down | ping fails | 🔴 CRITICAL | 1h |
| `data-freshness-warning` | Data Freshness | lag > 3h | 🟡 WARNING | 30min |
| `station-stale-warning` | Station Stale | no data > 2h | 🟡 WARNING | 30min |

> **Severity prefix:** All Telegram messages are prefixed with `🔴 CRITICAL:` or `🟡 WARNING:` for quick visual triage.

### Weekly Report

Automated Telegram report every **Monday at 09:00 (UTC+7)** via `dag_weekly_report`.

**Content:**
- City AQI averages (7-day)
- Top 5 worst stations
- 7-day trend vs previous week
- Dominant pollutant per city

**Manual trigger:**
```bash
docker compose exec airflow-webserver airflow dags trigger dag_weekly_report
```

### Smoke Test

On-demand E2E smoke test: `dag_smoke_test` (schedule=None).

```bash
docker compose exec airflow-webserver airflow dags trigger dag_smoke_test
```

### Operational Runbook

Full operational guide: `docs/runbook.md`
