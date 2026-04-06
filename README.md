# Nền tảng Phân tích Chất lượng Không khí Việt Nam

Pipeline Data Engineering end-to-end: AQICN + Sensors.Community + OpenWeather → Python → ClickHouse → dbt → Streamlit

**Stack**: Python (Requests), ClickHouse, dbt, Apache Airflow, Streamlit, Prometheus + Grafana  
**Deployment**: Docker Compose

## Cấu trúc repository

- `dbt/`          → Data transformation models (staging → marts)
- `airflow/`      → DAGs & orchestration
- `python_jobs/`  → Python jobs for data ingestion
- `grafana/`      → Grafana provisioning (dashboards, datasources, alerting)
- `prometheus/`   → Prometheus configuration & scrape rules
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
| cAdvisor | 8080 | Container metrics |
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
| cAdvisor | 256MB | 0.25 |
| **Total** | **~7.5GB** | **~7** |


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
```
