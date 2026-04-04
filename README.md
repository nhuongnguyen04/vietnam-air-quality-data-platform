# Nền tảng Phân tích Chất lượng Không khí Việt Nam

Pipeline Data Engineering end-to-end: AQICN + Sensors.Community + OpenWeather → Python → ClickHouse → dbt → Streamlit

**Stack**: Python (Requests), ClickHouse, dbt, Apache Airflow, Streamlit, Prometheus + Grafana  
**Deployment**: Docker Compose

## Cấu trúc repository

- `dbt/`          → Data transformation models (staging → marts)
- `airflow/`      → DAGs & orchestration
- `python_jobs/`  → Python jobs for data ingestion
- `docker/`       → Compose files & env
- `monitoring/`   → Grafana dashboards

## Hardware Requirements

- **Minimum:** 16GB RAM, 4 CPU cores
- **Recommended:** 16GB RAM, 8 CPU cores
- Docker Desktop (Mac/Windows) or Docker Engine on Linux
- At least 20GB free disk space (ClickHouse data + logs)

### Resource Allocation

The Docker Compose stack allocates the following resources per service:

| Service | Memory | CPUs |
|---------|--------|------|
| ClickHouse | 3GB | 2 |
| PostgreSQL | 1GB | 1 |
| Airflow Scheduler | 512MB | 1 |
| Airflow Dag-Processor | 512MB | 1 |
| Airflow Triggerer | 512MB | 1 |
| **Phase 0 Total** | **~6GB** | **5** |

Future phases (Grafana, OpenMetadata) add approximately 3.5GB more, for a fully deployed stack of approximately 9.5GB.


---

## Streamlit Analytics Dashboard (Phase 3.2)

Real-time AQI analytics dashboard for Vietnam — Streamlit-based analytics (Phase 3.2).

### Running Locally

```bash
pip install -r python_jobs/dashboard/requirements.txt
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_USER=admin
export CLICKHOUSE_PASSWORD=<your-password>
streamlit run python_jobs/dashboard/app.py
# Dashboard: http://localhost:8501
```

### Running with Docker Compose

```bash
docker compose up -d dashboard
# Dashboard: http://localhost:8501
```

### Pages

| Page | Description | Data Source |
|------|-------------|-------------|
| Overview | AQI trends, city comparison, metrics | mart_analytics__trends, mart_analytics__geographic |
| Pollutants | PM2.5/PM10/O3/NO2 analysis, exceedance rates | mart_kpis__pollutant_concentrations |
| Source Comparison | AQICN vs Sensors.Community vs OpenWeather | mart_air_quality__daily_summary |
| Forecast | Forecast vs actual AQI, accuracy metrics | mart_analytics__forecast_accuracy |
| Alerts | Recent AQI alerts, frequency timeline | mart_air_quality__alerts |
