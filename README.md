# Nền tảng Phân tích Chất lượng Không khí Việt Nam

Pipeline Data Engineering end-to-end: OpenAQ + AQICN → Python → ClickHouse → dbt → Superset

**Stack**: Python (Requests), ClickHouse, dbt, Apache Airflow, Prometheus + Grafana, Superset  
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

Future phases (Superset, Grafana, OpenMetadata) add approximately 5.5GB more, for a fully deployed stack of approximately 11.5GB.

