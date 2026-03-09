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

