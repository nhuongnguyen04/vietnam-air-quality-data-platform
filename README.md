# Nền tảng Phân tích Chất lượng Không khí Việt Nam

Pipeline Data Engineering end-to-end: OpenAQ + AQICN → NiFi → ClickHouse → dbt → Superset

**Stack**: Apache NiFi, ClickHouse, dbt, Apache Airflow, Prometheus + Grafana, Superset  
**Deployment**: Docker Compose

## Cấu trúc repository

- `dbt/`          → Data transformation models (staging → marts)
- `airflow/`      → DAGs & orchestration
- `nifi/`         → Ingest flows
- `docs/`         → Quy ước, kế hoạch, samples
- `docker/`       → Compose files & env
- `monitoring/`   → Grafana dashboards

