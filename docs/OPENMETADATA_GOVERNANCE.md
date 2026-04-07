# OpenMetadata Governance Guide

## Overview

OpenMetadata 1.12.4 cung cấp centralized data catalog cho Vietnam Air Quality Data Platform. Dbt là source of truth cho data quality tests.

## Catalog Structure

### Databases & Tables

| Layer | Tables | OM Tier |
|-------|--------|---------|
| Raw | raw_aqicn_*, raw_sensorscm_*, raw_openweather_* | Tier3 |
| Staging | stg_aqicn__*, stg_openweather__*, stg_sensorscm__* | Tier3 |
| Intermediate | int_* | Tier2 |
| Mart | fct_*, dim_*, mart_air_quality__* | Tier1/Tier2 |

### Tags

| Tag | Usage |
|-----|-------|
| `Data.AirQuality` | All air quality tables |
| `Data.Vietnam` | Tables with Vietnam location data |
| `Data.NoPII` | All tables (no personal data) |
| `Data.Raw` | Raw ingestion tables |
| `Data.AQICN` / `Data.OpenWeather` / `Data.SensorsCommunity` | Source attribution |
| `Data.Alerts` | Alert/event tables |
| `Data.Dashboard` | Dashboard-ready tables |
| `Data.Infrastructure` | Infrastructure metadata tables |
| `Tier.Tier1` | Mission-critical (fct_hourly_aqi, mart_air_quality__*) |
| `Tier.Tier2` | Analytical (dim_*, other marts) |
| `Tier.Tier3` | Raw/staging |

### Glossary

**AirQuality Glossary** (7 terms):

| Term | Description |
|------|-------------|
| AQI | Air Quality Index, 0–500 scale (US EPA) |
| PM25 | Fine Particulate Matter ≤ 2.5µm |
| PM10 | Coarse Particulate Matter ≤ 10µm |
| Ozone (O₃) | Ground-level ozone, secondary pollutant |
| NO₂ | Nitrogen Dioxide, traffic-related |
| SO₂ | Sulfur Dioxide, industrial emissions |
| CO | Carbon Monoxide, combustion byproduct |

## Standard Operating Procedures

### SOP-01: Thêm nguồn dữ liệu mới

1. **Tạo staging model** trong `dbt/models/staging/<source>/`
   - Đặt tên: `stg_<source>__<entity>.sql`
   - Thêm column docs: `{{ doc('<column_name>') }}`
   - Thêm tests trong `schema.yml`

2. **Thêm raw table** vào `scripts/init-clickhouse.sql`
   - Chọn engine phù hợp: MergeTree (append), ReplacingMergeTree (dedup)

3. **Cập nhật sources.yml** (`dbt/models/_sources.yml`)
   ```yaml
   - name: <source>
     description: ...
     tables:
       - name: <table>
   ```

4. **Thêm vào DAG ingest** (`airflow/dags/dag_ingest_hourly.py`)
   - Thêm `@task` mới cho source
   - Thêm vào fan-in: `[...existing, new_source_task] >> ...`

5. **Chạy OM ingestion** (manual trigger)
   - OM UI → Settings → Services → ClickHouse → Run

6. **Cập nhật catalog curation**
   - Thêm entry vào `curation_bulk.py` `RAW_CURATION` list
   - DAG `dag_openmetadata_curation` sẽ tự động sync

### SOP-02: Đổi tên cột trong dbt model

1. **Cập nhật model SQL** — đổi tên column
2. **Cập nhật `{{ doc() }}`** — giữ nguyên hoặc tạo doc mới
3. **Chạy `dbt run`** — tạo `target/manifest.json` và `target/catalog.json`
4. **OM dbt ingestion** tự động sync (hourly schedule)
   - Hoặc trigger manual: OM UI → dbt service → Run
5. **Verify** — OM Lineage tab hiển thị column name mới

### SOP-03: Xử lý dashboard lỗi (break/trace)

1. **Xác định bảng gốc** — OM Lineage graph trace từ dashboard table ngược về raw tables
2. **Kiểm tra data freshness** — `ingestion_control` table, Grafana dashboard
3. **Kiểm tra dbt errors** — OM Quality → Tests tab, `target/run_results.json`
4. **Check source API** — verify API endpoint hoạt động, rate limit OK

### SOP-04: Chạy OM Ingestion thủ công

1. **ClickHouse scan:** OM UI → Settings → Services → Vietnam Air Quality ClickHouse → Run
2. **dbt lineage:** OM UI → Settings → Services → Vietnam Air Quality dbt → Run
3. **Airflow DAGs:** OM UI → Settings → Services → Vietnam Air Quality Airflow → Run

### SOP-05: Khắc phục lỗi OM Ingestion

**Ingestion fails silently:**
```bash
# Check ingestion logs
docker compose logs openmetadata-ingestion | grep -A 20 "ERROR"

# Check OM server logs
docker compose logs openmetadata | grep -A 10 "ERROR"
```

**ClickHouse connector fails:**
```bash
# Verify om_reader credentials
clickhouse-client --user om_reader --password om_reader_secure_pass --query "SELECT 1"

# Check network from om-ingestion container
docker compose exec openmetadata-ingestion wget -qO- --timeout=10 http://clickhouse:8123/ping
```

**Elasticsearch health:**
```bash
curl http://localhost:9200/_cluster/health
# green/yellow là OK; red cần investigation
```

## OM Airflow Connector

Project Airflow (`apache/airflow:3.1.7`, port 8090) kết nối với OM qua OM Airflow Connector.

**Connection:** `http://airflow-webserver:8080/api/v1/` (internal network)
**Auth:** Basic Auth — `admin` / `admin` (project Airflow credentials)

**DAGs visible in OM:**
- `dag_ingest_hourly` — hourly measurement ingestion
- `dag_transform` — dbt transformation
- `dag_openmetadata_curation` — catalog curation
- `dag_ingest_historical` — historical backfill (manual trigger)
- `dag_metadata_update` — metadata refresh (daily)
- `dag_sensorscm_poll` — Sensors.Community polling

## Data Quality

### dbt Tests (Source of Truth)

46 dbt tests chạy trong `dag_transform` (schedule: `30 * * * *`). OM đọc kết quả từ `target/run_results.json` qua dbt ingestion pipeline.

**Test coverage:**
- Mart tables: not_null, unique, accepted_values
- Dimension tables: not_null on primary key, referential integrity
- Raw tables: row count freshness, timestamp validity

### OM Quality Dashboard

OM Quality tab hiển thị:
- All tests across tables
- Pass/fail status with timestamps
- Test execution history

### Alerting on Quality Failures

dbt test failures không trigger alerts tự động trong OM. Để alerting:
1. Dùng dbt test failures trong `dag_transform` (Airflow retry/failure notification)
2. Hoặc cấu hình OM webhook notifications cho test failures

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| OM UI blank page | Restart `docker compose restart openmetadata` |
| Catalog empty | Run ClickHouse ingestion manually |
| dbt lineage missing | Ensure `dbt run` has run at least once |
| DAGs not in OM | Check Airflow basic auth enabled; re-run Airflow connector ingestion |
| Elasticsearch red | `docker compose restart elasticsearch`; check memory |
| OM server OOM | Increase `mem_limit` from 2g to 3g in docker-compose.yml |