# OpenMetadata Integration

## Directories

- `data/` — OM server persistent data (mounted as volume)
- `elasticsearch-data/` — Elasticsearch index data (auto-created by container)
- `ingestion-configs/` — OM Ingestion workflow YAML configs (mounted into om-ingestion container)

## Overview

This directory contains OpenMetadata ingestion configurations for the Vietnam Air Quality Data Platform.

OpenMetadata provides:
- **Data Catalog**: All ClickHouse tables (raw + staging + intermediate + marts) documented and searchable
- **Lineage**: dbt model lineage graph (staging → intermediate → marts flow)
- **Data Quality**: SQL tests, freshness monitoring, profiler statistics

## Architecture

```
dbt run (dag_transform)
  └── target/
        ├── manifest.json    ← dbt lineage graph
        ├── catalog.json     ← column metadata
        └── run_results.json ← test results

OpenMetadata Ingestion
  └── OM UI / API
        ├── ClickHouse service  ← scans tables (raw/stg/int/mart)
        └── dbt service         ← ingest manifest → lineage graph
```

## Services (Phase 4.01+)

All services run in the main `docker-compose.yml`:

| Service | Container | Port | Purpose |
|---------|-----------|------|---------|
| openmetadata | openmetadata | 8585 | OM server + UI |
| om-ingestion | openmetadata-ingestion | 8080 | OM Airflow-based ingestion scheduler |
| elasticsearch | openmetadata-elasticsearch | (internal: 9200) | OM search index |
| postgres | airflow-postgres1 | 5432 | Shared: Airflow metadata + OM metadata |

> **Note:** OpenMetadata requires 4GB+ RAM total.

## Credentials

| | |
|-|-|
| OM Login | `admin@open-metadata.org` / `admin` |
| OM DB Host | `postgres` (shared with Airflow) |
| OM DB Port | `5432` |
| OM DB Database | `openmetadata_db` |
| OM DB User | `openmetadata_user` |
| OM DB Password | `openmetadata_password` |
| ClickHouse Reader | `om_reader` / `om_reader_secure_pass` |

## Setup Instructions (One-time)

### Step 1: Start OpenMetadata

OpenMetadata starts together with the main stack:

```bash
# From project root — ensure the network exists first
docker network create air-quality-network 2>/dev/null || true
docker compose up -d postgres
sleep 10

# Now start OM services
docker compose up -d elasticsearch openmetadata om-ingestion

# Wait for OM to be healthy (~3 minutes)
docker compose ps openmetadata
```

### Step 2: Add ClickHouse Service (via OM UI)

1. Navigate to http://localhost:8585
2. Login with `admin@open-metadata.org` / `admin`
3. Go to **Settings** (gear icon) → **All Services** → **Add Service** → **Database** → **ClickHouse**
4. Fill in:

| Field | Value |
|-------|-------|
| Name | `Vietnam Air Quality ClickHouse` |
| Host | `clickhouse` |
| Port | `8123` |
| Username | `om_reader` |
| Password | `om_reader_secure_pass` |
| Database Name | `air_quality` |
| Scheme | `http` |

5. Connection Options:
   - `allow_no_schemas`: `false`
   - `clickhouse_compatibility`: `true`

6. **Ingestion Pipeline**:
   - Schedule: **hourly** (`0 * * * *`)
   - Enable: ✅ Include Tables
   - ❌ Mark Deleted Tables = `false` (ClickHouse DDL not true drops)
   - Filter: Include `air_quality`, Exclude `system.`, `_tmp`, `_bytes`

7. **Save → Run Ingestion immediately**

### Step 3: Add dbt Service (via OM UI)

> **Prerequisite**: `dag_transform` must have run at least once so that
> `dbt/dbt_tranform/target/manifest.json`, `catalog.json`, `run_results.json`
> exist. If not, run manually:
> ```bash
> docker compose exec airflow-scheduler dbt run --target production
> ```

1. Go to **Settings** → **All Services** → **Add Service** → **dbt** → **dbt Local**
2. Fill in:

| Field | Value |
|-------|-------|
| Name | `Vietnam Air Quality dbt` |
| dbt Manifest File Path | `/opt/dbt-artifacts/manifest.json` |
| dbt Catalog File Path | `/opt/dbt-artifacts/catalog.json` |
| dbt Run Results File Path | `/opt/dbt-artifacts/run_results.json` |
| dbt Classification | `enabled` |
| Include Tags | ✅ |
| Mark Deleted Tables | ✅ |

3. **Ingestion Pipeline**: Schedule: **hourly** (`0 * * * *`)
4. **Save → Run Ingestion immediately**

### Step 5: Add Streamlit Service (via custom connector)

1. Ensure the `om-ingestion` container is running and has the latest volumes.
2. Run the ingestion command from the project root:
   ```bash
   docker compose exec om-ingestion metadata ingest -c /opt/airflow/plugins/ingestion-configs/streamlit-workflow.yaml
   ```
3. This will create:
   - A **Dashboard Service** named `Streamlit`
   - A **Dashboard** named `VN_Air_Quality_Dashboard`
   - **Charts** for each Streamlit page with descriptions extracted from docstrings.
   - **Lineage** between ClickHouse tables and Streamlit charts based on SQL queries found in the code.

### Step 6: Verify (via OM API)

```bash
# Login and get token
TOKEN=$(curl -s -X POST http://localhost:8585/api/v1/users/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin@open-metadata.org","password":"admin"}' | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

# Check table count
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8585/api/v1/tables?database=air_quality&limit=100" | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Total tables: {d[\"total\"]}')"

# Check lineage edges
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8585/api/v1/lineage" | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Lineage edges: {len(d.get(\"data\",[]))}')"
```

**Expected**:
- Total tables: ≥ 20 (all raw + staging + intermediate + marts)
- Lineage edges: > 0 (stg → int → mart flow visible)

## Workflow YAML Files

These files are also available for CLI-based ingestion:

```bash
# ClickHouse ingestion (via OM ingestion CLI)
openmetadata ingest -file ./openmetadata/ingestion-configs/clickhouse-workflow.yaml

# dbt ingestion
openmetadata ingest -file ./openmetadata/ingestion-configs/dbt-workflow.yaml
```

## Maintenance

### Refresh OM Catalog Manually

```bash
# Trigger ClickHouse re-scan
curl -X POST http://localhost:8585/api/v1/pipelines/trigger/<pipeline-id> \
  -H "Authorization: Bearer $TOKEN"

# Trigger dbt re-ingestion
curl -X POST http://localhost:8585/api/v1/pipelines/trigger/<dbt-pipeline-id> \
  -H "Authorization: Bearer $TOKEN"
```

### Stop OpenMetadata

```bash
docker compose stop elasticsearch openmetadata om-ingestion
```
