# Plan — Docker Compose Test & Verify Môi Trường Hoàn Chỉnh

## Context
- **Project:** Vietnam Air Quality Data Platform
- **Mode:** `quick` — atomic, self-contained, no research phase
- **STATE:** Phase 0 (Foundation), Wave 1 done, Wave 2 plan 03 (CI Pipeline Bootstrap) pending
- **Target:** Verify full stack Docker Compose environment is healthy before Phase 1

## Background
- Docker Compose config exists at `docker-compose.yml` with 6 services: `clickhouse`, `postgres`, `airflow-webserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`
- `.env` exists with all required secrets/tokens
- Multiple stale containers exist from previous experiments (nifi, openmetadata, superset, etc.)
- No containers from this project currently running

## Tasks

### Task 1 — Dọn dẹp stale containers rác
**Action:** `docker ps -a --format "{{.Names}}" | grep -E "^(dataflow|nifi|openmetadata|vanna|airflow_elt|postgres_source|superset|airbyte|clickhouse_server)" | xargs -r docker rm -f 2>/dev/null`
**Why:** Các container cũ chiếm tài nguyên và có thể gây port conflict (e.g. superset 8088, clickhouse cũ 8123/9000)
**Verification:** `docker ps --format "{{.Names}}" | grep -E "dataflow|nifi|openmetadata|vanna|airflow_elt|postgres_source|superset|airbyte|clickhouse_server"` → empty

### Task 2 — Khởi động Docker Compose stack
**Action:** `docker compose up -d` (từ project root `/home/nhuong/vietnam-air-quality-data-platform/`)
**Why:** Start toàn bộ 6 services theo docker-compose.yml
**Expected:** all 6 services: clickhouse, postgres, airflow-webserver, airflow-scheduler, airflow-dag-processor, airflow-triggerer
**Timeout:** 5 phút cho lần đầu (build image + init DB)

### Task 3 — Verify tất cả services healthy
**Actions:**
1. `docker compose ps` → all services `healthy` hoặc `running`
2. `docker exec clickhouse clickhouse-client --query "SELECT 1"` → `1`
3. `docker exec airflow-webserver1 curl -sf http://localhost:8080/api/v2/monitor/health` → `{"status":"healthy"}`
4. `docker exec airflow-postgres1 pg_isready -U airflow` → `accepting connections`
5. Kiểm tra logs không có `FATAL` hoặc `ERROR` repeated patterns trong 60s đầu
**Why:** Đảm bảo stack hoạt động đầy đủ trước khi tiếp tục Phase 1

## Exit Criteria
- ✅ All 6 services running with `healthy` status
- ✅ ClickHouse ping OK, postgres accepting connections
- ✅ Airflow webserver health check returns 200
- ✅ No FATAL errors in logs

## Risks & Notes
- **Port 8088 conflict:** superset đã chiếm port 8088 trên host → cần confirm docker-compose airflow chạy đúng port 8090 (mapped 8090:8080)
- **Airflow health check port:** docker-compose dùng `8090:8080` nhưng healthcheck vẫn dùng `localhost:8080` trong container — đúng vì healthcheck chạy trong container
- **First start:** Airflow cần init DB qua `airflow db migrate` → có thể mất 1-2 phút
