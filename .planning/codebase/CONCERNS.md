# Known Concerns & Technical Debt

## 🔴 High Priority

### 1. API Secrets in .env (Critical)
The `.env` file contains plaintext API tokens and secrets committed or accessible on disk:
```
OPENAQ_API_TOKEN=f939555d8d416ec60bdd233ac7526dd3f8c6304c017d9d03c9b25c37d9a122c0
AQICN_API_TOKEN=9873cbcfaffab43aa61d281ada181c486e1f38df
AIRFLOW_API_SECRET_KEY=594n3CS/Z/B8lGqaRrliBxA39XhJoTy3vawaY9DnxDnFldA80To40yr8hY/9QoYk
AIRFLOW_API_AUTH_JWT_SECRET=SfNYLbZTGOeFtKxleNjuhDRo9mlIoJykyTFLYXuE2M6cZRhKzKvaFYqDP2kA9BTi
AIRFLOW_WEBSERVER_SECRET_KEY=nK3gDHz/jf567peunEwET0/mx0CMLSzoNL0u9N8nNHeX6t99dtC3rgeKLIL86Ufa
```
**Fix**: Use Docker secrets, Vault, or a secrets manager.

### 2. AIRFLOW__CORE__FERNET_KEY= (Empty)
Encryption key for connections/passwords in Airflow DB is empty — credentials stored in PostgreSQL are unencrypted.
**Fix**: Generate and set a Fernet key.

### 3. Missing Superset in docker-compose.yml
README states "Superset" is part of the stack, but there is no Superset service in docker-compose.yml. The `monitoring/` directory with Grafana dashboards also exists but is not referenced in the compose file.
**Fix**: Add Superset and Grafana services, or remove from README.

### 4. API Rate Limit Handling
Python jobs use `TokenBucketRateLimiter` for client-side rate limiting, but no retry logic at the API response level. External APIs may return 429 (rate limited) without automatic backoff.
**Fix**: Add tenacity-based retry with exponential backoff to `APIClient`.

## 🟡 Medium Priority

### 5. Docker-in-Docker (docker.sock Mount)
Airflow services mount `/var/run/docker.sock` — allows containers to spawn sibling containers. This is a security risk in production.
**Fix**: Use Kubernetes operator or Airflow executor without Docker-in-Docker.

### 6. Airflow Auth is basic_auth Only
`AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth` — no OAuth/OIDC. Passwords transmitted in plain HTTP headers.
**Fix**: Use HTTPS + OAuth provider, or at minimum enable HTTPS.

### 7. ClickHouse Deduplication in Python
Dedup logic lives in Python jobs, not in ClickHouse. If two batches insert the same data, duplicates will exist in `raw_openaq_measurements` until a MergeTree merge deduplicates (MergeTree doesn't dedup by default — only ReplacingMergeTree does).
**Fix**: Use `ReplacingMergeTree` engine for `raw_openaq_measurements` with a version column, or add dedup step post-insert.

### 8. No GitHub Actions / CI Pipeline
No `.github/workflows/` directory found. Code is deployed directly via Docker Compose with no automated testing.
**Fix**: Add CI workflow: lint → dbt test → docker build.

### 9. dbt `target/` Directory Tracked in Git
The `dbt/dbt_tranform/target/` directory (compiled/run artifacts) is not in `.gitignore`. This should be excluded to prevent large diffs.
**Fix**: Add `target/` to `.gitignore`.

## 🟢 Low Priority / Hygiene

### 10. No Python Unit Tests
`python_jobs/` has no `tests/` directory, no pytest config, no mocking. All validation is manual or via production runs.
**Fix**: Add pytest with mocked API responses and ClickHouse fixture.

### 11. ClickHouse DB Name Mismatch
`.env` sets `CLICKHOUSE_DB=air_quality` (underscore), docker-compose.yml uses `${CLICKHOUSE_DB}` but init script references `${CLICKHOUSE_DB}` — potential inconsistency depending on which .env loads.
**Fix**: Standardize to `airquality` (no underscore) across all configs.

### 12. DAG Email on Failure Without SMTP Config
`email_on_failure: True` is set on `dag_transform`, `dag_ingest_historical`, and `dag_metadata_update`, but no SMTP server is configured in `airflow.cfg`.
**Fix**: Configure SMTP or remove `email_on_failure`.

### 13. dbt `target/` and `dbt_packages/` Paths in Docker
Airflow mounts `dbt/dbt_tranform` directly. The `target/` and `dbt_packages/` paths are inside the project directory. This means dbt runs on the host can conflict with runs inside the container.
**Fix**: Mount `target/` and `dbt_packages/` as separate named volumes.

### 14. Historical DAG Has No Protection Against Concurrent Runs
`dag_ingest_historical` has `max_active_runs: 1` but no semaphore or lock — a user could trigger it twice in the Airflow UI.
**Fix**: Add ExternalTaskMarker or a lock mechanism.

### 15. No Data Freshness Monitoring
No alerting on stale data (e.g., no new measurements in the last N hours). A broken API would silently produce outdated analytics.
**Fix**: Add Airflow SLA + email alerts, or Prometheus exporter for data freshness.

### 16. Airflow LocalExecutor
`AIRFLOW__CORE__EXECUTOR=LocalExecutor` — no parallelism across workers. Scales poorly with many concurrent DAGs.
**Fix**: Move to Celery or Kubernetes executor for production scale.
