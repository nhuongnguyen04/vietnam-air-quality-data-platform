# Phase 3: Visualization & Monitoring — Research

**Researched:** 2026-04-03
**Domain:** Superset 4.x (analytics dashboards) + Grafana 11.x (operational dashboards + alerting)
**Confidence:** HIGH

---

## Summary

Phase 3 deploys two visualization/ops platforms alongside the existing 7-service stack: **Superset 4.x** for analyst dashboards (AQI, trends, forecasts) querying mart tables, and **Grafana 11.x** for operational monitoring (pipeline health, data freshness) querying `ingestion.control`. Public read-only access eliminates auth overhead on both tools. YAML provisioning is the established pattern for both: Grafana natively reads `provisioning/datasources/` and `provisioning/dashboards/` YAML files at startup; Superset uses CLI (`superset import-dashboards`) via an init container plus a `superset_config.py` for programmatic database connection. Grafana's ClickHouse plugin (`grafana-clickhouse-datasource`) is installed as a container plugin and connects via HTTP to `http://clickhouse:8123`. Telegram alerting uses Grafana's file-provisioned contact points and alert rules with a 30-min deduplication window.

---

## User Constraints (from 03-CONTEXT.md)

### Locked Decisions

| Decision | Choice | Notes |
|----------|--------|-------|
| D-01 | Superset public read-only (no login) | `PUBLIC_ROLE_LIKE_GAMMA = True` |
| D-02 | Grafana anonymous access | `GF_AUTH_ANONYMOUS_ENABLED=true` |
| D-03 | Superset YAML-first dashboard provisioning | `superset import-dashboards` CLI via init script |
| D-04 | Grafana YAML provisioning | `grafana/provisioning/datasources/`, `grafana/provisioning/dashboards/`, `grafana/provisioning/alerting/` |
| D-05 | Telegram webhook for alerts | `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` env vars |
| D-06 | Critical alerts only to Telegram | Pipeline failures + AQI > 200 |
| D-07 | Freshness warnings logged only (no Telegram) | 30-min alert deduplication |
| D-08–D-10 | Superset = analyst dashboards, Grafana = operational dashboards | No scope overlap |
| D-11 | Superset: `apache/superset:4.1.0`, port **8088**, 1GB RAM | Health check: `curl --fail http://localhost:8088/health` |
| D-12 | Grafana: `grafana/grafana:11.4.0`, port **3000**, 512MB RAM | Health check: `curl --fail http://localhost:3000/api/health` |
| D-13 | Port conflicts resolved | 8088, 3000, 8090, 8123/9000 all distinct |
| D-14 | No existing `grafana/` or `superset/` dirs | Created from scratch |
| D-15 | Grafana ClickHouse: `grafana-clickhouse-datasource` plugin | HTTP `http://clickhouse:8123`, database `airquality` |

### Out of Scope

- Multi-user authentication for Superset (Phase 5)
- Row-level security in Superset
- PagerDuty integration
- S3/object storage for report archival

---

## Standard Stack

### Core

| Library / Image | Version | Purpose | Why Standard |
|----------------|---------|---------|-------------|
| `apache/superset` | 4.1.0 | Analyst BI dashboards | Apache top-level project; ClickHouse native via `clickhouse-connect` driver |
| `grafana/grafana` | 11.4.0 | Operational monitoring + alerting | De facto standard for data observability; YAML provisioning native |
| `grafana-clickhouse-datasource` | (latest) | Grafana → ClickHouse connector | Official ClickHouse plugin; HTTP interface avoids driver issues |
| `clickhouse-connect` | 0.9.2 | Superset → ClickHouse connector | Already in project; recommended driver for Superset 4.x |

### Supporting

| Library | Purpose | When to Use |
|---------|---------|-------------|
| `supersetapiclient` (PyPI) | Python SDK for Superset REST API | Dashboard export/import scripting in `superset-init.sh` |
| Grafana YAML provisioning | `apiVersion: 1` datasource/dashboard/alert files | `grafana/provisioning/{datasources,dashboards,alerting}/` |
| `superset_config.py` | Programmatic Superset config | ClickHouse URI, guest access flags, feature flags |
| `superset-init.sh` | Init container script | Create admin user + import dashboards on first boot |
| Telegram Bot API | Alert notifications | Grafana contact point webhook |
| Docker `GF_*` env vars | Grafana config via env | `GF_AUTH_ANONYMOUS_ENABLED`, `GF_SECURITY_ALLOW_EMBEDDING`, plugin install |

### Installation

```bash
# No pip/npm installs needed — all are Docker-based
# Grafana plugin installed inside container via:
GF_INSTALL_PLUGINS="grafana-clickhouse-datasource"
```

```python
# Superset Python SDK (used in superset-init.sh)
# Installed in superset-init container only:
pip install supersetapiclient
```

---

## Architecture Patterns

### Project Structure (created)

```
vietnam-air-quality-data-platform/
├── docker-compose.yml                  # ADD: superset, grafana services
├── .env                                # ADD: SUPERSET_*, GRAFANA_*, TELEGRAM_*
├── superset/                           # NEW — Plan 3.1
│   ├── superset_config.py              # ClickHouse connection, guest access
│   ├── superset-init.sh                # Create admin user + import dashboards
│   └── dashboards/                     # YAML dashboard exports (Plan 3.2)
│       ├── 01_aqi_overview.yaml
│       ├── 02_trends.yaml
│       ├── 03_pollutant_analysis.yaml
│       ├── 04_source_comparison.yaml
│       └── 05_forecast_vs_actual.yaml
└── grafana/                            # NEW — Plan 3.3
    ├── provisioning/
    │   ├── datasources/
    │   │   └── clickhouse.yml           # ClickHouse + PostgreSQL datasources
    │   ├── dashboards/
    │   │   └── dashboard.yml            # Dashboard providers (refs JSON files)
    │   ├── dashboard-files/
    │   │   ├── pipeline-health.json
    │   │   └── data-freshness.json
    │   └── alerting/
    │       ├── contact-points.yml       # Telegram webhook
    │       └── alert-rules.yml          # AQI > 200, pipeline failures
    └── grafana.ini                     # Anonymous access, plugins
```

### Pattern 1: Grafana YAML Provisioning (native, first-class)

Grafana reads YAML files from `/etc/grafana/provisioning/{datasources,dashboards,alerting}/` at startup. Files are mounted from `grafana/provisioning/` on the host. No UI required for setup.

**Datasource provisioning** (`grafana/provisioning/datasources/clickhouse.yml`):
```yaml
apiVersion: 1
datasources:
  - name: ClickHouse AirQuality
    type: vertamedia-clickhouse-datasource
    access: proxy
    url: http://clickhouse:8123
    database: airquality
    editable: false
    jsonData:
      username: ${CLICKHOUSE_USER}
      port: 8123
      protocol: http
    secureJsonData:
      password: ${CLICKHOUSE_PASSWORD}
  - name: Airflow Metadata
    type: postgres
    access: proxy
    url: postgres:5432
    database: airflow
    user: airflow
    secureJsonData:
      password: airflow
    jsonData:
      sslmode: disable
```

**Dashboard file provisioning** (`grafana/provisioning/dashboards/dashboard.yml`):
```yaml
apiVersion: 1
providers:
  - name: 'Air Quality Dashboards'
    orgId: 1
    folder: 'Air Quality'
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    allowUiUpdates: true
    options:
      path: /etc/grafana/dashboards
```

**Alert contact point** (`grafana/provisioning/alerting/contact-points.yml`):
```yaml
apiVersion: 1
contactPoints:
  - name: telegram-critical
    receivers:
      - uid: telegram-critical
        type: telegram
        settings:
          url: ${TELEGRAM_WEBHOOK_URL}
          chatid: ${TELEGRAM_CHAT_ID}
        disableResolveMessage: false
```

**Alert rules** (`grafana/provisioning/alerting/alert-rules.yml`):
```yaml
apiVersion: 1
groups:
  - name: air-quality-alerts
    folder: Air Quality Alerts
    interval: 1m
    rules:
      - uid: aqi-critical
        title: AQI Critical (>200)
        condition: C
        data:
          - refId: A
            relativeTimeRange:
              from: 300
              to: 0
            datasourceUid: clickhouse-datasource
            model:
              queryText: >
                SELECT max(normalized_aqi)
                FROM fct_hourly_aqi
                WHERE timestamp_utc >= now() - interval 1 hour
              datasource:
                type: vertamedia-clickhouse-datasource
          - refId: C
            queryType: threshold
            reducer: last
        noDataState: NoData
        execErrState: Error
        for: 5m
        annotations:
          summary: "AQI exceeded 200 (Very Unhealthy) at {{ $values.A.Value }}"
        labels:
          severity: critical
```

**Source:** [Grafana provisioning docs](https://grafana.com/docs/grafana/latest/administration/provisioning/), [Contact point provisioning](https://grafana.com/docs/grafana/latest/alerting/set-up/provision-alerting-resources/file-provisioning/), [Anonymous auth](https://grafana.com/docs/grafana/latest/setup-grafana/configure-access/configure-authentication/anonymous-auth/)

### Pattern 2: Superset `superset_config.py` + Init Container

Superset has no native YAML datasource provisioning. The pattern is: `superset_config.py` sets up the ClickHouse connection and guest access programmatically; an init container runs `superset import-dashboards` to load dashboard YAML.

**`superset/superset_config.py`**:
```python
import os
from superset.config import *

# ClickHouse connection via SQLAlchemy URI
CLICKHOUSE_URI = (
    f"clickhouse+connect://{os.environ.get('CLICKHOUSE_USER', 'admin')}:"
    f"{os.environ.get('CLICKHOUSE_PASSWORD', 'admin123456')}"
    f"@{os.environ.get('CLICKHOUSE_HOST', 'clickhouse')}:8123/"
    f"{os.environ.get('CLICKHOUSE_DB', 'air_quality')}"
)

SQLALCHEMY_DATABASE_URI = CLICKHOUSE_URI
SQLALCHEMY_EXTRAS = {"connect_args": {"connect_timeout": 30}}

# Guest/public access — no login required
PUBLIC_ROLE_LIKE_GAMMA = True
GUEST_TOKEN_JWT_SECRET = os.environ.get("SUPERSET_GUEST_TOKEN_SECRET", "change-me-in-prod")
ALLOW_GUEST_DASHBOARD_ACCESS = True

# Caching
CACHE_TIMEOUT = 900  # 15 minutes (Plan 3.1)

# Feature flags for Superset 4.x
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "ALERT_REPORTS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "SQL_LAB_BACKEND_PERSISTENCE": True,
}

# Security
WTF_CSRF_ENABLED = False  # Disabled for internal read-only deployment
PUBLIC_USER_LANDING_PAGE = "/sqllab/"
```

**`superset/superset-init.sh`** (Plan 3.2):
```bash
#!/bin/bash
set -e

echo "Waiting for Superset to start..."
sleep 30

# Create admin user
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER:-admin}" \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" \
  --firstname Admin \
  --lastname User \
  --email admin@vietnam-aqi.local || true

# Import dashboards
superset import-dashboards -f /app/dashboards/dashboards_export.zip || true

echo "Superset initialization complete."
exec "$@"
```

**Source:** [Superset DB dependencies](https://superset.apache.org/docs/configuration/database-dependencies/), [Superset 4 REST API auth](https://preset.io/blog/superset-4-rest-api), [CLI import/export](https://superset.apache.org/docs/using-superset/importing-exporting)

### Pattern 3: Grafana Anonymous Access (Docker env vars)

Grafana anonymous access is enabled via environment variable in `docker-compose.yml`. No ini file edit needed:

```yaml
grafana:
  image: grafana/grafana:11.4.0
  environment:
    - GF_AUTH_ANONYMOUS_ENABLED=true
    - GF_AUTH_ANONYMOUS_ORG_NAME=Main Org.
    - GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer
    - GF_SECURITY_ALLOW_EMBEDDING=true
    - GF_INSTALL_PLUGINS=grafana-clickhouse-datasource
```

The anonymous org role must match the datasource org (default: `Main Org.` = orgId 1).

**Source:** [Grafana anonymous auth](https://grafana.com/docs/grafana/latest/setup-grafana/configure-access/configure-authentication/anonymous-auth/), [Docker configure](https://grafana.com/docs/grafana/latest/setup-grafana/configure-docker/)

### Pattern 4: Superset Dashboard Export/Import

Superset exports dashboards as **ZIP archives containing YAML + JSON** (not a raw YAML file). The correct workflow:

1. Build dashboards in Superset UI → export via `superset export-dashboards`
2. Commit the ZIP to `superset/dashboards/dashboards_export.zip`
3. Init container imports on boot: `superset import-dashboards -f /app/dashboards/dashboards_export.zip`

For Plan 3.2, the workflow is reversed: create YAML/JSON configs in code → commit to `superset/dashboards/` → init container imports.

The ZIP structure:
```
dashboards_export.zip
├── metadata.yaml
├── databases/
│   └── database_export.yaml
├── datasets/
│   └── dataset_export.yaml
└── dashboards/
    └── dashboard_export.yaml
```

Alternatively, use `supersetapiclient` Python SDK to import programmatically.

**Source:** [Superset importing/exporting](https://superset.apache.org/docs/using-superset/importing-exporting), [Superset 4 REST API](https://preset.io/blog/superset-4-rest-api)

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|------------|-------------|-----|
| Grafana datasource setup | Manual UI click-ops | YAML provisioning files | Grafana natively reads `provisioning/datasources/*.yml` — manual config is lost on restart |
| Superset ClickHouse connection | Custom Flask app | `superset_config.py` + `clickhouse+connect://` URI | `clickhouse-connect` is the officially recommended driver; custom code misses Superset's SQLAlchemy lifecycle |
| Grafana alerting | GUI-only alert config | YAML provisioning in `grafana/provisioning/alerting/` | CI/CD reproducibility; GUI-managed alerts are orphaned on crash |
| Grafana ClickHouse connector | Generic HTTP datasource | `grafana-clickhouse-datasource` plugin | Official plugin handles ClickHouse-specific query syntax, datetime handling, TLS |
| Superset guest access | Build custom auth middleware | `PUBLIC_ROLE_LIKE_GAMMA = True` in `superset_config.py` | Superset's native guest token system; custom auth risks SQL injection |
| Dashboard versioning | Manual screenshot/versioning | YAML/JSON configs in git | D-03/D-04 lock this in; manual = drift |

**Key insight:** Both Superset and Grafana are designed for YAML/GitOps provisioning. Fighting this (manual UI config, custom scripts) creates fragile, non-reproducible deployments.

---

## Common Pitfalls

### Pitfall 1: Superset ZIP Export vs. YAML Confusion

**What goes wrong:** Dashboards export as ZIP, not raw YAML. Plans that say "export to YAML" will get a ZIP. Attempting to edit dashboard YAML directly in the ZIP without proper tooling causes corruption.

**Why it happens:** Superset 3.0+ uses ZIP archives containing YAML+JSON for the import/export feature. The ZIP IS the YAML provisioning format.

**How to avoid:** Always use `superset export-dashboards -f output.zip` and `superset import-dashboards -f input.zip`. For version-controlled dashboard configs, treat the ZIP as the artifact. Alternatively use `supersetapiclient` for programmatic management.

**Warning signs:** File size 0 bytes after export, import fails with "invalid format", dashboards missing charts.

---

### Pitfall 2: Grafana Datasource Org Mismatch

**What goes wrong:** Anonymous users can't see datasources. Dashboard shows "datasource not found" errors even though the datasource exists.

**Why it happens:** Anonymous users default to orgId 1 ("Main Org."). If datasources are provisioned to a different org, anonymous users see empty datasource lists.

**How to avoid:** Always ensure `GF_AUTH_ANONYMOUS_ORG_NAME=Main Org.` matches the provisioning org. Use `orgId: 1` explicitly in datasource YAML. Verify with `curl http://localhost:3000/api/datasources` when authenticated.

**Warning signs:** Dashboard panels show red "Datasource not found" errors for anonymous users.

---

### Pitfall 3: Superset `PUBLIC_ROLE_LIKE_GAMMA` Alone Not Sufficient in 4.x

**What goes wrong:** Even with `PUBLIC_ROLE_LIKE_GAMMA = True`, users still see a login screen.

**Why it happens:** Superset 4.x requires `GUEST_TOKEN_JWT_SECRET` to be set for guest/anonymous mode to work. The older `PUBLIC_ROLE_LIKE_GAMMA` alone was sufficient in 3.x but is not enough in 4.x due to the guest token architecture.

**How to avoid:** Set both `PUBLIC_ROLE_LIKE_GAMMA = True` AND `GUEST_TOKEN_JWT_SECRET` in `superset_config.py`. Use a long random string for the secret.

**Warning signs:** `/superset/welcome/` redirects to login in Superset 4.x even with `PUBLIC_ROLE_LIKE_GAMMA`.

---

### Pitfall 4: Grafana ClickHouse Datasource Protocol Mismatch

**What goes wrong:** Grafana shows "Connection refused" or empty query results from ClickHouse.

**Why it happens:** The `grafana-clickhouse-datasource` uses the HTTP API (port 8123) internally. Using the native protocol (`clickhouse:9000`) in the datasource URL fails because the plugin only supports HTTP. Additionally, Grafana container networking must allow outbound HTTP to `clickhouse:8123`.

**How to avoid:** Always use `http://clickhouse:8123` (HTTP) not `http://clickhouse:9000` (native) in the datasource URL. Verify container is on `air-quality-network`.

**Warning signs:** Query returns no data, "Connection refused" in datasource test.

---

### Pitfall 5: Superset Init Container Race Condition

**What goes wrong:** `superset-init.sh` runs before Superset itself is ready, causing import failures.

**Why it happens:** Docker Compose `depends_on` only waits for container start, not readiness. Superset can take 60–120 seconds to initialize its metadata database.

**How to avoid:** Add `healthcheck` to Superset service and `condition: service_healthy` to init container dependency. Include `sleep 30` + a retry loop in `superset-init.sh`.

**Warning signs:** `superset import-dashboards` fails with "Superset not ready", "503 Service Unavailable".

---

### Pitfall 6: Grafana Alert Deduplication Misconfiguration

**What goes wrong:** Telegram receives duplicate alerts every minute instead of every 30 minutes.

**Why it happens:** Alert rules default to evaluating every 1 minute with no deduplication. The 30-minute dedup must be configured either as a Grafana "for" duration (pending period before firing) or via alert group `group_wait`/`group_interval`.

**How to avoid:** Set `for: 5m` on alert rules (alert must persist for 5 min before firing) AND configure `group_wait: 30m` in the notification policy. This ensures each unique alert fires at most once per 30-minute window.

**Warning signs:** Telegram receives the same alert multiple times within 30 minutes.

---

## Code Examples

### Docker Compose Service Definitions (Plan 3.5)

**Superset service** (`docker-compose.yml`):
```yaml
superset:
  image: apache/superset:4.1.0
  container_name: superset
  restart: unless-stopped
  ports:
    - "8088:8088"
  volumes:
    - ./superset/superset_config.py:/etc/pythonpath/superset/superset_config.py
    - ./superset/dashboards:/app/dashboards
  environment:
    - SUPERSET_CONFIG_PATH=/etc/pythonpath/superset/superset_config.py
    - CLICKHOUSE_HOST=clickhouse
    - CLICKHOUSE_PORT=8123
    - CLICKHOUSE_USER=${CLICKHOUSE_USER}
    - CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD}
    - CLICKHOUSE_DB=${CLICKHOUSE_DB}
    - SUPERSET_GUEST_TOKEN_SECRET=${SUPERSET_GUEST_TOKEN_SECRET}
  depends_on:
    clickhouse:
      condition: service_healthy
  mem_limit: 1g
  healthcheck:
    test: ["CMD", "curl", "--fail", "http://localhost:8088/health"]
    interval: 30s
    timeout: 10s
    retries: 5
    start_period: 120s
  networks:
    - air-quality-network
```

**Grafana service** (`docker-compose.yml`):
```yaml
grafana:
  image: grafana/grafana:11.4.0
  container_name: grafana
  restart: unless-stopped
  ports:
    - "3000:3000"
  volumes:
    - ./grafana/provisioning:/etc/grafana/provisioning
    - ./grafana/grafana.ini:/etc/grafana/grafana.ini
    - grafana-data:/var/lib/grafana
  environment:
    - GF_AUTH_ANONYMOUS_ENABLED=true
    - GF_AUTH_ANONYMOUS_ORG_NAME=Main Org.
    - GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer
    - GF_SECURITY_ALLOW_EMBEDDING=true
    - GF_INSTALL_PLUGINS=grafana-clickhouse-datasource
    - CLICKHOUSE_HOST=clickhouse
    - CLICKHOUSE_PORT=8123
    - CLICKHOUSE_USER=${CLICKHOUSE_USER}
    - CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD}
    - CLICKHOUSE_DB=${CLICKHOUSE_DB}
    - TELEGRAM_WEBHOOK_URL=https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage
    - TELEGRAM_CHAT_ID=${TELEGRAM_CHAT_ID}
  depends_on:
    clickhouse:
      condition: service_healthy
  mem_limit: 512m
  healthcheck:
    test: ["CMD", "curl", "--fail", "http://localhost:3000/api/health"]
    interval: 30s
    timeout: 10s
    retries: 5
  networks:
    - air-quality-network
```

**Volume** (add to `volumes:` section):
```yaml
volumes:
  grafana-data:
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|-------------|-----------------|-------------|--------|
| Superset per-DB config in UI | `superset_config.py` programmatic config | Superset 2.x+ | Reproducible, version-controllable |
| Superset ZIP-only manual export | `supersetapiclient` Python SDK | Superset 4.x (2024) | CI/CD pipeline integration |
| Grafana `graph` panel (deprecated) | Grafana 11.x scenes-based panels | Grafana 10.x→11.x | Better performance, responsive dashboards |
| Grafana provisioning via API | Native YAML file provisioning | Grafana 5+ (stable) | GitOps, version control |
| Grafana ClickHouse via JSON API datasource | `grafana-clickhouse-datasource` plugin | 2021+ | Native query editor, datetime handling |
| Alert deduplication via external script | Grafana native `for:` + `group_wait:` | Grafana 8.x+ | No custom deduplication logic needed |

**Deprecated/outdated:**
- `PUBLIC_ROLE_LIKE_GAMMA` alone for Superset 4.x guest access — requires `GUEST_TOKEN_JWT_SECRET` as well
- `grafana/grafana-oss` Docker image — deprecated in 12.4.0+, use `grafana/grafana`
- `allow_embedding: true` in `security` ini section — now `GF_SECURITY_ALLOW_EMBEDDING` env var

---

## Open Questions

1. **Dashboard JSON export vs. dashboard-as-code approach**
   - What's unclear: Whether to hand-author dashboard JSON or generate it from the Mart table schema
   - Recommendation: Start with hand-authored dashboard JSON using the dbt Mart table column names; treat as version-controlled code

2. **Superset dataset/chart references after import**
   - What's unclear: When importing dashboards into a fresh Superset, dataset IDs may conflict. Does Superset auto-map datasets by name?
   - Recommendation: Import dashboards after a `dbt run` ensures mart tables exist; use `--force` flag to overwrite

3. **Grafana alert rule evaluation frequency vs. data freshness**
   - What's unclear: Grafana evaluates alert rules at its own interval (`evaluation_interval`, default 1m). If data arrives hourly, frequent evaluation creates noisy NoData states.
   - Recommendation: Set Grafana `evaluation_interval` to `5m` to match data arrival cadence; use `noDataState: NoData` carefully

4. **Superset `CACHE_TIMEOUT = 900` vs. dbt incremental refresh cadence**
   - What's unclear: Mart tables refresh hourly (dag_transform every 30 min). Is 15-min Superset cache timeout appropriate?
   - Recommendation: 15 min is fine for analyst dashboards (D-01 spec); if users need real-time, reduce to 5 min

---

## Environment Availability

> No external dependencies beyond Docker (already verified).

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Docker | Container runtime | ✓ (per prior tasks) | 24.x | — |
| ClickHouse | Superset + Grafana datasource | ✓ | 25.12 | — |
| PostgreSQL | Grafana Airflow metadata datasource | ✓ | 15 | — |
| `grafana-clickhouse-datasource` plugin | Grafana → ClickHouse | ✓ (installed in container) | latest | — |
| `clickhouse-connect` | Superset → ClickHouse | ✓ (in requirements.txt) | 0.9.2 | — |
| Telegram Bot API | Grafana alert notifications | ✓ (public internet) | — | — |

**Missing dependencies with no fallback:**
- None identified. All required tools are Docker-based or already in the stack.

---

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (existing project infrastructure) |
| Config file | `pytest.ini` (in repo root) |
| Quick run command | `pytest tests/ -x -q --ignore=tests/test_*_int.py` |
| Full suite command | `pytest tests/ -v` |

### Phase Requirements → Test Map

Phase 3 requirements are primarily infrastructure (Docker Compose, config files) with no new Python code or dbt models. The existing test infrastructure (pytest + ClickHouse integration tests) covers the mart tables that feed dashboards.

| Area | Behavior | Test Type | Verification |
|------|----------|-----------|--------------|
| Plan 3.1 | Superset starts and connects to ClickHouse | Smoke | `curl --fail http://localhost:8088/health` exits 0 |
| Plan 3.1 | Grafana starts and connects to ClickHouse | Smoke | `curl --fail http://localhost:3000/api/health` exits 0 |
| Plan 3.1 | Grafana anonymous access works | Smoke | `curl http://localhost:3000/api/dashboards` returns 200 (no auth) |
| Plan 3.2 | Superset dashboards load in UI | Manual | Verify 5 dashboards visible in Superset |
| Plan 3.3 | Grafana dashboards render | Manual | Verify Pipeline Health + Data Freshness dashboards |
| Plan 3.4 | Grafana alert rule fires | Manual trigger | Manually insert AQI > 200 record, verify Telegram |
| Plan 3.5 | All 9 services start cleanly | Smoke | `docker compose ps` shows all healthy |

### Wave 0 Gaps

None — Phase 3 is primarily Docker Compose and YAML configuration. No new Python test files are required. Existing `tests/` directory covers the mart tables that feed into Superset dashboards.

---

## Sources

### Primary (HIGH confidence)

- [Grafana provisioning docs](https://grafana.com/docs/grafana/latest/administration/provisioning/) — YAML datasource/dashboard/alert provisioning format, `apiVersion: 1`
- [Grafana anonymous auth](https://grafana.com/docs/grafana/latest/setup-grafana/configure-access/configure-authentication/anonymous-auth/) — ini config + env var `GF_AUTH_ANONYMOUS_ENABLED`
- [Grafana Docker configure](https://grafana.com/docs/grafana/latest/setup-grafana/configure-docker/) — `GF_INSTALL_PLUGINS`, env var datasource config
- [Grafana alerting file provisioning](https://grafana.com/docs/grafana/latest/alerting/set-up/provision-alerting-resources/file-provisioning/) — contactPoints YAML, alertRules YAML, Telegram webhook
- [Superset DB dependencies](https://superset.apache.org/docs/configuration/database-dependencies/) — `clickhouse-connect` as recommended driver
- [Superset importing/exporting](https://superset.apache.org/docs/using-superset/importing-exporting/) — CLI commands, ZIP format

### Secondary (MEDIUM confidence)

- [Superset 4 REST API / guest access](https://preset.io/blog/superset-4-rest-api) — JWT auth, `PUBLIC_ROLE_LIKE_GAMMA`, `GUEST_TOKEN_JWT_SECRET`
- [Superset 4 REST API deep dive](https://preset.io/blog/superset-4-rest-api) — CSRF token, JWT Bearer auth, import/export endpoints
- `grafana-clickhouse-datasource` GitHub — plugin install, query format (inferred from Grafana plugin convention)

### Tertiary (LOW confidence)

- Grafana `grafana-clickhouse-datasource` exact YAML datasource `type` field name — requires GitHub README fetch (404 on attempt); cross-referenced via Grafana plugin ecosystem docs

---

## Metadata

**Confidence breakdown:**
- Standard stack: **HIGH** — Versions from official docs, confirmed via D-11/D-12 in 03-CONTEXT.md
- Architecture: **HIGH** — Grafana YAML provisioning is native/first-class; Superset init pattern well-documented
- Pitfalls: **MEDIUM** — Known Superset 4.x vs 3.x differences confirmed via multiple sources; Grafana org mismatch is standard Docker/Grafana issue

**Research date:** 2026-04-03
**Valid until:** 2026-05-03 (30 days — Superset 4.x and Grafana 11.x are stable releases)
