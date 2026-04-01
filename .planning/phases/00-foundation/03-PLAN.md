---
wave: 2
depends_on:
  - .planning/phases/00-foundation/01-PLAN.md
  - .planning/phases/00-foundation/02-PLAN.md
files_modified:
  - .github/workflows/ci.yml
  - docker-compose.test.yml
  - dbt/dbt_tranform/profiles.yml
  - .sqlfluff
  - .ruff.toml
  - requirements.txt
autonomous: false
---

# Plan 0.3 — CI Pipeline Bootstrap

**Plan:** 0.3
**Phase:** 00-foundation
**Wave:** 2 (sequential: requires 0.1 audit and 0.2 Docker hardening complete)
**Owner:** data engineering

---
```yaml
wave: 2
depends_on:
  - .planning/phases/00-foundation/01-PLAN.md
  - .planning/phases/00-foundation/02-PLAN.md
files_modified:
  - .github/workflows/ci.yml
  - docker-compose.test.yml
  - dbt/dbt_tranform/profiles.yml
  - .sqlfluff
  - .ruff.toml
  - requirements.txt
autonomous: false
```

---

## Goal

GitHub Actions CI pipeline with two jobs: (1) `lint` — `ruff check` + `sqlfluff lint`; (2) `dbt-ci` — `dbt deps && dbt seed && dbt run && dbt test` against a live ClickHouse service. Pipeline blocks merge to main on failure (D-14/D-15).

---

## <task id="ci-ruff">

<read_first>
- `requirements.txt` (no `ruff` present)
</read_first>

<action>
Add `ruff==0.11.0` as a pinned dependency to `requirements.txt`. Insert after the `sqlfluff` lines (line 4):

```
# Linting
ruff==0.11.0
```
</action>

<acceptance_criteria>
- `grep "ruff==0.11.0" requirements.txt` returns 1 match
- `grep "^ruff" requirements.txt | grep -v "^#"` confirms a non-comment line
- `pip install ruff==0.11.0 && ruff --version` outputs `0.11.0`
</acceptance_criteria>

</task>

---

## <task id="ci-ruff-config">

<read_first>
- None (no existing ruff config)
</read_first>

<action>
Create `.ruff.toml` in repo root with the following exact content:

```toml
# .ruff.toml — Python linter config for Vietnam Air Quality Data Platform

line-length = 120
target-version = "py310"

[lint]
select = [
    "E",    # pycodestyle errors
    "W",    # pycodestyle warnings
    "F",    # Pyflakes
    "I",    # isort
    "B",    # flake8-bugbear
    "C4",   # flake8-comprehensions
    "UP",   # pyupgrade
]
ignore = [
    "E501",   # line too long — project uses 120 but some lines are longer
    "B008",   # do not perform function calls in argument defaults
    "C901",   # function too complex
]
```
</action>

<acceptance_criteria>
- `.ruff.toml` exists in repo root
- `.ruff.toml` contains `target-version = "py310"`
- `.ruff.toml` contains `select = [` with `"E"` as first entry
- `.ruff.toml` ignores `E501`
- `ruff check python_jobs/ --config .ruff.toml` exits 0 on current codebase
</acceptance_criteria>

</task>

---

## <task id="ci-sqlfluff-config">

<read_first>
- `dbt/dbt_tranform/dbt_project.yml` (for project name confirmation)
- No existing `.sqlfluff` file
</read_first>

<action>
Create `.sqlfluff` in repo root with the following exact content:

```yaml
# .sqlfluff — SQL linter config for Vietnam Air Quality Data Platform
# Lints both raw SQL and dbt Jinja SQL files

[sqlfluff]
dialect = clickhouse
templater = dbt
indentation = 4

[sqlfluff:rules]
capitalisation_policy = consistent
consistent_column_aliases = True

[sqlfluff:rules:aliasing.length]
min_alias_length = 2

[sqlfluff:rules:capitalisation.keywords]
force = True

[sqlfluff:templater:dbt]
project_dir = dbt/dbt_tranform
profiles_dir = dbt/dbt_tranform
```
</action>

<acceptance_criteria>
- `.sqlfluff` exists in repo root
- `.sqlfluff` contains `dialect = clickhouse`
- `.sqlfluff` contains `templater = dbt`
- `.sqlfluff` contains `project_dir = dbt/dbt_tranform`
- `sqlfluff lint dbt/dbt_tranform/ --config .sqlfluff` exits 0 or exits with fixable violations only
</acceptance_criteria>

</task>

---

## <task id="ci-dbt-profiles">

<read_first>
- `dbt/dbt_tranform/profiles.yml` (full file — only `dev` and `production` targets exist)
</read_first>

<action>
Append the following `ci` target to `dbt/dbt_tranform/profiles.yml` under the `outputs:` section (after `production:`). Add it after the `production:` block closes:

```yaml
    ci:
      type: clickhouse
      host: "127.0.0.1"
      port: 8123
      user: "default"
      password: ""
      database: "default"
      schema: "default"
      threads: 2
      timeout: 300
      retry_count: 3
      compression: lz4
```

Key settings:
- `host: "127.0.0.1"` — GitHub Actions services expose at this IP (not `localhost`)
- `password: ""` — CI ClickHouse has no auth
- `database: "default"` — CI schema initialized by `init-schema` service (not `air_quality`)
</action>

<acceptance_criteria>
- `profiles.yml` contains `ci:` target
- `grep -A 10 "ci:" dbt/dbt_tranform/profiles.yml` shows `host: "127.0.0.1"` as first line
- `grep -A 10 "ci:" dbt/dbt_tranform/profiles.yml` shows `password: ""`
- `grep -A 10 "ci:" dbt/dbt_tranform/profiles.yml` shows `database: "default"`
- `dbt debug --target ci --project-dir dbt/dbt_tranform --profiles-dir dbt/dbt_tranform` exits 0 (when ClickHouse is available)
</acceptance_criteria>

</task>

---

## <task id="ci-docker-test">

<read_first>
- `docker-compose.yml` (for service definition patterns)
</read_first>

<action>
Create `docker-compose.test.yml` in repo root with the following exact content:

```yaml
# docker-compose.test.yml — Minimal CI test stack
# Used by .github/workflows/ci.yml for dbt integration tests
# Does NOT include Airflow, Superset, Grafana, or OpenMetadata

version: '3.8'

services:
  clickhouse:
    image: clickhouse/clickhouse-server:25.12
    container_name: ci-clickhouse
    ports:
      - "8123:8123"
      - "9000:9000"
    environment:
      - CLICKHOUSE_DB=air_quality
      - CLICKHOUSE_USER=admin
      - CLICKHOUSE_PASSWORD=admin123456
      - CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "localhost:8123/ping"]
      interval: 10s
      timeout: 5s
      retries: 10
    tmpfs:
      - /var/lib/clickhouse
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"

  init-schema:
    image: clickhouse/clickhouse-server:25.12
    container_name: ci-init-schema
    depends_on:
      clickhouse:
        condition: service_healthy
    entrypoint: ["bash", "-c"]
    command: |
      sleep 5 && \
      clickhouse-client --host clickhouse --user admin --password admin123456 --query "CREATE DATABASE IF NOT EXISTS air_quality" && \
      clickhouse-client --host clickhouse --user admin --password admin123456 --query "CREATE TABLE IF NOT EXISTS air_quality.raw_aqicn_measurements (source LowCardinality(String) DEFAULT 'aqicn', ingest_time DateTime DEFAULT now(), ingest_batch_id String, ingest_date Date MATERIALIZED toDate(ingest_time), station_id String, time_s Nullable(String), time_tz Nullable(String), time_v Nullable(String), time_iso Nullable(String), aqi Nullable(String), dominentpol Nullable(String), pollutant LowCardinality(String), value Nullable(String), attributions Nullable(String), debug_sync Nullable(String), raw_payload String CODEC(ZSTD(1))) ENGINE = MergeTree() PARTITION BY toYYYYMM(ingest_date) ORDER BY (station_id, time_v, pollutant, ingest_time) SETTINGS index_granularity = 8192, allow_nullable_key = 1" && \
      clickhouse-client --host clickhouse --user admin --password admin123456 --query "CREATE TABLE IF NOT EXISTS air_quality.raw_aqicn_stations (source LowCardinality(String) DEFAULT 'aqicn', ingest_time DateTime DEFAULT now(), ingest_batch_id String, ingest_date Date MATERIALIZED toDate(ingest_time), station_id String, station_name Nullable(String), latitude Nullable(String), longitude Nullable(String), station_time Nullable(String), aqi Nullable(String), city_url Nullable(String), city_location Nullable(String), raw_payload String CODEC(ZSTD(1))) ENGINE = ReplacingMergeTree(ingest_time) PARTITION BY toYYYYMM(ingest_date) ORDER BY (station_id, ingest_date, ingest_time) SETTINGS index_granularity = 8192, allow_nullable_key = 1"
    restart: "no"

networks:
  default:
    name: air-quality-network
```
</action>

<acceptance_criteria>
- `docker-compose.test.yml` exists in repo root
- `grep -c "services:" docker-compose.test.yml` returns 2 (clickhouse + init-schema)
- `grep "clickhouse/clickhouse-server:25.12" docker-compose.test.yml` returns 1 match
- `docker compose -f docker-compose.test.yml config` exits 0 (valid YAML)
- `docker compose -f docker-compose.test.yml up -d clickhouse && sleep 30 && docker compose -f docker-compose.test.yml ps` shows clickhouse healthy
- `docker compose -f docker-compose.test.yml down` cleans up without error
</acceptance_criteria>

</task>

---

## <task id="ci-workflow">

<read_first>
- `docker-compose.test.yml` (created above)
- `dbt/dbt_tranform/profiles.yml` (updated above with `ci` target)
</read_first>

<action>
Create `.github/workflows/ci.yml` with the following exact content:

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

jobs:
  lint:
    name: Lint
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python 3.10
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'
          cache: 'pip'

      - name: Install lint dependencies
        run: pip install ruff==0.11.0 sqlfluff==3.5.0 'sqlfluff-templater-dbt==3.5.0'

      - name: Lint Python files with ruff
        run: ruff check python_jobs/ airflow/dags/ --config .ruff.toml

      - name: Lint SQL files with sqlfluff
        run: sqlfluff lint dbt/dbt_tranform/ --config .sqlfluff --format github-annotation

  dbt-ci:
    name: dbt CI
    runs-on: ubuntu-latest
    needs: lint
    services:
      clickhouse:
        image: clickhouse/clickhouse-server:25.12
        ports:
          - 8123:8123
          - 9000:9000
        options: >-
          --health-cmd "wget --spider -q localhost:8123/ping"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 10
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python 3.10
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'
          cache: 'pip'

      - name: Install Python dependencies
        run: pip install dbt-core==1.10.13 dbt-clickhouse==1.9.5 clickhouse-connect==0.9.2

      - name: Initialize ClickHouse schema
        run: docker compose -f docker-compose.test.yml up init-schema

      - name: dbt deps
        run: dbt deps --project-dir dbt/dbt_tranform --profiles-dir dbt/dbt_tranform

      - name: dbt seed
        run: dbt seed --target ci --project-dir dbt/dbt_tranform --profiles-dir dbt/dbt_tranform
        continue-on-error: true

      - name: dbt run
        run: dbt run --target ci --project-dir dbt/dbt_tranform --profiles-dir dbt/dbt_tranform

      - name: dbt test
        run: dbt test --target ci --project-dir dbt/dbt_tranform --profiles-dir dbt/dbt_tranform
```

Key design decisions:
- `dbt-ci` uses `needs: lint` — lint must pass before dbt runs
- GitHub Actions `services:` block provides ClickHouse at `localhost:8123`
- `docker compose -f docker-compose.test.yml up init-schema` initializes the schema
- `continue-on-error: true` on `dbt seed` — seeds may not exist initially (not a failure)
- `cancel-in-progress: true` — redundant CI runs cancel each other
</action>

<acceptance_criteria>
- `.github/workflows/ci.yml` exists in repo root
- `.github/workflows/ci.yml` triggers on `push` and `pull_request` to `main`
- `.github/workflows/ci.yml` has two jobs: `lint` and `dbt-ci`
- `dbt-ci` has `needs: lint`
- `lint` job runs `ruff check python_jobs/` and `sqlfluff lint dbt/`
- `dbt-ci` job uses GitHub Actions `services:` block for ClickHouse
- `dbt-ci` job runs `dbt run --target ci`
- `.github/workflows/ci.yml` is valid YAML (verified by GitHub Actions on first push)
</acceptance_criteria>

</task>

---

## Verification

1. `ruff check python_jobs/ airflow/dags/ --config .ruff.toml` exits 0 on current codebase
2. `sqlfluff lint dbt/dbt_tranform/ --config .sqlfluff` exits 0 or only shows fixable violations
3. `docker compose -f docker-compose.test.yml config` exits 0 (valid YAML)
4. `dbt deps --project-dir dbt/dbt_tranform --profiles-dir dbt/dbt_tranform` succeeds
5. `docker compose -f docker-compose.test.yml up -d clickhouse && docker compose -f docker-compose.test.yml ps` shows clickhouse healthy
6. `docker compose -f docker-compose.test.yml up init-schema` succeeds
7. `dbt run --target ci --project-dir dbt/dbt_tranform --profiles-dir dbt/dbt_tranform` completes without error (when ClickHouse is running)
8. `.github/workflows/ci.yml` is valid YAML

---

*Plan author: gsd:quick*
