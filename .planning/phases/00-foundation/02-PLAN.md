---
wave: 1
depends_on: []
files_modified:
  - docker-compose.yml
autonomous: false
---

# Plan 0.2 — Docker Compose Resource Hardening

**Plan:** 0.2
**Phase:** 00-foundation
**Wave:** 1 (parallel with 0.1)
**Owner:** data engineering

---
```yaml
wave: 1
depends_on: []
files_modified:
  - docker-compose.yml
  - README.md
autonomous: false
```

---

## Goal

Add `mem_limit`, `cpus`, and health checks to all Docker Compose services per decisions D-16 through D-20.

---

## RAM Allocation (D-17)

| Service | mem_limit | cpus | Notes |
|---------|-----------|------|-------|
| clickhouse | `3g` | `2` | |
| postgres | `1g` | `1` | |
| airflow-scheduler | `512m` | `1` | |
| airflow-dag-processor | `512m` | `1` | |
| airflow-triggerer | `512m` | `1` | |
| airflow-webserver | **none** | **none** | D-20: do NOT add limit |
| airflow-permissions | **none** | **none** | Ephemeral init container |
| **Phase 0 Total** | **~6g** | **5** | |

Future phases add Superset(1g) + Grafana(512m) + OpenMetadata(4g) = +5.5g → ~11.5g total (within 13GB Docker pool).

---

## <task id="docker-limits">

<read_first>
- `docker-compose.yml` (full file — services at lines 4–303)
</read_first>

<action>
Add `mem_limit` and `cpus` to each service in `docker-compose.yml` using top-level YAML syntax (compatible with `docker compose` v1 and v2):

Add to `clickhouse` service (after line 15 `hard: 262144`):
```yaml
    mem_limit: 3g
    cpus: '2'
```

Add to `postgres` service (after `environment:` block, before `healthcheck:`):
```yaml
    mem_limit: 1g
    cpus: '1'
```

Add to `airflow-scheduler` service (after volumes block, before environment block — insert before `environment:`):
```yaml
    mem_limit: 512m
    cpus: '1'
```

Add to `airflow-dag-processor` service (same pattern):
```yaml
    mem_limit: 512m
    cpus: '1'
```

Add to `airflow-triggerer` service (same pattern):
```yaml
    mem_limit: 512m
    cpus: '1'
```

**Do NOT add `mem_limit` or `cpus` to `airflow-webserver` (D-20).**
**Do NOT add `mem_limit` or `cpus` to `airflow-permissions` (ephemeral init container).**
</action>

<acceptance_criteria>
- `grep -n "mem_limit: 3g" docker-compose.yml` returns 1 match (clickhouse)
- `grep -n "cpus: '2'" docker-compose.yml` returns 1 match (clickhouse)
- `grep -n "mem_limit: 1g" docker-compose.yml` returns 1 match (postgres)
- `grep -n "mem_limit: 512m" docker-compose.yml | wc -l` returns 3 (scheduler + dag-processor + triggerer)
- `grep "mem_limit" docker-compose.yml | grep -c "airflow-webserver"` returns 0 (D-20 compliance)
- `grep -c "mem_limit" docker-compose.yml` returns 5 (clickhouse + postgres + 3 airflow services)
</acceptance_criteria>

</task>

---

## <task id="docker-healthchecks">

<read_first>
- `docker-compose.yml` (lines 111–120: existing `airflow-webserver` healthcheck)
- `docker-compose.yml` (lines 122–173: `airflow-scheduler` service — no healthcheck)
</read_first>

<action>
Add healthchecks to `airflow-scheduler`, `airflow-dag-processor`, and `airflow-triggerer`. Use the same HTTP endpoint as the existing `airflow-webserver` healthcheck:

For `airflow-scheduler` (insert after `depends_on:` block, before `logging:`):
```yaml
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/api/v2/monitor/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 60s
```

For `airflow-dag-processor` (same location, before `logging:`):
```yaml
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/api/v2/monitor/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 60s
```

For `airflow-triggerer` (same location, before `logging:`):
```yaml
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/api/v2/monitor/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 60s
```

Do NOT change the `depends_on:` conditions for any service (they currently use `condition: service_started`, not `condition: service_healthy`).
</action>

<acceptance_criteria>
- `grep -c "healthcheck:" docker-compose.yml` returns 6 (clickhouse + postgres + webserver + scheduler + dag-processor + triggerer)
- `grep -A 4 "airflow-scheduler:" docker-compose.yml | grep "healthcheck:"` returns 1 match
- `grep -A 4 "airflow-dag-processor:" docker-compose.yml | grep "healthcheck:"` returns 1 match
- `grep -A 4 "airflow-triggerer:" docker-compose.yml | grep "healthcheck:"` returns 1 match
- All three new healthchecks use `curl --fail http://localhost:8080/api/v2/monitor/health`
- No `depends_on:` conditions changed to `service_healthy` for scheduler/dag-processor/triggerer
</acceptance_criteria>

</task>

---

## <task id="docker-readme">

<read_first>
- `README.md` (existing content)
</read_first>

<action>
Add a "Hardware Requirements" section to `README.md`. Insert after the "Quick Start" section or before any "Architecture" section. Use this exact content:

```markdown
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
```
</action>

<acceptance_criteria>
- `grep -n "Hardware Requirements" README.md` returns a heading match
- `grep -n "16GB" README.md` returns the RAM requirement
- `grep -n "4 CPU" README.md` returns the CPU requirement
- `grep -n "ClickHouse.*3GB" README.md` returns the ClickHouse allocation row
- `grep -n "Phase 0 Total.*6GB" README.md` returns the Phase 0 total row
</acceptance_criteria>

</task>

---

## Verification

1. `grep -c "mem_limit" docker-compose.yml` returns 5 (D-16 compliance)
2. `grep "mem_limit" docker-compose.yml | grep -c "airflow-webserver"` returns 0 (D-20 compliance)
3. `grep -c "healthcheck:" docker-compose.yml` returns 6
4. `grep -n "16GB" README.md` returns the RAM requirement
5. `docker compose config` runs without YAML errors
6. `docker compose up -d` starts all services (after applying changes)
7. `docker compose ps` shows all services healthy within 120s (after applying changes)

---

*Plan author: gsd:quick*
