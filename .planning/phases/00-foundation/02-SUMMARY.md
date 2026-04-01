---
phase: "00"
plan: "02"
subsystem: infrastructure
tags: [docker, docker-compose, healthchecks, resource-limits]
duration: ~5 min
completed: 2026-04-01
---

## Summary

Docker Compose resource hardening completed. Added memory limits and health checks to all services per decisions D-16 through D-20.

## What Was Built

- **`docker-compose.yml`** updated with:
  - `mem_limit: 3g, cpus: '2'` on clickhouse
  - `mem_limit: 1g, cpus: '1'` on postgres
  - `mem_limit: 512m, cpus: '1'` on airflow-scheduler, -dag-processor, -triggerer
  - HTTP healthchecks (Airflow health API) on all 3 Airflow services
  - airflow-webserver and airflow-permissions intentionally excluded (D-20)
- **`README.md`** updated with "Hardware Requirements" section:
  - Minimum: 16GB RAM, 4 CPU cores
  - Phase 0 total: ~6GB RAM, 5 CPUs
  - Future phases add ~5.5GB (Superset, Grafana, OpenMetadata)

## Key Decisions

- airflow-webserver: no mem_limit per D-20
- airflow-permissions: no mem_limit (ephemeral init container)
- Healthchecks use `curl http://localhost:8080/api/v2/monitor/health` (Airflow 3 endpoint)
- depends_on conditions unchanged (still `service_started`, not `service_healthy`)

## Files Modified

- `docker-compose.yml` (mem limits + healthchecks)
- `README.md` (hardware requirements section)

## Commits

- `07ea6e6` — feat(docker): add mem limits and healthchecks to all services
- `e9d874f` — docs(readme): add hardware requirements section
