# Phase 0: Foundation & Stabilization - Context

**Gathered:** 2026-04-01
**Status:** Ready for planning

<domain>
## Phase Boundary

Stabilize brownfield baseline before adding new components. This phase audits the existing codebase to surface hardcoded assumptions, bootstraps CI, hardens Docker Compose, establishes a stability baseline, and creates the ingestion control infrastructure.

Scope is exactly the 5 plans from ROADMAP.md. Scope creep is not allowed.
</domain>

<decisions>
## Implementation Decisions

### Audit Scope (Plan 0.1)
- **D-01:** Focused audit — inventory only what's needed for Phase 1: OpenAQ schema assumptions, dedup strategy (ReplacingMergeTree concern from CONCERNS #7), Airflow env var capture pattern, ClickHouse raw table schemas
- **D-02:** Document findings in `.planning/codebase/AUDIT.md` — this becomes required reading before Phase 1 planning
- **D-03:** NOT a full inventory of all 16 CONCERNS items — only audit what affects Phase 1 scope

### CI Test Data Strategy (Plan 0.3)
- **D-04:** Full dbt run in CI — seed data + `dbt run` + `dbt test` against test ClickHouse instance
- **D-05:** NOT `dbt compile` only — must verify actual logic, not just syntax
- **D-06:** Test ClickHouse instance in CI: spin up via `docker compose -f docker-compose.test.yml` (minimal: ClickHouse + test runner only, no Superset/Grafana/OpenMetadata)

### CI Runner (Plan 0.3)
- **D-07:** GitHub-hosted Ubuntu runners (no self-hosted)
- **D-08:** Standard GitHub Actions workflow at `.github/workflows/ci.yml`

### ClickHouse DB Name Fix (CONCERNS #11)
- **D-09:** Fix DB name inconsistency during Phase 0 — standardize to one name across all configs
- **D-10:** Coordinate with Plan 0.1 audit to determine which name to standardize to (decision deferred to audit findings)
- **D-11:** Fix must be completed before Phase 0 ends — not deferred to later phase

### Baseline Stability Run (Plan 0.4)
- **D-12:** AQICN-only baseline run must achieve 100% DAG success rate over 7 consecutive days before Phase 1 begins
- **D-13:** This run also validates the ClickHouse DB name fix from D-09/D-10

### CI Pipeline Blocking (Plan 0.3)
- **D-14:** Merge to main branch blocked if CI fails — no exceptions
- **D-15:** All PRs must pass CI before merge

### Docker Compose Resource Limits (Plan 0.2)
- **D-16:** Add `mem_limit` and `cpu_limit` to ALL services (existing + future)
- **D-17:** Minimum: ClickHouse 4GB, Airflow 2GB, PostgreSQL 1GB
- **D-18:** Health checks must be added to all services (CONCERNS shows some already have, some don't)

</decisions>

<specifics>
## Specific Ideas

No specific external references or "I want it like X" moments — open to standard approaches for all Phase 0 decisions.
</specifics>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Existing Codebase
- `.planning/codebase/CONCERNS.md` — Critical concerns to address in Phase 0; CONCERNS #7 (MergeTree dedup), #8 (no CI), #11 (DB name mismatch), #15 (no freshness monitoring) directly relevant
- `.planning/codebase/CONVENTIONS.md` — All established conventions: Python style, dbt naming, Airflow DAG patterns, Docker Compose structure
- `.planning/codebase/ARCHITECTURE.md` — Current architecture to audit against
- `.planning/codebase/STACK.md` — Current tech stack versions

### Project Planning
- `.planning/PROJECT.md` — Core value, constraints, active requirements
- `.planning/ROADMAP.md` § Phase 0 — Phase 0 full plan descriptions (Plans 0.1–0.5)
- `.planning/research/PITFALLS.md` § 5.1, 5.3 — Docker resource exhaustion and no CI pitfalls: prevention strategies apply to Plans 0.2 and 0.3
- `.planning/research/ARCHITECTURE.md` § 6 — Docker Compose resource ceiling (~20GB RAM, 9 CPU cores) informs Plan 0.2 limits
</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `python_jobs/common/api_client.py` — existing APIClient base; CI lint must check this
- `python_jobs/common/rate_limiter.py` — TokenBucketRateLimiter; lint must check
- Existing Docker Compose health checks for ClickHouse and PostgreSQL can serve as templates for other services
- `airflow/config/entrypoint.sh` — existing entrypoint; template for health check setup

### Established Patterns
- Airflow TaskFlow API (`@dag`, `@task`) — standard pattern for all new DAGs
- dbt `profiles.yml` target structure — `production` + `dev` already exist; `ci` target needed for Plan 0.3
- `sqlfluff==3.5.0` already in requirements.txt — ready for CI lint step
- `ruff check` not yet in requirements — needs to be added for Python lint step

### Integration Points
- CI must trigger on `push` to main AND on `pull_request` events
- `docker-compose.test.yml` must reference same `.env` pattern as production
- `ingestion.control` table (Plan 0.5) connects to Grafana dashboards in Phase 3
- Audit findings feed directly into Phase 1 Plan 1.1–1.5 source-specific implementations

### Known Gaps from CONCERNS.md
- `ruff check` not in requirements.txt — must be added in Plan 0.3
- `dbt target/` already in `.gitignore` — confirmed good
- `.github/workflows/` directory doesn't exist — must be created
</code_context>

<deferred>
## Deferred Ideas

None — discussion stayed within Phase 0 scope.

</deferred>

---

*Phase: 00-foundation*
*Context gathered: 2026-04-01*
