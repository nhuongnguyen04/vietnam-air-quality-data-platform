# Phase 0: Foundation & Stabilization - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-01
**Phase:** 00-foundation
**Areas discussed:** Audit Scope, CI Test Data Strategy, CI Runner, ClickHouse DB Name Fix

---

## Audit Scope

| Option | Description | Selected |
|--------|-------------|----------|
| Full audit | Inventory all 16 CONCERNS items, ClickHouse, dbt, Airflow, OpenAQ schema | |
| **Audit tập trung** | **Only inventory what's needed for Phase 1: OpenAQ schema, dedup strategy, Airflow env var capture** | ✓ |

**User's choice:** Audit tập trung
**Notes:** Only audit what affects Phase 1 scope. Full CONCERNS inventory deferred.

---

## CI Test Data Strategy

| Option | Description | Selected |
|--------|-------------|----------|
| dbt compile only | Verify SQL syntax only — fast, sufficient for PR checks | |
| **Full dbt run (prod-like)** | **Seed data + dbt run + dbt test — slower but verifies actual logic** | ✓ |
| Tiny sample dataset | Subset of production data — balance of speed and reliability | |

**User's choice:** Full dbt run (prod-like)
**Notes:** Must verify actual logic, not just syntax.

---

## CI Runner

| Option | Description | Selected |
|--------|-------------|----------|
| **Ubuntu (Recommended)** | GitHub-hosted runners, free for public repos, sufficient | ✓ |
| Self-hosted | Custom runner — requires setup but full control | |

**User's choice:** Ubuntu (Recommended)
**Notes:** Standard GitHub-hosted runners.

---

## ClickHouse DB Name Fix (CONCERNS #11)

| Option | Description | Selected |
|--------|-------------|----------|
| **Có, fix luôn** | **Standardize DB name across all configs before Phase 0 ends** | ✓ |
| Không, để Phase 1+ | Fix when needed — don't touch what's working | |

**User's choice:** Có, fix luôn
**Notes:** Standardize now to prevent confusion later. Coordinate with Plan 0.1 audit to determine target name.

---

## Claude's Discretion

The following were decided by the roadmap itself (no discussion needed):
- Plans 0.2, 0.4, 0.5 scope — clearly defined in ROADMAP.md
- Deduplication fix (CONCERNS #7) — addressed in Plan 0.1 audit and Plan 2.4
- Resource limits — standard defaults (ClickHouse 4GB, Airflow 2GB, PostgreSQL 1GB)
- CI blocking strategy — standard: block merge on CI failure

---

## Resource Limits Discussion

| Question | Options Presented | Selected |
|-----------|-----------------|---------|
| Resource limits scope | Giới hạn chi tiết / Cấu trúc limits / Đủ rồi | Giới hạn chi tiết |
| Hardware available | Tôi có 15GB RAM khả dụng cho Docker | 15GB |
| OpenMetadata RAM | Đủ 15GB / Giảm OM / Giảm service khác | Đủ 15GB cho tất cả |
| Proposed limits | Đồng ý / Điều chỉnh | Điều chỉnh |
| Adjustment direction | OpenMetadata / ClickHouse / Airflow | Giảm headroom xuống 1-2GB, 13GB cho containers |
| Final limits | Đồng ý / Giảm OM / Giảm ClickHouse | Đồng ý |

**User's notes:** Giảm xuống một chút, muốn thừa ra 1-2GB RAM tức là sử dụng 13GB, 2GB dự phòng khi full load. OpenMetadata dùng chung postgres vs airflow.

**Final RAM limits (D-17):**
- ClickHouse: 3GB
- PostgreSQL: 1GB
- Airflow scheduler: 512MB
- Airflow dag-processor: 512MB
- Airflow triggerer: 512MB
- OpenMetadata (server + MySQL + Elasticsearch): 4GB total
- Superset: 1GB
- Grafana: 512MB
- Total container pool: ~11GB ✓ (fits in 13GB)

**Minimum hardware (D-19):** 16GB RAM host machine (13GB Docker, 2GB host OS headroom)

---

## Deferred Ideas

None — discussion stayed within Phase 0 scope.
