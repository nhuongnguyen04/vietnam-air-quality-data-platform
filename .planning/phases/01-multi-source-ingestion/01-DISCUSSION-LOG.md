# Phase 1: Multi-Source Ingestion - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-01
**Phase:** 01-multi-source-ingestion
**Areas discussed:** Dedup Strategy, Historical Backfill Scope, Sensors.Community Quality Handling, MONRE Fallback Priority

---

## Dedup Strategy

| Option | Description | Selected |
|--------|-------------|----------|
| ReplacingMergeTree + ingest_time | Server-side dedup keyed on ingest_time. Eliminates Python race condition. Matches existing aqicn_measurements approach. | ✓ |
| ReplacingMergeTree + UUID batch_id | UUID per ingestion batch. Enables idempotent replay of specific batches. More audit-friendly but more complex. | |
| Keep MergeTree + Python dedup | Follow existing aqicn pattern. Simpler but keeps race condition risk. | |

**User's choice:** ReplacingMergeTree + ingest_time
**Notes:** Aligns with existing AQICN forecast table pattern. Clean solution, no Python dedup complexity.

---

## Historical Backfill Scope

| Option | Description | Selected |
|--------|-------------|----------|
| 3 months | Sufficient for trend analysis. 90-day window covers seasonal patterns. Balanced cost vs coverage. | ✓ |
| 1 year | Full annual cycle. Highest coverage but longer backfill time and more API calls. | |
| No historical backfill | Live only from Phase 1 go-live date. Simpler, faster to ship. | |

**User's choice:** 3 months
**Notes:** Good coverage for seasonal patterns without excessive API load.

---

## Sensors.Community Quality Handling

| Option | Description | Selected |
|--------|-------------|----------|
| Flag + insert all data | Insert all data including outliers. Add quality_flag column. Downstream dbt models can filter. Full audit trail. | ✓ |
| Exclude + log outliers | Drop records where PM2.5 < 0 or > 500 µg/m³. Cleaner raw tables but lose raw evidence. | |
| Accept default (no filtering) | Store everything, surface in metrics. No quality_flag. | |

**User's choice:** Flag + insert all data
**Notes:** Preserves audit trail. Flag values: 'implausible', 'outlier', 'valid'. Downstream dbt (Phase 2) filters on 'valid'.

---

## MONRE Fallback Priority

| Option | Description | Selected |
|--------|-------------|----------|
| Async discovery (no timeline) | MONRE discovery continues indefinitely. Phase 1 ships with 3 sources. Grafana panel shows 'discovering'. | |
| 2-week timebox | Invest 2 weeks of focused effort (Plan 1.3). If no access confirmed, archive and ship Phase 1 with 3 sources. | ✓ |
| Skip MONRE for now | Nice-to-have. Prioritize shipping 3-source Phase 1 on schedule. | |

**User's choice:** 2-week timebox
**Notes:** Focused discovery effort with a hard boundary. Ships Phase 1 with 3 sources if MONRE is inaccessible. Grafana panel always shows a status.

---

## Deferred Ideas

None — discussion stayed within Phase 1 scope.
