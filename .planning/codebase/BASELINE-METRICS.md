# AQICN-Only Stability Baseline — Metrics

**Start date:** YYYY-MM-DD (fill in when Plan 0.4 begins)
**End date:** YYYY-MM-DD + 7 days
**Configuration:** OpenAQ tasks disabled; AQICN + forecast only
**Phase:** Phase 0 (Plan 0.4)

## Daily Metrics

| Day | Date | dag_ingest_hourly | dag_transform | Rows Today | Total Rows | Runtime (s) | API Errors | OOM Events |
|-----|------|-------------------|---------------|------------|------------|-------------|-----------|------------|
| 1 | | | | | | | | |
| 2 | | | | | | | | |
| 3 | | | | | | | | |
| 4 | | | | | | | | |
| 5 | | | | | | | | |
| 6 | | | | | | | | |
| 7 | | | | | | | | |

## Incident Log

| Date | Incident | Resolution |
|------|----------|------------|
| | | |

## Baseline Metrics (fill in after Day 7)

- **DAG success rate:** X/7 runs
- **Total rows ingested:** N
- **Storage growth:** ~X MB/day
- **dag_ingest_hourly avg runtime:** N seconds
- **dag_transform avg runtime:** N seconds
- **API errors:** N total

## DB Name Validation (D-13)

All ClickHouse queries used `air_quality` database. Confirm DB name is consistently `air_quality`:
- [ ] No query errors due to wrong DB name
- [ ] All tables accessible under `air_quality`

## Recommendations

_(fill in after 7 days of observation)_
