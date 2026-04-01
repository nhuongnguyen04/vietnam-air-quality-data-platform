---
wave: 3
depends_on:
  - .planning/phases/00-foundation/03-PLAN.md
files_modified:
  - airflow/dags/dag_ingest_hourly.py
  - .planning/codebase/BASELINE-METRICS.md
autonomous: false
---

# Plan 0.4 — AQICN-Only Stability Baseline

**Plan:** 0.4
**Phase:** 00-foundation
**Wave:** 3 (sequential: requires 0.3 CI to be running)
**Owner:** data engineering
**Type:** Operational runbook — minimal code changes, primarily monitoring work

---
```yaml
wave: 3
depends_on:
  - .planning/phases/00-foundation/03-PLAN.md
files_modified:
  - airflow/dags/dag_ingest_hourly.py
  - .planning/codebase/BASELINE-METRICS.md
autonomous: false
```

---

## Goal

Run the AQICN-only ingestion pipeline for 7 consecutive days to establish a stability baseline. Validates that Plans 0.2 (resource limits) and 0.3 (CI) are working before Phase 1 begins. Confirms DB name consistency (D-13). Confirms 100% DAG success rate (D-12).

---

## <task id="baseline-disable-openaq">

<read_first>
- `airflow/dags/dag_ingest_hourly.py` (lines 154–174: `run_openaq_measurements_ingestion` task)
- `airflow/dags/dag_ingest_hourly.py` (lines 225–235: task wiring)
</read_first>

<action>
Comment out the OpenAQ task from the DAG wiring in `airflow/dags/dag_ingest_hourly.py`. The task definition stays in the file (commented out) for easy restoration.

**Change task wiring (line 235):**

From:
```python
check_clickhouse >> metadata >> [openaq, aqicn, forecast] >> completion
```

To:
```python
# [openaq, aqicn, forecast] — DISABLED openaq for Plan 0.4 baseline
check_clickhouse >> metadata >> [aqicn, forecast] >> completion
```

**Comment out the task definition function (lines 154–174):**

From:
```python
@task
def run_openaq_measurements_ingestion():
    """Run OpenAQ measurements ingestion."""
    ...
```

To:
```python
# @task
# def run_openaq_measurements_ingestion():
#     """Run OpenAQ measurements ingestion. DISABLED for Plan 0.4 AQICN-only baseline."""
#     ...
```

**Comment out the task instantiation (line 229):**

From:
```python
openaq = run_openaq_measurements_ingestion()
```

To:
```python
# openaq = run_openaq_measurements_ingestion()  # DISABLED for 0.4 baseline
```

Keep all three changes so Phase 1 can re-enable by uncommenting.
</action>

<acceptance_criteria>
- `grep -n "openaq" airflow/dags/dag_ingest_hourly.py | grep -v "^.*#.*openaq" | grep "openaq"` returns 0 uncommented OpenAQ references in active wiring
- `grep "def run_openaq_measurements_ingestion" airflow/dags/dag_ingest_hourly.py` returns the function definition (commented out)
- `grep "def run_openaq_measurements_ingestion" airflow/dags/dag_ingest_hourly.py | grep "^#"` confirms it is commented
- Task dependency line contains `[aqicn, forecast]` (not `[openaq, aqicn, forecast]`)
</acceptance_criteria>

</task>

---

## <task id="baseline-restart-airflow">

<read_first>
- `docker-compose.yml` (to confirm service names)
</read_first>

<action>
Restart Airflow services to pick up DAG changes:

```bash
docker compose restart airflow-scheduler airflow-dag-processor
sleep 30
docker compose ps
```

Then check the Airflow UI at `http://localhost:8090` to confirm `dag_ingest_hourly` has updated task list.
</action>

<acceptance_criteria>
- `docker compose ps | grep airflow-scheduler` shows `Up` status
- Airflow UI at `http://localhost:8090` shows `dag_ingest_hourly` with only `aqicn_measurements_ingestion` and `aqicn_forecast_ingestion` tasks (no OpenAQ task)
- No DAG parse errors in `docker compose logs airflow-dag-processor | grep -i error`
</acceptance_criteria>

</task>

---

## <task id="baseline-monitor">

<read_first>
- `airflow/dags/dag_ingest_hourly.py` (for expected run times)
- `airflow/dags/dag_transform.py` (for transform runs)
</read_first>

<action>
Track the following metrics daily for 7 consecutive days. Create `.planning/codebase/BASELINE-METRICS.md` with the structure below. Perform checks at the same time each day (e.g., 08:00 local time).

**Daily checks:**
```bash
# 1. Service health
docker compose ps

# 2. Memory usage (no OOM events)
docker stats --no-stream

# 3. AQICN rows today
docker compose exec clickhouse clickhouse-client \
  --query "SELECT count(*) FROM air_quality.raw_aqicn_measurements WHERE ingest_date = today()"

# 4. DAG status
# Check via Airflow UI or CLI

# 5. Error count in logs
docker compose logs airflow-scheduler --since 1h | grep -ic error

# 6. Total rows in table
docker compose exec clickhouse clickhouse-client \
  --query "SELECT count(*) FROM air_quality.raw_aqicn_measurements"
```

**`.planning/codebase/BASELINE-METRICS.md` structure:**

```markdown
# AQICN-Only Stability Baseline — Metrics

**Start date:** YYYY-MM-DD
**End date:** YYYY-MM-DD + 7 days
**Configuration:** OpenAQ tasks disabled; AQICN + forecast only

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

## Baseline Metrics

- **DAG success rate:** X/7
- **Total rows ingested:** N
- **Storage growth:** ~X MB/day
- **dag_ingest_hourly avg runtime:** N seconds
- **dag_transform avg runtime:** N seconds
- **API errors:** N total
```

Fill in all rows with actual observed values.
</action>

<acceptance_criteria>
- `.planning/codebase/BASELINE-METRICS.md` created with all column headers
- Day 1 data captured within 24 hours of Plan 0.4 start
- All 7 days have data (no blank rows)
- `docker stats --no-stream` shows no `OOMKilled` events throughout the 7 days
- `raw_aqicn_measurements` row count at Day 7 > Day 1 (linear growth, no super-linear spikes)
</acceptance_criteria>

</task>

---

## <task id="baseline-results">

<read_first>
- `.planning/codebase/BASELINE-METRICS.md` (populated with 7 days of data)
</read_first>

<action>
After 7 consecutive days, update `.planning/codebase/BASELINE-METRICS.md` with a summary section:

```markdown
## Summary

- **DAG success rate:** X/7 runs
- **Total rows ingested:** N
- **Storage growth:** ~X MB/day
- **dag_ingest_hourly avg runtime:** N seconds
- **dag_transform avg runtime:** N seconds

### DB Name Validation (D-13)
All ClickHouse queries used `air_quality` database. Confirm DB name is consistently `air_quality`:
- No query errors due to wrong DB name
- All tables accessible under `air_quality`

### Recommendations
...
```

Also update `STATE.md` Phase 0 status:
```markdown
| 0 — Foundation & Stabilization | ✅ Complete | 5 | 5 |
```
Add a note:
```markdown
## Baseline Run (Plan 0.4)
**Status:** ✅ Complete
**DAG success rate:** 7/7
**Dates:** YYYY-MM-DD to YYYY-MM-DD
```
</action>

<acceptance_criteria>
- `.planning/codebase/BASELINE-METRICS.md` has summary section with all aggregate metrics
- `STATE.md` Phase 0 row updated to "✅ Complete | 5 | 5"
- `STATE.md` contains baseline summary with dates and success rate
- If any failures occurred: root cause documented in `BASELINE-METRICS.md` incident log
</acceptance_criteria>

</task>

---

## Re-enabling OpenAQ After Baseline

After the 7-day baseline is complete and Phase 1 begins:

1. Uncomment the `run_openaq_measurements_ingestion` function definition (remove leading `# ` from 3 lines)
2. Uncomment the `openaq = run_openaq_measurements_ingestion()` instantiation
3. Change task dependency back to: `check_clickhouse >> metadata >> [openaq, aqicn, forecast] >> completion`
4. Restart: `docker compose restart airflow-scheduler airflow-dag-processor`

---

## Success Criteria (must_haves)

1. 7/7 `dag_ingest_hourly` runs succeed (all tasks green)
2. 7/7 `dag_transform` runs succeed
3. No `OOMKilled` events in `docker stats --no-stream`
4. `raw_aqicn_measurements` row count grows linearly (no super-linear spikes)
5. `BASELINE-METRICS.md` exists with all 7 days of data documented

---

*Plan author: gsd:quick*
