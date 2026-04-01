---
phase: 1
slug: multi-source-ingestion
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-04-01
---

# Phase 1 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | `pytest.ini` — Wave 0 creates |
| **Quick run command** | `pytest tests/ -q --tb=short` |
| **Full suite command** | `pytest tests/ -v --tb=short` |
| **Estimated runtime** | ~60 seconds |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/ -q --tb=short`
- **After every plan wave:** Run `pytest tests/ -v --tb=short`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 60 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 1-01-01 | 01 | 1 | REQ-INGEST-OW | unit | `pytest tests/test_openweather.py -q` | ❌ W0 | ⬜ pending |
| 1-01-02 | 01 | 1 | REQ-INGEST-OW | integration | `pytest tests/test_openweather_int.py -q` | ❌ W0 | ⬜ pending |
| 1-02-01 | 02 | 1 | REQ-INGEST-WAQI | unit | `pytest tests/test_waqi.py -q` | ❌ W0 | ⬜ pending |
| 1-02-02 | 02 | 1 | REQ-INGEST-WAQI | integration | `pytest tests/test_waqi_int.py -q` | ❌ W0 | ⌜ pending |
| 1-03-01 | 03 | 1 | REQ-INGEST-SC | unit | `pytest tests/test_sensorscm.py -q` | ❌ W0 | ⬜ pending |
| 1-03-02 | 03 | 1 | REQ-INGEST-SC | integration | `pytest tests/test_sensorscm_int.py -q` | ❌ W0 | ⬜ pending |
| 1-04-01 | 04 | 2 | REQ-DECOMMISSION | unit | `pytest tests/test_decommission.py -q` | ❌ W0 | ⬜ pending |
| 1-05-01 | 05 | 2 | REQ-OPTIMIZE | integration | `pytest tests/test_rate_limiter.py -q` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/conftest.py` — shared fixtures (ClickHouse mock, rate limiter mock, env var setup)
- [ ] `tests/test_openweather.py` — stubs: test_client_init, test_measurement_parsing, test_clickhouse_write
- [ ] `tests/test_waqi.py` — stubs: test_client_init, test_bbox_parsing, test_clickhouse_write
- [ ] `tests/test_sensorscm.py` — stubs: test_client_init, test_quality_flag_assignment, test_bbox_filter
- [ ] `tests/test_decommission.py` — stubs: test_openaq_removed, test_tables_archived, test_dag_clean
- [ ] `tests/test_rate_limiter.py` — stubs: test_parallel_ingestion, test_429_retry, test_control_table_update
- [ ] `pytest.ini` — `testpaths = tests`, `python_files = test_*.py`, `python_classes = Test*`
- [ ] `pip install pytest` in requirements.txt if not already present

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Zero HTTP 429 errors across all sources over 7 days | REQ-OPTIMIZE | Requires sustained production observation | Monitor Airflow task logs for 429 status codes over 7-day window |
| DAG runtime < 5 minutes (parallel execution) | REQ-OPTIMIZE | Requires timed production run | `airflow tasks states <dag_id> <date>`; calculate total wall time |
| Sensors.Community end-to-end latency < 15 min | REQ-INGEST-SC | Requires production clock comparison | Compare `data.timestamp` in ClickHouse vs DAG start time |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 60s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
