---
phase: 09-fix-v-refactor-openmetadata
review_path: .planning/phases/09-fix-v-refactor-openmetadata/09-REVIEW.md
generated: 2026-04-21T07:27:15Z
fix_scope: critical_warning
findings_in_scope: 5
fixed: 5
skipped: 0
out_of_scope: 2
iteration: 1
status: all_fixed
---

# Phase 09: Code Review Fix Report

## Summary

Applied all in-scope findings from `09-REVIEW.md` for default scope (`critical` + `warning`).

## Fixed Findings

1. `CR-01` Hardcoded fallback admin credentials removed from runtime code; scripts now require `OM_ADMIN_USER` and `OM_ADMIN_PASSWORD`.
2. `WR-01` Replaced dynamic DAG `start_date` with fixed date (`2026-04-01`) to prevent scheduler drift.
3. `WR-02` Added explicit `timeout=30` to OpenMetadata `PUT`/`DELETE` API calls in dashboard provisioning script.
4. `WR-03` Replaced partial space substitution with full URL encoding via `urllib.parse.quote(..., safe='')` for entity FQN lookup.
5. `WR-04` Updated glossary term payload to use `glossary_id` argument directly (`{"id": glossary_id, "type": "glossary"}`).

## Out of Scope

- `IN-01` README source list consistency.
- `IN-02` README page count consistency.

## Validation

- `python -m py_compile airflow/dags/dag_openmetadata_curation.py python_jobs/jobs/openmetadata/create_streamlit_dashboard.py python_jobs/jobs/openmetadata/setup_glossary.py`

## Files Updated

- `airflow/dags/dag_openmetadata_curation.py`
- `python_jobs/jobs/openmetadata/create_streamlit_dashboard.py`
- `python_jobs/jobs/openmetadata/setup_glossary.py`
