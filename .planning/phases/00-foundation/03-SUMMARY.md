---
phase: "00"
plan: "03"
subsystem: ci
tags: [ci, github-actions, lint, dbt, ruff, sqlfluff]
duration: ~15 min
completed: 2026-04-01
---

## Summary

CI pipeline bootstrap completed. GitHub Actions pipeline with lint and dbt CI jobs set up.

## What Was Built

- **`requirements.txt`** — added `ruff==0.11.0`
- **`.ruff.toml`** — Python linter config: py310, 120 char line length, E/W/F/I/B/C4/UP rules
- **`.sqlfluff`** — SQL linter config: clickhouse dialect, dbt templater, 4-space indent
- **`docker-compose.test.yml`** — minimal ClickHouse CI stack with init-schema service
- **`.github/workflows/ci.yml`** — GitHub Actions pipeline:
  - `lint` job: `ruff check` + `sqlfluff lint`
  - `dbt-ci` job: `dbt deps && seed && run && test` against live ClickHouse
  - `dbt-ci needs: lint` — blocks dbt runs on lint failure

## Files Modified

- `requirements.txt` (added ruff)
- `.ruff.toml` (created)
- `.sqlfluff` (created)
- `docker-compose.test.yml` (created)
- `.github/workflows/ci.yml` (created)
- `dbt/dbt_tranform/profiles.yml` (added `ci:` target — committed via git hash-object)

## Known Issue

`dbt_tranform/profiles.yml` is owned by root (Docker bind mount) — working tree cannot be updated directly. Committed via `git hash-object`. When CI clones the repo fresh, the committed version with `ci` target will be present.

## Commits

- `47ea07b` — feat(ci): add CI pipeline with lint and dbt CI jobs (Plan 0.3)
- `77745bd` — feat(dbt): add ci target to profiles.yml (Plan 0.3)

## Next

Plan 04: Disable OpenAQ tasks and begin 7-day stability baseline monitoring.
