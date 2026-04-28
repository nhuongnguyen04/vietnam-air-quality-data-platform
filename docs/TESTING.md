<!-- generated-by: gsd-doc-writer -->
# Testing

This repository has three distinct test surfaces: the default Python pytest suite under `tests/python`, a separate pytest suite for the internal text-to-SQL service under `python_jobs/text_to_sql/tests`, and dbt data tests under `dbt/dbt_tranform/`.

## Test Framework and Setup

### Python pytest suites

- `pytest` is the primary test runner for repository Python code. It is declared in `requirements.txt` without a pinned version.
- Root pytest configuration lives in `pytest.ini`.
- Default discovery is limited to `tests/python` via `testpaths = tests/python`.
- The configured markers are `unit`, `integration`, `live`, and `requires_clickhouse`.
- By default, pytest excludes live tests with `addopts = -q --tb=short --strict-markers -m "not live"`.

Install the root dependencies before running Python tests:

```bash
pip install -r requirements.txt
```

### dbt tests

- Warehouse tests use dbt's built-in `dbt test` runner from `dbt-core==1.10.13` with `dbt-clickhouse==1.10.0`.
- The dbt project is configured in `dbt/dbt_tranform/dbt_project.yml`.
- The checked-in profile is `dbt/dbt_tranform/profiles.yml`.
- The `dev` target reads `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, and `CLICKHOUSE_DB`, with local defaults pointing at a ClickHouse instance on `localhost:8123`.

For local dbt test runs, start the minimal ClickHouse stack first:

```bash
docker compose -f docker-compose.test.yml up -d clickhouse init-schema
```

## Running Tests

### Default Python suite

Run the repository's default pytest suite exactly as configured in `pytest.ini`:

```bash
pytest
```

Run only fast local tests:

```bash
pytest tests/python -m "not integration and not live"
```

Run only integration tests:

```bash
pytest tests/python -m integration
```

Run a single Python test file:

```bash
pytest tests/python/common/test_api_client.py
```

### Text-to-SQL service tests

`python_jobs/text_to_sql/tests` is not part of the root `testpaths`, so it must be invoked explicitly:

```bash
pytest python_jobs/text_to_sql/tests
```

Run a single text-to-SQL test file:

```bash
pytest python_jobs/text_to_sql/tests/test_api_contract.py
```

### Live tests

Live tests are opt-in and require `RUN_LIVE_TESTS=1` plus the relevant service credentials:

```bash
RUN_LIVE_TESTS=1 OPENWEATHER_API_TOKEN=... pytest tests/python/jobs/openweather/test_openweather_live.py -m live
```

```bash
RUN_LIVE_TESTS=1 TELEGRAM_BOT_TOKEN=... TELEGRAM_CHAT_ID=... pytest tests/python/jobs/alerting/test_telegram_client_live.py -m live
```

### dbt tests

After ClickHouse is available, run the dbt validation flow from the project directory:

```bash
cd dbt/dbt_tranform
dbt deps
dbt seed --target dev
dbt run --target dev
dbt test --target dev
```

Run dbt tests for a narrower slice:

```bash
cd dbt/dbt_tranform
dbt test --select mart_air_quality__dashboard --target dev
```

## Writing New Tests

### Naming and placement

- Use `test_*.py` filenames, `Test*` class names, and `test_*` function names to match `pytest.ini`.
- Put general repository tests under `tests/python/` if you want them included by the default `pytest` command.
- Put text-to-SQL service tests under `python_jobs/text_to_sql/tests/` when they exercise that service boundary; these tests currently require an explicit pytest path and are not run by the root `pytest` command.
- dbt data tests live in model or seed YAML files under `dbt/dbt_tranform/models/` and `dbt/dbt_tranform/seeds/`. The checked-in project already uses built-in tests such as `not_null` and `unique` plus package tests such as `dbt_utils.unique_combination_of_columns`.

### Markers

- Use `@pytest.mark.unit` for mock-only tests.
- Use `@pytest.mark.integration` for file-based or multi-module integration checks.
- Use `@pytest.mark.live` for tests that call external services.
- Use `@pytest.mark.requires_clickhouse` when a test needs a running ClickHouse service.

### Shared helpers and patterns

- `tests/python/conftest.py` provides shared fixtures such as `mock_clickhouse_client` and `sample_openweather_response`.
- `python_jobs/text_to_sql/tests/conftest.py` provides service-focused fixtures including `temp_semantic_dir`, `fake_vanna_runtime`, `fake_clickhouse_executor`, and `text_to_sql_app`.
- Existing tests favor `monkeypatch` and `MagicMock` for outbound HTTP and client doubles.
- Airflow DAG checks in `tests/python/airflow/test_dag_ingest_hourly.py` are file-based assertions against DAG source text rather than full scheduler bootstraps.
- Text-to-SQL API contract tests call route endpoints from `create_app()` directly instead of using an HTTP test client.

## Coverage Requirements

No coverage threshold is configured in this repository.

| Type | Threshold |
| --- | --- |
| Lines | Not configured |
| Branches | Not configured |
| Functions | Not configured |
| Statements | Not configured |

No `.coveragerc`, `pytest-cov` usage, or CI coverage upload step was detected in the repository.

## CI Integration

Tests run in `.github/workflows/ci.yml` under the `CI` workflow.

- Trigger: `push` to `main`, `develop`, `feature/**`, and `fix/**`
- Trigger: `pull_request` targeting `main`
- Job `python-unit`: installs `requirements.txt` and runs `pytest tests/python -m "not integration and not live"` with `INGEST_MODE=csv`
- Job `python-integration`: installs `requirements.txt` and runs `pytest tests/python -m integration` with `INGEST_MODE=csv`
- Job `compile`: installs `dbt-core==1.10.13` and `dbt-clickhouse==1.10.0`, runs `dbt deps`, then `dbt compile --target dev`
- Job `validate`: starts a ClickHouse service, creates the `air_quality` database, then runs `dbt seed --target dev` and `dbt run --select +mart_air_quality__dashboard --target dev`
- Job `test`: starts ClickHouse again, runs `dbt seed --target dev`, `dbt run --target dev`, and `dbt test --target dev`

The current CI workflow does not invoke `pytest python_jobs/text_to_sql/tests`, so that suite must be run explicitly in local validation unless CI is extended.
