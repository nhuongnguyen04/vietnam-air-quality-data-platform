<!-- generated-by: gsd-doc-writer -->
# Development

## Local Setup

This repository is developed as a Docker Compose platform with Python-based ingestion code, an Airflow image, a dbt project, and optional dashboard/text-to-SQL services. For full-stack work, you need Docker Compose. For local Python, align with CI on `Python 3.10`; the dashboard and text-to-SQL containers use `python:3.11-slim`, and the Airflow image is built from `apache/airflow:3.1.7`.

1. Clone the repository and enter the project directory.

```bash
git clone https://github.com/nhuongnguyen04/vietnam-air-quality-data-platform.git
cd vietnam-air-quality-data-platform
```

2. Create a virtual environment and install the main development dependencies.

```bash
python3.10 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If you want to run the dashboard or text-to-SQL service directly outside Docker, install their service-specific requirements as well:

```bash
pip install -r python_jobs/dashboard/requirements.txt
pip install -r python_jobs/text_to_sql/requirements.txt
```

3. Create a local environment file and fill in the required secrets.

```bash
cp .env.example .env
```

At minimum, replace the placeholder ClickHouse, Airflow, Grafana, and Google Drive values in `.env.example` with real secrets before starting the stack. See [CONFIGURATION.md](./CONFIGURATION.md) for the full inventory and the variables that are still optional or legacy-only.

4. Start the services you need.

```bash
docker compose up -d
```

For narrower development loops, start only the relevant services:

- `docker compose up -d clickhouse` for dbt work against a local warehouse
- `docker compose up -d dashboard` for the Streamlit UI
- `docker compose up -d text-to-sql dashboard` for Ask Data development
- `docker compose up -d grafana` for monitoring UI work

## Build Commands

This project does not define a root `package.json` or `Makefile`. The commands below are the checked-in development commands used by README instructions, Dockerfiles, Airflow DAGs, and GitHub Actions.

| Command | Description |
| --- | --- |
| `pip install -r requirements.txt` | Install the main local toolchain: dbt, Airflow providers, ingestion dependencies, linting tools, and pytest. |
| `pip install -r requirements-ingest.txt` | Install the lighter ingestion-only dependency set used by `.github/workflows/scheduled_ingestion.yml`. |
| `pip install -r python_jobs/dashboard/requirements.txt` | Install the direct-run dependencies for the Streamlit dashboard. |
| `pip install -r python_jobs/text_to_sql/requirements.txt` | Install the direct-run dependencies for the FastAPI text-to-SQL service. |
| `docker compose up -d` | Start the full local platform defined in `docker-compose.yml`. |
| `docker compose up -d dashboard` | Start only the Streamlit dashboard service. |
| `docker compose up -d text-to-sql dashboard` | Start the Ask Data FastAPI service and the Streamlit dashboard together. |
| `docker compose up -d grafana` | Start the Grafana service for monitoring work. |
| `cd dbt/dbt_tranform && dbt deps` | Install dbt packages from `packages.yml`. |
| `cd dbt/dbt_tranform && dbt compile --target dev` | Compile the dbt project against the `dev` target from `profiles.yml`. |
| `cd dbt/dbt_tranform && dbt seed --target dev` | Load checked-in seed data into the dev ClickHouse target. |
| `cd dbt/dbt_tranform && dbt run --select +dm_air_quality_overview_daily +dm_aqi_current_status +dm_traffic_hourly_trend --target dev` | Run the current dashboard/text-to-SQL dependency graph used in CI validation. |
| `cd dbt/dbt_tranform && dbt run --target dev` | Run the full dbt project against the dev target. |
| `cd dbt/dbt_tranform && dbt test --target dev` | Run dbt tests against the dev target. |
| `pytest tests/python -m "not integration and not live"` | Run the fast Python test subset used by the `python-unit` CI job. |
| `pytest tests/python -m integration` | Run the integration subset used by the `python-integration` CI job. |
| `pytest python_jobs/text_to_sql/tests` | Run the text-to-SQL service tests that live outside the root `tests/python` suite. |
| `ruff check python_jobs/ airflow/dags/ --config .ruff.toml` | Run the Python linter on the ingestion code and DAGs. |
| `sqlfluff lint dbt/dbt_tranform/ --format github-annotation` | Lint ClickHouse/dbt SQL using the checked-in dbt templater configuration. |
| `cd python_jobs/dashboard && streamlit run app.py --server.port=8501 --server.address=0.0.0.0` | Run the dashboard directly, matching its Dockerfile command. |
| `cd python_jobs/text_to_sql && uvicorn app:app --host 0.0.0.0 --port 8000` | Run the text-to-SQL API directly, matching its Dockerfile command. |

## Code Style

Python and SQL quality checks are configured in-repo, but only part of that enforcement is currently blocking in CI.

- **Ruff**: The Python linter is configured in [`.ruff.toml`](../.ruff.toml). It targets `py310`, uses a `120` character line length, and enables `pycodestyle`, `Pyflakes`, `isort`, `flake8-bugbear`, `flake8-comprehensions`, and `pyupgrade` rules. Run it with `ruff check python_jobs/ airflow/dags/ --config .ruff.toml`.
- **SQLFluff**: SQL linting is configured in [`.sqlfluff`](../.sqlfluff). It uses the `clickhouse` dialect with the `dbt` templater and points both `project_dir` and `profiles_dir` at `dbt/dbt_tranform`. Run it with `sqlfluff lint dbt/dbt_tranform/ --format github-annotation`.
- **Pytest conventions**: [`pytest.ini`](../pytest.ini) sets `tests/python` as the default suite, uses `test_*.py` naming, enables `--strict-markers`, and defines `unit`, `integration`, `live`, and `requires_clickhouse` markers. The default addopts exclude `live` tests.
- **Current CI behavior**: `.github/workflows/ci.yml` runs both `ruff` and `sqlfluff` as blocking checks in the `lint` job. A failure in either command fails the job.
- **Not detected**: No checked-in `pre-commit` config, `EditorConfig`, Prettier config, or Biome config was found at the repository root.

## Branch Conventions

The repository does not include a dedicated `CONTRIBUTING.md` or documented branching policy. The explicit conventions come from Git and GitHub Actions:

- The remote default branch is `main`.
- `.github/workflows/ci.yml` runs on pushes to `main`, `develop`, `feature/**`, and `fix/**`.
- Pull request CI runs only for PRs that target `main`.

In practice, `feature/<name>` and `fix/<name>` are the only non-default branch naming patterns that are explicitly recognized by repository automation.

## PR Process

No pull request template is checked in, so the review contract is defined mainly by the CI workflow and the project layout.

- Open pull requests against `main` if you need GitHub Actions PR validation.
- Run the checks that match your change surface before opening the PR: `ruff` for Python, `sqlfluff` plus `dbt compile/run/test` for warehouse changes, and `pytest` for shared Python code.
- Expect the CI workflow in `.github/workflows/ci.yml` to execute `lint`, then `python-unit`, then both `python-integration` and `text-to-sql-tests`, followed by `compile`, `validate`, and `test`.
- The `validate` and `test` jobs spin up a temporary ClickHouse service and exercise dbt `seed`, `run`, and `test`, so dbt changes should be reviewed with local ClickHouse compatibility in mind.
- Because the `lint` job is blocking, Python and dbt style regressions should be fixed before the PR is considered ready.
