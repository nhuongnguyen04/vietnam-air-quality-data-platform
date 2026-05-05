<!-- generated-by: gsd-doc-writer -->
# Getting Started

This project is easiest to run as a local Docker Compose stack: ClickHouse, Airflow, dbt, the Streamlit dashboard, the guarded Ask Data FastAPI service, and optional monitoring/catalog services all bootstrap from the checked-in compose and Dockerfiles.

## Prerequisites

- `Docker Engine` or Docker Desktop with the `docker compose` plugin installed.
- `>= 16 GB RAM`, `>= 4 CPU cores`, and about `20 GB` of free disk if you plan to run the full local stack from `docker-compose.yml`.
- `Python >= 3.10` only if you want to run `pytest`, `dbt`, or helper scripts outside the containers. CI uses Python `3.10`, while the dashboard and text-to-SQL images build from `python:3.11-slim`.
- Before you run ingestion DAGs, set real values for `OPENWEATHER_API_TOKEN` and `TOMTOM_API_KEY`. Set `GROQ_API_KEY` as well if you want the Ask Data `/ask` endpoint to generate SQL.

## Installation Steps

1. Clone the repository and enter the project directory.

```bash
git clone https://github.com/nhuongnguyen04/vietnam-air-quality-data-platform.git
cd vietnam-air-quality-data-platform
```

2. Create a local environment file.

```bash
cp .env.example .env
```

3. Edit `.env` and set at least these values before the first run:

```bash
CLICKHOUSE_PASSWORD=your_clickhouse_password_here
OPENWEATHER_API_TOKEN=your_openweather_api_token_here
TOMTOM_API_KEY=your_tomtom_api_key_here
TEXT_TO_SQL_PREVIEW_SECRET=replace-with-a-long-random-secret
TEXT_TO_SQL_VANNA_CLIENT=chromadb
TEXT_TO_SQL_VANNA_PERSIST_DIRECTORY=/data/vanna
AIRFLOW__CORE__FERNET_KEY=replace-with-a-generated-fernet-key
AIRFLOW_API_SECRET_KEY=change-me-in-prod-use-long-random-string
AIRFLOW_API_AUTH_JWT_SECRET=change-me-in-prod-use-long-random-string
AIRFLOW_WEBSERVER_SECRET_KEY=change-me-in-prod-use-long-random-string
```

4. Build the local images used by Airflow, the dashboard, and text-to-SQL.

```bash
docker compose build
```

If you also want local non-container tooling for `pytest` or dbt commands, create a virtual environment and install the checked-in Python requirements:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## First Run

1. Start the core platform services.

```bash
docker compose up -d clickhouse postgres airflow-init airflow-scheduler airflow-dag-processor airflow-triggerer airflow-webserver dashboard text-to-sql
```

2. Verify that the stack is up.

```bash
docker compose ps
curl http://localhost:8123/ping
curl http://localhost:8000/health
```

3. Open the local interfaces:

- Airflow: `http://localhost:8090` with `admin` / `admin`
- Streamlit dashboard: `http://localhost:8501`
- ClickHouse HTTP endpoint: `http://localhost:8123`

4. If you want the full stack, including Grafana, Prometheus, and OpenMetadata, start everything:

```bash
docker compose up -d
```

## Common Setup Issues

- `.env.example` now includes the common Compose secrets and Google Drive variables, but optional integrations such as OpenMetadata may still require extra settings beyond the minimum local stack.
- The dashboard refuses to start if `CLICKHOUSE_PASSWORD` is missing. `python_jobs/dashboard/lib/clickhouse_client.py` raises `CLICKHOUSE_PASSWORD environment variable is required.`
- The text-to-SQL service will fail fast if `TEXT_TO_SQL_PREVIEW_SECRET` is missing, and `/ask` will fail until `GROQ_API_KEY` is set because `python_jobs/text_to_sql/vanna_runtime.py` requires it for SQL generation.
- In the default `chromadb` mode, the text-to-SQL service also needs a writable `TEXT_TO_SQL_VANNA_PERSIST_DIRECTORY`; otherwise Vanna cannot initialize its local store.
- Airflow now requires `AIRFLOW__CORE__FERNET_KEY`; leave it unset and the Compose stack will not have encrypted connection storage.
- If `docker compose up` fails on port allocation, check for local conflicts on `3000`, `5432`, `8090`, `8123`, `8501`, `8585`, `9090`, `9100`, and `9187`, or remap them in `docker-compose.yml`.

## Next Steps

- Read [CONFIGURATION.md](./CONFIGURATION.md) for the full environment variable inventory, defaults, and per-service config files.
- Read [ARCHITECTURE.md](./ARCHITECTURE.md) for the ingestion, dbt, serving, and monitoring data flow.
- Use `.github/workflows/ci.yml` and `pytest.ini` as the current references for local validation commands until dedicated development and testing guides are added.
