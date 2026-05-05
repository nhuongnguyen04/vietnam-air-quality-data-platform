# Text-to-SQL Runtime Contract

This directory defines the internal service boundary for Phase 10's Ask Data flow.

## Runtime Decision

The implementation is pinned behind a local runtime wrapper so the rest of the codebase never depends directly on a moving provider API. The wrapper must expose a `generate_sql` capability without executing SQL, and it remains the only place where a Vanna-style client or cloud LLM provider may be called.

The runtime now uses Vanna OSS directly, with a local ChromaDB vector store and Groq-hosted `qwen/qwen3-32b` as the underlying LLM via OpenAI-compatible APIs.
It is expected to keep a persistent local Chroma store on disk so normal process restarts reuse the current collection instead of retraining from scratch.

The pre-implementation verification checklist for the provider wrapper is:

1. Confirm the selected package or provider still exposes a SQL-generation method equivalent to `generate_sql`.
2. Confirm SQL generation can be separated from execution.
3. Confirm the provider can be configured with mart-only semantic assets from this repository.
4. Confirm all provider credentials are supplied through environment variables.
5. Confirm the local service contract below still matches the pinned runtime implementation.

## Service Contract

The service must expose these endpoints:

- `GET /health`
- `GET /metadata/tables`
- `POST /ask`
- `POST /execute`

`POST /ask` generates a SQL preview and explanation only. It must never execute SQL as part of preview generation.

`POST /execute` runs only previously previewed SQL after server-side revalidation. The service must bind execution to the preview token or preview hash returned by `POST /ask`.

## Safety Contract

- SQL generation must stay separate from execution.
- Only read-only analytics queries are allowed.
- The service accepts only the mart/fact semantic surface defined under `python_jobs/text_to_sql/semantic/`.
- Local SQL validation remains mandatory even if the upstream provider offers its own safety controls.
- Vanna permission controls are not sufficient on their own; local SQL validation plus read-only ClickHouse permissions remain mandatory.
- Generated SQL must be revalidated on `POST /execute`, not trusted because it passed preview earlier.

## Environment Variables

Only cloud-provider and service credentials belong in environment variables. The application must not hardcode them in Streamlit or service code.

Expected environment variables include:

- `GROQ_API_KEY`
- `GROQ_MODEL`
- `TEXT_TO_SQL_VANNA_CLIENT`
- `TEXT_TO_SQL_VANNA_COLLECTION`
- `TEXT_TO_SQL_VANNA_PERSIST_DIRECTORY`
- `TEXT_TO_SQL_VANNA_REBUILD`
- `TEXT_TO_SQL_URL`
- `TEXT_TO_SQL_CLICKHOUSE_USER`
- `TEXT_TO_SQL_CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_HOST`
- `CLICKHOUSE_PORT`
- `CLICKHOUSE_DB`

Recommended runtime defaults:

- `TEXT_TO_SQL_VANNA_CLIENT=chromadb`
- `TEXT_TO_SQL_VANNA_COLLECTION=air_quality_ask_data`
- `TEXT_TO_SQL_VANNA_PERSIST_DIRECTORY=/data/vanna`
- `TEXT_TO_SQL_CLICKHOUSE_USER=aqi_reader`
- `GROQ_MODEL=qwen/qwen3-32b`

## Persistent Store Lifecycle

- First boot builds the mart-only catalog bundle, computes a semantic fingerprint, trains a Vanna collection, and writes a manifest next to the Chroma store.
- Later restarts reuse the active collection when the semantic fingerprint, model, and client mode are unchanged.
- If semantic assets change, the runtime builds a new collection automatically.
- `TEXT_TO_SQL_VANNA_REBUILD=true` forces a fresh collection build even when the fingerprint is unchanged.
- If the local store becomes corrupt, remove the persisted directory, then restart with `TEXT_TO_SQL_VANNA_REBUILD=true`.

## ClickHouse Contract

The text-to-SQL service must use a dedicated read-only ClickHouse user. It may query only the approved mart/fact analytics surface and must never rely on broader database permissions.

## Migration Gate

Before treating the Vanna path as production-ready by default, the repo should satisfy all of these:

1. The mart-only catalog/training bundle builds cleanly from repo-owned metadata.
2. The bilingual eval corpus under `python_jobs/text_to_sql/evals/ask_data_eval_cases.yml` is present and green through the repo-local eval runner.
3. The Vanna path keeps unsafe output rate at zero on the migration gate corpus.
4. `aqi_reader` exists in ClickHouse and is used by the service instead of reusing `om_reader`.
5. A normal text-to-SQL service restart reuses the current Chroma collection instead of retraining the full catalog.
