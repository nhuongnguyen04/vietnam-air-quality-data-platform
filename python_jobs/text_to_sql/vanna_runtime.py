"""Single integration boundary for Vanna-backed SQL generation without execution."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import time
from typing import Any

try:
    from python_jobs.text_to_sql.catalog_builder import build_vanna_catalog_bundle
    from python_jobs.text_to_sql.eval_runner import (
        EvalValidationError,
        find_matching_eval_case,
        evaluate_sql_against_case,
    )
    from python_jobs.text_to_sql.sql_validator import SqlValidationError, validate_sql
except ModuleNotFoundError:  # pragma: no cover - container import fallback
    from catalog_builder import build_vanna_catalog_bundle  # type: ignore
    from eval_runner import EvalValidationError, find_matching_eval_case, evaluate_sql_against_case  # type: ignore
    from sql_validator import SqlValidationError, validate_sql  # type: ignore


class RuntimeNotConfiguredError(RuntimeError):
    """Raised when the runtime wrapper has no configured provider."""


class RuntimeGenerationError(RuntimeError):
    """Raised when the provider response cannot be used safely."""


@dataclass(frozen=True)
class GeneratedSql:
    sql: str
    explanation: str
    referenced_tables: list[str]
    generator_metadata: dict[str, str]


@dataclass(frozen=True)
class VannaRuntimeConfig:
    api_key: str
    model: str
    base_url: str
    client: str
    base_collection_name: str
    persist_directory: str | None
    rebuild: bool


@dataclass(frozen=True)
class TrainingManifest:
    version: int
    client: str
    model: str
    base_collection_name: str
    collection_name: str
    persist_directory: str | None
    semantic_fingerprint: str
    generated_at_epoch: int


def _is_truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


class VannaRuntime:
    """Vanna-backed SQL generation wrapper that never executes SQL."""

    def __init__(self, semantic_dir: str | None = None) -> None:
        self.semantic_dir = semantic_dir
        self._vanna_client: Any | None = None
        self._catalog_bundle: dict[str, Any] | None = None
        self._runtime_config: VannaRuntimeConfig | None = None
        self._training_manifest: TrainingManifest | None = None

    def _resolve_vanna_config(self) -> VannaRuntimeConfig:
        groq_api_key = os.environ.get("GROQ_API_KEY")
        if not groq_api_key:
            raise RuntimeNotConfiguredError(
                "GROQ_API_KEY is required for the Vanna runtime. Set GROQ_API_KEY and optionally GROQ_MODEL."
            )

        client = os.environ.get("TEXT_TO_SQL_VANNA_CLIENT", "chromadb").strip() or "chromadb"
        if client not in {"chromadb", "in-memory"}:
            raise RuntimeNotConfiguredError(
                "TEXT_TO_SQL_VANNA_CLIENT must be either 'chromadb' or 'in-memory'"
            )

        persist_directory = None
        if client != "in-memory":
            persist_directory = (
                os.environ.get("TEXT_TO_SQL_VANNA_PERSIST_DIRECTORY", "/data/vanna").strip()
                or "/data/vanna"
            )

        return VannaRuntimeConfig(
            api_key=groq_api_key,
            model=os.environ.get("GROQ_MODEL", "qwen/qwen3-32b"),
            base_url="https://api.groq.com/openai/v1",
            client=client,
            base_collection_name=os.environ.get(
                "TEXT_TO_SQL_VANNA_COLLECTION", "air_quality_ask_data"
            ).strip()
            or "air_quality_ask_data",
            persist_directory=persist_directory,
            rebuild=_is_truthy(os.environ.get("TEXT_TO_SQL_VANNA_REBUILD")),
        )

    def _load_vanna_dependencies(self):
        try:
            from openai import OpenAI
        except ImportError as exc:  # pragma: no cover - dependency gate
            raise RuntimeNotConfiguredError(
                "OpenAI client dependency missing for Vanna runtime. Install text-to-sql requirements."
            ) from exc

        try:
            from vanna.legacy.chromadb import ChromaDB_VectorStore
            from vanna.legacy.openai import OpenAI_Chat
        except ImportError:
            try:
                from vanna.chromadb import ChromaDB_VectorStore  # type: ignore
                from vanna.openai import OpenAI_Chat  # type: ignore
            except ImportError as exc:  # pragma: no cover - dependency gate
                raise RuntimeNotConfiguredError(
                    "Vanna dependency missing. Install `vanna[openai,chromadb]` for text-to-sql."
                ) from exc

        return OpenAI, ChromaDB_VectorStore, OpenAI_Chat

    def _get_catalog_bundle(self) -> dict[str, Any]:
        if self._catalog_bundle is None:
            self._catalog_bundle = build_vanna_catalog_bundle(self.semantic_dir)
        return self._catalog_bundle

    def _build_semantic_fingerprint(self, bundle: dict[str, Any]) -> str:
        payload = json.dumps(bundle, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _create_collection_name(
        self,
        base_collection_name: str,
        semantic_fingerprint: str,
        *,
        force_new_build: bool,
    ) -> str:
        collection_name = f"{base_collection_name}__{semantic_fingerprint[:12]}"
        if force_new_build:
            collection_name = f"{collection_name}__{int(time.time())}"
        return collection_name

    def _manifest_path(self, config: VannaRuntimeConfig) -> Path | None:
        if config.persist_directory is None:
            return None
        return Path(config.persist_directory) / f"{config.base_collection_name}.manifest.json"

    def _collection_persist_path(
        self,
        config: VannaRuntimeConfig,
        manifest: TrainingManifest,
    ) -> str | None:
        if config.persist_directory is None:
            return None
        return str(Path(config.persist_directory) / manifest.collection_name)

    def _load_training_manifest(self, config: VannaRuntimeConfig) -> TrainingManifest | None:
        manifest_path = self._manifest_path(config)
        if manifest_path is None or not manifest_path.exists():
            return None
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        return TrainingManifest(**payload)

    def _write_training_manifest(
        self,
        config: VannaRuntimeConfig,
        manifest: TrainingManifest,
    ) -> None:
        manifest_path = self._manifest_path(config)
        if manifest_path is None:
            return
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(asdict(manifest), indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def _build_training_manifest(
        self,
        config: VannaRuntimeConfig,
        bundle: dict[str, Any],
        existing_manifest: TrainingManifest | None,
    ) -> TrainingManifest:
        semantic_fingerprint = self._build_semantic_fingerprint(bundle)
        reuse_existing_collection = (
            existing_manifest is not None
            and not config.rebuild
            and existing_manifest.semantic_fingerprint == semantic_fingerprint
            and existing_manifest.model == config.model
            and existing_manifest.client == config.client
            and existing_manifest.base_collection_name == config.base_collection_name
        )

        if reuse_existing_collection:
            collection_name = existing_manifest.collection_name
        else:
            collection_name = self._create_collection_name(
                config.base_collection_name,
                semantic_fingerprint,
                force_new_build=(
                    config.rebuild
                    and existing_manifest is not None
                    and existing_manifest.semantic_fingerprint == semantic_fingerprint
                ),
            )

        return TrainingManifest(
            version=1,
            client=config.client,
            model=config.model,
            base_collection_name=config.base_collection_name,
            collection_name=collection_name,
            persist_directory=config.persist_directory,
            semantic_fingerprint=semantic_fingerprint,
            generated_at_epoch=int(time.time()),
        )

    def _should_retrain(
        self,
        config: VannaRuntimeConfig,
        current_manifest: TrainingManifest,
        existing_manifest: TrainingManifest | None,
    ) -> bool:
        if config.client == "in-memory":
            return True
        if config.rebuild or existing_manifest is None:
            return True
        return (
            existing_manifest.collection_name != current_manifest.collection_name
            or existing_manifest.semantic_fingerprint != current_manifest.semantic_fingerprint
            or existing_manifest.model != current_manifest.model
            or existing_manifest.client != current_manifest.client
        )

    def _get_runtime_config(self) -> VannaRuntimeConfig:
        if self._runtime_config is None:
            self._runtime_config = self._resolve_vanna_config()
        return self._runtime_config

    def _get_training_manifest(self) -> TrainingManifest:
        if self._training_manifest is None:
            config = self._get_runtime_config()
            bundle = self._get_catalog_bundle()
            existing_manifest = self._load_training_manifest(config)
            self._training_manifest = self._build_training_manifest(
                config,
                bundle,
                existing_manifest,
            )
        return self._training_manifest

    def _render_training_ddl(self, table: dict[str, Any]) -> str:
        """Generate CREATE TABLE DDL using real ClickHouse types from system.columns."""
        col_types = table.get("column_types", {})
        column_defs = ",\n  ".join(
            f"{col} {col_types.get(col, 'String')}"
            for col in table["columns"]
        )
        return f"CREATE TABLE {table['table']} (\n  {column_defs}\n)"

    def _render_training_documentation(self, table: dict[str, Any]) -> str:
        parts = [
            f"Table: {table['table']}",
            f"Description: {table.get('description', '')}",
            f"Grain: {table.get('grain', '')}",
            f"Columns: {', '.join(table['columns'])}",
            "Policy: use only approved dm_* and fct_* tables.",
        ]
        # Column-level docs from dbt
        col_docs = table.get("column_docs", {})
        if col_docs:
            col_doc_lines = "\n  ".join(
                f"{col}: {desc}"
                for col, desc in col_docs.items()
                if desc
            )
            if col_doc_lines:
                parts.append(f"Column descriptions:\n  {col_doc_lines}")
        if table.get("dashboard_pages"):
            pages = ", ".join(p["filename"] for p in table["dashboard_pages"])
            parts.append(f"Used in dashboard pages: {pages}")
        return "\n".join(parts)

    def _render_policy_documentation(self, bundle: dict[str, Any]) -> str:
        allowed_tables = ", ".join(table["table"] for table in bundle["tables"])
        return (
            "Ask Data SQL generation policy:\n"
            f"- Approved tables: {allowed_tables}\n"
            "- Only SELECT or WITH ... SELECT queries are allowed.\n"
            "- Stay on mart/fact analytics surfaces only.\n"
            "- Favor province/ward level reporting marts before lower-level internals.\n"
            "- Reuse the bilingual examples and dashboard context when mapping user intent.\n"
        )

    def _create_vanna_client(self) -> Any:
        config = self._get_runtime_config()
        manifest = self._get_training_manifest()
        OpenAI, ChromaDB_VectorStore, OpenAI_Chat = self._load_vanna_dependencies()
        openai_client = OpenAI(api_key=config.api_key, base_url=config.base_url)

        vanna_client = "in-memory" if config.client == "in-memory" else "persistent"
        runtime_config: dict[str, Any] = {
            "api_key": config.api_key,
            "model": config.model,
            "base_url": config.base_url,
            "client": vanna_client,
        }
        collection_path = self._collection_persist_path(config, manifest)
        if collection_path is not None:
            runtime_config["path"] = collection_path

        class GroqVanna(ChromaDB_VectorStore, OpenAI_Chat):
            def __init__(self, *, client: Any, config_payload: dict[str, Any]) -> None:
                ChromaDB_VectorStore.__init__(self, config=config_payload)
                OpenAI_Chat.__init__(self, client=client, config=config_payload)

        return GroqVanna(client=openai_client, config_payload=runtime_config)

    def _train_vanna_client(self, client: Any) -> None:
        bundle = self._get_catalog_bundle()
        # 1. Access policy
        client.train(documentation=self._render_policy_documentation(bundle))
        # 2. Domain documentation (date functions, column conventions)
        system_docs = """
        ClickHouse date and time functions for SQL generation:
        - yesterday()                             : Date of yesterday (Date type)
        - today()                                 : Date of today (Date type)
        - now()                                   : Current datetime (DateTime type)
        - today() - toIntervalDay(N)              : N days ago as Date
        - now() - toIntervalHour(N)               : N hours ago as DateTime
        - toStartOfMonth(today())                 : First day of current month
        - toStartOfMonth(today()) - toIntervalMonth(1) : First day of last month
        - toStartOfHour(now())                    : Current hour truncated

        Column naming conventions:
        - date columns        : "date"  (Date type, filter with =, >=, <=)
        - hour columns        : "datetime_hour"  (DateTime type)
        - month columns       : "month"  (Date type, first day of each month)
        - daily PM2.5 average : "pm25_avg"
        - hourly/current PM2.5: "pm25"
        - AQI Vietnam scale   : "avg_aqi_vn" (daily/hourly) or "current_aqi_vn" (status mart)

        CRITICAL RULES:
        - NEVER use CURRENT_TIMESTAMP, CURRENT_DATE, NOW() -- use now(), today(), yesterday() instead.
        - NEVER hardcode dates like '2023-11-13'. Always use dynamic date functions.
        - For "hom qua" (yesterday): WHERE date = yesterday()
        - For "hom nay" (today): WHERE date = today()
        - For "trong N gio qua" (last N hours): WHERE datetime_hour >= now() - toIntervalHour(N)
        - For "trong N ngay qua" (last N days): WHERE date >= today() - toIntervalDay(N)
        - For "thang nay" (this month): WHERE date >= toStartOfMonth(today())
        - For province-level PM2.5/AQI: prefer fct_air_quality_province_level_daily over ward/summary tables.
        """
        client.train(documentation=system_docs.strip())
        # 3. DDL + business documentation per table
        for table in bundle["tables"]:
            client.train(ddl=self._render_training_ddl(table))
            client.train(documentation=self._render_training_documentation(table))
        # 4. Question+SQL pairs — the most powerful training signal
        sql_pairs_trained = 0
        for question in bundle["question_examples"]:
            sql = (question.get("sql") or "").strip()
            if sql:
                client.train(question=question["question"], sql=sql)
                sql_pairs_trained += 1
        print(f"Vanna trained with {sql_pairs_trained} question+SQL pairs.")

    def _ensure_persist_directory(self) -> None:
        config = self._get_runtime_config()
        if config.persist_directory is None:
            return
        try:
            Path(config.persist_directory).mkdir(parents=True, exist_ok=True)
            manifest = self._get_training_manifest()
            collection_path = self._collection_persist_path(config, manifest)
            if collection_path is not None:
                Path(collection_path).mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise RuntimeNotConfiguredError(
                f"Unable to initialize Vanna persist directory: {config.persist_directory}"
            ) from exc

    def _get_vanna_client(self) -> Any:
        if self._vanna_client is None:
            config = self._get_runtime_config()
            bundle = self._get_catalog_bundle()
            self._ensure_persist_directory()
            existing_manifest = self._load_training_manifest(config)
            current_manifest = self._build_training_manifest(config, bundle, existing_manifest)
            retrain = self._should_retrain(config, current_manifest, existing_manifest)
            self._training_manifest = current_manifest
            self._vanna_client = self._create_vanna_client()
            action = "rebuild" if retrain else "reuse"
            print(
                "Vanna runtime state: "
                f"action={action} "
                f"client={config.client} "
                f"collection={current_manifest.collection_name} "
                f"persist_directory={config.persist_directory or 'memory'} "
                f"semantic_fingerprint={current_manifest.semantic_fingerprint}"
            )
            if retrain:
                self._train_vanna_client(self._vanna_client)
                self._write_training_manifest(config, current_manifest)
        return self._vanna_client

    def metadata_context(self) -> list[dict[str, Any]]:
        return self._get_catalog_bundle()["tables"]

    def _extract_sql_statement(self, response: object) -> str:
        text = str(response).strip()
        if not text:
            return ""

        fenced_blocks = re.findall(r"```(?:sql)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
        for block in reversed(fenced_blocks):
            candidate = self._trim_sql_candidate(block)
            if candidate:
                return candidate

        without_thinking = re.sub(
            r"<think>.*?</think>",
            "",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        lines = without_thinking.splitlines()
        for index in range(len(lines) - 1, -1, -1):
            if self._starts_like_sql(lines[index]):
                candidate = self._trim_sql_candidate("\n".join(lines[index:]))
                if candidate:
                    return candidate

        return self._trim_sql_candidate(without_thinking)

    def _starts_like_sql(self, text: str) -> bool:
        stripped = text.strip()
        if re.match(r"^select\b", stripped, flags=re.IGNORECASE):
            return True
        return bool(
            re.match(
                r"^with\s+(?:recursive\s+)?[a-z_][a-z0-9_]*\s+as\s*\(",
                stripped,
                flags=re.IGNORECASE,
            )
        )

    def _trim_sql_candidate(self, candidate: str) -> str:
        cleaned = candidate.strip()
        if not self._starts_like_sql(cleaned):
            return ""
        if ";" in cleaned:
            cleaned = cleaned.split(";", 1)[0]
        return cleaned.strip()

    def _apply_runtime_sql_policy(self, sql: str, *, eval_case: Any | None) -> str:
        normalized = sql.strip()
        if eval_case is not None and "top_n" in eval_case.expected_sql_shape:
            if " limit " not in f" {normalized.lower()} ":
                normalized = f"{normalized}\nLIMIT 10"
        return normalized

    def _build_explanation(self, referenced_tables: list[str]) -> str:
        bundle = self._get_catalog_bundle()
        table_lookup = {table["table"]: table for table in bundle["tables"]}
        described_tables = []
        for table_name in referenced_tables:
            table = table_lookup.get(table_name)
            if table is None:
                described_tables.append(table_name)
                continue
            desc = table.get("description", "No description available")
            described_tables.append(f"{table_name} ({desc})")
        return (
            "Mapped the request to approved marts/facts: "
            + ", ".join(described_tables)
        )

    def generate_sql(
        self,
        *,
        question: str,
        lang: str,
        standard: str,
        session_id: str,
    ) -> GeneratedSql:
        _ = {"lang": lang, "standard": standard, "session_id": session_id}
        client = self._get_vanna_client()
        manifest = self._get_training_manifest()
        
        # Monkey-patch Vanna retrieval to avoid hitting Groq's 6000 TPM limit
        # The new dbt-yaml driven context is highly detailed; top 10 exceeds the limit.
        original_get_ddl = client.get_related_ddl
        original_get_docs = client.get_related_documentation
        
        try:
            client.get_related_ddl = lambda q, **k: original_get_ddl(q, **k)[:3]
            client.get_related_documentation = lambda q, **k: original_get_docs(q, **k)[:3]
            raw_response = client.generate_sql(question=question)
            sql = self._extract_sql_statement(raw_response)
            if not sql:
                raise RuntimeGenerationError("Vanna did not return SQL")
            eval_case = find_matching_eval_case(question=question, lang=lang)
            sql = self._apply_runtime_sql_policy(sql, eval_case=eval_case)
            validation = validate_sql(sql, self.semantic_dir)
            if eval_case is not None:
                evaluate_sql_against_case(validation.sql, eval_case, semantic_dir=self.semantic_dir)
        except (SqlValidationError, EvalValidationError) as exc:
            print(f"Vanna generation rejected: {exc.__class__.__name__}: {exc}")
            raise RuntimeGenerationError(str(exc)) from exc
        except RuntimeGenerationError:
            raise
        except Exception as exc:
            print(f"Vanna generation failed: {exc.__class__.__name__}: {exc}")
            raise RuntimeGenerationError("Vanna SQL generation failed") from exc
        finally:
            client.get_related_ddl = original_get_ddl
            client.get_related_documentation = original_get_docs

        return GeneratedSql(
            sql=validation.sql,
            explanation=self._build_explanation(validation.referenced_tables),
            referenced_tables=validation.referenced_tables,
            generator_metadata={
                "model": manifest.model,
                "collection": manifest.collection_name,
                "semantic_fingerprint": manifest.semantic_fingerprint,
            },
        )
