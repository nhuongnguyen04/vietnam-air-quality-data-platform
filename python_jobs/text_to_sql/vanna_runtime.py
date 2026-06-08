"""Single integration boundary for Vanna-backed SQL generation without execution."""
from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

try:
    from python_jobs.text_to_sql.catalog_builder import build_vanna_catalog_bundle
    from python_jobs.text_to_sql.eval_runner import (
        EvalValidationError,
        evaluate_sql_against_case,
        find_matching_eval_case,
    )
    from python_jobs.text_to_sql.sql_validator import SqlValidationError, validate_sql
    from python_jobs.text_to_sql.sql_extractor import extract_sql_statement
    from python_jobs.text_to_sql.training import train_vanna_client
except ModuleNotFoundError:  # pragma: no cover - container import fallback
    from catalog_builder import build_vanna_catalog_bundle  # type: ignore
    from eval_runner import EvalValidationError, evaluate_sql_against_case, find_matching_eval_case  # type: ignore
    from sql_validator import SqlValidationError, validate_sql  # type: ignore
    from sql_extractor import extract_sql_statement  # type: ignore
    from training import train_vanna_client  # type: ignore


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
        self._client_lock = threading.Lock()

    def _resolve_vanna_config(self) -> VannaRuntimeConfig:
        ckey_key = os.environ.get("CKEY_API_KEY")
        groq_api_key = os.environ.get("GROQ_API_KEY")
        
        if ckey_key:
            api_key = ckey_key
            model = os.environ.get("CKEY_MODEL_TEXT_TO_SQL", "claude-haiku-4.5")
            base_url = os.environ.get("CKEY_BASE_URL", "https://ckey.vn/v1")
        elif groq_api_key:
            api_key = groq_api_key
            model = os.environ.get("GROQ_MODEL", "qwen/qwen3-32b")
            base_url = "https://api.groq.com/openai/v1"
        else:
            raise RuntimeNotConfiguredError(
                "Vui lòng thiết lập CKEY_API_KEY (ưu tiên) hoặc GROQ_API_KEY."
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
            api_key=api_key,
            model=model,
            base_url=base_url,
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
        if self._vanna_client is not None:
            return self._vanna_client

        with self._client_lock:
            if self._vanna_client is not None:
                return self._vanna_client

            config = self._get_runtime_config()
            bundle = self._get_catalog_bundle()
            self._ensure_persist_directory()
            existing_manifest = self._load_training_manifest(config)
            current_manifest = self._build_training_manifest(config, bundle, existing_manifest)
            retrain = self._should_retrain(config, current_manifest, existing_manifest)
            self._training_manifest = current_manifest
            vanna_client = self._create_vanna_client()
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
                train_vanna_client(vanna_client, bundle)
                self._write_training_manifest(config, current_manifest)

            self._vanna_client = vanna_client
        return self._vanna_client

    def metadata_context(self) -> list[dict[str, Any]]:
        return self._get_catalog_bundle()["tables"]



    def _apply_runtime_sql_policy(self, sql: str, *, eval_case: Any | None) -> str:
        normalized = sql.strip()
        if eval_case is not None and "top_n" in eval_case.expected_sql_shape:
            if not re.search(r"\blimit\b", normalized, re.IGNORECASE):
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

        try:
            raw_response = client.generate_sql(question=question)
            sql = extract_sql_statement(raw_response)
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
