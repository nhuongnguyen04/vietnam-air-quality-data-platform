#!/usr/bin/env python3
"""
Rebuild raw_tomtom_traffic with legacy backfill and Google Drive archive replay.

Workflow:
1. Create a rebuilt table matching scripts/init-clickhouse.sql.
2. Backfill legacy rows from the existing raw_tomtom_traffic for a fixed time window.
3. Scan Google Drive archived TomTom CSV files after the backfill cutoff.
4. Cut over via table rename and replay the delta from the backup table.

This script is intentionally self-contained because the regular gdrive_sync flow
only filters columns and cannot transform legacy TomTom rows into the new schema.
"""

import argparse
import csv
import io
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import clickhouse_connect
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from scipy.spatial import cKDTree


logger = logging.getLogger("rebuild_raw_tomtom_traffic")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_START = "2026-04-10 00:00:00"
DEFAULT_END = "2026-04-17 09:00:00"
DEFAULT_REBUILT_TABLE = "raw_tomtom_traffic_rebuilt"
DEFAULT_BACKUP_TABLE = "raw_tomtom_traffic_backup"
DEFAULT_ARCHIVE_DAY = "2026/04/17"
DEFAULT_ARCHIVE_START_FILE = "tomtom_traf_20260417_1008.csv"
TARGET_COLUMNS = [
    "source",
    "traffic_source",
    "ingest_time",
    "ingest_batch_id",
    "ward_code",
    "ward_name",
    "province_name",
    "latitude",
    "longitude",
    "nearest_highway_type",
    "distance_to_road_km",
    "timestamp_utc",
    "current_speed",
    "free_flow_speed",
    "current_travel_time",
    "free_flow_travel_time",
    "confidence",
    "road_closure",
    "raw_payload",
]
LEGACY_REQUIRED_COLUMNS = {
    "latitude",
    "longitude",
    "timestamp_utc",
    "current_speed",
    "free_flow_speed",
}


@dataclass
class ArchiveFile:
    file_id: str
    name: str
    rel_path: str
    modified_time: Optional[datetime]


class WardMapper:
    """Nearest-ward mapper backed by vietnam_wards_with_osm.csv."""

    def __init__(self, seed_path: Path):
        wards = pd.read_csv(seed_path)
        wards["ward_code"] = wards["code"].apply(self._format_ward_code)
        wards["ward_name"] = wards["ward"].astype(str)
        wards["province_name"] = wards["province"].astype(str)
        wards["nearest_highway_type"] = (
            wards["nearest_highway_type"].fillna("unknown").astype(str)
        )
        wards["distance_to_road_km"] = pd.to_numeric(
            wards["distance_to_road_km"], errors="coerce"
        ).fillna(0.0)
        wards["lat"] = pd.to_numeric(wards["lat"], errors="coerce")
        wards["lon"] = pd.to_numeric(wards["lon"], errors="coerce")

        valid = wards.dropna(subset=["lat", "lon"]).reset_index(drop=True)
        if valid.empty:
            raise ValueError(f"No valid coordinates found in {seed_path}")

        self.wards = valid
        self.tree = cKDTree(valid[["lat", "lon"]].to_numpy())

    @staticmethod
    def _format_ward_code(value: object) -> str:
        try:
            return str(int(float(value))).zfill(5)
        except (TypeError, ValueError):
            return ""

    def enrich(self, frame: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Map latitude/longitude to nearest ward metadata."""
        if frame.empty:
            return frame.copy(), frame.iloc[0:0].copy()

        enriched = frame.copy()
        lat = pd.to_numeric(enriched["latitude"], errors="coerce")
        lon = pd.to_numeric(enriched["longitude"], errors="coerce")
        valid_mask = lat.notna() & lon.notna()

        quarantine = enriched.loc[~valid_mask].copy()
        if quarantine.empty is False:
            quarantine["quarantine_reason"] = "missing_lat_lon"

        if valid_mask.any():
            coords = np.column_stack([lat[valid_mask].to_numpy(), lon[valid_mask].to_numpy()])
            _, indices = self.tree.query(coords, k=1)
            ward_rows = self.wards.iloc[np.atleast_1d(indices)].reset_index(drop=True)

            target_index = enriched.index[valid_mask]
            enriched.loc[target_index, "ward_code"] = ward_rows["ward_code"].to_numpy()
            enriched.loc[target_index, "ward_name"] = ward_rows["ward_name"].to_numpy()
            enriched.loc[target_index, "province_name"] = ward_rows["province_name"].to_numpy()
            enriched.loc[target_index, "nearest_highway_type"] = ward_rows[
                "nearest_highway_type"
            ].to_numpy()
            enriched.loc[target_index, "distance_to_road_km"] = ward_rows[
                "distance_to_road_km"
            ].to_numpy()

        mapped = enriched.loc[valid_mask].copy()
        mapped = mapped[mapped["ward_code"].astype(str) != ""].copy()
        return mapped, quarantine


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def parse_dt(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def dt_sql(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def normalize_dt_series(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    try:
        return parsed.dt.tz_convert(None)
    except TypeError:
        return parsed.dt.tz_localize(None)


def get_ch_client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "admin"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "admin"),
        database=os.environ.get("CLICKHOUSE_DB", "air_quality"),
    )


def get_drive_service():
    drive_root = os.environ.get("GDRIVE_ROOT_FOLDER_ID")
    client_id = os.environ.get("GDRIVE_CLIENT_ID")
    client_secret = os.environ.get("GDRIVE_CLIENT_SECRET")
    refresh_token = os.environ.get("GDRIVE_REFRESH_TOKEN")
    if not all([drive_root, client_id, client_secret, refresh_token]):
        raise ValueError(
            "Missing Google Drive OAuth variables: "
            "GDRIVE_ROOT_FOLDER_ID, GDRIVE_CLIENT_ID, GDRIVE_CLIENT_SECRET, GDRIVE_REFRESH_TOKEN"
        )

    creds = Credentials(
        None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    if not creds.valid:
        creds.refresh(Request())
    return build("drive", "v3", credentials=creds), drive_root


def find_folder(service, name: str, parent_id: str) -> Optional[str]:
    query = (
        f"name = '{name}' and '{parent_id}' in parents "
        "and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    )
    result = service.files().list(q=query, fields="files(id, name)").execute()
    files = result.get("files", [])
    return files[0]["id"] if files else None


def list_children(service, parent_id: str) -> List[dict]:
    query = f"'{parent_id}' in parents and trashed = false"
    response = service.files().list(
        q=query,
        fields="files(id, name, mimeType, modifiedTime)",
        pageSize=1000,
    ).execute()
    return response.get("files", [])


def archive_day_to_key(archive_day: str) -> Tuple[int, int, int]:
    parts = archive_day.strip("/").split("/")
    if len(parts) != 3:
        raise ValueError(f"archive_day must be YYYY/MM/DD, got: {archive_day}")
    return int(parts[0]), int(parts[1]), int(parts[2])


def list_archive_tomtom_files(
    service,
    drive_root_id: str,
    archive_day: str,
    start_file_name: str,
    onward: bool = False,
) -> List[ArchiveFile]:
    archived_id = find_folder(service, "archived", drive_root_id)
    if not archived_id:
        logger.warning("Archived folder not found in Google Drive")
        return []

    start_key = archive_day_to_key(archive_day)
    files: List[ArchiveFile] = []

    for year in list_children(service, archived_id):
        if year["mimeType"] != "application/vnd.google-apps.folder":
            continue
        year_num = int(year["name"])
        for month in list_children(service, year["id"]):
            if month["mimeType"] != "application/vnd.google-apps.folder":
                continue
            month_num = int(month["name"])
            for day in list_children(service, month["id"]):
                if day["mimeType"] != "application/vnd.google-apps.folder":
                    continue
                day_num = int(day["name"])
                current_key = (year_num, month_num, day_num)
                if onward:
                    if current_key < start_key:
                        continue
                elif current_key != start_key:
                    continue

                tomtom_id = find_folder(service, "tomtom", day["id"])
                if not tomtom_id:
                    continue

                current_archive_day = f"{year['name']}/{month['name']}/{day['name']}"
                for item in list_children(service, tomtom_id):
                    if item["mimeType"] == "application/vnd.google-apps.folder":
                        continue
                    if current_key == start_key and item["name"] < start_file_name:
                        continue
                    modified = item.get("modifiedTime")
                    files.append(
                        ArchiveFile(
                            file_id=item["id"],
                            name=item["name"],
                            rel_path=f"{current_archive_day}/tomtom/{item['name']}",
                            modified_time=pd.to_datetime(modified, utc=True).to_pydatetime()
                            if modified
                            else None,
                        )
                    )

    files.sort(key=lambda f: (f.modified_time or datetime.min.replace(tzinfo=timezone.utc), f.name))
    return files


def infer_file_timestamp(name: str) -> Optional[datetime]:
    stem = Path(name).stem
    parts = stem.split("_")
    if len(parts) < 4:
        return None
    candidate = f"{parts[-2]}_{parts[-1]}"
    try:
        return datetime.strptime(candidate, "%Y%m%d_%H%M").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def query_columns(client, database: str, table: str) -> List[str]:
    result = client.query(
        f"SELECT name FROM system.columns WHERE database = '{database}' AND table = '{table}' ORDER BY position"
    )
    return [row[0] for row in result.result_rows]


def table_exists(client, database: str, table: str) -> bool:
    result = client.query(
        f"SELECT count() FROM system.tables WHERE database = '{database}' AND name = '{table}'"
    )
    return bool(result.result_rows and result.result_rows[0][0] > 0)


def create_rebuilt_table(client, database: str, table: str) -> None:
    client.command(f"DROP TABLE IF EXISTS {database}.{table}")
    client.command(
        f"""
        CREATE TABLE {database}.{table}
        (
            source LowCardinality(String) DEFAULT 'tomtom',
            traffic_source LowCardinality(String),
            ingest_time DateTime DEFAULT now(),
            ingest_batch_id String,
            ward_code String,
            ward_name String,
            province_name String,
            latitude Float64,
            longitude Float64,
            nearest_highway_type LowCardinality(String),
            distance_to_road_km Float32,
            timestamp_utc DateTime,
            current_speed Float32,
            free_flow_speed Float32,
            current_travel_time Int32,
            free_flow_travel_time Int32,
            confidence Float32,
            road_closure UInt8,
            raw_payload String CODEC(ZSTD(1))
        )
        ENGINE = ReplacingMergeTree(ingest_time)
        PARTITION BY toYYYYMM(timestamp_utc)
        ORDER BY (ward_code, timestamp_utc, traffic_source)
        SETTINGS index_granularity = 8192
        """
    )


def fetch_table_slice(
    client,
    database: str,
    table: str,
    columns: Sequence[str],
    start_dt: datetime,
    end_dt: datetime,
) -> pd.DataFrame:
    select_cols = ", ".join(columns)
    query = f"""
        SELECT {select_cols}
        FROM {database}.{table}
        WHERE ingest_time >= toDateTime('{dt_sql(start_dt)}')
          AND ingest_time < toDateTime('{dt_sql(end_dt)}')
        ORDER BY ingest_time
    """
    result = client.query(query)
    if not result.result_rows:
        return pd.DataFrame(columns=list(columns))
    return pd.DataFrame(result.result_rows, columns=result.column_names)


def fetch_table_after(
    client,
    database: str,
    table: str,
    columns: Sequence[str],
    start_dt: datetime,
) -> pd.DataFrame:
    select_cols = ", ".join(columns)
    query = f"""
        SELECT {select_cols}
        FROM {database}.{table}
        WHERE ingest_time >= toDateTime('{dt_sql(start_dt)}')
        ORDER BY ingest_time
    """
    result = client.query(query)
    if not result.result_rows:
        return pd.DataFrame(columns=list(columns))
    return pd.DataFrame(result.result_rows, columns=result.column_names)


def dedupe_records(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    working = frame.copy()
    working["ingest_time"] = normalize_dt_series(working["ingest_time"])
    working["timestamp_utc"] = normalize_dt_series(working["timestamp_utc"])
    working = working.sort_values("ingest_time")
    working = working.drop_duplicates(
        subset=["ward_code", "timestamp_utc", "traffic_source"],
        keep="last",
    )
    return working[TARGET_COLUMNS]


def transform_legacy_rows(
    frame: pd.DataFrame,
    mapper: WardMapper,
    batch_id_prefix: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if frame.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS), frame.iloc[0:0].copy()

    missing = LEGACY_REQUIRED_COLUMNS - set(frame.columns)
    if missing:
        raise ValueError(f"Legacy data is missing required columns: {sorted(missing)}")

    transformed = pd.DataFrame()
    transformed["source"] = frame.get("source", pd.Series(["tomtom"] * len(frame))).fillna("tomtom")
    transformed["traffic_source"] = "legacy_backfill"
    transformed["ingest_time"] = normalize_dt_series(
        frame.get("ingest_time", pd.Series([datetime.now(timezone.utc)] * len(frame)))
    )
    fallback_batch_id = (
        f"{batch_id_prefix}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    )
    batch_series = frame.get("ingest_batch_id", pd.Series([fallback_batch_id] * len(frame)))
    transformed["ingest_batch_id"] = batch_series.fillna(fallback_batch_id).astype(str)
    transformed["latitude"] = pd.to_numeric(frame["latitude"], errors="coerce")
    transformed["longitude"] = pd.to_numeric(frame["longitude"], errors="coerce")
    transformed["timestamp_utc"] = normalize_dt_series(frame["timestamp_utc"])
    transformed["current_speed"] = pd.to_numeric(frame["current_speed"], errors="coerce").fillna(0.0)
    transformed["free_flow_speed"] = pd.to_numeric(frame["free_flow_speed"], errors="coerce").fillna(0.0)
    transformed["current_travel_time"] = 0
    transformed["free_flow_travel_time"] = 0
    transformed["confidence"] = pd.to_numeric(frame.get("confidence"), errors="coerce").fillna(0.0)
    transformed["road_closure"] = 0
    transformed["raw_payload"] = frame.get("raw_payload", pd.Series([""] * len(frame))).fillna("").astype(str)

    enriched, quarantine = mapper.enrich(transformed)
    if not quarantine.empty:
        quarantine["source_stage"] = "legacy_transform"

    enriched["nearest_highway_type"] = enriched["nearest_highway_type"].fillna("unknown").astype(str)
    enriched["distance_to_road_km"] = pd.to_numeric(
        enriched["distance_to_road_km"], errors="coerce"
    ).fillna(0.0)
    for text_col in ["ward_code", "ward_name", "province_name"]:
        enriched[text_col] = enriched[text_col].fillna("").astype(str)
    return dedupe_records(enriched), quarantine


def normalize_current_rows(
    frame: pd.DataFrame,
    mapper: WardMapper,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if frame.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS), frame.iloc[0:0].copy()

    normalized = frame.copy()
    for column in TARGET_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = None

    normalized["source"] = normalized["source"].fillna("tomtom").astype(str)
    normalized["traffic_source"] = normalized["traffic_source"].fillna("api").astype(str)
    normalized["latitude"] = pd.to_numeric(normalized["latitude"], errors="coerce")
    normalized["longitude"] = pd.to_numeric(normalized["longitude"], errors="coerce")
    normalized["current_speed"] = pd.to_numeric(normalized["current_speed"], errors="coerce").fillna(0.0)
    normalized["free_flow_speed"] = pd.to_numeric(
        normalized["free_flow_speed"], errors="coerce"
    ).fillna(0.0)
    normalized["current_travel_time"] = pd.to_numeric(
        normalized["current_travel_time"], errors="coerce"
    ).fillna(0)
    normalized["free_flow_travel_time"] = pd.to_numeric(
        normalized["free_flow_travel_time"], errors="coerce"
    ).fillna(0)
    normalized["confidence"] = pd.to_numeric(normalized["confidence"], errors="coerce").fillna(0.0)
    normalized["road_closure"] = pd.to_numeric(normalized["road_closure"], errors="coerce").fillna(0).astype(int)
    normalized["ingest_time"] = normalize_dt_series(normalized["ingest_time"])
    normalized["timestamp_utc"] = normalize_dt_series(normalized["timestamp_utc"])
    normalized["raw_payload"] = normalized["raw_payload"].fillna("").astype(str)
    normalized["ingest_batch_id"] = normalized["ingest_batch_id"].fillna("").astype(str)

    missing_admin = (
        normalized["ward_code"].isna()
        | normalized["ward_name"].isna()
        | normalized["province_name"].isna()
        | normalized["nearest_highway_type"].isna()
        | normalized["distance_to_road_km"].isna()
    )
    enriched, quarantine = mapper.enrich(normalized.loc[missing_admin].copy())
    if not quarantine.empty:
        quarantine["source_stage"] = "current_enrichment"

    if not enriched.empty:
        normalized.loc[enriched.index, ["ward_code", "ward_name", "province_name"]] = enriched[
            ["ward_code", "ward_name", "province_name"]
        ]
        normalized.loc[enriched.index, "nearest_highway_type"] = enriched["nearest_highway_type"]
        normalized.loc[enriched.index, "distance_to_road_km"] = enriched["distance_to_road_km"]

    normalized["ward_code"] = normalized["ward_code"].fillna("").astype(str)
    normalized["ward_name"] = normalized["ward_name"].fillna("").astype(str)
    normalized["province_name"] = normalized["province_name"].fillna("").astype(str)
    normalized["nearest_highway_type"] = normalized["nearest_highway_type"].fillna("unknown").astype(str)
    normalized["distance_to_road_km"] = pd.to_numeric(
        normalized["distance_to_road_km"], errors="coerce"
    ).fillna(0.0)

    result = normalized[normalized["ward_code"] != ""].copy()
    return dedupe_records(result), quarantine


def insert_frame(client, database: str, table: str, frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    client.insert_df(f"{database}.{table}", frame[TARGET_COLUMNS])
    return len(frame)


def download_csv(service, file_id: str) -> pd.DataFrame:
    content = service.files().get_media(fileId=file_id).execute()
    handle = io.StringIO(content.decode("utf-8"))
    reader = csv.DictReader(handle)
    return pd.DataFrame(list(reader))


def filter_archive_frame(frame: pd.DataFrame, threshold: datetime) -> pd.DataFrame:
    if frame.empty:
        return frame
    working = frame.copy()
    candidates = []
    for column in ["ingest_time", "timestamp_utc"]:
        if column in working.columns:
            parsed = pd.to_datetime(working[column], errors="coerce", utc=True)
            candidates.append(parsed)
    if not candidates:
        return working
    effective = candidates[0]
    for series in candidates[1:]:
        effective = effective.fillna(series)
    return working.loc[effective >= threshold].copy()


def is_legacy_shape(columns: Iterable[str]) -> bool:
    column_set = set(columns)
    return "ward_code" not in column_set or "traffic_source" not in column_set


def process_archive_files(
    client,
    service,
    drive_root_id: str,
    database: str,
    rebuilt_table: str,
    mapper: WardMapper,
    start_threshold: datetime,
    quarantine_dir: Path,
    archive_day: str,
    start_file_name: str,
    archive_day_onward: bool = False,
) -> Tuple[int, List[Path]]:
    inserted = 0
    reports: List[Path] = []

    files = list_archive_tomtom_files(
        service=service,
        drive_root_id=drive_root_id,
        archive_day=archive_day,
        start_file_name=start_file_name,
        onward=archive_day_onward,
    )
    logger.info(
        "Found %s archived TomTom files under archived/%s%s starting from %s",
        len(files),
        archive_day,
        "/..." if archive_day_onward else "/tomtom",
        start_file_name,
    )
    for archive_file in files:
        file_ts = infer_file_timestamp(archive_file.name)
        if file_ts and file_ts < start_threshold:
            continue

        frame = download_csv(service, archive_file.file_id)
        frame = filter_archive_frame(frame, start_threshold)
        if frame.empty:
            continue

        if is_legacy_shape(frame.columns):
            transformed, quarantine = transform_legacy_rows(frame, mapper, "archive_legacy")
        else:
            transformed, quarantine = normalize_current_rows(frame, mapper)

        inserted += insert_frame(client, database, rebuilt_table, transformed)
        if not quarantine.empty:
            report_path = quarantine_dir / f"quarantine_{archive_file.name}"
            quarantine.to_csv(report_path, index=False)
            reports.append(report_path)
            logger.warning(
                "Quarantined %s rows from %s into %s",
                len(quarantine),
                archive_file.rel_path,
                report_path,
            )
    return inserted, reports


def verify_row_counts(client, database: str, table: str, start_dt: datetime, end_dt: datetime) -> List[Tuple]:
    result = client.query(
        f"""
        SELECT toDate(ingest_time) AS ingest_date, count()
        FROM {database}.{table}
        WHERE ingest_time >= toDateTime('{dt_sql(start_dt)}')
          AND ingest_time < toDateTime('{dt_sql(end_dt)}')
        GROUP BY ingest_date
        ORDER BY ingest_date
        """
    )
    return result.result_rows


def replay_delta(
    client,
    database: str,
    backup_table: str,
    target_table: str,
    mapper: WardMapper,
    watermark: datetime,
    quarantine_dir: Path,
) -> Tuple[int, Optional[Path]]:
    backup_columns = query_columns(client, database, backup_table)
    delta = fetch_table_after(client, database, backup_table, backup_columns, watermark)
    if delta.empty:
        return 0, None

    if is_legacy_shape(delta.columns):
        transformed, quarantine = transform_legacy_rows(delta, mapper, "delta_replay")
    else:
        transformed, quarantine = normalize_current_rows(delta, mapper)

    inserted = insert_frame(client, database, target_table, transformed)
    report_path = None
    if not quarantine.empty:
        report_path = quarantine_dir / "quarantine_cutover_delta.csv"
        quarantine.to_csv(report_path, index=False)
    return inserted, report_path


def perform_cutover(
    client,
    database: str,
    source_table: str,
    rebuilt_table: str,
    backup_table: str,
    mapper: WardMapper,
    quarantine_dir: Path,
    drop_backup: bool,
) -> None:
    watermark = datetime.now(timezone.utc)
    logger.info("Cutover watermark: %s", dt_sql(watermark))

    if table_exists(client, database, backup_table):
        client.command(f"DROP TABLE {database}.{backup_table}")

    client.command(
        f"RENAME TABLE {database}.{source_table} TO {database}.{backup_table}, "
        f"{database}.{rebuilt_table} TO {database}.{source_table}"
    )
    logger.info("Renamed %s -> %s and %s -> %s", source_table, backup_table, rebuilt_table, source_table)

    replayed, quarantine_report = replay_delta(
        client=client,
        database=database,
        backup_table=backup_table,
        target_table=source_table,
        mapper=mapper,
        watermark=watermark,
        quarantine_dir=quarantine_dir,
    )
    logger.info("Replayed %s delta rows after cutover", replayed)
    if quarantine_report:
        logger.warning("Delta replay quarantine written to %s", quarantine_report)

    if drop_backup:
        client.command(f"DROP TABLE {database}.{backup_table}")
        logger.info("Dropped backup table %s", backup_table)
    else:
        logger.info("Backup table retained as %s for manual verification", backup_table)


def load_env() -> None:
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(env_path)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Rebuild raw_tomtom_traffic")
    parser.add_argument("--start", default=DEFAULT_START, help="Backfill start in UTC: YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--end", default=DEFAULT_END, help="Backfill end in UTC: YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--source-table", default="raw_tomtom_traffic")
    parser.add_argument("--rebuilt-table", default=DEFAULT_REBUILT_TABLE)
    parser.add_argument("--backup-table", default=DEFAULT_BACKUP_TABLE)
    parser.add_argument("--database", default=os.environ.get("CLICKHOUSE_DB", "air_quality"))
    parser.add_argument("--seed-path", default=str(PROJECT_ROOT / "dbt/dbt_tranform/seeds/vietnam_wards_with_osm.csv"))
    parser.add_argument("--chunk-hours", type=int, default=6)
    parser.add_argument("--archive-day", default=DEFAULT_ARCHIVE_DAY)
    parser.add_argument("--archive-start-file", default=DEFAULT_ARCHIVE_START_FILE)
    parser.add_argument("--archive-day-onward", action="store_true")
    parser.add_argument("--archive-only", action="store_true")
    parser.add_argument("--skip-archive", action="store_true")
    parser.add_argument("--skip-cutover", action="store_true")
    parser.add_argument("--drop-backup", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    return parser


def main() -> int:
    load_env()
    parser = build_arg_parser()
    args = parser.parse_args()
    setup_logging(args.log_level)

    start_dt = parse_dt(args.start)
    end_dt = parse_dt(args.end)
    if end_dt <= start_dt:
        raise ValueError("--end must be after --start")

    seed_path = Path(args.seed_path)
    if not seed_path.exists():
        raise FileNotFoundError(f"Seed path not found: {seed_path}")

    quarantine_dir = PROJECT_ROOT / "tmp" / f"tomtom_rebuild_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    mapper = WardMapper(seed_path)
    client = get_ch_client()
    quarantine_reports: List[Path] = []
    target_table_for_archive = args.rebuilt_table

    if not args.archive_only:
        logger.info("Creating rebuilt table %s.%s", args.database, args.rebuilt_table)
        create_rebuilt_table(client, args.database, args.rebuilt_table)

        source_columns = query_columns(client, args.database, args.source_table)
        logger.info("Source table columns: %s", ", ".join(source_columns))

        total_backfilled = 0
        current_start = start_dt
        while current_start < end_dt:
            current_end = min(current_start + timedelta(hours=args.chunk_hours), end_dt)
            logger.info(
                "Backfilling source slice %s -> %s",
                dt_sql(current_start),
                dt_sql(current_end),
            )
            source_frame = fetch_table_slice(
                client=client,
                database=args.database,
                table=args.source_table,
                columns=source_columns,
                start_dt=current_start,
                end_dt=current_end,
            )
            if is_legacy_shape(source_frame.columns):
                transformed, quarantine = transform_legacy_rows(source_frame, mapper, "legacy_backfill")
            else:
                transformed, quarantine = normalize_current_rows(source_frame, mapper)

            total_backfilled += insert_frame(client, args.database, args.rebuilt_table, transformed)
            if not quarantine.empty:
                report_path = quarantine_dir / f"quarantine_backfill_{dt_sql(current_start).replace(':', '-')}.csv"
                quarantine.to_csv(report_path, index=False)
                quarantine_reports.append(report_path)
            current_start = current_end

        logger.info("Backfilled %s rows into %s", total_backfilled, args.rebuilt_table)

        source_counts = verify_row_counts(client, args.database, args.source_table, start_dt, end_dt)
        rebuilt_counts = verify_row_counts(client, args.database, args.rebuilt_table, start_dt, end_dt)
        logger.info("Source daily counts: %s", source_counts)
        logger.info("Rebuilt daily counts: %s", rebuilt_counts)
    else:
        target_table_for_archive = args.source_table
        logger.info(
            "Archive-only mode: insert archived TomTom files directly into %s.%s",
            args.database,
            target_table_for_archive,
        )

    if not args.skip_archive:
        service, drive_root = get_drive_service()
        archive_inserted, archive_reports = process_archive_files(
            client=client,
            service=service,
            drive_root_id=drive_root,
            database=args.database,
            rebuilt_table=target_table_for_archive,
            mapper=mapper,
            start_threshold=end_dt,
            quarantine_dir=quarantine_dir,
            archive_day=args.archive_day,
            start_file_name=args.archive_start_file,
            archive_day_onward=args.archive_day_onward,
        )
        quarantine_reports.extend(archive_reports)
        logger.info("Inserted %s rows from archived Google Drive files", archive_inserted)

    if not args.archive_only and not args.skip_cutover:
        perform_cutover(
            client=client,
            database=args.database,
            source_table=args.source_table,
            rebuilt_table=args.rebuilt_table,
            backup_table=args.backup_table,
            mapper=mapper,
            quarantine_dir=quarantine_dir,
            drop_backup=args.drop_backup,
        )

    if quarantine_reports:
        logger.warning("Quarantine reports written to %s", quarantine_dir)
    else:
        logger.info("No quarantined rows detected")
    return 0


if __name__ == "__main__":
    sys.exit(main())
