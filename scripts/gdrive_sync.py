import os
import json
import logging
import csv
import io
import concurrent.futures
from datetime import datetime
from threading import local
from typing import List, Dict, Any, Optional
import clickhouse_connect
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

# Configuration
DRIVE_ROOT_ID = os.environ.get("GDRIVE_ROOT_FOLDER_ID")
CLIENT_ID = os.environ.get("GDRIVE_CLIENT_ID")
CLIENT_SECRET = os.environ.get("GDRIVE_CLIENT_SECRET")
REFRESH_TOKEN = os.environ.get("GDRIVE_REFRESH_TOKEN")
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CH_USER = os.environ.get("CLICKHOUSE_USER", "admin")
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "admin")
CH_DB = os.environ.get("CLICKHOUSE_DB", "air_quality")
MAX_WORKERS = int(os.environ.get("MAX_SYNC_WORKERS", "5"))

SCOPES = ['https://www.googleapis.com/auth/drive']

# Table Mapping
TABLE_MAPPING = {
    "aqiin_raw": "raw_aqiin_measurements",
    "ow_meas": "raw_openweather_measurements",
    "ow_weat": "raw_openweather_meteorology",
    "tomtom_traf": "raw_tomtom_traffic"
}

# Thread-local storage for GDrive and ClickHouse clients
thread_local = local()

def get_drive_service():
    if not hasattr(thread_local, "drive_service"):
        if not all([CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN]):
            raise ValueError("Missing OAuth credentials: GDRIVE_CLIENT_ID, GDRIVE_CLIENT_SECRET, or GDRIVE_REFRESH_TOKEN")
        
        creds = Credentials(
            None,
            refresh_token=REFRESH_TOKEN,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            scopes=SCOPES
        )
        
        # Refresh token if needed
        if not creds.valid:
            creds.refresh(Request())
            
        from googleapiclient.discovery import build
        thread_local.drive_service = build('drive', 'v3', credentials=creds)
    return thread_local.drive_service

def get_ch_client():
    if not hasattr(thread_local, "ch_client"):
        thread_local.ch_client = clickhouse_connect.get_client(
            host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS, database=CH_DB
        )
    return thread_local.ch_client

def find_folder(service, name, parent_id):
    query = f"name = '{name}' and '{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None

def find_or_create_folder(service, name, parent_id):
    fid = find_folder(service, name, parent_id)
    if fid: return fid
    
    meta = {'name': name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
    folder = service.files().create(body=meta, fields='id').execute()
    return folder.get('id')

def get_archive_hierarchy(service, root_id):
    """Pre-calculate and create archive folder mapping to avoid race conditions."""
    now = datetime.now()
    archived_id = find_or_create_folder(service, "archived", root_id)
    year_id = find_or_create_folder(service, str(now.year), archived_id)
    month_id = find_or_create_folder(service, f"{now.month:02d}", year_id)
    day_id = find_or_create_folder(service, f"{now.day:02d}", month_id)
    
    # Pre-create source subfolders
    source_mapping = {}
    for source in set(k.split('_')[0] for k in TABLE_MAPPING.keys()):
        source_mapping[source] = find_or_create_folder(service, source, day_id)
        
    return day_id, source_mapping

def process_file_task(file_info, archive_source_mapping):
    """Worker task to process a single file."""
    service = get_drive_service()
    ch_client = get_ch_client()
    
    file_id = file_info['id']
    filename = file_info['name']
    
    try:
        # 1. Download
        request = service.files().get_media(fileId=file_id)
        content = request.execute()
        
        # 2. Parse and Insert
        prefix = "_".join(filename.split('_')[:2])
        table = TABLE_MAPPING.get(prefix)
        
        if not table:
            logger.warning(f"No table mapping for file: {filename}")
            return False

        f = io.StringIO(content.decode('utf-8'))
        reader = csv.DictReader(f)
        records = list(reader)
        
        if records:
            ch_client.insert(table, [list(r.values()) for r in records], column_names=list(records[0].keys()))
            logger.info(f"Inserted {len(records)} rows from {filename}")
        
        # 3. Archive
        source_name = filename.split('_')[0]
        target_folder_id = archive_source_mapping.get(source_name)
        
        if not target_folder_id:
            # Fallback for unexpected sources
            logger.warning(f"Target folder not cached for source: {source_name}")
            return False

        # Move file (re-parent)
        file_meta = service.files().get(fileId=file_id, fields='parents').execute()
        previous_parents = ",".join(file_meta.get('parents'))
        
        service.files().update(
            fileId=file_id,
            addParents=target_folder_id,
            removeParents=previous_parents,
            fields='id, parents'
        ).execute()
        
        return True
    except Exception as e:
        logger.error(f"Error processing {filename}: {e}", exc_info=True)
        return False

def main():
    if not DRIVE_ROOT_ID:
        logger.error("GDRIVE_ROOT_FOLDER_ID not set")
        return
    
    # Initial setup in main thread to get global IDs
    service = get_drive_service()
    landing_zone_id = find_folder(service, "landing_zone", DRIVE_ROOT_ID)
    if not landing_zone_id:
        logger.info("Landing zone not found")
        return

    archive_day_id, source_mapping = get_archive_hierarchy(service, DRIVE_ROOT_ID)
    
    # 1. Discover all files across subfolders
    subfolders_resp = service.files().list(
        q=f"'{landing_zone_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false",
        fields="files(id, name)"
    ).execute()
    
    all_files = []
    for folder in subfolders_resp.get('files', []):
        query = f"'{folder['id']}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed = false"
        files_resp = service.files().list(q=query, fields="files(id, name)").execute()
        all_files.extend(files_resp.get('files', []))
    
    if not all_files:
        logger.info("No files found in landing zone")
        return

    logger.info(f"Starting parallel sync for {len(all_files)} files using {MAX_WORKERS} workers...")
    
    # 2. Parallel Processing
    success_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        futures = {executor.submit(process_file_task, f, source_mapping): f for f in all_files}
        
        for future in concurrent.futures.as_completed(futures):
            f_info = futures[future]
            try:
                if future.result():
                    success_count += 1
            except Exception as e:
                logger.error(f"Worker crashed for {f_info['name']}: {e}")

    logger.info(f"Parallel sync complete. Successfully processed: {success_count}/{len(all_files)}")

if __name__ == "__main__":
    main()
