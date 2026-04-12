import os
import json
import logging
import csv
import io
import sys
import concurrent.futures
from datetime import datetime
from threading import local
from typing import List, Dict, Any, Optional
import clickhouse_connect
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

# Increase CSV field size limit for large JSON payloads
csv.field_size_limit(sys.maxsize)

# Initialize Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("gdrive_sync")

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

# Table Mapping (Folder path relative to landing_zone -> Table name)
PATH_TO_TABLE = {
    "aqi_in": "raw_aqiin_measurements",
    "openweather/measurements": "raw_openweather_measurements",
    "openweather/weather": "raw_openweather_meteorology",
    "tomtom/traffic": "raw_tomtom_traffic"
}

# Legacy Prefix Mapping (Fallback)
TABLE_MAPPING = {
    "aqiin_raw": "raw_aqiin_measurements",
    "aqiin_meas": "raw_aqiin_measurements",
    "ow_meas": "raw_openweather_measurements",
    "ow_weat": "raw_openweather_meteorology",
    "tomtom_traf": "raw_tomtom_traffic"
}

# Thread-local storage for GDrive and ClickHouse clients
thread_local = local()

def format_value(val: Any) -> str:
    """Formats a value for a SQL INSERT statement."""
    if val is None or val == '':
        return 'NULL'
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, bool):
        return '1' if val else '0'
    # Escape single quotes and wrap in quotes
    safe_val = str(val).replace("'", "''")
    return f"'{safe_val}'"

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
    
    # Pre-create source subfolders based on PATH_TO_TABLE and TABLE_MAPPING
    source_mapping = {}
    sources = set([k.split('/')[0] for k in PATH_TO_TABLE.keys()] + [k.split('_')[0] for k in TABLE_MAPPING.keys()])
    for source in sources:
        source_mapping[source] = find_or_create_folder(service, source, day_id)
        
    return day_id, source_mapping

def list_files_recursive(service, folder_id, current_rel_path=""):
    """Recursively list all files in a folder and its subfolders."""
    results = []
    
    # List both files and folders
    query = f"'{folder_id}' in parents and trashed = false"
    response = service.files().list(
        q=query, 
        fields="files(id, name, mimeType)"
    ).execute()
    
    items = response.get('files', [])
    for item in items:
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            # Recursive call for subfolder
            sub_path = f"{current_rel_path}/{item['name']}" if current_rel_path else item['name']
            results.extend(list_files_recursive(service, item['id'], sub_path))
        else:
            # It's a file
            item['rel_path'] = current_rel_path
            results.append(item)
            
    return results

def process_file_task(file_info, archive_source_mapping):
    """Worker task to process a single file."""
    service = get_drive_service()
    ch_client = get_ch_client()
    
    file_id = file_info['id']
    filename = file_info['name']
    rel_path = file_info.get('rel_path', '')
    
    try:
        # 1. Determine Target Table
        table = PATH_TO_TABLE.get(rel_path)
        
        if not table:
            # Fallback to prefix-based mapping
            prefix = "_".join(filename.split('_')[:2])
            table = TABLE_MAPPING.get(prefix)
            
        if not table:
            logger.warning(f"No table mapping for file: {filename} at path: {rel_path}")
            return False

        # 2. Download
        request = service.files().get_media(fileId=file_id)
        content = request.execute()
        
        # 3. Parse and Insert
        f = io.StringIO(content.decode('utf-8'))
        reader = csv.DictReader(f)
        records = list(reader)
        
        if records:
            # Get target table columns to filter out extra fields
            try:
                table_cols = ch_client.query(f"SELECT name FROM system.columns WHERE database = '{CH_DB}' AND table = '{table}'").result_rows
                valid_cols = {row[0] for row in table_cols}
                logger.debug(f"Valid columns for {table}: {valid_cols}")
            except Exception as e:
                logger.warning(f"Could not fetch columns for {table}: {e}. Falling back to original columns.")
                valid_cols = set(records[0].keys())

            # Build SQL INSERT statement (allows ClickHouse to handle type conversion)
            # Filter columns to only include those that exist in the target table
            all_cols = list(records[0].keys())
            final_cols = [c for c in all_cols if c in valid_cols]
            
            if not final_cols:
                logger.error(f"No valid columns found for table {table} in file {filename}")
                return
                
            columns_str = ", ".join(final_cols)
            
            values_list = []
            for record in records:
                row_vals = [format_value(record.get(col)) for col in final_cols]
                values_list.append(f"({', '.join(row_vals)})")
            
            sql = f"INSERT INTO {table} ({columns_str}) VALUES {', '.join(values_list)}"
            
            try:
                ch_client.command(sql)
                logger.info(f"Inserted {len(records)} rows from {filename} into {table}")
            except Exception as e:
                logger.error(f"Failed to insert into {table} from {filename}: {e}")
                # Log a snippet of the SQL for debugging (careful with sensitive data)
                logger.debug(f"SQL Snippet: {sql[:200]}...")
                raise
        
        # 4. Archive
        # Source name is the first part of the relative path or filename
        source_name = rel_path.split('/')[0] if rel_path else filename.split('_')[0]
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
    
    # 1. Discover all files recursively
    logger.info(f"Scanning for files in landing_zone (ID: {landing_zone_id})...")
    all_files = list_files_recursive(service, landing_zone_id)
    
    if not all_files:
        logger.info("No files found in landing zone")
        return

    logger.info(f"Found {len(all_files)} files. Starting parallel sync using {MAX_WORKERS} workers...")
    
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