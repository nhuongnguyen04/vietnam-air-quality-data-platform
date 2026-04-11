#!/usr/bin/env python3
import os
import json
import logging
from pathlib import Path
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Configuration
DRIVE_ROOT_ID = os.environ.get("GDRIVE_ROOT_FOLDER_ID")
CLIENT_ID = os.environ.get("GDRIVE_CLIENT_ID")
CLIENT_SECRET = os.environ.get("GDRIVE_CLIENT_SECRET")
REFRESH_TOKEN = os.environ.get("GDRIVE_REFRESH_TOKEN")
LOCAL_LANDING_ZONE = os.environ.get("CSV_OUTPUT_DIR", "landing_zone")

SCOPES = ['https://www.googleapis.com/auth/drive']

def get_drive_service():
    """Authenticate and return the Drive service using Refresh Token."""
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
        
    return build('drive', 'v3', credentials=creds)

def find_or_create_folder(service, name, parent_id):
    """Find a folder by name or create it if not exists."""
    query = f"name = '{name}' and '{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    
    if files:
        return files[0]['id']
    
    # Create folder
    file_metadata = {
        'name': name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_id]
    }
    folder = service.files().create(body=file_metadata, fields='id').execute()
    logger.info(f"Created folder: {name} under {parent_id}")
    return folder.get('id')

def get_target_folder_id(service, filename):
    """Determine GDrive folder ID based on filename convention."""
    # Convention: [source]_[type]_[timestamp].csv
    # e.g., aqiin_raw_..., ow_meas_..., ow_weat_..., tomtom_traf_...
    
    # 1. Get/Create landing_zone folder
    landing_zone_id = find_or_create_folder(service, "landing_zone", DRIVE_ROOT_ID)
    
    parts = filename.split('_')
    source_prefix = parts[0].lower()
    type_prefix = parts[1].lower()
    
    if source_prefix == "aqiin":
        return find_or_create_folder(service, "aqi_in", landing_zone_id)
    elif source_prefix == "ow":
        ow_id = find_or_create_folder(service, "openweather", landing_zone_id)
        if type_prefix == "meas":
            return find_or_create_folder(service, "measurements", ow_id)
        else:
            return find_or_create_folder(service, "weather", ow_id)
    elif source_prefix == "tomtom":
        tomtom_id = find_or_create_folder(service, "tomtom", landing_zone_id)
        return find_or_create_folder(service, "traffic", tomtom_id)
    
    return landing_zone_id

def upload_file(service, local_path):
    """Upload a single file to the correct GDrive folder."""
    filename = local_path.name
    folder_id = get_target_folder_id(service, filename)
    
    file_metadata = {
        'name': filename,
        'parents': [folder_id]
    }
    media = MediaFileUpload(str(local_path), mimetype='text/csv')
    
    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()
    
    logger.info(f"Uploaded {filename} with ID: {file.get('id')}")
    return file.get('id')

def main():
    if not DRIVE_ROOT_ID:
        logger.error("GDRIVE_ROOT_FOLDER_ID not set")
        return

    try:
        service = get_drive_service()
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        return

    path = Path(LOCAL_LANDING_ZONE)
    if not path.exists():
        logger.warning(f"Local landing zone {LOCAL_LANDING_ZONE} does not exist")
        return

    files_uploaded = 0
    for file_path in path.glob("*.csv"):
        try:
            upload_file(service, file_path)
            # Remove local file after successful upload
            file_path.unlink()
            files_uploaded += 1
        except Exception as e:
            logger.error(f"Failed to upload {file_path.name}: {e}")

    logger.info(f"Ingestion upload job finished. Total files uploaded: {files_uploaded}")

if __name__ == "__main__":
    main()
