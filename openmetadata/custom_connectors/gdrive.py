import os
import logging
from typing import Iterable

from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

from metadata.generated.schema.api.data.createContainer import CreateContainerRequest
from metadata.generated.schema.entity.services.storageService import StorageServiceType
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ingestion.api.steps import Source
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.api.status import Status
from metadata.ingestion.api.models import Either

logger = logging.getLogger("GoogleDriveSource")

class GoogleDriveSource(Source):
    """
    Custom OpenMetadata Source to ingest Google Drive folders as Containers.
    """

    def __init__(self, config: WorkflowSource, metadata_config: OpenMetadata):
        super().__init__()
        self.config = config
        self.metadata = metadata_config
        
        # Get credentials from environment
        self.client_id = os.environ.get("GDRIVE_CLIENT_ID")
        self.client_secret = os.environ.get("GDRIVE_CLIENT_SECRET")
        self.refresh_token = os.environ.get("GDRIVE_REFRESH_TOKEN")
        self.root_id = os.environ.get("GDRIVE_ROOT_FOLDER_ID")
        self.service_name = self.config.serviceName

        if not all([self.client_id, self.client_secret, self.refresh_token]):
            raise ValueError("Missing Google Drive OAuth environment variables.")

        self.service = self._get_drive_service()

    def _get_drive_service(self):
        creds = Credentials(
            None,
            refresh_token=self.refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=self.client_id,
            client_secret=self.client_secret,
            scopes=['https://www.googleapis.com/auth/drive']
        )
        if not creds.valid:
            creds.refresh(Request())
        return build('drive', 'v3', credentials=creds)

    @classmethod
    def create(cls, config_dict, metadata_config: OpenMetadata, pipeline_name: str = None):
        config = WorkflowSource.parse_obj(config_dict)
        return cls(config, metadata_config)

    def prepare(self):
        """Prepare is called before _iter."""
        pass

    def _iter(self) -> Iterable[CreateContainerRequest]:
        """
        Iterate through GDrive folders and yield CreateContainerRequest.
        """
        if not self.root_id:
            logger.warning("No GDRIVE_ROOT_FOLDER_ID provided. Skipping ingestion.")
            return

        yield from self._crawl_folders(self.root_id)

    def _crawl_folders(self, folder_id, parent_fqn=None):
        """
        Recursive function to find folders and emit Containers.
        """
        query = f"'{folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        results = self.service.files().list(q=query, fields="files(id, name, description)").execute()
        folders = results.get('files', [])

        for folder in folders:
            name = folder['name']
            gdrive_id = folder['id']
            description = folder.get('description', f"Google Drive folder: {name}")

            # Create the Container request
            container_request = CreateContainerRequest(
                name=name,
                displayName=name,
                description=description,
                service=self.service_name,
                sourceUrl=f"https://drive.google.com/drive/folders/{gdrive_id}",
            )
            
            yield Either(right=container_request)
            self.status.scanned(name)

            # Recurse into children
            yield from self._crawl_folders(gdrive_id)

    def test_connection(self) -> bool:
        try:
            self.service.files().get(fileId=self.root_id).execute()
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def get_status(self) -> Status:
        return self.status

    def close(self):
        pass
