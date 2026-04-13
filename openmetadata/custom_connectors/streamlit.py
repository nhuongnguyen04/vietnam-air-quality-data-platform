import os
import re
import logging
from typing import Iterable, List, Optional, Tuple

from metadata.generated.schema.api.data.createChart import CreateChartRequest
from metadata.generated.schema.api.data.createDashboard import CreateDashboardRequest
from metadata.generated.schema.api.data.createDashboardDataModel import CreateDashboardDataModelRequest
from metadata.generated.schema.entity.data.dashboardDataModel import DataModelType
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.type.entityLineage import EntitiesEdge, EntityLineage
from metadata.generated.schema.type.basic import FullyQualifiedEntityName
from metadata.generated.schema.metadataIngestion.workflow import Source as WorkflowSource
from metadata.ingestion.api.steps import Source
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.api.status import Status
from metadata.ingestion.api.models import Either

logger = logging.getLogger("StreamlitSource")

class StreamlitDashboardSource(Source):
    """
    Custom OpenMetadata Source to ingest Streamlit pages as Charts and Dashboards,
    including automated SQL lineage and Data Models.
    """

    def __init__(self, config: WorkflowSource, metadata_config: OpenMetadata):
        super().__init__()
        self.config = config
        self.metadata = metadata_config
        
        # Paths
        self.dashboard_dir = "/opt/airflow/plugins/dashboard"
        self.pages_dir = os.path.join(self.dashboard_dir, "pages")
        self.service_name = self.config.serviceName
        self.dashboard_name = "VN Air Quality Analytics"
        # Variables for ClickHouse FQN (configurable via ENV or defaults)
        self.clickhouse_service = os.getenv("CLICKHOUSE_SERVICE", "ClickHouse")
        self.clickhouse_db = os.getenv("CLICKHOUSE_DB", "air_quality")
        self.clickhouse_schema = os.getenv("CLICKHOUSE_SCHEMA", "air_quality")

    @classmethod
    def create(cls, config_dict, metadata_config: OpenMetadata, pipeline_name: str = None):
        config = WorkflowSource.parse_obj(config_dict)
        return cls(config, metadata_config)

    def prepare(self):
        pass

    def _iter(self) -> Iterable[Either]:
        """
        Iterate through Streamlit pages and yield Metadata entities.
        """
        if not os.path.exists(self.pages_dir):
            logger.warning(f"Pages directory {self.pages_dir} not found. Skipping.")
            return

        charts_fqns = []
        lineage_tasks: List[Tuple[str, Optional[str], List[str]]] = []

        # 1. Process each page as a Chart
        for filename in sorted(os.listdir(self.pages_dir)):
            if filename.endswith(".py") and not filename.startswith("__"):
                page_path = os.path.join(self.pages_dir, filename)
                chart_name = self._get_chart_name(filename)
                description, sql_queries = self._parse_streamlit_file(page_path)

                # Create Data Model if SQL is found
                datamodel_fqn = None
                if sql_queries:
                    datamodel_name = f"{chart_name}_Query"
                    try:
                        dm_type = DataModelType.SupersetDataModel
                    except AttributeError:
                        dm_type = list(DataModelType)[0]

                    datamodel_req = CreateDashboardDataModelRequest(
                        name=datamodel_name,
                        displayName=f"SQL Query for {chart_name}",
                        description=f"Automated SQL data model extracted from {filename}",
                        service=self.service_name,
                        dataModelType=dm_type,
                        sql=sql_queries[0],
                        columns=[]
                    )
                    yield Either(right=datamodel_req)
                    datamodel_fqn = f"{self.service_name}.{datamodel_name}"

                # Create Chart
                chart_req = CreateChartRequest(
                    name=chart_name,
                    displayName=chart_name.replace("_", " "),
                    description=description or f"Streamlit page: {chart_name}",
                    service=self.service_name,
                    sourceUrl=f"http://localhost:8501/{chart_name}"
                )
                yield Either(right=chart_req)
                chart_fqn = f"{self.service_name}.{chart_name}"
                charts_fqns.append(chart_fqn)

                # Collect lineage info for later processing (after entities are created)
                if sql_queries:
                    lineage_tasks.append((chart_fqn, datamodel_fqn, sql_queries))
                
                self.status.scanned(chart_name)

        # 2. Create main Dashboard containing all charts
        dashboard_req = CreateDashboardRequest(
            name="VN_Air_Quality_Dashboard",
            displayName=self.dashboard_name,
            description="Vietnam Air Quality Data Platform - Main Streamlit Dashboard",
            service=self.service_name,
            charts=charts_fqns
        )
        yield Either(right=dashboard_req)

        # 3. Process Lineage (Table -> DataModel / Chart)
        # We process this after creating entities in this iteration.
        for chart_fqn, datamodel_fqn, sql_queries in lineage_tasks:
            for sql in sql_queries:
                # Extract tables like air_quality.table_name or just table_name
                tables = re.findall(r'FROM\s+([a-zA-Z0-9_\.]+)', sql, re.IGNORECASE)
                for table_name in set(tables):
                    # Clean table name (remove database prefix if present)
                    clean_name = table_name.split('.')[-1]
                    # Map to configurable FQN hierarchy: Service.Database.Schema.<table_name>
                    table_fqn = f"{self.clickhouse_service}.{self.clickhouse_db}.{self.clickhouse_schema}.{clean_name}"
                    
                    try:
                        # Lineage: Table -> Chart
                        yield Either(right=AddLineageRequest(
                            edge=EntitiesEdge(
                                fromEntity=self._get_entity_reference("table", table_fqn),
                                toEntity=self._get_entity_reference("chart", chart_fqn)
                            )
                        ))
                        
                        if datamodel_fqn:
                            # Lineage: Table -> DataModel
                            yield Either(right=AddLineageRequest(
                                edge=EntitiesEdge(
                                    fromEntity=self._get_entity_reference("table", table_fqn),
                                    toEntity=self._get_entity_reference("dashboardDataModel", datamodel_fqn)
                                )
                            ))
                    except Exception as e:
                        logger.warning(f"Failed to create lineage request for {table_fqn} -> {chart_fqn}: {e}")

    def _get_chart_name(self, filename: str) -> str:
        name = re.sub(r'^\d+_', '', filename)
        name = name.replace(".py", "")
        return name

    def _parse_streamlit_file(self, file_path: str):
        description = ""
        sql_queries = []
        with open(file_path, 'r') as f:
            content = f.read()
            doc_match = re.search(r'"""(.*?)"""', content, re.DOTALL)
            if doc_match:
                description = doc_match.group(1).strip()
            sql_matches = re.findall(r'"""\s*(SELECT.*?)"""', content, re.IGNORECASE | re.DOTALL)
            for sql in sql_matches:
                if "FROM" in sql.upper():
                    sql_queries.append(sql.strip())
        return description, sql_queries

    def _get_entity_reference(self, entity_type: str, fqn: str):
        entity = self.metadata.get_by_name(entity=entity_type, fqn=fqn)
        if not entity:
            raise ValueError(f"Could not find {entity_type} with FQN {fqn}")
        from metadata.generated.schema.type.entityReference import EntityReference
        return EntityReference(
            id=entity.id,
            type=entity_type
        )

    def test_connection(self) -> bool:
        return os.path.exists(self.dashboard_dir)

    def get_status(self) -> Status:
        return self.status

    def close(self):
        pass
