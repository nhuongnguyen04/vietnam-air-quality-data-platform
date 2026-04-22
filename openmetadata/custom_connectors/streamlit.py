import os
import re
import logging
from typing import Iterable, List, Optional, Tuple

from metadata.generated.schema.api.data.createChart import CreateChartRequest
from metadata.generated.schema.api.data.createDashboard import CreateDashboardRequest
from metadata.generated.schema.api.data.createDashboardDataModel import CreateDashboardDataModelRequest
from metadata.generated.schema.entity.data.chart import Chart
from metadata.generated.schema.entity.data.dashboardDataModel import DashboardDataModel
from metadata.generated.schema.entity.data.dashboardDataModel import DataModelType
from metadata.generated.schema.entity.data.table import Table
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
        self.service_name = self.config.serviceName
        self.dashboard_name = "Phân tích Chất lượng Không khí Việt Nam"
        # Variables for ClickHouse FQN (configurable via ENV or defaults)
        self.clickhouse_service = os.getenv("CLICKHOUSE_SERVICE", "ClickHouse")
        self.clickhouse_db = os.getenv("CLICKHOUSE_DB", "air_quality")
        self.clickhouse_schema = os.getenv("CLICKHOUSE_SCHEMA", "air_quality")
        self._entity_classes = {
            "table": Table,
            "chart": Chart,
            "dashboardDataModel": DashboardDataModel,
        }

    @classmethod
    def create(cls, config_dict, metadata_config: OpenMetadata, pipeline_name: str = None):
        config = WorkflowSource.parse_obj(config_dict)
        return cls(config, metadata_config)

    def prepare(self):
        pass

    def _iter(self) -> Iterable[Either]:
        import yaml
        
        # Path to the metadata YAML file
        metadata_file = os.path.join(self.dashboard_dir, "dashboard_metadata.yml")
        if not os.path.exists(metadata_file):
            logger.warning(f"Metadata file {metadata_file} not found. Skipping.")
            return

        with open(metadata_file, "r", encoding="utf-8") as f:
            yaml_data = yaml.safe_load(f)

        dashboard_info = yaml_data.get("dashboard", {})
        pages = dashboard_info.get("pages", [])

        charts_fqns = []
        lineage_tasks: List[Tuple[str, Optional[str], List[str]]] = []

        # 1. Process each page as a Chart
        for page in pages:
            filename = page.get("filename", "")
            chart_name = page.get("name", "Unknown Chart").replace(" ", "_")
            display_name = page.get("display_name", chart_name)
            description = page.get("description", "")
            source_tables = page.get("source_tables", [])

            # Create Data Model if tables are found
            datamodel_fqn = None
            if source_tables:
                datamodel_name = f"{chart_name}_Query"
                try:
                    dm_type = DataModelType.SupersetDataModel
                except AttributeError:
                    dm_type = list(DataModelType)[0]

                # Generate a dummy SQL query for OpenMetadata display
                dummy_sql = "SELECT * FROM " + ", ".join([f"{self.clickhouse_db}.{t}" for t in source_tables])
                
                datamodel_req = CreateDashboardDataModelRequest(
                    name=datamodel_name,
                    displayName=f"Data Source for {chart_name}",
                    description=f"Automated Data Model extracted from Streamlit configuration",
                    service=self.service_name,
                    dataModelType=dm_type,
                    sql=dummy_sql,
                    columns=[]
                )
                yield Either(right=datamodel_req)
                datamodel_fqn = f"{self.service_name}.{datamodel_name}"

            # Create Chart
            chart_req = CreateChartRequest(
                name=chart_name,
                displayName=display_name,
                description=description or f"Streamlit page: {chart_name}",
                service=self.service_name,
                sourceUrl="http://localhost:8501"
            )
            yield Either(right=chart_req)
            chart_fqn = f"{self.service_name}.{chart_name}"
            charts_fqns.append(chart_fqn)

            # Collect lineage info for later processing
            if source_tables:
                lineage_tasks.append((chart_fqn, datamodel_fqn, source_tables))
            
            self.status.scanned(chart_name)

        # 2. Create main Dashboard containing all charts
        dashboard_req = CreateDashboardRequest(
            name=dashboard_info.get("name", "VN_Air_Quality_Dashboard").replace(" ", "_"),
            displayName=dashboard_info.get("name", self.dashboard_name),
            description=dashboard_info.get("description", "Vietnam Air Quality Data Platform - Main Streamlit Dashboard"),
            service=self.service_name,
            charts=charts_fqns
        )
        yield Either(right=dashboard_req)

        # 3. Process Lineage (Table -> DataModel / Chart)
        for chart_fqn, datamodel_fqn, source_tables in lineage_tasks:
            for table_name in source_tables:
                clean_name = table_name.split('.')[-1]
                if not clean_name:
                    continue

                table_fqn = f"{self.clickhouse_service}.{self.clickhouse_db}.{self.clickhouse_schema}.{clean_name}"
                
                try:
                    table_ref = self._get_entity_reference("table", table_fqn)
                    chart_ref = self._get_entity_reference("chart", chart_fqn)
                    if table_ref and chart_ref:
                        yield Either(right=AddLineageRequest(
                            edge=EntitiesEdge(fromEntity=table_ref, toEntity=chart_ref)
                        ))

                    if datamodel_fqn:
                        datamodel_ref = self._get_entity_reference("dashboardDataModel", datamodel_fqn)
                        if table_ref and datamodel_ref:
                            yield Either(right=AddLineageRequest(
                                edge=EntitiesEdge(fromEntity=table_ref, toEntity=datamodel_ref)
                            ))
                except Exception as e:
                    logger.debug(f"Skipping lineage request for {table_fqn} -> {chart_fqn}: {e}")

    def _get_entity_reference(self, entity_type: str, fqn: str):
        entity_cls = self._entity_classes.get(entity_type)
        if entity_cls is None:
            raise ValueError(f"Unsupported entity type: {entity_type}")

        entity = self.metadata.get_by_name(entity=entity_cls, fqn=fqn)
        if not entity:
            logger.debug(f"Could not find {entity_type} with FQN {fqn}")
            return None
        from metadata.generated.schema.type.entityReference import EntityReference
        return EntityReference(
            id=entity.id,
            type=entity_type,
        )

    def test_connection(self) -> bool:
        return os.path.exists(os.path.join(self.dashboard_dir, "dashboard_metadata.yml"))

    def get_status(self) -> Status:
        return self.status

    def close(self):
        pass
