import uuid
from datetime import datetime, timezone
import os
from openlineage.client.client import OpenLineageClient
from openlineage.client.event_v2 import (
    Dataset,
    InputDataset,
    OutputDataset,
    Job,
    Run,
    RunEvent,
    RunState,
)
from openlineage.client.facet_v2 import (
    nominal_time_run,
    sql_job,
)
from openlineage.client.transport import HttpConfig
from openlineage.client.transport.http import ApiKeyTokenProvider, HttpTransport


class OpenHexaOpenLineageClient:
    def __init__(
        self,
        url: str,
        workspace_slug: str,
        pipeline_slug: str,
        pipeline_run_id: str | None = None,
        api_key: str | None = None,
        endpoint: str = "/api/v1/lineage",
        producer: str = "https://github.com/BLSQ/openhexa",  # Default producer identifier,
    ):
        http_config = HttpConfig(
            url=url,
            endpoint=endpoint,
            timeout=5,
            verify=False,
        )
        if api_key:
            http_config.auth = ApiKeyTokenProvider({"apiKey": api_key})
        self.client = OpenLineageClient(transport=HttpTransport(http_config))
        self.namespace = workspace_slug
        self.job_name = pipeline_slug
        self.run_id = pipeline_run_id or str(uuid.uuid4())
        self.producer = producer

    @classmethod
    def from_env(cls, workspace_slug: str, pipeline_slug: str, pipeline_run_id: str | None = None):
        return cls(
            url=os.environ["OPENLINEAGE_URL"],
            endpoint=os.getenv("OPENLINEAGE_ENDPOINT", "/api/v1/lineage"),
            api_key=os.getenv("OPENLINEAGE_API_KEY", None),
            workspace_slug=workspace_slug,
            pipeline_slug=pipeline_slug,
            pipeline_run_id=pipeline_run_id,
        )

    def emit_run_event(
        self,
        event_type: RunState,
        task_name: str,
        inputs: list[InputDataset] | None = None,
        outputs: list[OutputDataset] | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        sql: str | None = None,
    ):
        now = datetime.now(timezone.utc)
        event_time = (start_time or now).isoformat()

        run_facets = {}
        if start_time:
            run_facets["nominalTime"] = nominal_time_run.NominalTimeRunFacet(
                nominalStartTime=start_time.isoformat(),
                nominalEndTime=end_time.isoformat() if end_time else None,
            )

        run = Run(runId=self.run_id, facets=run_facets)

        job_facets = {}
        if sql:
            job_facets["sql"] = sql_job.SQLJobFacet(query=sql)

        job_name = f"{self.job_name}.{task_name}" if task_name else self.job_name
        job = Job(namespace=self.namespace, name=job_name, facets=job_facets)

        event = RunEvent(
            eventType=event_type,
            eventTime=event_time,
            run=run,
            job=job,
            producer=self.producer,
            inputs=inputs or [],
            outputs=outputs or [],
        )
        self.client.emit(event)

    def create_input_dataset(self, name: str) -> InputDataset:
        return InputDataset(namespace=self.namespace, name=name)

    def create_output_dataset(self, name: str) -> OutputDataset:
        return OutputDataset(namespace=self.namespace, name=name)

    def dataset_from_postgres_table(self, connection_name: str, db_name: str, table_name: str) -> Dataset:
        return Dataset(
            namespace=self.namespace,
            name=f"{db_name}.{table_name}",
            facets={
                "source": {
                    "name": connection_name,
                    "type": "postgresql",
                },
                "database": {
                    "name": db_name,
                    "type": "postgres",
                },
            },
        )

    def dataset_from_file(self, file_url: str) -> Dataset:
        return Dataset(namespace=self.namespace, name=file_url)

    def dataset_from_dashboard(self, dashboard_url: str) -> Dataset:
        return Dataset(namespace=self.namespace, name=dashboard_url)

    def dataset_from_connection(self, connection_name: str, connection_type: str) -> Dataset:
        return Dataset(
            namespace=self.namespace,
            name=connection_name,
            facets={
                "source": {
                    "name": connection_name,
                    "type": connection_type,
                }
            },
        )
