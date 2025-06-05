import uuid
from datetime import datetime, timezone
from typing import List, Optional

from openlineage.client.client import OpenLineageClient, OpenLineageClientOptions
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


class OpenHexaOpenLineageClient:
    def __init__(
        self,
        url: str,
        workspace_slug: str,
        pipeline_slug: str,
        pipeline_run_id: Optional[str] = None,
        api_key: Optional[str] = None,
        producer: str = "https://github.com/openhexa",
    ):
        self.client = OpenLineageClient(
            url=url,
            options=OpenLineageClientOptions(api_key=api_key) if api_key else None,
        )
        self.namespace = workspace_slug
        self.job_name = pipeline_slug
        self.run_id = pipeline_run_id or str(uuid.uuid4())
        self.producer = producer

    def emit_run_event(
        self,
        event_type: RunState,
        inputs: Optional[List[InputDataset]] = None,
        outputs: Optional[List[OutputDataset]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        sql: Optional[str] = None,
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

        job = Job(namespace=self.namespace, name=self.job_name, facets=job_facets)

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

    def create_input_dataset(self, namespace: str, name: str) -> InputDataset:
        return InputDataset(namespace=namespace, name=name)

    def create_output_dataset(self, namespace: str, name: str) -> OutputDataset:
        return OutputDataset(namespace=namespace, name=name)

    def dataset_from_postgres_table(
        self, connection_name: str, db_name: str, table_name: str
    ) -> Dataset:
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
