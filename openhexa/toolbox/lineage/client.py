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
        producer: str = "https://github.com/BLSQ/openhexa", 
        enable_pipeline_jobs: bool = False,
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
        self.enable_pipeline_jobs = enable_pipeline_jobs
        self._task_events = []

    @classmethod
    def from_env(cls, workspace_slug: str, pipeline_slug: str, pipeline_run_id: str | None = None):
        return cls(
            url=os.environ["OPENLINEAGE_URL"],
            endpoint=os.getenv("OPENLINEAGE_ENDPOINT", "/api/v1/lineage"),
            api_key=os.getenv("OPENLINEAGE_API_KEY", None),
            workspace_slug=workspace_slug,
            pipeline_slug=pipeline_slug,
            pipeline_run_id=pipeline_run_id,
            enable_pipeline_jobs=os.getenv("OPENLINEAGE_ENABLE_PIPELINE_JOBS", "false").lower() == "true",
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

        if task_name:
            base_uuid = uuid.UUID(self.run_id)
            task_run_id = str(uuid.uuid5(base_uuid, task_name))
        else:
            task_run_id = self.run_id
        run = Run(runId=task_run_id, facets=run_facets)

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
        
        if self.enable_pipeline_jobs and task_name:
            self._task_events.append({
                'task_name': task_name,
                'inputs': inputs or [],
                'outputs': outputs or [],
                'event_type': event_type,
            })
        
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

    def emit_pipeline_start_event(self, inputs: list[InputDataset] |
                                  None = None, outputs: list[OutputDataset] | None = None):
        if not self.enable_pipeline_jobs:
            return
            
        aggregated_inputs, aggregated_outputs = self._get_pipeline_io()
        self._emit_pipeline_event(
            event_type=RunState.START,
            inputs=inputs or aggregated_inputs,
            outputs=outputs or aggregated_outputs,
        )

    def emit_pipeline_complete_event(self, inputs: list[InputDataset] |
                                     None = None, outputs: list[OutputDataset] |
                                     None = None):
        if not self.enable_pipeline_jobs:
            return
            
        aggregated_inputs, aggregated_outputs = self._get_pipeline_io()
        self._emit_pipeline_event(
            event_type=RunState.COMPLETE,
            inputs=inputs or aggregated_inputs,
            outputs=outputs or aggregated_outputs,
        )

    def _get_pipeline_io(self):
        all_inputs = set()
        all_outputs = set()
        internal_datasets = set()
        
        for event in self._task_events:
            for inp in event['inputs']:
                all_inputs.add(inp.name)
            for out in event['outputs']:
                all_outputs.add(out.name)
                
        for event in self._task_events:
            for out in event['outputs']:
                if out.name in all_inputs:
                    internal_datasets.add(out.name)
        
        pipeline_inputs = [self.create_input_dataset(name) for name in all_inputs - internal_datasets]
        pipeline_outputs = [self.create_output_dataset(name) for name in all_outputs - internal_datasets]
        
        return pipeline_inputs, pipeline_outputs

    def _emit_pipeline_event(self, event_type: RunState, inputs: list[InputDataset], outputs: list[OutputDataset]):
        now = datetime.now(timezone.utc)
        event_time = now.isoformat()

        run = Run(runId=self.run_id, facets={})

        task_count = len(set(event['task_name'] for event in self._task_events))
        job_facets = {
            "pipeline": {
                "_producer": self.producer,
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/PipelineJobFacet.json",
                "type": "PIPELINE",
                "taskCount": task_count,
                "inputCount": len(inputs),
                "outputCount": len(outputs),
            }
        }

        job = Job(namespace=self.namespace, name=self.job_name, facets=job_facets)

        event = RunEvent(
            eventType=event_type,
            eventTime=event_time,
            run=run,
            job=job,
            producer=self.producer,
            inputs=inputs,
            outputs=outputs,
        )
        
        self.client.emit(event)
