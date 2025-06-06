from datetime import datetime
from typing import Union

from openlineage.client.event_v2 import InputDataset, OutputDataset, RunState
from openlineage.client.generated.base import EventType as LineageEventType
from .client import OpenHexaOpenLineageClient

_client: OpenHexaOpenLineageClient | None = None

def init_client(*args, **kwargs):
    global _client
    _client = OpenHexaOpenLineageClient(*args, **kwargs)

def event(
    event_type: OpenHexaOpenLineageClient,
    *,
    task_name: str,
    inputs: list[str | InputDataset] | None = None,
    outputs: list[str | OutputDataset] | None = None,
    sql: str | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
):
    if _client is None:
        raise RuntimeError("Lineage client not initialized. Call `lineage.init_client(...)` first.")

    input_objs = _wrap_datasets(inputs, is_input=True)
    output_objs = _wrap_datasets(outputs, is_input=False)

    _client.emit_run_event(
        event_type=event_type,
        task_name=task_name,
        inputs=input_objs,
        outputs=output_objs,
        start_time=start_time,
        sql=sql
    )

def _wrap_datasets(datasets: list[str | InputDataset | OutputDataset] | None, is_input: bool) -> list[InputDataset] | list[OutputDataset]:
    if not datasets:
        return []

    result = []
    for d in datasets:
        if isinstance(d, str):
            namespace, name = d.split(".", 1)
            ds = _client.create_input_dataset(namespace, name) if is_input else _client.create_output_dataset(namespace, name)
            result.append(ds)
        else:
            result.append(d)
    return result
