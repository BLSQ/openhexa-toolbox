from typing import Optional, List, Union
from datetime import datetime
from openlineage.client.event_v2 import InputDataset, OutputDataset, RunState

from .client import OpenHexaOpenLineageClient
from .event_types import EventType

_client: Optional[OpenHexaOpenLineageClient] = None

def init_client(*args, **kwargs):
    global _client
    _client = OpenHexaOpenLineageClient(*args, **kwargs)

def event(
    event_type: EventType,
    *,
    inputs: Optional[List[Union[str, InputDataset]]] = None,
    outputs: Optional[List[Union[str, OutputDataset]]] = None,
    sql: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
):
    if _client is None:
        raise RuntimeError("Lineage client not initialized. Call `lineage.init_client(...)` first.")

    input_objs = _wrap_datasets(inputs, is_input=True)
    output_objs = _wrap_datasets(outputs, is_input=False)

    if event_type == EventType.EMIT_RUN_START:
        _client.emit_run_event(RunState.START, inputs=input_objs, outputs=output_objs, start_time=start_time, sql=sql)
    elif event_type == EventType.EMIT_RUN_COMPLETE:
        _client.emit_run_event(RunState.COMPLETE, inputs=input_objs, outputs=output_objs, end_time=end_time, sql=sql)
    else:
        raise ValueError(f"Unsupported event type: {event_type}")

def _wrap_datasets(datasets, is_input: bool):
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
