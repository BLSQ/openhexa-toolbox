from datetime import datetime

from openlineage.client.event_v2 import InputDataset, OutputDataset
from openlineage.client.generated.base import EventType

from .client import OpenHexaOpenLineageClient

_client: OpenHexaOpenLineageClient | None = None


def init_client(*args, **kwargs):
    global _client
    _client = OpenHexaOpenLineageClient(*args, **kwargs)


def init_client_from_env(
    workspace_slug: str,
    pipeline_slug: str,
    pipeline_run_id: str | None = None,
):
    """
    Initialize the lineage client using environment variables for `url` and `api_key`.
    Requires workspace_slug and pipeline_slug to be passed explicitly.
    """
    global _client
    _client = OpenHexaOpenLineageClient.from_env(
        workspace_slug=workspace_slug,
        pipeline_slug=pipeline_slug,
        pipeline_run_id=pipeline_run_id,
    )


def event(
    event_type: EventType,
    *,
    task_name: str,
    inputs: list[str | InputDataset] | None = None,
    outputs: list[str | OutputDataset] | None = None,
    sql: str | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
):
    if _client is None:
        raise RuntimeError(
            "Lineage client not initialized. Call `init_client(...)` or `init_client_from_env(...)` first."
        )

    input_objs = _wrap_datasets(inputs, is_input=True)
    output_objs = _wrap_datasets(outputs, is_input=False)

    _client.emit_run_event(
        event_type=event_type,
        task_name=task_name,
        inputs=input_objs,
        outputs=output_objs,
        start_time=start_time,
        end_time=end_time,
        sql=sql,
    )


def _wrap_datasets(
    datasets: list[str | InputDataset | OutputDataset] | None, is_input: bool
) -> list[InputDataset] | list[OutputDataset]:
    if not datasets:
        return []

    result = []
    for d in datasets:
        if isinstance(d, str):
            ds = _client.create_input_dataset(d) if is_input else _client.create_output_dataset(d)
            result.append(ds)
        else:
            result.append(d)
    return result


def is_initialized() -> bool:
    return _client is not None


__all__ = ["EventType"]
