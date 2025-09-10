# OpenHexa Lineage Client

This module provides an opinionated wrapper around the [OpenLineage](https://openlineage.io) client for use within **OpenHexa pipelines**. 
It enables tracking of dataset inputs/outputs and emits lineage events at the **task level**, scoped per workspace and pipeline run.

This functionality is **bundled with the OpenHexa Toolbox** — you don't need to install anything separately.

## Requirements 
OpenHexa SDK > 2.8.1

---

## Environment Variables

The following environment variables can be set to configure the lineage client:

| Name | Description | Required | Example |
|------|-------------|----------|---------|
| `OPENLINEAGE_URL` | The URL of the OpenLineage server | Yes | `https://lineage.openhexa.org` |
| `OPENLINEAGE_API_KEY` | The API key to authenticate with the OpenLineage server | No | `your_api_key` |
| `OPENLINEAGE_ENDPOINT` | The endpoint to send lineage events to | No | `/api/v1/lineage` (default) |
| `OPENLINEAGE_ENABLE_PIPELINE_JOBS` | Enable pipeline-level job tracking | No | `true` or `false` (default: `false`) |

## Pipeline-Level Job Tracking

**New**: The lineage client now supports pipeline-level job tracking, which creates `PIPELINE` type jobs that aggregate inputs and outputs from all individual tasks in a pipeline.

### How Pipeline I/O is Determined

The pipeline-level inputs and outputs are calculated by analyzing all task events:
- **Pipeline Inputs**: Datasets that are consumed by tasks but not produced by any task within the pipeline (external sources)
- **Pipeline Outputs**: Datasets that are produced by tasks but not consumed by any other task within the pipeline (final outputs)
- **Internal Datasets**: Intermediate datasets that are both produced and consumed within the pipeline are filtered out from the pipeline level

## Quick Start
```python
from datetime import datetime, timezone
from openhexa.toolbox import lineage
from openhexa.toolbox.lineage import EventType
from openhexa.sdk import current_run, pipeline, workspace

# Initialize once at the start of your pipeline
lineage.init_client(
    url="https://lineage.openhexa.org",
    workspace_slug=workspace.slug,
    pipeline_slug=current_run.get_pipeline().code,
    pipeline_run_id=current_run.pipeline_run_id,
    enable_pipeline_jobs=True  # Enable pipeline-level job tracking
)
# or initialize from environment variables
lineage.init_client_from_env(
    workspace_slug=workspace.slug,
    pipeline_slug=current_run.get_pipeline().code,
    pipeline_run_id=current_run.pipeline_run_id,
)
# You can check if the client is initialized correctly
if lineage.is_initialized():
    print("OpenLineage client is initialized successfully.")
else:
    print("OpenLineage client initialization failed.")

# Emit START event for a task
start_time = datetime.now(timezone.utc)
lineage.event(
    event_type=EventType.START,
    task_name="extract_users",
    inputs=["openhexa.iaso.users"],
    outputs=["openhexa.postgres.analytics_users"],
    sql="SELECT * FROM users",
    start_time=start_time
)

# Emit COMPLETE event for the same task
lineage.event(
    event_type=EventType.COMPLETE,
    task_name="extract_users",
    inputs=["openhexa.iaso.users"],
    outputs=["openhexa.postgres.analytics_users"],
    start_time=start_time,
    end_time=datetime.now(timezone.utc)
)

```

## Pipeline-Level Events

When `enable_pipeline_jobs=True`, you can also emit pipeline-level events that provide a high-level view of the entire data transformation:

```python
# Start pipeline-level tracking (optional - I/O will be auto-calculated from tasks)
lineage.pipeline_start()

# Execute your individual tasks...
lineage.event(EventType.START, task_name="extract_data", inputs=["external_database"], outputs=["raw_data"])
lineage.event(EventType.COMPLETE, task_name="extract_data", inputs=["external_database"], outputs=["raw_data"])

lineage.event(EventType.START, task_name="transform_data", inputs=["raw_data"], outputs=["clean_data"])
lineage.event(EventType.COMPLETE, task_name="transform_data", inputs=["raw_data"], outputs=["clean_data"])

lineage.event(EventType.START, task_name="generate_report", inputs=["clean_data"], outputs=["final_report"])
lineage.event(EventType.COMPLETE, task_name="generate_report", inputs=["clean_data"], outputs=["final_report"])

# Complete pipeline-level tracking
lineage.pipeline_complete()

# If you don't specify inputs/outputs, they are automatically calculated:
# Pipeline inputs: ["external_database"] (not produced by any task)
# Pipeline outputs: ["final_report"] (not consumed by any task)
# Internal datasets: ["raw_data", "clean_data"] (filtered out from pipeline view)

```
## Dataset References

Datasets can be passed to the `lineage.event()` function in two ways:

- As strings: in the format `"namespace.dataset_name"`  
  Example: `"openhexa.iaso.users"`. 
  These are converted to OpenLineage `InputDataset` or `OutputDataset` objects automatically.
- As explicit dataset objects  : `InputDataset`/ `OutputDataset` objects,  
  Example: `lineage.InputDataset(namespace="openhexa.iaso", name="users")`, created using the helper methods provided by the client.
  You can use this for advanced metadata (e.g. connection information, schema, etc.).

The client provides helper methods to construct datasets for various source types.

---

## Pipeline & Task Mapping

Each lineage event follows this mapping convention:

| Concept       | Mapped to                         | Job Type | Example |
|---------------|-----------------------------------|----------|---------|
| Workspace     | OpenLineage `namespace`           | -        | `my_workspace` |
| Pipeline      | OpenLineage job name              | `PIPELINE` | `data_processing_pipeline` |
| Task          | OpenLineage `job.name = pipeline.task_name` | `BATCH` | `data_processing_pipeline.extract_data` |
| Pipeline Run  | OpenLineage `run.runId`           | -        | `12345-abcd-6789` |

### Job Types

- **PIPELINE Jobs**: High-level view of the entire data transformation, showing only external inputs and final outputs
- **BATCH Jobs**: Individual task execution within the pipeline, showing detailed step-by-step lineage

This dual-level approach ensures that both high-level data flow and detailed task execution are traceable under one unified pipeline execution context.

---

## Supported Dataset Types

You can define structured datasets with the following helper methods from the client:

```python
from openhexa.toolbox import lineage
from openhexa.sdk import workspace, connections

# A file-based dataset
file_path = f"{workspace.files_path}/activities.json"
file_dataset = lineage._client.dataset_from_file(file_path)

# A PostgreSQL table
conn = connections["postgres_connection"]
pg_dataset = lineage._client.dataset_from_postgres_table(
    connection_name=conn.name,
    db_name="analytics",
    table_name="users"
)

# A BI dashboard dataset
dashboard_dataset = lineage._client.dataset_from_dashboard(
    "https://datastudio.google.com/reporting/abc123"
)

# A generic API or external connection
api_conn = connections["iaso"]
generic_dataset = lineage._client.dataset_from_connection(
    connection_name=api_conn.name,
    connection_type="IASOConnection"
)

```

---

## Best Practices

### Pipeline Jobs

1. **Enable pipeline jobs for complex workflows**: Use `enable_pipeline_jobs=True` when you have multi-step pipelines that benefit from a high-level overview.

2. **Let I/O be auto-calculated**: Don't specify inputs/outputs in `pipeline_start()` and `pipeline_complete()` unless you have specific requirements. The automatic calculation usually produces the correct result.

3. **Include external sources**: Always include external data sources (databases, APIs, files) in your first task's inputs so they appear as pipeline inputs.

4. **Handle errors properly**: If a pipeline fails, the `pipeline_complete()` call may not be reached, leaving the pipeline in a "running" state. This is expected behavior and helps identify failed executions.

### Task Events

1. **Use consistent dataset names**: Keep dataset names consistent between tasks to properly track lineage flow.

2. **Include both START and COMPLETE events**: Always emit both events for successful tasks, and FAIL events for failed tasks.

3. **Use proper timing**: Use `start_time` for START events and `end_time` for COMPLETE/FAIL events.

### Example: Complete Pipeline

```python
from datetime import datetime, timezone
from openhexa.toolbox import lineage
from openhexa.toolbox.lineage import EventType
from openhexa.sdk import current_run, workspace

# Initialize with pipeline jobs enabled
lineage.init_client_from_env(
    workspace_slug=workspace.slug,
    pipeline_slug=current_run.get_pipeline().code,
    pipeline_run_id=current_run.pipeline_run_id,
)

# Set environment variable to enable pipeline jobs
# OPENLINEAGE_ENABLE_PIPELINE_JOBS=true

try:
    # Start pipeline-level tracking
    lineage.pipeline_start()
    
    # Task 1: Extract data
    lineage.event(EventType.START, task_name="extract", inputs=["external_api"], outputs=["raw_data"])
    # ... do extraction work ...
    lineage.event(EventType.COMPLETE, task_name="extract", inputs=["external_api"], outputs=["raw_data"])
    
    # Task 2: Process data
    lineage.event(EventType.START, task_name="process", inputs=["raw_data"], outputs=["processed_data"])
    # ... do processing work ...
    lineage.event(EventType.COMPLETE, task_name="process", inputs=["raw_data"], outputs=["processed_data"])
    
    # Task 3: Export results
    lineage.event(EventType.START, task_name="export", inputs=["processed_data"], outputs=["final_report"])
    # ... do export work ...
    lineage.event(EventType.COMPLETE, task_name="export", inputs=["processed_data"], outputs=["final_report"])
    
    # Complete pipeline-level tracking
    lineage.pipeline_complete()
    # This creates a PIPELINE job with:
    # - Inputs: ["external_api"] (external source)
    # - Outputs: ["final_report"] (final result)
    # - Internal: ["raw_data", "processed_data"] (filtered out)
    
except Exception as e:
    current_run.log_error(f"Pipeline failed: {e}")
    # Note: pipeline_complete() is not called, so pipeline remains in RUNNING state
    raise

```
