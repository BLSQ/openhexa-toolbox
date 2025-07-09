# OpenHexa Lineage Client

This module provides an opinionated wrapper around the [OpenLineage](https://openlineage.io) client for use within **OpenHexa pipelines**. 
It enables tracking of dataset inputs/outputs and emits lineage events at the **task level**, scoped per workspace and pipeline run.

This functionality is **bundled with the OpenHexa Toolbox** — you don't need to install anything separately.

## Requirements 
OpenHexa SDK > 2.8.1

---

## Quick Start
To initialise OpenLineage client from env variables you need to set the following environment variables:
```bash
export OPENLINEAGE_URL="https://lineage.openhexa.org" # (required) - the OpenLineage server URL
export OPENLINEAGE_ENDPOINT ="api/v1/lineage" # (optional) - if you want to use a custom endpoint
export OPENLINEAGE_API_KEY="your_api_key" #(optional) - if you want to use API key authentication
```
```python
from datetime import datetime, timezone
from openhexa.toolbox import lineage
from openhexa.toolbox.lineage import EventType
from openhexa.sdk import current_run, pipeline, workspace

# Initialize once at the start of your pipeline
lineage.init_client(
    url ="https://lineage.openhexa.org",
    workspace_slug=workspace.slug,
    pipeline_slug=current_run.get_pipeline().code,
    pipeline_run_id=current_run.pipeline_run_id,
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

| Concept       | Mapped to                         |
|---------------|-----------------------------------|
| Workspace     | OpenLineage `namespace`           |
| Pipeline      | OpenLineage job prefix            |
| Task          | OpenLineage `job.name = pipeline.task_name` |
| Pipeline Run  | OpenLineage `run.runId`           |

This ensures that task-level runs are traceable under one unified pipeline execution context.

---

## Supported Dataset Types

You can define structured datasets with the following helper methods from the client:

```
python
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
