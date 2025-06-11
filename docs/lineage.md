# OpenHexa Lineage Client

This module provides an opinionated wrapper around the [OpenLineage](https://openlineage.io) client for use within **OpenHexa pipelines**. It enables tracking of dataset inputs/outputs and emits lineage events at the **task level**, scoped per workspace and pipeline run.

This functionality is **bundled with the OpenHexa Toolbox** — you don't need to install anything separately.

---

## Quick Start

```python
from lineage import init_client, event, EventType
from openhexa.sdk import current_run
from datetime import datetime

# Initialize once at the start of your pipeline
init_client(
    url="http://your-marquez-host:5000",
    workspace_slug=workspace.slug,
    pipeline_slug=pipeline.slug,
    pipeline_run_id=current_run.id,
)

# Emit START event for a task
event(
    EventType.START,
    task_name="extract_users",
    inputs=["openhexa.iaso.users"],
    outputs=["openhexa.postgres.analytics_users"],
    sql="SELECT * FROM users",
    start_time=datetime.utcnow()
)

# Emit COMPLETE event for the same task
event(
    EventType.COMPLETE,
    task_name="extract_users",
    inputs=["openhexa.iaso.users"],
    outputs=["openhexa.postgres.analytics_users"],
    end_time=datetime.utcnow()
)
```
## Dataset References

Datasets can be passed to the `event()` function in two ways:

- As simple strings in the form `"namespace.dataset_name"`  
  Example: `"openhexa.iaso.users"`
- As fully constructed OpenLineage `InputDataset` or `OutputDataset` objects  
  (used for advanced metadata customization)

The client provides helper methods to construct datasets for various source types.

---

## Pipeline & Task Mapping

Each lineage event emitted by the client follows this mapping:

| Concept       | Mapped to                         |
|---------------|-----------------------------------|
| Workspace     | OpenLineage `namespace`           |
| Pipeline      | OpenLineage job prefix            |
| Task          | OpenLineage `job.name = pipeline.task_name` |
| Pipeline Run  | OpenLineage `run.runId`           |

This ensures that all task-level jobs are tracked under a single pipeline execution context, enabling full traceability.

---

## Supported Dataset Types

You can define structured datasets with the following helper methods from the client:

```
python
from openhexa.sdk import workspace, connections

# A file-based dataset (input or output)
file_path = f"{workspace.files_path}/activities.json"
dataset = lineage_client.dataset_from_file(file_path)

# A PostgreSQL table using a connection
conn = connections["postgres_connection"]
dataset = lineage_client.dataset_from_postgres_table(
    connection_name=conn.name,
    db_name="analytics",
    table_name="users"
)

# A dashboard or BI URL
dataset = lineage_client.dataset_from_dashboard("https://datastudio.google.com/reporting/abc123")

# A generic connection as a dataset (e.g., APIs)
conn = connections["iaso"]
dataset = lineage_client.dataset_from_connection(connection_name=conn.name, connection_type="IASOConnection")

```