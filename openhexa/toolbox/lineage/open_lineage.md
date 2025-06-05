# OpenHexa Lineage Client

This module provides an opinionated wrapper around the [OpenLineage](https://openlineage.io) client for use within **OpenHexa pipelines**. It enables tracking of dataset inputs/outputs and emits lineage events at the **task level**, scoped per workspace and pipeline run.

This functionality is **bundled with the OpenHexa Toolbox** — you don't need to install anything separately.

---

## Quick Start

```python
from lineage import init_client, event, LineageEventType
from openhexa.sdk import current_pipeline, current_run
from datetime import datetime

# Initialize once at the start of your pipeline
init_client(
    url="http://your-marquez-host:5000",
    workspace_slug=current_pipeline.workspace.slug,
    pipeline_slug=current_pipeline.slug,
    pipeline_run_id=current_run.id,
)

# Emit START event for a task
event(
    LineageEventType.START,
    task_name="extract_users",
    inputs=["openhexa.iaso.users"],
    outputs=["openhexa.postgres.analytics_users"],
    sql="SELECT * FROM users",
    start_time=datetime.utcnow()
)

# Emit COMPLETE event for the same task
event(
    LineageEventType.COMPLETE,
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
from openhexa.sdk import F
client.dataset_from_postgres_table("postgres_conn", "analytics", "users")
client.dataset_from_file("gs://bucket/path.csv")
client.dataset_from_dashboard("https://datastudio.google.com/...")
client.dataset_from_connection("iaso", "api")
```