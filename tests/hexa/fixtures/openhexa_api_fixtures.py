openhexa_mocked_workspaces = {
    "workspaces": {
        "items": [
            {"slug": "demo-workspace-14f264", "name": "Demo Workspace"},
            {"slug": "demo-custom-env-11a27f", "name": "With custom env"},
        ],
        "totalItems": 2,
        "totalPages": 1,
    }
}

openhexa_mocked_workspaces_pipelines = {
    "pipelines": {
        "items": [
            {
                "id": "04143a0c-db65-430a-977f-f834cedc8a7c",
                "name": "dataset_param",
                "code": "dataset-param",
                "type": "zipFile",
            },
            {"id": "22261aca-b960-4d4a-b5d9-57c5ca21c405", "name": "lqas", "code": "lqas", "type": "zipFile"},
        ],
        "totalItems": 2,
        "totalPages": 1,
    }
}

openhexa_mocked_pipeline = {
    "pipelineByCode": {
        "id": "04143a0c-db65-430a-977f-f834cedc8a7c",
        "code": "dataset-param",
        "name": "dataset_param",
        "currentVersion": {"id": "8cd863b8-11f3-4020-b7be-a701f339459b"},
        "createdAt": "2024-02-13T09:58:58.370Z",
        "updatedAt": "2024-02-13T09:58:58.370Z",
        "type": "zipFile",
    }
}

openhexa_mocked_pipeline_run = {
    "pipelineRun": {
        "id": "cf47a668-2772-4a60-b226-431933d22640",
        "version": {"name": "4"},
        "status": "queued",
        "config": {},
        "executionDate": "2024-08-09T11:14:26.701Z",
        "duration": 0,
        "triggerMode": "manual",
        "pipeline": {"id": "77d90864-3bcb-42f8-812d-9a894138ac80"},
    }
}
