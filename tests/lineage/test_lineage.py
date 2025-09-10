import json
from datetime import timezone, datetime

import pytest
import responses

from tests.lineage.fixtures.lineage import emit_event_example


class TestLineage:
    @pytest.fixture
    def mock_responses(self):
        with responses.RequestsMock() as rsps:
            yield rsps

    def test_init_client(self):
        from openhexa.toolbox import lineage

        lineage.init_client(url="http://localhost:8080", workspace_slug="default", pipeline_slug="test_pipeline")
        assert lineage._client is not None
        assert lineage._client.client is not None
        assert lineage._client.namespace == "default"
        assert lineage.is_initialized() is True

    def test_init_client_from_env(self, monkeypatch):
        from openhexa.toolbox import lineage

        monkeypatch.setenv("OPENLINEAGE_URL", "http://localhost:8080")
        monkeypatch.setenv("OPENLINEAGE_API_KEY", "test_api_key")

        lineage.init_client_from_env(
            workspace_slug="default",
            pipeline_slug="test_pipeline",
            pipeline_run_id="12345",
        )

        assert lineage._client is not None
        assert lineage._client.client is not None
        assert lineage._client.namespace == "default"
        assert lineage._client.run_id == "12345"
        assert lineage.is_initialized() is True

    def test_emit_run_event(self, mock_responses):
        from openhexa.toolbox import lineage
        from openhexa.toolbox.lineage import EventType

        lineage.init_client(
            url="http://localhost:3000",
            workspace_slug="default",
            pipeline_slug="test_pipeline",
            pipeline_run_id="abe36e6a-8af7-4718-a753-c6d2054d1ecf",
        )

        mock_responses.add(responses.POST, "http://localhost:3000/api/v1/lineage", json={}, status=200)

        lineage.event(
            event_type=EventType.START,
            task_name="test_task",
            inputs=[],
            outputs=[],
            start_time=datetime(2025, 6, 12, 7, 24, 48, 727726, tzinfo=timezone.utc),
            end_time=datetime(2025, 6, 12, 7, 27, 48, 727726, tzinfo=timezone.utc),
        )
        assert mock_responses.calls[0].request.url == "http://localhost:3000/api/v1/lineage"
        assert mock_responses.calls[0].request.method == "POST"
        assert json.loads(mock_responses.calls[0].request.body) == emit_event_example

    def test_unique_run_ids_for_different_tasks(self, mock_responses):
        from openhexa.toolbox import lineage
        from openhexa.toolbox.lineage import EventType

        lineage.init_client(
            url="http://localhost:3000",
            workspace_slug="default",
            pipeline_slug="test_pipeline",
            pipeline_run_id="abe36e6a-8af7-4718-a753-c6d2054d1ecf",
        )

        mock_responses.add(responses.POST, "http://localhost:3000/api/v1/lineage", json={}, status=200)

        lineage.event(
            event_type=EventType.COMPLETE,
            task_name="test1",
            outputs=["dataset1.csv"],
        )

        lineage.event(
            event_type=EventType.COMPLETE,
            task_name="test2",
            outputs=["dataset2.csv"],
        )

        lineage.event(
            event_type=EventType.COMPLETE,
            task_name="test1",
            outputs=["dataset2.csv"],
        )

        assert len(mock_responses.calls) == 3

        event1 = json.loads(mock_responses.calls[0].request.body)
        event2 = json.loads(mock_responses.calls[1].request.body)
        event3 = json.loads(mock_responses.calls[2].request.body)

        assert event1["run"]["runId"] == "381a6a74-ad3b-549c-967b-58585197f90a"
        assert event2["run"]["runId"] == "d8bb693b-23bb-54f0-8605-29032f10d5d5"
        assert event3["run"]["runId"] == "381a6a74-ad3b-549c-967b-58585197f90a"  # Same task, same run ID

        assert event1["outputs"][0]["name"] == "dataset1.csv"
        assert event2["outputs"][0]["name"] == "dataset2.csv"
        assert event3["outputs"][0]["name"] == "dataset2.csv"

    def test_pipeline_jobs_disabled_by_default(self):
        from openhexa.toolbox import lineage

        lineage.init_client(url="http://localhost:8080", workspace_slug="default", pipeline_slug="test_pipeline")
        assert lineage._client.enable_pipeline_jobs is False

    def test_pipeline_jobs_can_be_enabled(self):
        from openhexa.toolbox import lineage

        lineage.init_client(
            url="http://localhost:8080", 
            workspace_slug="default", 
            pipeline_slug="test_pipeline",
            enable_pipeline_jobs=True
        )
        assert lineage._client.enable_pipeline_jobs is True

    def test_pipeline_jobs_from_env_variable(self, monkeypatch):
        from openhexa.toolbox import lineage

        monkeypatch.setenv("OPENLINEAGE_URL", "http://localhost:8080")
        monkeypatch.setenv("OPENLINEAGE_ENABLE_PIPELINE_JOBS", "true")

        lineage.init_client_from_env(workspace_slug="default", pipeline_slug="test_pipeline")
        assert lineage._client.enable_pipeline_jobs is True

    def test_pipeline_start_and_complete_events(self, mock_responses):
        from openhexa.toolbox import lineage
        from openhexa.toolbox.lineage import EventType

        lineage.init_client(
            url="http://localhost:3000",
            workspace_slug="default",
            pipeline_slug="test_pipeline",
            pipeline_run_id="abe36e6a-8af7-4718-a753-c6d2054d1ecf",
            enable_pipeline_jobs=True
        )

        mock_responses.add(responses.POST, "http://localhost:3000/api/v1/lineage", json={}, status=200)

        lineage.event(
            event_type=EventType.START,
            task_name="task1",
            inputs=["input1.csv"],
            outputs=["intermediate.csv"],
        )

        lineage.event(
            event_type=EventType.COMPLETE,
            task_name="task2", 
            inputs=["intermediate.csv"],
            outputs=["output1.csv"],
        )

        lineage.pipeline_start()
        lineage.pipeline_complete()

        assert len(mock_responses.calls) == 4
        
        pipeline_start_event = json.loads(mock_responses.calls[2].request.body)
        pipeline_complete_event = json.loads(mock_responses.calls[3].request.body)
        
        assert pipeline_start_event["job"]["name"] == "test_pipeline"
        assert pipeline_start_event["eventType"] == "START"
        assert "pipeline" in pipeline_start_event["job"]["facets"]
        assert pipeline_start_event["job"]["facets"]["pipeline"]["type"] == "PIPELINE"
        assert pipeline_start_event["job"]["facets"]["pipeline"]["taskCount"] == 2
        
        assert pipeline_complete_event["job"]["name"] == "test_pipeline"
        assert pipeline_complete_event["eventType"] == "COMPLETE"
        
        assert len(pipeline_start_event["inputs"]) == 1
        assert pipeline_start_event["inputs"][0]["name"] == "input1.csv"
        assert len(pipeline_start_event["outputs"]) == 1
        assert pipeline_start_event["outputs"][0]["name"] == "output1.csv"
